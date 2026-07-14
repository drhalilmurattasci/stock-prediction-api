"""Golden and invariant tests for the owned technical-indicator formulas."""

from __future__ import annotations

import math
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from ml.features.technical import (
    CALCULATION_VERSION,
    BollingerValues,
    IndicatorConfig,
    MacdValues,
    TechnicalIndicators,
    average_true_range,
    bollinger_bands,
    calculate_indicators,
    ema,
    indicator_policy_hash,
    log_returns,
    macd,
    required_observations,
    return_volatility,
    rsi,
    simple_returns,
    sma,
)


def _assert_series(
    actual: tuple[float | None, ...],
    expected: tuple[float | None, ...],
) -> None:
    assert len(actual) == len(expected)
    assert tuple(value is None for value in actual) == tuple(value is None for value in expected)
    for actual_value, expected_value in zip(actual, expected, strict=True):
        if expected_value is not None:
            assert actual_value == pytest.approx(expected_value, rel=1e-12, abs=1e-12)


def _small_config() -> IndicatorConfig:
    return IndicatorConfig(
        return_period=1,
        sma_period=3,
        ema_period=3,
        volatility_period=3,
        rsi_period=3,
        macd_fast_period=2,
        macd_slow_period=4,
        macd_signal_period=2,
        bollinger_period=3,
        bollinger_standard_deviations=2.0,
        atr_period=3,
    )


def _ohlc(
    closes: list[float],
) -> tuple[list[datetime], list[float], list[float], list[float], list[float]]:
    return (
        [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=index) for index in range(len(closes))],
        closes.copy(),
        [value + 1.0 for value in closes],
        [value - 1.0 for value in closes],
        closes.copy(),
    )


def _all_series(output: TechnicalIndicators) -> tuple[tuple[float | None, ...], ...]:
    return (
        output.simple_return,
        output.log_return,
        output.sma,
        output.ema,
        output.return_volatility,
        output.rsi,
        output.macd.line,
        output.macd.signal,
        output.macd.histogram,
        output.bollinger.lower,
        output.bollinger.middle,
        output.bollinger.upper,
        output.atr,
    )


def test_sma_and_sma_seeded_ema_match_hand_calculated_goldens() -> None:
    values = [2.0, 4.0, 8.0, 16.0, 32.0]

    _assert_series(sma(values, 3), (None, None, 14 / 3, 28 / 3, 56 / 3))
    _assert_series(ema(values, 3), (None, None, 14 / 3, 31 / 3, 127 / 6))


def test_simple_and_log_returns_match_hand_calculated_goldens() -> None:
    values = [100.0, 110.0, 99.0, 118.8]

    _assert_series(simple_returns(values), (None, 0.1, -0.1, 0.2))
    _assert_series(
        log_returns(values),
        (None, math.log(1.1), math.log(0.9), math.log(1.2)),
    )
    _assert_series(simple_returns([10.0, 11.0, 12.0, 15.0], 2), (None, None, 0.2, 4 / 11))


def test_returns_preserve_a_one_ulp_change_at_large_magnitude() -> None:
    previous = 1e308
    current = math.nextafter(previous, math.inf)
    relative_change = (current - previous) / previous

    assert relative_change > 0.0
    assert simple_returns([previous, current])[-1] == relative_change
    assert log_returns([previous, current])[-1] == math.log1p(relative_change)


def test_return_volatility_is_unannualized_sample_stdev_of_simple_returns() -> None:
    # These prices produce one-step returns [1, 2, 3, 5]. The two rolling
    # three-return sample standard deviations are 1 and sqrt(7/3).
    values = [1.0, 2.0, 6.0, 24.0, 144.0]

    _assert_series(
        return_volatility(values, 3),
        (None, None, None, 1.0, math.sqrt(7 / 3)),
    )


def test_wilder_rsi_matches_hand_calculated_recurrence() -> None:
    _assert_series(
        rsi([1.0, 2.0, 3.0, 2.0, 2.0, 4.0], 3),
        (None, None, None, 200 / 3, 200 / 3, 260 / 3),
    )


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([4.0, 4.0, 4.0, 4.0], 50.0),
        ([1.0, 2.0, 3.0, 4.0], 100.0),
        ([4.0, 3.0, 2.0, 1.0], 0.0),
    ],
)
def test_rsi_pins_flat_up_and_down_edge_semantics(values: list[float], expected: float) -> None:
    assert rsi(values, 3)[-1] == expected


def test_macd_nonlinear_golden_pins_both_ema_recurrences() -> None:
    result = macd([1.0, 2.0, 4.0, 8.0, 16.0, 32.0], 2, 3, 2)

    assert isinstance(result, MacdValues)
    _assert_series(result.line, (None, None, 5 / 6, 11 / 9, 239 / 108, 2791 / 648))
    _assert_series(result.signal, (None, None, None, 37 / 36, 589 / 324, 845 / 243))
    _assert_series(result.histogram, (None, None, None, 7 / 36, 32 / 81, 1613 / 1944))


def test_bollinger_bands_use_population_standard_deviation() -> None:
    result = bollinger_bands([1.0, 2.0, 3.0, 4.0], 3, 2.0)
    width = 2.0 * math.sqrt(2 / 3)

    assert isinstance(result, BollingerValues)
    _assert_series(result.middle, (None, None, 2.0, 3.0))
    _assert_series(result.lower, (None, None, 2.0 - width, 3.0 - width))
    _assert_series(result.upper, (None, None, 2.0 + width, 3.0 + width))


def test_wilder_atr_matches_hand_calculated_true_ranges() -> None:
    result = average_true_range(
        [10.0, 12.0, 13.0, 15.0, 14.0],
        [8.0, 9.0, 11.0, 12.0, 10.0],
        [9.0, 11.0, 12.0, 13.0, 11.0],
        3,
    )

    _assert_series(result, (None, None, None, 8 / 3, 28 / 9))


def test_default_required_observations_is_explained_by_macd_signal_warmup() -> None:
    config = IndicatorConfig()

    assert required_observations(config) == 34
    assert config.macd_slow_period + config.macd_signal_period - 1 == 34
    assert required_observations(_small_config()) == 5


def test_complete_bundle_is_aligned_finite_versioned_and_does_not_mutate_inputs() -> None:
    closes = [float(value) for value in range(10, 20)]
    inputs = _ohlc(closes)
    before = tuple(tuple(values) for values in inputs)

    result = calculate_indicators(*inputs, config=_small_config())

    assert result.calculation_version == CALCULATION_VERSION
    assert result.policy_hash == indicator_policy_hash(_small_config())
    assert result.config == _small_config()
    assert len(result.observed_at) == len(closes)
    assert all(len(values) == len(closes) for values in _all_series(result))
    assert all(values[-1] is not None for values in _all_series(result))
    assert all(
        value is None or math.isfinite(value) for values in _all_series(result) for value in values
    )
    assert tuple(tuple(values) for values in inputs) == before


def test_suffix_change_cannot_change_any_earlier_indicator() -> None:
    base_closes = [float(value) for value in range(10, 20)]
    changed_closes = [*base_closes[:-1], base_closes[-1] + 5.0]
    base = calculate_indicators(*_ohlc(base_closes), config=_small_config())
    changed = calculate_indicators(*_ohlc(changed_closes), config=_small_config())

    for base_values, changed_values in zip(_all_series(base), _all_series(changed), strict=True):
        assert base_values[:-1] == changed_values[:-1]


def test_config_and_outputs_are_immutable() -> None:
    config = _small_config()
    result = calculate_indicators(*_ohlc([float(value) for value in range(10, 20)]), config)

    with pytest.raises(FrozenInstanceError):
        config.sma_period = 5  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.calculation_version = "changed"  # type: ignore[misc]


def test_policy_identity_binds_every_config_parameter() -> None:
    base = IndicatorConfig()
    base_hash = indicator_policy_hash(base)

    changed_hashes = set()
    for field in fields(base):
        value = getattr(base, field.name)
        replacement = value + 0.5 if isinstance(value, float) else value + 1
        changed = replace(base, **{field.name: replacement})
        changed_hash = indicator_policy_hash(changed)
        assert changed_hash != base_hash
        changed_hashes.add(changed_hash)

    assert len(changed_hashes) == len(fields(base))


def test_bundle_rejects_reverse_duplicate_or_naive_timestamps() -> None:
    values = _ohlc([float(value) for value in range(10, 20)])
    timestamps, *ohlc = values

    with pytest.raises(ValueError, match="strictly increasing"):
        calculate_indicators(list(reversed(timestamps)), *ohlc, config=_small_config())
    duplicate = [*timestamps[:-1], timestamps[-2]]
    with pytest.raises(ValueError, match="strictly increasing"):
        calculate_indicators(duplicate, *ohlc, config=_small_config())
    naive = [*timestamps[:-1], timestamps[-1].replace(tzinfo=None)]
    with pytest.raises(ValueError, match="timezone-aware"):
        calculate_indicators(naive, *ohlc, config=_small_config())


def test_bundle_normalizes_timestamps_to_utc() -> None:
    plus_three = timezone(timedelta(hours=3))
    timestamps, *ohlc = _ohlc([float(value) for value in range(10, 20)])
    shifted = [value.astimezone(plus_three) for value in timestamps]

    result = calculate_indicators(shifted, *ohlc, config=_small_config())

    assert result.observed_at == tuple(timestamps)


def test_primitives_accept_numpy_and_pandas_iterables() -> None:
    expected = (None, None, 2.0, 3.0)

    _assert_series(sma(np.array([1.0, 2.0, 3.0, 4.0]), 3), expected)
    _assert_series(sma(pd.Series([1.0, 2.0, 3.0, 4.0]), 3), expected)


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: sma([], 2), "must not be empty"),
        (lambda: sma([1.0, float("nan")], 2), "finite"),
        (lambda: sma([1.0, True], 2), "real number"),
        (lambda: sma([1.0, 2.0], 0), "at least 1"),
        (lambda: simple_returns([0.0, 1.0]), "positive"),
        (lambda: log_returns([1.0, 0.0]), "positive"),
        (lambda: return_volatility([1.0, 2.0], 1), "at least 2"),
        (lambda: macd([1.0, 2.0], 3, 2, 1), "less than"),
        (lambda: bollinger_bands([1.0, 2.0], 2, float("inf")), "finite"),
        (lambda: simple_returns([math.ulp(0.0), 1e308]), "overflowed"),
    ],
)
def test_primitives_reject_invalid_or_nonfinite_inputs(call: object, match: str) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        call()  # type: ignore[operator]


def test_atr_and_bundle_reject_invalid_ohlc_geometry_or_lengths() -> None:
    with pytest.raises(ValueError, match="equal lengths"):
        average_true_range([2.0, 3.0], [1.0], [1.5, 2.5], 1)
    with pytest.raises(ValueError, match="within its high-low"):
        average_true_range([2.0, 3.0], [1.0, 2.0], [1.5, 4.0], 1)
    with pytest.raises(ValueError, match="opens"):
        calculate_indicators(
            [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=index) for index in range(5)],
            [99.0] * 5,
            [12.0] * 5,
            [8.0] * 5,
            [10.0] * 5,
            config=_small_config(),
        )


def test_bundle_refuses_an_all_warmup_result() -> None:
    config = _small_config()

    with pytest.raises(ValueError, match="requires at least 5 observations"):
        calculate_indicators(*_ohlc([10.0, 11.0, 12.0, 13.0]), config=config)


def test_scaled_arithmetic_keeps_representable_large_results_finite() -> None:
    assert sma([1e308, 1e308], 2)[-1] == 1e308
    assert ema([1e308, 1e308, 1e308], 2)[-1] == 1e308
    volatility = return_volatility([1.0, 1e308, 1.0], 2)[-1]
    assert volatility is not None and math.isfinite(volatility)
    assert volatility == pytest.approx(math.sqrt(0.5) * 1e308, rel=1e-15)


def test_rsi_preserves_a_small_representable_value() -> None:
    value = rsi([1e17, 1.0, 2.0], 2)[-1]

    assert value is not None and value > 0.0
    assert value == pytest.approx(1e-15, rel=1e-12)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"sma_period": True},
        {"volatility_period": 1},
        {"macd_fast_period": 4, "macd_slow_period": 4},
        {"bollinger_standard_deviations": 0.0},
    ],
)
def test_config_rejects_ambiguous_formula_parameters(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        IndicatorConfig(**kwargs)  # type: ignore[arg-type]
