"""Owned, causal technical-indicator calculations.

Every function consumes observations in chronological order and returns an
equally sized immutable tuple. Structural warm-up is represented by ``None``
rather than NaN. Formula choices are deliberately explicit so dependency
upgrades cannot silently change model features or public analytics.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from numbers import Real

CALCULATION_VERSION = "owned-indicators-v1"

type IndicatorValues = tuple[float | None, ...]


@dataclass(frozen=True)
class IndicatorConfig:
    """Version-one formula parameters for the complete indicator bundle."""

    return_period: int = 1
    sma_period: int = 20
    ema_period: int = 20
    volatility_period: int = 20
    rsi_period: int = 14
    macd_fast_period: int = 12
    macd_slow_period: int = 26
    macd_signal_period: int = 9
    bollinger_period: int = 20
    bollinger_standard_deviations: float = 2.0
    atr_period: int = 14

    def __post_init__(self) -> None:
        for name in (
            "return_period",
            "sma_period",
            "ema_period",
            "rsi_period",
            "macd_fast_period",
            "macd_slow_period",
            "macd_signal_period",
            "bollinger_period",
            "atr_period",
        ):
            _positive_period(getattr(self, name), name)
        _positive_period(self.volatility_period, "volatility_period", minimum=2)
        if self.macd_fast_period >= self.macd_slow_period:
            raise ValueError("macd_fast_period must be less than macd_slow_period")
        _positive_finite(
            self.bollinger_standard_deviations,
            "bollinger_standard_deviations",
        )


@dataclass(frozen=True)
class MacdValues:
    """Aligned MACD line, signal line, and histogram."""

    line: IndicatorValues
    signal: IndicatorValues
    histogram: IndicatorValues


@dataclass(frozen=True)
class BollingerValues:
    """Aligned lower, middle, and upper Bollinger bands."""

    lower: IndicatorValues
    middle: IndicatorValues
    upper: IndicatorValues


@dataclass(frozen=True)
class TechnicalIndicators:
    """Complete, aligned output of :func:`calculate_indicators`."""

    calculation_version: str
    policy_hash: str
    config: IndicatorConfig
    observed_at: tuple[datetime, ...]
    simple_return: IndicatorValues
    log_return: IndicatorValues
    sma: IndicatorValues
    ema: IndicatorValues
    return_volatility: IndicatorValues
    rsi: IndicatorValues
    macd: MacdValues
    bollinger: BollingerValues
    atr: IndicatorValues


def required_observations(config: IndicatorConfig | None = None) -> int:
    """Return the minimum observations needed for every configured output.

    RSI, return volatility, and ATR need one prior close. The MACD signal is
    available after ``slow`` observations establish its line and ``signal-1``
    further MACD values establish the signal EMA.
    """

    config = IndicatorConfig() if config is None else config
    if not isinstance(config, IndicatorConfig):
        raise TypeError("config must be an IndicatorConfig")
    return max(
        config.return_period + 1,
        config.sma_period,
        config.ema_period,
        config.volatility_period + 1,
        config.rsi_period + 1,
        config.macd_slow_period + config.macd_signal_period - 1,
        config.bollinger_period,
        config.atr_period + 1,
    )


def indicator_policy_hash(config: IndicatorConfig | None = None) -> str:
    """Return the canonical SHA-256 identity of formulas plus parameters."""

    config = IndicatorConfig() if config is None else config
    if not isinstance(config, IndicatorConfig):
        raise TypeError("config must be an IndicatorConfig")
    document = {
        "calculation_version": CALCULATION_VERSION,
        "parameters": {
            "atr_period": config.atr_period,
            "bollinger_period": config.bollinger_period,
            "bollinger_standard_deviations": float(config.bollinger_standard_deviations),
            "ema_period": config.ema_period,
            "macd_fast_period": config.macd_fast_period,
            "macd_signal_period": config.macd_signal_period,
            "macd_slow_period": config.macd_slow_period,
            "return_period": config.return_period,
            "rsi_period": config.rsi_period,
            "sma_period": config.sma_period,
            "volatility_period": config.volatility_period,
        },
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def simple_returns(values: Iterable[float], period: int = 1) -> IndicatorValues:
    """Lagged fractional returns ``value[t] / value[t-period] - 1``."""

    data = _finite_series(values, "values", positive=True)
    lag = _positive_period(period, "period")
    output: list[float | None] = [None] * len(data)
    for index in range(lag, len(data)):
        denominator = data[index - lag]
        output[index] = _finite_result(
            (data[index] - denominator) / denominator,
            "simple return",
        )
    return tuple(output)


def log_returns(values: Iterable[float], period: int = 1) -> IndicatorValues:
    """Lagged natural-log returns for a strictly positive series."""

    data = _finite_series(values, "values", positive=True)
    lag = _positive_period(period, "period")
    output: list[float | None] = [None] * len(data)
    for index in range(lag, len(data)):
        previous = data[index - lag]
        relative_change = (data[index] - previous) / previous
        if -0.5 < relative_change < 0.5:
            value = math.log1p(relative_change)
        else:
            # A direct ratio can under/overflow across the full float range
            # even when the mathematically equivalent log difference is finite.
            value = math.log(data[index]) - math.log(previous)
        output[index] = _finite_result(
            value,
            "log return",
        )
    return tuple(output)


def sma(values: Iterable[float], period: int) -> IndicatorValues:
    """Simple moving average over exactly ``period`` observations."""

    data = _finite_series(values, "values")
    window = _positive_period(period, "period")
    output: list[float | None] = [None] * len(data)
    for index in range(window - 1, len(data)):
        output[index] = _finite_result(
            _stable_mean(data[index - window + 1 : index + 1], "simple moving average"),
            "simple moving average",
        )
    return tuple(output)


def ema(values: Iterable[float], period: int) -> IndicatorValues:
    """SMA-seeded exponential moving average with ``alpha=2/(period+1)``."""

    data = _finite_series(values, "values")
    span = _positive_period(period, "period")
    output: list[float | None] = [None] * len(data)
    if len(data) < span:
        return tuple(output)
    seed = _stable_mean(data[:span], "EMA seed")
    output[span - 1] = seed
    alpha = 2.0 / (span + 1.0)
    previous = seed
    for index in range(span, len(data)):
        previous = _finite_result(
            alpha * data[index] + (1.0 - alpha) * previous,
            "exponential moving average",
        )
        output[index] = previous
    return tuple(output)


def return_volatility(values: Iterable[float], period: int) -> IndicatorValues:
    """Unannualized sample stdev (ddof=1) of one-step simple returns."""

    data = _finite_series(values, "values")
    window = _positive_period(period, "period", minimum=2)
    returns = simple_returns(data)
    output: list[float | None] = [None] * len(data)
    for index in range(window, len(data)):
        sample = returns[index - window + 1 : index + 1]
        if any(value is None for value in sample):
            continue
        concrete = tuple(value for value in sample if value is not None)
        output[index] = _stable_standard_deviation(
            concrete,
            ddof=1,
            name="return volatility",
        )
    return tuple(output)


def rsi(values: Iterable[float], period: int) -> IndicatorValues:
    """Wilder RSI; a perfectly flat window is explicitly neutral at 50."""

    data = _finite_series(values, "values", positive=True)
    window = _positive_period(period, "period")
    output: list[float | None] = [None] * len(data)
    if len(data) <= window:
        return tuple(output)
    changes = [data[index] - data[index - 1] for index in range(1, len(data))]
    average_gain = _stable_mean(
        tuple(max(change, 0.0) for change in changes[:window]),
        "RSI average gain",
    )
    average_loss = _stable_mean(
        tuple(max(-change, 0.0) for change in changes[:window]),
        "RSI average loss",
    )
    output[window] = _rsi_value(average_gain, average_loss)
    for index in range(window + 1, len(data)):
        change = changes[index - 1]
        average_gain = _finite_result(
            average_gain + (max(change, 0.0) - average_gain) / window,
            "RSI average gain",
        )
        average_loss = _finite_result(
            average_loss + (max(-change, 0.0) - average_loss) / window,
            "RSI average loss",
        )
        output[index] = _rsi_value(average_gain, average_loss)
    return tuple(output)


def macd(
    values: Iterable[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> MacdValues:
    """SMA-seeded MACD line, SMA-seeded signal EMA, and histogram."""

    data = _finite_series(values, "values")
    fast = _positive_period(fast_period, "fast_period")
    slow = _positive_period(slow_period, "slow_period")
    signal_window = _positive_period(signal_period, "signal_period")
    if fast >= slow:
        raise ValueError("fast_period must be less than slow_period")
    fast_values = ema(data, fast)
    slow_values = ema(data, slow)
    line: list[float | None] = [None] * len(data)
    for index in range(slow - 1, len(data)):
        fast_value = fast_values[index]
        slow_value = slow_values[index]
        if fast_value is not None and slow_value is not None:
            line[index] = _finite_result(fast_value - slow_value, "MACD line")

    available_line = tuple(value for value in line[slow - 1 :] if value is not None)
    signal_tail = ema(available_line, signal_window) if available_line else ()
    signal: list[float | None] = [None] * len(data)
    histogram: list[float | None] = [None] * len(data)
    for offset, value in enumerate(signal_tail):
        index = slow - 1 + offset
        if value is not None:
            signal[index] = value
            line_value = line[index]
            if line_value is None:  # pragma: no cover - protected by construction
                raise RuntimeError("MACD alignment invariant failed")
            histogram[index] = _finite_result(line_value - value, "MACD histogram")
    return MacdValues(tuple(line), tuple(signal), tuple(histogram))


def bollinger_bands(
    values: Iterable[float],
    period: int,
    standard_deviations: float = 2.0,
) -> BollingerValues:
    """Population-stdev Bollinger bands around the matching SMA."""

    data = _finite_series(values, "values")
    window = _positive_period(period, "period")
    width = _positive_finite(standard_deviations, "standard_deviations")
    middle = sma(data, window)
    lower: list[float | None] = [None] * len(data)
    upper: list[float | None] = [None] * len(data)
    for index in range(window - 1, len(data)):
        mean = middle[index]
        if mean is None:  # pragma: no cover - protected by construction
            raise RuntimeError("Bollinger alignment invariant failed")
        sample = data[index - window + 1 : index + 1]
        deviation = _stable_standard_deviation(
            sample,
            ddof=0,
            name="Bollinger standard deviation",
        )
        lower[index] = _finite_result(mean - width * deviation, "lower Bollinger band")
        upper[index] = _finite_result(mean + width * deviation, "upper Bollinger band")
    return BollingerValues(tuple(lower), middle, tuple(upper))


def average_true_range(
    highs: Iterable[float],
    lows: Iterable[float],
    closes: Iterable[float],
    period: int,
) -> IndicatorValues:
    """Wilder ATR using TR[1..period] as its first seed window.

    ``TR[0]`` is undefined because no previous close exists. Consequently the
    first ATR is aligned at index ``period``, matching common TA-Lib indexing.
    """

    high = _finite_series(highs, "highs")
    low = _finite_series(lows, "lows")
    close = _finite_series(closes, "closes")
    _equal_lengths(high=high, low=low, close=close)
    _validate_hlc(high, low, close)
    window = _positive_period(period, "period")
    output: list[float | None] = [None] * len(close)
    if len(close) <= window:
        return tuple(output)
    true_ranges: list[float | None] = [None]
    for index in range(1, len(close)):
        true_ranges.append(
            _finite_result(
                max(
                    high[index] - low[index],
                    abs(high[index] - close[index - 1]),
                    abs(low[index] - close[index - 1]),
                ),
                "true range",
            )
        )
    seed_values = tuple(value for value in true_ranges[1 : window + 1] if value is not None)
    previous = _stable_mean(seed_values, "ATR seed")
    output[window] = previous
    for index in range(window + 1, len(close)):
        current = true_ranges[index]
        if current is None:  # pragma: no cover - protected by construction
            raise RuntimeError("ATR alignment invariant failed")
        previous = _finite_result(
            previous + (current - previous) / window,
            "average true range",
        )
        output[index] = previous
    return tuple(output)


def calculate_indicators(
    observed_at: Iterable[datetime],
    opens: Iterable[float],
    highs: Iterable[float],
    lows: Iterable[float],
    closes: Iterable[float],
    config: IndicatorConfig | None = None,
) -> TechnicalIndicators:
    """Calculate the complete v1 indicator bundle from chronological OHLC."""

    config = IndicatorConfig() if config is None else config
    if not isinstance(config, IndicatorConfig):
        raise TypeError("config must be an IndicatorConfig")
    timestamps = _chronological_timestamps(observed_at)
    open_values = _finite_series(opens, "opens", nonnegative=True)
    high_values = _finite_series(highs, "highs", nonnegative=True)
    low_values = _finite_series(lows, "lows", nonnegative=True)
    close_values = _finite_series(closes, "closes", positive=True)
    _equal_lengths(
        observed_at=timestamps,
        open=open_values,
        high=high_values,
        low=low_values,
        close=close_values,
    )
    _validate_ohlc(open_values, high_values, low_values, close_values)
    required = required_observations(config)
    if len(close_values) < required:
        raise ValueError(
            f"indicator bundle requires at least {required} observations; "
            f"received {len(close_values)}"
        )
    output = TechnicalIndicators(
        calculation_version=CALCULATION_VERSION,
        policy_hash=indicator_policy_hash(config),
        config=config,
        observed_at=timestamps,
        simple_return=simple_returns(close_values, config.return_period),
        log_return=log_returns(close_values, config.return_period),
        sma=sma(close_values, config.sma_period),
        ema=ema(close_values, config.ema_period),
        return_volatility=return_volatility(close_values, config.volatility_period),
        rsi=rsi(close_values, config.rsi_period),
        macd=macd(
            close_values,
            config.macd_fast_period,
            config.macd_slow_period,
            config.macd_signal_period,
        ),
        bollinger=bollinger_bands(
            close_values,
            config.bollinger_period,
            config.bollinger_standard_deviations,
        ),
        atr=average_true_range(
            high_values,
            low_values,
            close_values,
            config.atr_period,
        ),
    )
    _validate_output(output, len(close_values))
    return output


def _rsi_value(average_gain: float, average_loss: float) -> float:
    if average_gain == 0.0 and average_loss == 0.0:
        return 50.0
    if average_loss == 0.0:
        return 100.0
    if average_gain == 0.0:
        return 0.0
    if average_gain <= average_loss:
        relative_strength = average_gain / average_loss
        value = 100.0 * relative_strength / (1.0 + relative_strength)
    else:
        inverse_strength = average_loss / average_gain
        value = 100.0 / (1.0 + inverse_strength)
    return _finite_result(value, "RSI")


def _finite_series(
    values: Iterable[float],
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> tuple[float, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{name} must be an iterable of real numbers")
    try:
        iterator = iter(values)
    except TypeError as exc:
        raise TypeError(f"{name} must be an iterable of real numbers") from exc
    normalized: list[float] = []
    for index, value in enumerate(iterator):
        if isinstance(value, bool) or not isinstance(value, Real):
            raise TypeError(f"{name}[{index}] must be a real number")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"{name}[{index}] must be finite")
        if positive and number <= 0.0:
            raise ValueError(f"{name}[{index}] must be positive")
        if nonnegative and number < 0.0:
            raise ValueError(f"{name}[{index}] must be nonnegative")
        normalized.append(number)
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return tuple(normalized)


def _positive_period(value: int, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _positive_finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{name} must be a real number")
    number = float(value)
    if not math.isfinite(number) or number <= 0.0:
        raise ValueError(f"{name} must be positive and finite")
    return number


def _finite_result(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} overflowed to a nonfinite value")
    return value


def _stable_mean(values: tuple[float, ...], name: str) -> float:
    if not values:
        raise ValueError(f"{name} requires at least one value")
    scale = max(abs(value) for value in values)
    if scale == 0.0:
        return 0.0
    normalized_mean = _fsum((value / scale for value in values), name) / len(values)
    return _finite_result(scale * normalized_mean, name)


def _stable_standard_deviation(
    values: tuple[float, ...],
    *,
    ddof: int,
    name: str,
) -> float:
    if not 0 <= ddof < len(values):
        raise ValueError(f"{name} has insufficient values for ddof={ddof}")
    scale = max(abs(value) for value in values)
    if scale == 0.0:
        return 0.0
    normalized = tuple(value / scale for value in values)
    mean = _stable_mean(normalized, f"{name} mean")
    sum_squared = _fsum(((value - mean) ** 2 for value in normalized), name)
    return _finite_result(scale * math.sqrt(sum_squared / (len(values) - ddof)), name)


def _fsum(values: Iterable[float], name: str) -> float:
    try:
        result = math.fsum(values)
    except OverflowError as exc:
        raise ValueError(f"{name} overflowed to a nonfinite value") from exc
    return _finite_result(result, name)


def _chronological_timestamps(values: Iterable[datetime]) -> tuple[datetime, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError("observed_at must be an iterable of datetimes")
    try:
        iterator = iter(values)
    except TypeError as exc:
        raise TypeError("observed_at must be an iterable of datetimes") from exc
    normalized: list[datetime] = []
    for index, value in enumerate(iterator):
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"observed_at[{index}] must be timezone-aware")
        try:
            timestamp = value.astimezone(UTC)
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError(f"observed_at[{index}] cannot be normalized to UTC") from exc
        if normalized and timestamp <= normalized[-1]:
            raise ValueError("observed_at must be strictly increasing")
        normalized.append(timestamp)
    if not normalized:
        raise ValueError("observed_at must not be empty")
    return tuple(normalized)


def _equal_lengths(**series: tuple[object, ...]) -> None:
    lengths = {len(values) for values in series.values()}
    if len(lengths) != 1:
        names = ", ".join(series)
        raise ValueError(f"{names} must have equal lengths")


def _validate_hlc(
    high: tuple[float, ...],
    low: tuple[float, ...],
    close: tuple[float, ...],
) -> None:
    for index, (high_value, low_value, close_value) in enumerate(
        zip(high, low, close, strict=True)
    ):
        if high_value < low_value:
            raise ValueError(f"highs[{index}] must be greater than or equal to lows[{index}]")
        if high_value < close_value or low_value > close_value:
            raise ValueError(f"closes[{index}] must lie within its high-low range")


def _validate_ohlc(
    open_values: tuple[float, ...],
    high_values: tuple[float, ...],
    low_values: tuple[float, ...],
    close_values: tuple[float, ...],
) -> None:
    _validate_hlc(high_values, low_values, close_values)
    for index, (open_value, high_value, low_value) in enumerate(
        zip(open_values, high_values, low_values, strict=True)
    ):
        if high_value < open_value or low_value > open_value:
            raise ValueError(f"opens[{index}] must lie within its high-low range")


def _validate_output(output: TechnicalIndicators, length: int) -> None:
    series = (
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
    if len(output.observed_at) != length or any(len(values) != length for values in series):
        raise RuntimeError("indicator output alignment invariant failed")
    if output.calculation_version != CALCULATION_VERSION:
        raise RuntimeError("indicator calculation-version invariant failed")
    if output.policy_hash != indicator_policy_hash(output.config):
        raise RuntimeError("indicator policy-identity invariant failed")
    if any(value is not None and not math.isfinite(value) for values in series for value in values):
        raise RuntimeError("indicator output finiteness invariant failed")


__all__ = [
    "CALCULATION_VERSION",
    "BollingerValues",
    "IndicatorConfig",
    "IndicatorValues",
    "MacdValues",
    "TechnicalIndicators",
    "average_true_range",
    "bollinger_bands",
    "calculate_indicators",
    "ema",
    "indicator_policy_hash",
    "log_returns",
    "macd",
    "required_observations",
    "return_volatility",
    "rsi",
    "simple_returns",
    "sma",
]
