"""Golden-value and safety tests for deterministic forecasting baselines."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

import ml.models.baselines as baseline_module
from ml.models import DriftForecaster, Forecaster, NaiveForecaster, SeasonalNaiveForecaster


@pytest.mark.parametrize(
    "model",
    [NaiveForecaster(), DriftForecaster(), SeasonalNaiveForecaster(season_length=2)],
)
def test_baselines_conform_to_runtime_forecaster_protocol(model: object):
    assert isinstance(model, Forecaster)


def test_point_forecasts_match_hand_computed_values():
    assert NaiveForecaster().fit([10.0, 12.0, 11.0]).predict(3) == [11.0, 11.0, 11.0]
    assert DriftForecaster().fit([10.0, 12.0, 11.0]).predict(3) == [11.5, 12.0, 12.5]
    assert SeasonalNaiveForecaster(3).fit([1, 2, 3, 4, 5, 6]).predict(5) == [
        4.0,
        5.0,
        6.0,
        4.0,
        5.0,
    ]


def test_naive_empirical_quantiles_are_prefix_only_linear_and_median_centered():
    model = NaiveForecaster().fit([1.0, 3.0, 4.0, 10.0])

    # One-step prefix errors are [2, 1, 6], whose q25/median/q75 are
    # 1.5/2/4. Two-step errors are [3, 7], whose values are 4/5/6.
    assert model.predict_quantiles(2, [0.25, 0.5, 0.75]) == {
        0.25: [9.5, 9.0],
        0.5: [10.0, 10.0],
        0.75: [12.0, 11.0],
    }


def test_drift_empirical_quantiles_match_expanding_origin_errors():
    model = DriftForecaster().fit([1.0, 2.0, 4.0, 7.0, 11.0])

    assert model.predict_quantiles(2, [0.25, 0.5, 0.75]) == {
        0.25: [13.25, 15.75],
        0.5: [13.5, 16.0],
        0.75: [13.75, 16.25],
    }


def test_seasonal_quantiles_use_distinct_horizon_specific_error_distributions():
    model = SeasonalNaiveForecaster(2).fit([10, 20, 11, 19, 13, 18, 16, 16])

    # The second-step upper error is wider than the first-step error. This pins
    # horizon-specific seasonal uncertainty instead of copying one scale to all steps.
    assert model.predict_quantiles(2, [0.25, 0.5, 0.75]) == {
        0.25: [15.0, 16.0],
        0.5: [16.0, 16.0],
        0.75: [17.75, 19.0],
    }


def test_empirical_quantiles_sort_each_horizon_error_distribution_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sort_inputs: list[tuple[float, ...]] = []
    builtin_sorted = sorted

    def counting_sorted(values: Sequence[float]) -> list[float]:
        sort_inputs.append(tuple(values))
        return builtin_sorted(values)

    monkeypatch.setattr(baseline_module, "sorted", counting_sorted, raising=False)

    model = NaiveForecaster().fit([1.0, 4.0, 2.0, 8.0, 5.0, 11.0, 7.0, 15.0])
    forecasts = model.predict_quantiles(3, [0.1, 0.25, 0.5, 0.75, 0.9])

    assert list(forecasts) == [0.1, 0.25, 0.5, 0.75, 0.9]
    assert [len(values) for values in sort_inputs] == [7, 6, 5]


def test_predictions_are_fresh_and_cannot_mutate_fitted_state():
    values = [1.0, 2.0, 4.0, 7.0]
    model = NaiveForecaster().fit(values)
    values[-1] = 999.0

    first_points = model.predict(2)
    first_points[0] = 999.0
    assert model.predict(2) == [7.0, 7.0]

    first_quantiles = model.predict_quantiles(1, [0.25, 0.5, 0.75])
    first_quantiles[0.5][0] = 999.0
    assert model.predict_quantiles(1, [0.5]) == {0.5: [7.0]}


def test_fit_returns_independent_request_local_models() -> None:
    template = NaiveForecaster()
    first = template.fit([1.0, 2.0, 4.0])
    second = template.fit([10.0, 20.0, 40.0])

    assert first is not template and second is not template and first is not second
    assert first.predict(1) == [4.0]
    assert second.predict(1) == [40.0]
    with pytest.raises(RuntimeError, match="fit"):
        template.predict(1)


def test_model_versions_are_stable_and_seasonal_identity_includes_period():
    assert NaiveForecaster().model_version == "baseline-naive@1"
    assert DriftForecaster().model_version == "baseline-drift@1"
    assert SeasonalNaiveForecaster(5).model_version == "baseline-seasonal-naive-s5@1"


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (NaiveForecaster(), []),
        (DriftForecaster(), [1.0]),
        (SeasonalNaiveForecaster(3), [1.0, 2.0]),
    ],
)
def test_fit_enforces_each_models_minimum_history(model: Forecaster, values: list[float]):
    with pytest.raises(ValueError, match="at least"):
        model.fit(values)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), True, "1"])
def test_fit_rejects_nonfinite_and_nonreal_history_values(bad: object):
    with pytest.raises(ValueError, match="finite real"):
        NaiveForecaster().fit([1.0, bad])  # type: ignore[list-item]


@pytest.mark.parametrize("bad", [0, -1, 1.5, True])
def test_prediction_rejects_invalid_horizons(bad: object):
    with pytest.raises(ValueError, match="horizon"):
        NaiveForecaster().fit([1.0]).predict(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [0, -1, 1.5, True])
def test_constructor_rejects_invalid_season_lengths(bad: object):
    with pytest.raises(ValueError, match="season_length"):
        SeasonalNaiveForecaster(bad)  # type: ignore[arg-type]


def test_prediction_requires_fit():
    with pytest.raises(RuntimeError, match="fit"):
        NaiveForecaster().predict(1)
    with pytest.raises(RuntimeError, match="fit"):
        NaiveForecaster().predict_quantiles(1, [0.5])


@pytest.mark.parametrize(
    "quantiles",
    [[], [0.0], [1.0], [-0.1], [float("nan")], [float("inf")], [True], ["0.5"], [0.5, 0.5]],
)
def test_quantile_prediction_rejects_invalid_levels(quantiles: list[object]):
    with pytest.raises(ValueError, match="quantile"):
        NaiveForecaster().fit([1.0, 2.0, 3.0]).predict_quantiles(
            1,
            quantiles,  # type: ignore[arg-type]
        )


def test_quantile_prediction_fails_when_any_horizon_has_too_few_errors():
    model = NaiveForecaster().fit([1.0, 2.0, 4.0])

    with pytest.raises(ValueError, match="horizon step 2.*got 1"):
        model.predict_quantiles(2, [0.5])


def test_quantile_prediction_rejects_zero_dispersion_instead_of_fabricating_width():
    # The two one-step errors are both +1, so empirical quantiles would all
    # collapse to the point forecast despite non-median levels being requested.
    model = NaiveForecaster().fit([1.0, 2.0, 3.0])

    with pytest.raises(ValueError, match="horizon step 1.*zero dispersion"):
        model.predict_quantiles(1, [0.25, 0.5, 0.75])
