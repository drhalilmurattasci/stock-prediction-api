"""Hand-computed golden tests for forecast evaluation metrics."""

from __future__ import annotations

from collections.abc import Callable
from math import sqrt

import pytest

from ml.evaluation import (
    empirical_interval_coverage,
    interval_coverage,
    mae,
    mean_absolute_error,
    pinball_loss,
    rmse,
    root_mean_squared_error,
)


def test_point_metrics_match_hand_computed_values_and_aliases():
    actual = [1.0, 2.0, 3.0]
    predicted = [1.0, 4.0, 2.0]

    assert mean_absolute_error(actual, predicted) == 1.0
    assert root_mean_squared_error(actual, predicted) == pytest.approx(sqrt(5.0 / 3.0))
    assert mae(actual, predicted) == mean_absolute_error(actual, predicted)
    assert rmse(actual, predicted) == root_mean_squared_error(actual, predicted)


def test_pinball_loss_matches_hand_computed_asymmetric_value():
    assert pinball_loss([2.0], [0.0], 0.9) == pytest.approx(1.8)


def test_empirical_interval_coverage_is_inclusive_and_matches_golden_value():
    actual = [1.0, 2.0, 3.0, 4.0]
    lower = [0.0, 2.0, 3.1, 3.5]
    upper = [1.0, 2.5, 4.0, 4.5]

    assert empirical_interval_coverage(actual, lower, upper) == 0.75
    assert interval_coverage(actual, lower, upper) == 0.75


@pytest.mark.parametrize(
    "metric",
    [mean_absolute_error, root_mean_squared_error, pinball_loss],
)
def test_paired_metrics_reject_empty_inputs(metric: Callable[..., float]):
    args: tuple[object, ...] = ([], [], 0.5) if metric is pinball_loss else ([], [])
    with pytest.raises(ValueError, match="non-empty"):
        metric(*args)


@pytest.mark.parametrize(
    "metric",
    [mean_absolute_error, root_mean_squared_error, pinball_loss],
)
def test_paired_metrics_reject_length_mismatch(metric: Callable[..., float]):
    args: tuple[object, ...] = (
        ([1.0], [1.0, 2.0], 0.5) if metric is pinball_loss else ([1.0], [1.0, 2.0])
    )
    with pytest.raises(ValueError, match="equal lengths"):
        metric(*args)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), True])
def test_metrics_reject_nonfinite_or_nonreal_values(bad: object):
    with pytest.raises(ValueError, match="finite real"):
        mean_absolute_error([1.0, bad], [1.0, 2.0])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="finite real"):
        empirical_interval_coverage([1.0], [0.0], [bad])  # type: ignore[list-item]


@pytest.mark.parametrize("bad", [0.0, 1.0, -0.1, float("nan"), float("inf"), True])
def test_pinball_loss_rejects_invalid_quantile(bad: object):
    with pytest.raises(ValueError, match="quantile"):
        pinball_loss([1.0], [1.0], bad)  # type: ignore[arg-type]


def test_interval_coverage_rejects_empty_mismatched_and_inverted_intervals():
    with pytest.raises(ValueError, match="non-empty"):
        empirical_interval_coverage([], [], [])
    with pytest.raises(ValueError, match="equal lengths"):
        empirical_interval_coverage([1.0], [0.0, 1.0], [2.0])
    with pytest.raises(ValueError, match="lower bounds"):
        empirical_interval_coverage([1.0], [2.0], [0.0])


def test_metrics_handle_large_finite_errors_without_intermediate_overflow():
    assert mean_absolute_error([1e308], [0.0]) == 1e308
    assert root_mean_squared_error([1e308, 1e308], [0.0, 0.0]) == 1e308


def test_metrics_reject_finite_inputs_whose_difference_overflows():
    with pytest.raises(ValueError, match="non-finite error"):
        mean_absolute_error([1e308], [-1e308])
    with pytest.raises(ValueError, match="non-finite error"):
        root_mean_squared_error([1e308], [-1e308])
    with pytest.raises(ValueError, match="non-finite error"):
        pinball_loss([1e308], [-1e308], 0.5)
