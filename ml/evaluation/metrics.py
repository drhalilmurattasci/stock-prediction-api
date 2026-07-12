"""Small, dependency-free forecast accuracy and calibration metrics."""

from __future__ import annotations

from collections.abc import Sequence
from math import fsum, isfinite, sqrt
from numbers import Real


def _finite_values(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    try:
        raw_values = tuple(values)
    except TypeError as exc:
        raise ValueError(f"{name} must be a non-empty finite sequence") from exc
    if not raw_values:
        raise ValueError(f"{name} must be a non-empty finite sequence")

    normalized: list[float] = []
    for value in raw_values:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"{name} must contain only finite real numbers")
        number = float(value)
        if not isfinite(number):
            raise ValueError(f"{name} must contain only finite real numbers")
        normalized.append(number)
    return tuple(normalized)


def _paired_values(
    actual: Sequence[float],
    predicted: Sequence[float],
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    actual_values = _finite_values(actual, name="actual")
    predicted_values = _finite_values(predicted, name="predicted")
    if len(actual_values) != len(predicted_values):
        raise ValueError("actual and predicted must have equal lengths")
    return actual_values, predicted_values


def _probability(value: float, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite probability strictly between 0 and 1")
    probability = float(value)
    if not isfinite(probability) or not 0.0 < probability < 1.0:
        raise ValueError(f"{name} must be a finite probability strictly between 0 and 1")
    return probability


def _finite_error(observed: float, forecast: float) -> float:
    error = observed - forecast
    if not isfinite(error):
        raise ValueError("metric arithmetic produced a non-finite error")
    return error


def mean_absolute_error(actual: Sequence[float], predicted: Sequence[float]) -> float:
    """Return mean absolute point-forecast error."""

    actual_values, predicted_values = _paired_values(actual, predicted)
    count = len(actual_values)
    absolute_error = (
        abs(_finite_error(observed, forecast)) / count
        for observed, forecast in zip(actual_values, predicted_values, strict=True)
    )
    result = fsum(absolute_error)
    if not isfinite(result):
        raise ValueError("metric result is non-finite")
    return result


def root_mean_squared_error(actual: Sequence[float], predicted: Sequence[float]) -> float:
    """Return root mean squared point-forecast error."""

    actual_values, predicted_values = _paired_values(actual, predicted)
    errors = [
        _finite_error(observed, forecast)
        for observed, forecast in zip(actual_values, predicted_values, strict=True)
    ]
    scale = max(abs(error) for error in errors)
    if scale == 0.0:
        return 0.0
    mean_scaled_square = fsum((error / scale) ** 2 for error in errors) / len(errors)
    result = scale * sqrt(mean_scaled_square)
    if not isfinite(result):
        raise ValueError("metric result is non-finite")
    return result


def pinball_loss(
    actual: Sequence[float],
    predicted_quantile: Sequence[float],
    quantile: float,
) -> float:
    """Return mean pinball loss for one predicted quantile level."""

    actual_values, predicted_values = _paired_values(actual, predicted_quantile)
    level = _probability(quantile, name="quantile")
    losses: list[float] = []
    for observed, forecast in zip(actual_values, predicted_values, strict=True):
        error = _finite_error(observed, forecast)
        loss = max(level * error, (level - 1.0) * error)
        if not isfinite(loss):
            raise ValueError("metric result is non-finite")
        losses.append(loss / len(actual_values))
    return fsum(losses)


def empirical_interval_coverage(
    actual: Sequence[float],
    lower: Sequence[float],
    upper: Sequence[float],
) -> float:
    """Return the inclusive fraction of outcomes inside prediction intervals."""

    actual_values = _finite_values(actual, name="actual")
    lower_values = _finite_values(lower, name="lower")
    upper_values = _finite_values(upper, name="upper")
    if not (len(actual_values) == len(lower_values) == len(upper_values)):
        raise ValueError("actual, lower, and upper must have equal lengths")

    covered = 0
    for observed, lower_bound, upper_bound in zip(
        actual_values,
        lower_values,
        upper_values,
        strict=True,
    ):
        if lower_bound > upper_bound:
            raise ValueError("lower bounds must be less than or equal to upper bounds")
        covered += lower_bound <= observed <= upper_bound
    return covered / len(actual_values)


# Concise aliases used in evaluation reports and leaderboard column names.
mae = mean_absolute_error
rmse = root_mean_squared_error
interval_coverage = empirical_interval_coverage

__all__ = [
    "empirical_interval_coverage",
    "interval_coverage",
    "mae",
    "mean_absolute_error",
    "pinball_loss",
    "rmse",
    "root_mean_squared_error",
]
