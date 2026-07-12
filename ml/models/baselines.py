"""Deterministic forecasting baselines with empirical residual uncertainty.

Quantiles use expanding-origin historical errors computed separately at each
horizon. Every historical forecast sees only the prefix available at its origin,
so quantile construction cannot leak a future observation into that forecast.
The empirical errors are median-recentered to preserve the model's point forecast.

These intervals are an honest, lightweight baseline, but they are *not* conformal
intervals and have no validated coverage guarantee. Public calibration metadata
must remain ``none`` until coverage has been evaluated on held-out outcomes.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Sequence
from math import isfinite
from numbers import Integral, Real
from typing import Self


def _positive_integer(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _finite_history(values: Sequence[float], *, minimum: int) -> tuple[float, ...]:
    try:
        raw_values = tuple(values)
    except TypeError as exc:
        raise ValueError("values must be a finite sequence") from exc

    if len(raw_values) < minimum:
        raise ValueError(f"values must contain at least {minimum} observation(s)")

    history: list[float] = []
    for value in raw_values:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError("values must contain only finite real numbers")
        number = float(value)
        if not isfinite(number):
            raise ValueError("values must contain only finite real numbers")
        history.append(number)
    return tuple(history)


def _quantile_levels(quantiles: Sequence[float]) -> tuple[float, ...]:
    try:
        raw_quantiles = tuple(quantiles)
    except TypeError as exc:
        raise ValueError("quantiles must be a non-empty sequence") from exc

    if not raw_quantiles:
        raise ValueError("quantiles must be a non-empty sequence")

    levels: list[float] = []
    for quantile in raw_quantiles:
        if isinstance(quantile, bool) or not isinstance(quantile, Real):
            raise ValueError(
                "quantiles must contain only finite probabilities strictly between 0 and 1"
            )
        level = float(quantile)
        if not isfinite(level) or not 0.0 < level < 1.0:
            raise ValueError(
                "quantiles must contain only finite probabilities strictly between 0 and 1"
            )
        if level in levels:
            raise ValueError("quantiles must not contain duplicates")
        levels.append(level)
    return tuple(levels)


def _linear_quantile(values: Sequence[float], probability: float) -> float:
    """Return the linearly interpolated empirical quantile.

    The pinned rule is ``position = (n - 1) * probability``, matching the common
    type-7/default linear definition. Keeping it here avoids a NumPy dependency
    and makes golden-value behavior stable across environments.
    """

    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower_index = int(position)
    fraction = position - lower_index
    if fraction == 0.0:
        return ordered[lower_index]
    lower = ordered[lower_index]
    upper = ordered[lower_index + 1]
    return (1.0 - fraction) * lower + fraction * upper


class _EmpiricalResidualBaseline(ABC):
    """Shared state and leakage-free empirical quantile implementation."""

    model_version: str
    _minimum_history: int

    def __init__(self) -> None:
        self._history: tuple[float, ...] | None = None

    def fit(self, values: Sequence[float]) -> Self:
        history = _finite_history(values, minimum=self._minimum_history)
        fitted = copy.copy(self)
        fitted._history = history
        return fitted

    def predict(self, horizon: int) -> list[float]:
        steps = _positive_integer(horizon, name="horizon")
        history = self._fitted_history()
        return [self._checked_forecast(history, step) for step in range(1, steps + 1)]

    def predict_quantiles(
        self,
        horizon: int,
        quantiles: Sequence[float],
    ) -> dict[float, list[float]]:
        """Estimate uncalibrated quantiles from prefix-only historical errors.

        At least two expanding-origin errors are required for every requested
        horizon. Errors are estimated independently by horizon and recentered by
        their interpolated median, making the 0.5 quantile equal the raw point
        forecast while retaining the empirical error distribution's asymmetry.
        """

        steps = _positive_integer(horizon, name="horizon")
        levels = _quantile_levels(quantiles)
        history = self._fitted_history()
        points = [self._checked_forecast(history, step) for step in range(1, steps + 1)]
        forecasts: dict[float, list[float]] = {level: [] for level in levels}

        for step, point in enumerate(points, start=1):
            errors = self._expanding_origin_errors(history, step)
            if len(errors) < 2:
                raise ValueError(
                    f"horizon step {step} requires at least 2 prefix-only historical errors; "
                    f"got {len(errors)}"
                )
            if min(errors) == max(errors):
                raise ValueError(
                    f"horizon step {step} historical errors have zero dispersion; "
                    "cannot estimate a non-degenerate interval"
                )
            median_error = _linear_quantile(errors, 0.5)
            for level in levels:
                adjusted_error = _linear_quantile(errors, level) - median_error
                forecast = point + adjusted_error
                if not isfinite(forecast):
                    raise ValueError("quantile forecast is non-finite")
                forecasts[level].append(forecast)

        return forecasts

    def _fitted_history(self) -> tuple[float, ...]:
        if self._history is None:
            raise RuntimeError("fit must be called before prediction")
        return self._history

    def _checked_forecast(self, history: tuple[float, ...], step: int) -> float:
        forecast = self._forecast_at(history, step)
        if not isfinite(forecast):
            raise ValueError("point forecast is non-finite")
        return forecast

    def _expanding_origin_errors(
        self,
        history: tuple[float, ...],
        step: int,
    ) -> list[float]:
        errors: list[float] = []
        last_prefix_size = len(history) - step
        for prefix_size in range(self._minimum_history, last_prefix_size + 1):
            prefix = history[:prefix_size]
            actual = history[prefix_size + step - 1]
            error = actual - self._checked_forecast(prefix, step)
            if not isfinite(error):
                raise ValueError("historical forecast error is non-finite")
            errors.append(error)
        return errors

    @abstractmethod
    def _forecast_at(self, history: tuple[float, ...], step: int) -> float:
        """Return a one-indexed horizon forecast from an already valid history."""


class NaiveForecaster(_EmpiricalResidualBaseline):
    """Random-walk/last-value baseline."""

    model_version = "baseline-naive@1"
    _minimum_history = 1

    def _forecast_at(self, history: tuple[float, ...], step: int) -> float:
        return history[-1]


class DriftForecaster(_EmpiricalResidualBaseline):
    """Last value extrapolated by the history's end-to-end average drift."""

    model_version = "baseline-drift@1"
    _minimum_history = 2

    def _forecast_at(self, history: tuple[float, ...], step: int) -> float:
        slope = (history[-1] - history[0]) / (len(history) - 1)
        return history[-1] + slope * step


class SeasonalNaiveForecaster(_EmpiricalResidualBaseline):
    """Repeat the most recently observed season of a fixed length."""

    def __init__(self, season_length: int) -> None:
        self.season_length = _positive_integer(season_length, name="season_length")
        self._minimum_history = self.season_length
        self.model_version = f"baseline-seasonal-naive-s{self.season_length}@1"
        super().__init__()

    def _forecast_at(self, history: tuple[float, ...], step: int) -> float:
        seasonal_offset = (step - 1) % self.season_length
        return history[-self.season_length + seasonal_offset]
