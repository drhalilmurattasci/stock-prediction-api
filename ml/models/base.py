"""Common interface for interchangeable forecast models."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class Forecaster(Protocol):
    """Structural interface implemented by every serving-layer forecaster.

    Quantile forecasts map each requested probability to one value per horizon
    step. Implementations must return fresh containers so callers cannot mutate
    fitted model state through a prediction result.
    """

    model_version: str

    def fit(self, values: Sequence[float]) -> Forecaster:
        """Return a distinct fitted model for an oldest-to-newest history.

        Implementations must not mutate and return a shared template: serving
        code relies on the returned object being request-local and immutable
        with respect to later fits.
        """

        ...

    def predict(self, horizon: int) -> list[float]:
        """Return one point forecast for each future horizon step."""

        ...

    def predict_quantiles(
        self,
        horizon: int,
        quantiles: Sequence[float],
    ) -> dict[float, list[float]]:
        """Return horizon forecasts keyed by requested quantile level."""

        ...
