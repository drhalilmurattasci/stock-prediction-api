"""Pure forecast assembly plus the fail-closed serving dependency seam.

Baseline math can be implemented and verified before a production snapshot
resolver exists. The public routes still use :class:`UnavailableForecastService`
by default: they must not manufacture immutable snapshots, exchange-calendar
target dates, adjusted closes, or point-in-time availability evidence from the
current-only bars table.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from numbers import Real
from typing import Protocol, runtime_checkable
from uuid import UUID

from fastapi import Request

from app.core.exceptions import NotImplementedYet
from app.schemas.forecast import (
    DataSourceLineage,
    ForecastCalibration,
    ForecastHorizonUnit,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    ForecastTarget,
    LookaheadCheck,
)
from ml.models.base import Forecaster

_FLOAT_TOLERANCE = 1e-9
_PRICE_TARGETS = frozenset({"close", "adjusted_close"})
_SERIES_BASES = frozenset({"raw", "split_adjusted", "split_dividend_adjusted"})
_BASELINE_MODEL_PREFIXES = {
    "baseline_naive": "baseline-naive@",
    "baseline_drift": "baseline-drift@",
    "baseline_seasonal_naive": "baseline-seasonal-naive-s",
}


@dataclass(frozen=True)
class ForecastObservation:
    """One point-in-time-resolved model input."""

    observed_at: datetime
    available_at: datetime
    value: float


@dataclass(frozen=True)
class ResolvedForecastInput:
    """All data/calendar facts a resolver must establish before forecasting."""

    symbol: str
    target: ForecastTarget
    horizon_unit: ForecastHorizonUnit
    series_basis: str
    snapshot_id: str
    as_of: datetime
    observations: tuple[ForecastObservation, ...]
    target_times: tuple[datetime, ...]
    data_sources: tuple[DataSourceLineage, ...]
    currency: str | None
    availability_verified: bool


@dataclass(frozen=True)
class ForecastRunIdentity:
    """Stable run identity injected by an idempotent forecast job."""

    forecast_id: UUID
    generated_at: datetime
    model_version: str
    feature_set_hash: str
    code_version: str | None = None


@runtime_checkable
class ForecastService(Protocol):
    """Async serving seam shared by GET and retry-safe POST routes."""

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ForecastResponse: ...


class UnavailableForecastService:
    """Fail closed until immutable/PIT input resolution is implemented."""

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ForecastResponse:
        raise NotImplementedYet(
            f"Forecast execution for {request.symbol} needs an immutable snapshot resolver.",
            details={
                "contract": "ForecastRequest -> ForecastResponse",
                "idempotency_key": idempotency_key,
                "blockers": [
                    "immutable point-in-time snapshot resolution",
                    "exchange-calendar target timestamps",
                    "adjusted-close resolution",
                    "availability-aware lookahead proof",
                ],
            },
        )


_UNAVAILABLE_SERVICE = UnavailableForecastService()


def get_forecast_service(request: Request) -> ForecastService:
    """FastAPI dependency: snapshot-backed only when serving is explicitly enabled.

    With the forecast-serving hashes unset (the default) this keeps returning
    the fail-closed 501 service; nothing is served until an operator pins both
    the resolution policy and the trusted availability rule set.
    """
    # Local import: forecast_serving composes this module's pure assembly, so a
    # module-level import here would be a cycle.
    from app.services.forecast_serving import build_forecast_service

    settings = getattr(request.app.state, "settings", None)
    sessionmaker = getattr(request.app.state, "sessionmaker", None)
    if settings is None or sessionmaker is None:
        return _UNAVAILABLE_SERVICE
    service = build_forecast_service(settings, sessionmaker)
    return service if service is not None else _UNAVAILABLE_SERVICE


def assemble_baseline_forecast_response(
    request: ForecastRequest,
    resolved: ResolvedForecastInput,
    *,
    forecaster_factory: Callable[[], Forecaster],
    identity: ForecastRunIdentity,
) -> ForecastResponse:
    """Fit one fresh baseline and map its quantiles into the locked contract.

    The caller supplies every identity, snapshot, calendar, and availability
    fact. This function never calls a clock, creates a UUID, or invents a data
    snapshot, so identical immutable inputs produce identical response bytes.
    """
    observations, target_times, data_sources, as_of, generated_at = _validated_inputs(
        request, resolved, identity
    )
    forecaster = forecaster_factory()
    if not isinstance(forecaster, Forecaster):
        raise TypeError("forecaster_factory must return a Forecaster")
    fitted = forecaster.fit([item.value for item in observations])
    if fitted is forecaster:
        raise TypeError("Forecaster.fit must return a distinct request-local instance")
    if fitted.model_version != identity.model_version:
        raise ValueError("run identity model_version must match the fitted forecaster")
    _validate_model_selection(request, identity.model_version)

    raw_points = list(fitted.predict(request.horizon))
    levels = _requested_quantile_levels(request.interval_coverages)
    raw_quantiles = fitted.predict_quantiles(request.horizon, levels)
    quantile_paths = {
        level: _quantile_path(raw_quantiles, level, request.horizon) for level in levels
    }
    if len(raw_points) != request.horizon:
        raise ValueError("point forecast length must match the requested horizon")

    steps: list[ForecastStep] = []
    for index, target_time in enumerate(target_times):
        point = _target_value(request.target, raw_points[index])
        values = {
            level: _target_value(request.target, quantile_paths[level][index]) for level in levels
        }
        ordered_values = [values[level] for level in levels]
        if any(
            current > following + _FLOAT_TOLERANCE
            for current, following in zip(ordered_values, ordered_values[1:], strict=False)
        ):
            raise ValueError("forecaster returned crossing quantiles")
        if abs(values[0.5] - point) > _FLOAT_TOLERANCE:
            raise ValueError("forecaster point must equal its median quantile")

        quantiles = [ForecastQuantile(level=level, value=values[level]) for level in levels]
        intervals = []
        for coverage in request.interval_coverages:
            lower_level, upper_level = _coverage_levels(coverage)
            intervals.append(
                ForecastInterval(
                    coverage=coverage,
                    lower_quantile=lower_level,
                    upper_quantile=upper_level,
                    lower=values[lower_level],
                    upper=values[upper_level],
                )
            )
        steps.append(
            ForecastStep(
                step=index + 1,
                target_time=target_time,
                point=point,
                quantiles=quantiles,
                intervals=intervals,
            )
        )

    max_available_at = max(
        [item.available_at for item in observations]
        + [source.max_available_at for source in data_sources]
    )
    lookahead_status = "passed" if resolved.availability_verified else "not_run"
    lookahead_violations = (
        []
        if resolved.availability_verified
        else ["availability timestamps were not verified by the snapshot resolver"]
    )
    provenance = ForecastProvenance(
        forecast_id=identity.forecast_id,
        snapshot_id=resolved.snapshot_id,
        model_version=identity.model_version,
        series_basis=resolved.series_basis,
        feature_set_hash=identity.feature_set_hash,
        max_available_at=max_available_at,
        generated_at=generated_at,
        code_version=identity.code_version,
        data_sources=list(data_sources),
        lookahead_check=LookaheadCheck(
            status=lookahead_status,
            checked_at=generated_at,
            max_feature_available_at=max_available_at,
            violations=lookahead_violations,
        ),
    )
    return ForecastResponse(
        symbol=request.symbol,
        target=request.target,
        horizon=request.horizon,
        horizon_unit=request.horizon_unit,
        as_of=as_of,
        currency=resolved.currency,
        forecasts=steps,
        provenance=provenance,
        calibration=ForecastCalibration(
            calibration_set_version=f"uncalibrated:{identity.model_version}",
            method="none",
            sample_count=0,
            by_interval=[],
        ),
    )


def _validated_inputs(
    request: ForecastRequest,
    resolved: ResolvedForecastInput,
    identity: ForecastRunIdentity,
) -> tuple[
    tuple[ForecastObservation, ...],
    tuple[datetime, ...],
    tuple[DataSourceLineage, ...],
    datetime,
    datetime,
]:
    if not isinstance(resolved.symbol, str) or resolved.symbol.strip().upper() != request.symbol:
        raise ValueError("resolved symbol must match the requested symbol")
    if resolved.target != request.target:
        raise ValueError("resolved target must match the requested target")
    if resolved.horizon_unit != request.horizon_unit:
        raise ValueError("resolved horizon_unit must match the request")
    if resolved.series_basis not in _SERIES_BASES:
        raise ValueError("resolved series_basis is not supported")
    if request.target == "close" and resolved.series_basis != "raw":
        raise ValueError("close forecasts require a raw resolved series")
    if request.target == "adjusted_close" and resolved.series_basis == "raw":
        raise ValueError("adjusted_close forecasts require an adjusted resolved series")
    if not isinstance(resolved.snapshot_id, str) or not resolved.snapshot_id.strip():
        raise ValueError("snapshot_id must not be empty")
    if request.snapshot_id is not None and request.snapshot_id != resolved.snapshot_id:
        raise ValueError("resolved snapshot_id does not match the requested snapshot")
    as_of = _as_utc(resolved.as_of, "as_of")
    if (
        request.snapshot_id is None
        and request.as_of is not None
        and as_of > request.as_of.astimezone(UTC)
    ):
        raise ValueError("resolved snapshot is later than the requested as_of cutoff")
    generated_at = _as_utc(identity.generated_at, "generated_at")
    if generated_at < as_of:
        raise ValueError("generated_at must not be earlier than as_of")
    if not resolved.observations:
        raise ValueError("at least one forecast observation is required")
    if not resolved.data_sources:
        raise ValueError("at least one data source lineage row is required")
    if type(resolved.availability_verified) is not bool:
        raise ValueError("availability_verified must be a boolean")
    if len(resolved.target_times) != request.horizon:
        raise ValueError("target_times length must match the requested horizon")

    observations = tuple(
        ForecastObservation(
            observed_at=_as_utc(item.observed_at, "observation observed_at"),
            available_at=_as_utc(item.available_at, "observation available_at"),
            value=_finite(item.value, "observation value"),
        )
        for item in resolved.observations
    )
    for previous, current in zip(observations, observations[1:], strict=False):
        if previous.observed_at >= current.observed_at:
            raise ValueError("observation times must be strictly increasing")
    for item in observations:
        if item.observed_at > as_of:
            raise ValueError("observation time must not be later than as_of")
        if item.available_at < item.observed_at:
            raise ValueError("available_at must not be earlier than observed_at")
        if item.available_at > as_of:
            raise ValueError("observation availability must not be later than as_of")
    data_sources = _canonical_data_sources(resolved.data_sources)
    for source in data_sources:
        if source.max_available_at > as_of:
            raise ValueError("source availability must not be later than as_of")

    target_times = tuple(_as_utc(value, "target_time") for value in resolved.target_times)
    if target_times[0] <= as_of:
        raise ValueError("target times must be later than as_of")
    if any(
        current >= following
        for current, following in zip(target_times, target_times[1:], strict=False)
    ):
        raise ValueError("target times must be strictly increasing")
    if request.target in _PRICE_TARGETS and resolved.currency is None:
        raise ValueError("currency is required for price targets")
    if request.target not in _PRICE_TARGETS and resolved.currency is not None:
        raise ValueError("currency must be null for return targets")
    return observations, target_times, data_sources, as_of, generated_at


def _validate_model_selection(request: ForecastRequest, model_version: str) -> None:
    if request.model == "auto":
        return
    expected_prefix = _BASELINE_MODEL_PREFIXES.get(request.model)
    if expected_prefix is None:
        raise ValueError(f"{request.model} is not a baseline model selector")
    if not model_version.startswith(expected_prefix):
        raise ValueError("resolved baseline model does not match the requested model")


def _canonical_data_sources(
    sources: tuple[DataSourceLineage, ...],
) -> tuple[DataSourceLineage, ...]:
    grouped: dict[tuple[str, str, datetime], set[str]] = {}
    for source in sources:
        key = (
            source.name,
            source.snapshot_id,
            _as_utc(source.max_available_at, "source max_available_at"),
        )
        grouped.setdefault(key, set()).update(source.fields)
    return tuple(
        DataSourceLineage(
            name=name,
            snapshot_id=snapshot_id,
            max_available_at=max_available_at,
            fields=sorted(grouped[(name, snapshot_id, max_available_at)]),
        )
        for name, snapshot_id, max_available_at in sorted(grouped)
    )


def _requested_quantile_levels(coverages: list[float]) -> list[float]:
    levels = {0.5}
    for coverage in coverages:
        levels.update(_coverage_levels(coverage))
    return sorted(levels)


def _coverage_levels(coverage: float) -> tuple[float, float]:
    return round((1.0 - coverage) / 2.0, 12), round((1.0 + coverage) / 2.0, 12)


def _quantile_path(
    paths: dict[float, list[float]],
    requested_level: float,
    horizon: int,
) -> list[float]:
    match = next(
        (values for level, values in paths.items() if abs(level - requested_level) <= 1e-12),
        None,
    )
    if match is None:
        raise ValueError(f"forecaster omitted requested quantile {requested_level}")
    values = list(match)
    if len(values) != horizon:
        raise ValueError("quantile forecast length must match the requested horizon")
    return values


def _target_value(target: str, value: float) -> float:
    finite = _finite(value, "forecast value")
    return max(0.0, finite) if target in _PRICE_TARGETS else finite


def _finite(value: float, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{label} must be a finite real number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{label} must be finite")
    return converted


def _as_utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)
