"""/v1/forecast endpoints: probabilistic forecasts with calibrated intervals.

The Pydantic request/response contract is locked early so model work can evolve
behind a stable API surface. Both routes delegate through an injectable service;
the default remains fail-closed until immutable snapshot resolution exists.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AwareDatetime, ValidationError

from app.schemas.common import ErrorResponse
from app.schemas.forecast import (
    Coverage,
    ForecastHorizonUnit,
    ForecastModelSelector,
    ForecastRequest,
    ForecastResponse,
    ForecastTarget,
)
from app.services.forecasting import ForecastService, get_forecast_service

router = APIRouter(prefix="/forecast", tags=["forecast"])

ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    501: {
        "model": ErrorResponse,
        "description": "Forecast execution is planned for Phase 3.",
    }
}


@router.get(
    "/{symbol}",
    response_model=ForecastResponse,
    responses=ERROR_RESPONSES,
    summary="Probabilistic forecast for a symbol",
)
async def get_forecast(
    symbol: Annotated[
        str,
        Path(
            min_length=1,
            max_length=32,
            pattern=r"^[A-Za-z0-9.\-_:]+$",
            description="Canonical symbol accepted by the API, e.g. AAPL.",
        ),
    ],
    service: Annotated[ForecastService, Depends(get_forecast_service)],
    horizon: Annotated[
        int,
        Query(ge=1, le=252, description="Number of forecast steps to return."),
    ] = 5,
    horizon_unit: Annotated[
        ForecastHorizonUnit,
        Query(description="Unit for each forecast horizon step."),
    ] = "trading_day",
    target: Annotated[
        ForecastTarget,
        Query(description="Forecast target. Price targets use the response currency."),
    ] = "adjusted_close",
    as_of: Annotated[
        AwareDatetime | None,
        Query(description="Point-in-time cutoff. Defaults to the newest sealed data snapshot."),
    ] = None,
    snapshot_id: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=128,
            description="Pinned immutable data snapshot. Overrides as_of when provided.",
        ),
    ] = None,
    model: Annotated[
        ForecastModelSelector,
        Query(description="Model selector. 'auto' routes to the promoted champion."),
    ] = "auto",
    interval_coverages: Annotated[
        list[Coverage] | None,
        Query(
            alias="coverage",
            description="Repeatable central interval coverage query param, e.g. coverage=0.8.",
        ),
    ] = None,
) -> ForecastResponse:
    request_data = {
        "symbol": symbol,
        "horizon": horizon,
        "horizon_unit": horizon_unit,
        "target": target,
        "as_of": as_of,
        "snapshot_id": snapshot_id,
        "model": model,
    }
    if interval_coverages is not None:
        request_data["interval_coverages"] = interval_coverages
    try:
        request = ForecastRequest.model_validate(request_data)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
    return await service.forecast(request)


@router.post(
    "",
    response_model=ForecastResponse,
    responses=ERROR_RESPONSES,
    summary="Create a snapshot-pinned probabilistic forecast",
)
async def create_forecast(
    request: ForecastRequest,
    service: Annotated[ForecastService, Depends(get_forecast_service)],
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            description="Stable request key for retry-safe forecast creation.",
        ),
    ] = None,
) -> ForecastResponse:
    return await service.forecast(request, idempotency_key=idempotency_key)
