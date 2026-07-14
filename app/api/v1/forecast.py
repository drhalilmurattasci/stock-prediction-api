"""/v1/forecast endpoints: snapshot-pinned probabilistic baseline forecasts.

The Pydantic request/response contract is locked early so model work can evolve
behind a stable API surface. Both routes delegate through an injectable service;
serving stays fail-closed until policy/trust hashes are explicitly pinned.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AwareDatetime, ValidationError

from app.core.security import require_api_key
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
    422: {
        "model": ErrorResponse,
        "description": "The request shape, symbol, horizon, or query values are invalid.",
    },
    404: {
        "model": ErrorResponse,
        "description": "No verified sealed snapshot matches the requested series.",
    },
    409: {
        "model": ErrorResponse,
        "description": "The snapshot/request is incompatible or the forecast is not computable.",
    },
    503: {
        "model": ErrorResponse,
        "description": "Snapshot trust evidence or forecast-serving configuration is invalid.",
    },
    501: {
        "model": ErrorResponse,
        "description": "Serving is disabled or the selected model is not implemented.",
    },
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
        Query(
            description=(
                "Unit for each forecast horizon step. Policy v1 serves only trading_day; "
                "other contract values currently return 409."
            )
        ),
    ] = "trading_day",
    target: Annotated[
        ForecastTarget,
        Query(
            description=(
                "Forecast target. Raw close and locally reproducible split/dividend-adjusted "
                "close use separate operator-pinned policy epochs; unconfigured targets fail "
                "closed."
            )
        ),
    ] = "close",
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
        Query(
            description=(
                "Model selector. Until a champion registry exists, 'auto' honestly routes "
                "to baseline_naive."
            )
        ),
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
    principal: Annotated[str, Depends(require_api_key)],
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            description=(
                "Opaque retry key scoped to the current authenticated API credential and "
                "server identity-secret epoch. Reusing the same key and request replays "
                "the schema-validated stored forecast; changing the request returns 409."
            ),
        ),
    ] = None,
) -> ForecastResponse:
    return await service.forecast(
        request,
        idempotency_key=idempotency_key,
        principal=principal,
    )
