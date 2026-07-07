"""/v1/forecast endpoints: probabilistic forecasts with calibrated intervals.

The Pydantic request/response contract is locked early so model work can evolve
behind a stable API surface. Runtime implementation is planned for Phase 3; the
routes intentionally return HTTP 501 until then.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, Path, Query
from pydantic import AwareDatetime

from app.core.exceptions import NotImplementedYet
from app.schemas.common import ErrorResponse
from app.schemas.forecast import ForecastRequest, ForecastResponse, ForecastTarget

router = APIRouter(prefix="/forecast", tags=["forecast"])

ERROR_RESPONSES = {
    501: {
        "model": ErrorResponse,
        "description": "Forecast execution is planned for Phase 3.",
    }
}


@router.get(
    "/{symbol}",
    response_model=ForecastResponse,
    responses=ERROR_RESPONSES,
    summary="Probabilistic forecast for a symbol (contract only)",
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
    horizon: Annotated[
        int,
        Query(ge=1, le=252, description="Number of forecast steps to return."),
    ] = 5,
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
        str,
        Query(description="Model selector. 'auto' routes to the promoted champion."),
    ] = "auto",
    interval_coverages: Annotated[
        list[float] | None,
        Query(
            alias="coverage",
            description="Repeatable central interval coverage query param, e.g. coverage=0.8.",
        ),
    ] = None,
) -> ForecastResponse:
    raise NotImplementedYet(
        f"/v1/forecast/{symbol.upper()} is planned for Phase 3.",
        details={
            "contract": "ForecastResponse",
            "horizon": horizon,
            "target": target,
            "as_of": as_of,
            "snapshot_id": snapshot_id,
            "model": model,
            "interval_coverages": interval_coverages,
        },
    )


@router.post(
    "",
    response_model=ForecastResponse,
    responses=ERROR_RESPONSES,
    summary="Create a snapshot-pinned probabilistic forecast (contract only)",
)
async def create_forecast(
    request: ForecastRequest,
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
    raise NotImplementedYet(
        f"POST /v1/forecast for {request.symbol} is planned for Phase 3.",
        details={
            "contract": "ForecastRequest -> ForecastResponse",
            "idempotency_key": idempotency_key,
        },
    )
