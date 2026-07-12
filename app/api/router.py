"""Aggregates all versioned routers under the ``/v1`` prefix.

API-key auth is enforced at this aggregate level so every product endpoint
requires ``X-API-Key`` (when keys are configured); liveness/readiness/metrics
are mounted separately in ``app.main`` and stay unauthenticated on purpose.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from app.api.v1 import (
    backtest,
    forecast,
    fundamentals,
    indicators,
    news,
    prices,
    signals,
)
from app.core.security import require_api_key
from app.schemas.common import ErrorResponse

AUTH_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {
        "model": ErrorResponse,
        "description": "Missing, invalid, or unconfigured API-key authentication.",
    }
}

api_router = APIRouter(
    dependencies=[Depends(require_api_key)],
    responses=AUTH_RESPONSES,
)

for module in (prices, fundamentals, indicators, news, forecast, backtest, signals):
    api_router.include_router(module.router)
