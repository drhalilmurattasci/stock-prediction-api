"""Aggregates all versioned routers under the ``/v1`` prefix.

API-key auth is enforced at this aggregate level so every product endpoint
requires ``X-API-Key`` (when keys are configured); liveness/readiness/metrics
are mounted separately in ``app.main`` and stay unauthenticated on purpose.
"""

from __future__ import annotations

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

api_router = APIRouter(dependencies=[Depends(require_api_key)])

for module in (prices, fundamentals, indicators, news, forecast, backtest, signals):
    api_router.include_router(module.router)
