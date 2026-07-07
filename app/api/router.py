"""Aggregates all versioned routers under the ``/v1`` prefix."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import (
    backtest,
    forecast,
    fundamentals,
    indicators,
    news,
    prices,
    signals,
)

api_router = APIRouter()

for module in (prices, fundamentals, indicators, news, forecast, backtest, signals):
    api_router.include_router(module.router)
