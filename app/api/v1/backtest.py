"""/v1/backtest endpoints: walk-forward, cost-aware backtests (Phase 4).

Placeholder router — endpoints return HTTP 501 until Phase 4.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/backtest", tags=["backtest"])


@router.get("/{symbol}", summary="Backtest a signal for a symbol (not implemented)")
async def get_backtest(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/backtest/{symbol} is planned for Phase 4.")
