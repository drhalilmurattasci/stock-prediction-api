"""/v1/prices endpoints: OHLCV bars and quotes (Phase 2).

Placeholder router — endpoints return HTTP 501 until Phase 2.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/{symbol}", summary="OHLCV bars for a symbol (not implemented)")
async def get_prices(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/prices/{symbol} is planned for Phase 2.")
