"""/v1/indicators endpoints: technical indicators (Phase 2).

Placeholder router — endpoints return HTTP 501 until Phase 2.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/indicators", tags=["indicators"])


@router.get("/{symbol}", summary="Technical indicators for a symbol (not implemented)")
async def get_indicators(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/indicators/{symbol} is planned for Phase 2.")
