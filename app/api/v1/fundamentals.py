"""/v1/fundamentals endpoints: statements, ratios, key metrics (Phase 2).

Placeholder router — endpoints return HTTP 501 until Phase 2.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/fundamentals", tags=["fundamentals"])


@router.get("/{symbol}", summary="Fundamentals for a symbol (not implemented)")
async def get_fundamentals(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/fundamentals/{symbol} is planned for Phase 2.")
