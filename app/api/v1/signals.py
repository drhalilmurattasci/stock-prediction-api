"""/v1/signals endpoints: derived analytics signals (Phase 4).

Placeholder router — endpoints return HTTP 501 until Phase 4.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("/{symbol}", summary="Analytics signals for a symbol (not implemented)")
async def get_signals(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/signals/{symbol} is planned for Phase 4.")
