"""/v1/news endpoints: news and sentiment aggregates (Phase 2).

Serves only derived aggregates, never raw licensed vendor article text.
Placeholder router — endpoints return HTTP 501 until Phase 2.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.exceptions import NotImplementedYet

router = APIRouter(prefix="/news", tags=["news"])


@router.get("/{symbol}", summary="News/sentiment aggregates for a symbol (not implemented)")
async def get_news(symbol: str) -> dict:
    raise NotImplementedYet(f"/v1/news/{symbol} is planned for Phase 2.")
