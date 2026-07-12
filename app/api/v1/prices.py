"""Current-snapshot OHLCV reads from the normalized bars store.

The endpoint selects one explicit stored series (source, interval, multiplier,
and adjustment basis). It intentionally does not expose historical ``as_of``
reconstruction or claim to compute adjusted prices on read; those require the
future corporate-action and version-selection layers.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.etag import if_none_match_matches, strong_etag
from app.db.session import get_session
from app.schemas.common import ErrorResponse
from app.schemas.prices import PriceFilters, PricesResponse
from app.services.prices import read_prices

router = APIRouter(prefix="/prices", tags=["prices"])


CACHE_RESPONSE_HEADERS: dict[str, dict[str, Any]] = {
    "ETag": {
        "description": "Strong validator for the exact JSON response bytes.",
        "schema": {"type": "string"},
    },
    "Cache-Control": {
        "description": "Private-cache revalidation policy.",
        "schema": {"type": "string"},
    },
    "Vary": {
        "description": "Header dimensions that select the representation.",
        "schema": {"type": "string"},
    },
}

RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"headers": CACHE_RESPONSE_HEADERS},
    304: {
        "description": "The selected price representation has not changed.",
        "headers": CACHE_RESPONSE_HEADERS,
    },
    422: {"model": ErrorResponse, "description": "Invalid symbol or price filters."},
}


@router.get(
    "/{symbol}",
    response_model=PricesResponse,
    responses=RESPONSES,
    summary="Current stored OHLCV bars for a symbol",
)
async def get_prices(
    symbol: Annotated[
        str,
        Path(
            min_length=1,
            max_length=32,
            pattern=r"^[A-Za-z0-9.\-_:]+$",
            description="Canonical symbol accepted by the API, e.g. AAPL.",
        ),
    ],
    filters: Annotated[PriceFilters, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
    if_none_match: Annotated[list[str] | None, Header(alias="If-None-Match")] = None,
) -> Response:
    result = await read_prices(session, symbol, filters)
    payload = result.model_dump_json().encode("utf-8")
    etag = strong_etag(payload)
    headers = {
        "ETag": etag,
        "Cache-Control": "private, no-cache",
        "Vary": "X-API-Key",
    }
    combined_validators = ", ".join(if_none_match) if if_none_match else None
    if if_none_match_matches(combined_validators, etag):
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)
    return Response(content=payload, media_type="application/json", headers=headers)
