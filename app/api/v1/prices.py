"""Raw current-snapshot and immutable factor-pinned adjusted OHLCV reads.

The raw endpoint selects one explicit stored current series.  The adjusted
endpoint is separate so that raw response bytes and semantics remain stable;
it requires an exact immutable factor-set ID and reconstructs every bound raw
version and publication receipt before deriving any page.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.etag import if_none_match_matches, strong_etag
from app.db.session import get_session
from app.schemas.common import ErrorResponse
from app.schemas.prices import (
    AdjustedPriceFilters,
    AdjustedPricesResponse,
    PriceFilters,
    PricesResponse,
)
from app.services.adjusted_price_store import read_adjusted_prices
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

ADJUSTED_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"headers": CACHE_RESPONSE_HEADERS},
    304: {
        "description": "The exact factor-pinned adjusted representation has not changed.",
        "headers": CACHE_RESPONSE_HEADERS,
    },
    404: {
        "model": ErrorResponse,
        "description": "The exact immutable factor-set identity does not exist.",
    },
    409: {
        "model": ErrorResponse,
        "description": "The factor set does not belong to the requested symbol.",
    },
    422: {"model": ErrorResponse, "description": "Invalid symbol or adjusted-price filters."},
    503: {
        "model": ErrorResponse,
        "description": "The stored factor, receipt, action, or raw-version evidence is invalid.",
    },
}


@router.get(
    "/{symbol}/adjusted",
    response_model=AdjustedPricesResponse,
    responses=ADJUSTED_RESPONSES,
    summary="Factor-pinned split/dividend-adjusted OHLCV bars",
    description=(
        "Requires an exact immutable factor_set_id; no mutable latest-factor resolution "
        "or raw fallback is performed. The complete factor window and every exact raw and "
        "corporate-action receipt are validated before time filtering or pagination."
    ),
)
async def get_adjusted_prices(
    symbol: Annotated[
        str,
        Path(
            min_length=1,
            max_length=32,
            pattern=r"^[A-Za-z0-9.\-_:]+$",
            description="Canonical symbol accepted by the API, e.g. AAPL.",
        ),
    ],
    filters: Annotated[AdjustedPriceFilters, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
    if_none_match: Annotated[list[str] | None, Header(alias="If-None-Match")] = None,
) -> Response:
    result = await read_adjusted_prices(session, symbol, filters)
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
