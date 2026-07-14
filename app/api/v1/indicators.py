"""Current-snapshot technical indicators over canonical stored close bars."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.etag import if_none_match_matches, strong_etag
from app.db.session import get_session
from app.schemas.common import ErrorResponse
from app.schemas.indicators import IndicatorFilters, IndicatorsResponse
from app.services.indicators import read_indicators

router = APIRouter(prefix="/indicators", tags=["indicators"])


_CACHE_HEADERS: dict[str, dict[str, Any]] = {
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

_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"headers": _CACHE_HEADERS},
    304: {
        "description": "The selected indicator representation has not changed.",
        "headers": _CACHE_HEADERS,
    },
    409: {
        "model": ErrorResponse,
        "description": "Stored history is insufficient or violates the v1 window contract.",
    },
    422: {"model": ErrorResponse, "description": "Invalid symbol or window bound."},
}


@router.get(
    "/{symbol}",
    response_model=IndicatorsResponse,
    responses=_RESPONSES,
    summary="Causal indicators over a fixed current-snapshot XNYS window",
    description=(
        "Computes the owned v1 indicator bundle over at most the newest 258 consecutive "
        "raw daily polygon_open_close observations. Recursive values are relative to the "
        "selected window; structural warm-up is null. The optional end filters stored "
        "observation timestamps, not data availability, so this is not a point-in-time query."
        " Raw values may contain split or dividend discontinuities until the separate "
        "corporate-action reconciliation layer exists."
    ),
)
async def get_indicators(
    symbol: Annotated[
        str,
        Path(
            min_length=1,
            max_length=32,
            pattern=r"^[A-Za-z0-9.\-_:]+$",
            description="Canonical symbol accepted by the API, e.g. MSFT.",
        ),
    ],
    filters: Annotated[IndicatorFilters, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
    if_none_match: Annotated[list[str] | None, Header(alias="If-None-Match")] = None,
) -> Response:
    result = await read_indicators(session, symbol, filters)
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
