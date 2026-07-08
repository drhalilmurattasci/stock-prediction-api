"""Rate limiting via slowapi.

Keyed per API key (falling back to client IP). Storage defaults to in-memory
(fine for single-worker dev); set ``RATE_LIMIT_STORAGE_URI=redis://...`` so limits
are shared across Uvicorn workers in production.
"""

from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request

from app.config import Settings
from app.core.security import API_KEY_HEADER


def _rate_limit_key(request: Request) -> str:
    api_key = request.headers.get(API_KEY_HEADER)
    if api_key:
        return f"key:{api_key}"
    return f"ip:{get_remote_address(request)}"


def build_limiter(settings: Settings) -> Limiter:
    """Build a SlowAPI limiter from runtime settings."""
    return Limiter(
        key_func=_rate_limit_key,
        storage_uri=settings.rate_limit_storage_uri,
        default_limits=[settings.rate_limit_default],
        headers_enabled=True,
        enabled=settings.rate_limit_enabled,
    )
