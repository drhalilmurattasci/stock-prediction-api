"""Shared FastAPI dependencies: DB session, Redis client, settings, auth."""

from __future__ import annotations

from functools import lru_cache

import redis.asyncio as aioredis

from app.config import get_settings
from app.core.security import require_api_key
from app.db.session import get_session

__all__ = [
    "get_settings",
    "get_session",
    "get_redis",
    "check_redis",
    "require_api_key",
]


@lru_cache
def _redis_client() -> aioredis.Redis:
    settings = get_settings()
    return aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)


async def get_redis() -> aioredis.Redis:
    """FastAPI dependency returning a shared async Redis client."""
    return _redis_client()


async def check_redis() -> None:
    """Raise if Redis is unreachable (used by ``/readyz``)."""
    await _redis_client().ping()
