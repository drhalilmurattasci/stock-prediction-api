"""Shared FastAPI dependencies: DB session, Redis client, settings, auth."""

from __future__ import annotations

import redis.asyncio as aioredis
from fastapi import Request

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


async def get_redis(request: Request) -> aioredis.Redis:
    """FastAPI dependency returning a shared async Redis client."""
    return request.app.state.redis_cache


async def check_redis(redis_client: aioredis.Redis) -> None:
    """Raise if Redis is unreachable (used by ``/readyz``)."""
    await redis_client.ping()
