"""Real-Redis proof for the atomic shared rate-limit counter.

Opt-in so ordinary CI remains hermetic. Run against a throwaway Redis with:
``TEST_RATE_LIMIT_REDIS_URL=redis://127.0.0.1:6379/15 uv run pytest
tests/integration/test_rate_limit_redis_live.py -v``.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
import redis.asyncio as aioredis

from app.core.rate_limit import RedisRateLimitBackend

TEST_REDIS_URL = os.getenv("TEST_RATE_LIMIT_REDIS_URL")

pytestmark = pytest.mark.skipif(
    not TEST_REDIS_URL,
    reason="TEST_RATE_LIMIT_REDIS_URL is required for the live Redis gate",
)


async def test_concurrent_increments_are_atomic_and_every_bucket_expires() -> None:
    assert TEST_REDIS_URL is not None
    client = aioredis.from_url(
        TEST_REDIS_URL,
        socket_connect_timeout=1,
        socket_timeout=1,
        retry_on_timeout=False,
    )
    backend = RedisRateLimitBackend(client)
    bucket = f"stockapi-test:rate-limit:{uuid4().hex}"
    try:
        counts = await asyncio.gather(*(backend.increment(bucket, 5) for _ in range(64)))
        ttl = await client.ttl(bucket)

        assert sorted(counts) == list(range(1, 65))
        assert 0 < ttl <= 5
    finally:
        await client.delete(bucket)
        await backend.aclose()
