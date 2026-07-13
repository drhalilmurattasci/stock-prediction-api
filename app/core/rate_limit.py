"""Owned API rate limiting: prefix-scoped, hashed identities, fail-closed.

Replaces slowapi, whose middleware matched routes by literal path and therefore
never applied default limits to parameterized nested routes such as
``/v1/prices/{symbol}`` — production configs were silently unlimited. This
limiter enforces on everything under the versioned API prefix by construction.

Identities are HMAC-hashed before they reach shared storage so a raw API key
never appears in Redis keys or memory dumps. When the shared storage backend is
unavailable the limiter fails CLOSED (503) unless ``rate_limit_fail_open`` is
explicitly set: an outage must not silently lift every quota.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import redis.asyncio as aioredis
from fastapi import status
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.config import Settings
from app.core.security import API_KEY_HEADER
from app.schemas.common import ErrorBody, ErrorResponse

_RATE_PATTERN = re.compile(r"^\s*(\d+)\s*/\s*(second|minute|hour|day)s?\s*$")
_WINDOW_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}
#: Identity hashes are truncated: 128 bits is ample for bucket uniqueness.
_IDENTITY_HEX_CHARS = 32


def parse_rate(rate: str) -> tuple[int, int]:
    """Parse ``"120/minute"`` into ``(limit, window_seconds)``; fail fast otherwise."""
    match = _RATE_PATTERN.fullmatch(rate)
    if match is None:
        raise ValueError(f"unsupported rate limit spec: {rate!r}")
    limit = int(match.group(1))
    if limit < 1:
        raise ValueError("rate limit must allow at least one request")
    return limit, _WINDOW_SECONDS[match.group(2)]


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_after: int  # whole seconds until the current window ends


@runtime_checkable
class RateLimitBackend(Protocol):
    """Shared counter store: one atomic increment per (bucket, window)."""

    async def increment(self, bucket: str, ttl_seconds: int) -> int: ...

    async def aclose(self) -> None: ...


class MemoryRateLimitBackend:
    """Single-process fixed-window counters (dev/test default)."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._expiries: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def increment(self, bucket: str, ttl_seconds: int) -> int:
        now = time.monotonic()
        async with self._lock:
            for key, expires_at in list(self._expiries.items()):
                if expires_at <= now:
                    self._expiries.pop(key, None)
                    self._counts.pop(key, None)
            if bucket not in self._counts:
                self._expiries[bucket] = now + ttl_seconds
            self._counts[bucket] = self._counts.get(bucket, 0) + 1
            return self._counts[bucket]

    async def aclose(self) -> None:
        return None

    def buckets(self) -> tuple[str, ...]:
        """Test hook: every bucket identifier currently held."""
        return tuple(self._counts)


class RedisRateLimitBackend:
    """Multi-worker fixed-window counters over the shared Redis."""

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def increment(self, bucket: str, ttl_seconds: int) -> int:
        count = int(await self._client.incr(bucket))
        if count == 1:
            await self._client.expire(bucket, ttl_seconds)
        return count

    async def aclose(self) -> None:
        await self._client.aclose()


@dataclass
class ApiRateLimiter:
    """Fixed-window limiter keyed by hashed API key (or client IP)."""

    limit: int
    window_seconds: int
    backend: RateLimitBackend
    identity_secret: str
    enabled: bool = True
    fail_open: bool = False
    scope_prefix: str = "/v1"
    clock: Callable[[], float] = field(default=time.time)

    def identity_for(self, api_key: str | None, client_ip: str) -> str:
        if api_key:
            digest = hmac.new(
                self.identity_secret.encode("utf-8"),
                api_key.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()[:_IDENTITY_HEX_CHARS]
            return f"key:{digest}"
        return f"ip:{client_ip}"

    async def hit(self, identity: str) -> RateLimitDecision:
        now = self.clock()
        window_index = int(now // self.window_seconds)
        bucket = f"rl:{identity}:{self.window_seconds}:{window_index}"
        # Keep the key alive past the window edge so a boundary race cannot
        # resurrect a fresh counter inside the same window.
        count = await self.backend.increment(bucket, ttl_seconds=self.window_seconds * 2)
        reset_after = max(1, math.ceil((window_index + 1) * self.window_seconds - now))
        return RateLimitDecision(
            allowed=count <= self.limit,
            limit=self.limit,
            remaining=max(0, self.limit - count),
            reset_after=reset_after,
        )


def build_rate_limiter(settings: Settings) -> ApiRateLimiter:
    """Build the app limiter; a malformed rate spec fails app startup loudly."""
    limit, window_seconds = parse_rate(settings.rate_limit_default)
    backend: RateLimitBackend
    if settings.rate_limit_storage_uri.startswith(("redis://", "rediss://")):
        backend = RedisRateLimitBackend(aioredis.from_url(settings.rate_limit_storage_uri))
    elif settings.rate_limit_storage_uri.startswith("memory://"):
        backend = MemoryRateLimitBackend()
    else:
        raise ValueError(f"unsupported rate_limit_storage_uri: {settings.rate_limit_storage_uri!r}")
    return ApiRateLimiter(
        limit=limit,
        window_seconds=window_seconds,
        backend=backend,
        identity_secret=settings.jwt_secret,
        enabled=settings.rate_limit_enabled,
        fail_open=settings.rate_limit_fail_open,
        scope_prefix=settings.api_v1_prefix,
    )


def _envelope(request: Request, *, code: str, message: str, status_code: int) -> JSONResponse:
    body = ErrorResponse(
        error=ErrorBody(
            code=code,
            message=message,
            request_id=getattr(request.state, "request_id", None),
            details=None,
        )
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


def _apply_headers(response: Response, decision: RateLimitDecision) -> None:
    response.headers["RateLimit-Limit"] = str(decision.limit)
    response.headers["RateLimit-Remaining"] = str(decision.remaining)
    response.headers["RateLimit-Reset"] = str(decision.reset_after)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Enforce the shared per-identity quota on every versioned API route.

    Runs before routing and auth, so unauthenticated floods burn their own
    bucket instead of reaching the auth layer unmetered. Paths outside the
    versioned prefix (health, readiness, metrics) are never limited.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        limiter: ApiRateLimiter | None = getattr(request.app.state, "rate_limiter", None)
        if (
            limiter is None
            or not limiter.enabled
            or not request.url.path.startswith(limiter.scope_prefix)
        ):
            return await call_next(request)
        client_ip = request.client.host if request.client else "unknown"
        identity = limiter.identity_for(request.headers.get(API_KEY_HEADER), client_ip)
        try:
            decision = await limiter.hit(identity)
        except Exception:  # noqa: BLE001 - storage outage: honor the fail posture.
            if limiter.fail_open:
                return await call_next(request)
            return _envelope(
                request,
                code="rate_limit_unavailable",
                message="Rate limiting is temporarily unavailable; request refused.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if not decision.allowed:
            response: Response = _envelope(
                request,
                code="rate_limited",
                message="Rate limit exceeded; retry after the current window resets.",
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            )
            response.headers["Retry-After"] = str(decision.reset_after)
            _apply_headers(response, decision)
            return response
        response = await call_next(request)
        _apply_headers(response, decision)
        return response
