"""Rate limiting: real nested /v1 routes, hashed identities, fail posture.

Reproduces the production defect that motivated the rewrite: with slowapi, a
``1/minute`` limit never throttled parameterized nested routes (its middleware
matched literal paths only). Every test here drives the real app through real
``/v1/.../{symbol}`` routes.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.core.rate_limit import (
    ApiRateLimiter,
    MemoryRateLimitBackend,
    RedisRateLimitBackend,
    build_rate_limiter,
    parse_rate,
)
from app.db.session import get_session
from app.main import create_app

KEYS = "k-alpha,k-bravo"


def _app(rate: str = "1/minute", **settings_overrides: object):
    settings = Settings(
        app_env="test",
        rate_limit_enabled=True,
        rate_limit_default=rate,
        api_keys=KEYS,
        **settings_overrides,  # type: ignore[arg-type]
    )
    return create_app(settings)


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(_app()) as test_client:
        yield test_client


def test_one_per_minute_throttles_the_second_request_on_nested_routes(
    client: TestClient,
) -> None:
    # The slowapi regression scenario: parameterized nested route, prod-style limit.
    first = client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
    assert first.status_code == 501  # reaches the handler
    assert first.headers["RateLimit-Limit"] == "1"
    assert first.headers["RateLimit-Remaining"] == "0"
    assert int(first.headers["RateLimit-Reset"]) >= 1

    second = client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
    assert second.status_code == 429
    assert int(second.headers["Retry-After"]) >= 1
    assert second.headers["RateLimit-Remaining"] == "0"
    body = second.json()
    assert body["error"]["code"] == "rate_limited"
    assert body["error"]["request_id"]


def test_quota_is_shared_across_different_v1_endpoints(client: TestClient) -> None:
    assert client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"}).status_code == 501
    # A DIFFERENT nested endpoint must burn the same per-key bucket.
    assert client.get("/v1/signals/MSFT", headers={"X-API-Key": "k-alpha"}).status_code == 429


def test_real_prices_and_forecast_routes_share_one_quota() -> None:
    class EmptyScalars:
        def all(self) -> list[object]:
            return []

    class EmptyResult:
        def scalars(self) -> EmptyScalars:
            return EmptyScalars()

    class EmptySession:
        async def execute(self, statement: object) -> EmptyResult:
            return EmptyResult()

    async def empty_session():
        yield EmptySession()

    app = _app(rate="2/minute")
    app.dependency_overrides[get_session] = empty_session
    headers = {"X-API-Key": "k-alpha"}
    with TestClient(app) as test_client:
        prices = test_client.get("/v1/prices/MSFT", headers=headers)
        forecast = test_client.get("/v1/forecast/MSFT", headers=headers)
        exhausted = test_client.get("/v1/prices/MSFT", headers=headers)

    assert prices.status_code == 200
    assert prices.json()["symbol"] == "MSFT"
    assert forecast.status_code == 501
    assert exhausted.status_code == 429


def test_keys_are_isolated_from_each_other(client: TestClient) -> None:
    assert client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"}).status_code == 501
    assert client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"}).status_code == 429
    # The other key still has its own budget.
    assert client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-bravo"}).status_code == 501


def test_unauthenticated_requests_burn_an_ip_bucket_before_auth(client: TestClient) -> None:
    # Middleware runs before auth: a keyless flood is metered, not free 401s.
    assert client.get("/v1/fundamentals/AAPL").status_code == 401
    assert client.get("/v1/fundamentals/AAPL").status_code == 429


def test_rotating_invalid_keys_cannot_evade_the_pre_auth_ip_bucket(client: TestClient) -> None:
    first = client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "bogus-one"})
    second = client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "bogus-two"})

    assert first.status_code == 401
    assert second.status_code == 429
    backend = client.app.state.rate_limiter.backend
    assert isinstance(backend, MemoryRateLimitBackend)
    assert len(backend.buckets()) == 1
    assert backend.buckets()[0].startswith("rl:ip:")


def test_similar_but_out_of_scope_prefix_is_not_metered(client: TestClient) -> None:
    response = client.get("/v10/not-a-versioned-route", headers={"X-API-Key": "k-alpha"})
    assert response.status_code == 404
    assert "RateLimit-Limit" not in response.headers


def test_health_readiness_and_metrics_are_never_limited(client: TestClient) -> None:
    client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})  # exhaust
    for _ in range(3):
        assert client.get("/healthz").status_code == 200
        assert client.get("/metrics").status_code == 200
    assert "RateLimit-Limit" not in client.get("/healthz").headers


def test_raw_api_key_never_appears_in_storage_buckets(client: TestClient) -> None:
    client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
    backend = client.app.state.rate_limiter.backend
    assert isinstance(backend, MemoryRateLimitBackend)
    buckets = backend.buckets()
    assert buckets  # the hit was recorded
    assert all("k-alpha" not in bucket for bucket in buckets)
    assert all(bucket.startswith("rl:key:") for bucket in buckets)


def test_storage_outage_fails_closed_by_default() -> None:
    class BrokenBackend:
        async def increment(self, bucket: str, ttl_seconds: int) -> int:
            raise ConnectionError("redis is down")

        async def aclose(self) -> None:
            return None

        async def check(self) -> None:
            raise ConnectionError("redis is down")

    app = _app()
    app.state.rate_limiter.backend = BrokenBackend()
    with TestClient(app) as test_client:
        response = test_client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "rate_limit_unavailable"

    open_app = _app(rate_limit_fail_open=True)
    open_app.state.rate_limiter.backend = BrokenBackend()
    with TestClient(open_app) as test_client:
        response = test_client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
    assert response.status_code == 501  # passed through to the handler


def test_disabled_limiter_neither_throttles_nor_stamps_headers() -> None:
    settings = Settings(app_env="test", rate_limit_enabled=False, api_keys=KEYS)
    with TestClient(create_app(settings)) as test_client:
        for _ in range(3):
            response = test_client.get("/v1/fundamentals/AAPL", headers={"X-API-Key": "k-alpha"})
            assert response.status_code == 501
            assert "RateLimit-Limit" not in response.headers


def test_window_expiry_restores_the_budget() -> None:
    app = _app()
    limiter: ApiRateLimiter = app.state.rate_limiter
    fake_now = [1_000_000.0]
    limiter.clock = lambda: fake_now[0]
    with TestClient(app) as test_client:
        headers = {"X-API-Key": "k-alpha"}
        assert test_client.get("/v1/fundamentals/AAPL", headers=headers).status_code == 501
        assert test_client.get("/v1/fundamentals/AAPL", headers=headers).status_code == 429
        fake_now[0] += 60.0  # next fixed window
        assert test_client.get("/v1/fundamentals/AAPL", headers=headers).status_code == 501


def test_parse_rate_accepts_supported_specs_and_rejects_the_rest() -> None:
    assert parse_rate("120/minute") == (120, 60)
    assert parse_rate("1/second") == (1, 1)
    assert parse_rate("10/hours") == (10, 3600)
    for bad in ("0/minute", "many/minute", "5", "5/fortnight", ""):
        with pytest.raises(ValueError):
            parse_rate(bad)


def test_build_rate_limiter_selects_backend_and_fails_fast_on_misconfig() -> None:
    memory = build_rate_limiter(Settings(app_env="test", rate_limit_storage_uri="memory://"))
    assert isinstance(memory.backend, MemoryRateLimitBackend)

    redis_limiter = build_rate_limiter(
        Settings(
            app_env="test",
            rate_limit_storage_uri="redis://localhost:6379/1",
            rate_limit_storage_timeout_seconds=1.25,
        )
    )
    assert isinstance(redis_limiter.backend, RedisRateLimitBackend)
    connection_kwargs = redis_limiter.backend._client.connection_pool.connection_kwargs
    assert connection_kwargs["socket_connect_timeout"] == 1.25
    assert connection_kwargs["socket_timeout"] == 1.25
    assert connection_kwargs["retry_on_timeout"] is False

    with pytest.raises(ValueError):
        build_rate_limiter(Settings(app_env="test", rate_limit_storage_uri="s3://nope"))
    with pytest.raises(ValueError):
        build_rate_limiter(Settings(app_env="test", rate_limit_default="unbounded"))
    with pytest.raises(ValueError, match="requires shared Redis"):
        build_rate_limiter(Settings(app_env="production"))


async def test_redis_backend_sets_expiry_only_on_first_hit() -> None:
    class StubRedis:
        def __init__(self) -> None:
            self.counts: dict[str, int] = {}
            self.expirations: list[tuple[str, int]] = []
            self.eval_calls: list[tuple[int, str, int]] = []

        async def eval(self, script: str, numkeys: int, key: str, ttl: str) -> int:
            assert "redis.call('INCR', KEYS[1])" in script
            assert "redis.call('EXPIRE', KEYS[1]" in script
            self.eval_calls.append((numkeys, key, int(ttl)))
            self.counts[key] = self.counts.get(key, 0) + 1
            if self.counts[key] == 1:
                self.expirations.append((key, int(ttl)))
            return self.counts[key]

        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

    stub = StubRedis()
    backend = RedisRateLimitBackend(stub)  # type: ignore[arg-type]
    assert await backend.increment("rl:key:x:60:1", ttl_seconds=120) == 1
    assert await backend.increment("rl:key:x:60:1", ttl_seconds=120) == 2
    assert stub.expirations == [("rl:key:x:60:1", 120)]
    assert stub.eval_calls == [
        (1, "rl:key:x:60:1", 120),
        (1, "rl:key:x:60:1", 120),
    ]
