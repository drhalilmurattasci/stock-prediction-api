"""App factory and runtime wiring guardrails."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from fastapi import Request
from fastapi.testclient import TestClient

from app.config import Settings
from app.db.session import build_engine
from app.main import create_app


def test_settings_accepts_legacy_redis_url_alias():
    settings = Settings(REDIS_URL="redis://legacy-cache:6379/4")

    assert settings.redis_cache_url == "redis://legacy-cache:6379/4"
    assert settings.redis_url == "redis://legacy-cache:6379/4"


def test_lifespan_creates_app_owned_resources():
    async def ok_probe(_request: Request) -> None:
        return None

    settings = Settings(app_env="test", rate_limit_enabled=False)
    app = create_app(settings, readiness_probes=(("database", ok_probe),))

    with TestClient(app) as test_client:
        assert app.state.settings is settings
        assert app.state.limiter.enabled is False
        assert app.state.engine is not None
        assert app.state.sessionmaker is not None
        assert app.state.redis_cache is not None
        assert test_client.get("/readyz").status_code == 200


def test_database_engine_uses_explicit_pool_settings():
    settings = Settings(
        database_pool_size=3,
        database_max_overflow=4,
        database_pool_timeout=7,
    )
    engine = build_engine(settings)
    pool = cast(Any, engine.sync_engine.pool)

    try:
        assert pool.size() == 3
        assert pool._max_overflow == 4
        assert pool._timeout == 7
    finally:
        asyncio.run(engine.dispose())


def test_rate_limit_disabled_in_test_factory(client: Any):
    assert client.app.state.limiter.enabled is False
