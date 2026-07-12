"""App factory and runtime wiring guardrails."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from fastapi import Request
from fastapi.testclient import TestClient

from app.config import Settings
from app.db.session import build_engine, statement_timeout_connect_args
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


def test_statement_timeout_is_a_server_side_budget_and_opt_in():
    # Positive budget -> asyncpg server_settings enforcing statement_timeout.
    assert statement_timeout_connect_args(5_000) == {
        "server_settings": {"statement_timeout": "5000"}
    }
    # Disabled (0 or None) -> no connect args, so ingestion/migration engines
    # that omit the parameter are never capped by the API read budget.
    assert statement_timeout_connect_args(0) == {}
    assert statement_timeout_connect_args(None) == {}


def test_engines_accept_optional_statement_timeout():
    settings = Settings(app_env="test")
    capped = build_engine(settings, statement_timeout_ms=settings.api_statement_timeout_ms)
    uncapped = build_engine(settings)  # ingestion-style construction

    try:
        assert settings.api_statement_timeout_ms == 5_000
    finally:
        asyncio.run(capped.dispose())
        asyncio.run(uncapped.dispose())
