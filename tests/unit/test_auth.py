"""API-key auth enforcement on /v1 (P0 exit-criterion: auth actually gates).

require_api_key is attached to the /v1 aggregate router. When keys are
configured it must 401 unkeyed/wrong-keyed requests and pass correct ones
through to the handler; liveness/metrics must stay open; and with no keys
configured (dev) anonymous access is still allowed.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.db.session import get_session
from app.main import create_app

PRODUCT_ENDPOINT = "/v1/fundamentals/AAPL"


@pytest.fixture
def keyed_client() -> Iterator[TestClient]:
    async def ok_probe(_request: Request) -> None:
        return None

    app = create_app(
        Settings(app_env="test", rate_limit_enabled=False, api_keys="k-good,k-also"),
        readiness_probes=(("database", ok_probe), ("redis", ok_probe)),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_missing_key_is_401_with_error_envelope(keyed_client: TestClient) -> None:
    response = keyed_client.get(PRODUCT_ENDPOINT)
    assert response.status_code == 401
    body = response.json()
    assert "error" in body and body["error"]["code"]
    assert body["error"]["request_id"]  # correlation id still populated on auth failure
    assert response.headers["WWW-Authenticate"] == "X-API-Key"


def test_wrong_key_is_401(keyed_client: TestClient) -> None:
    response = keyed_client.get(PRODUCT_ENDPOINT, headers={"X-API-Key": "not-a-real-key"})
    assert response.status_code == 401


def test_prices_rejects_missing_key_before_resolving_database_session() -> None:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False, api_keys="k-good"))
    session_requested = False

    async def unexpected_session() -> AsyncSession:
        nonlocal session_requested
        session_requested = True
        raise AssertionError("database dependency must not run before API-key auth")

    app.dependency_overrides[get_session] = unexpected_session
    with TestClient(app) as test_client:
        response = test_client.get("/v1/prices/AAPL")

    assert response.status_code == 401
    assert session_requested is False


def test_correct_key_passes_auth_and_reaches_handler(keyed_client: TestClient) -> None:
    # Auth passes -> the request reaches the (still-stub) handler, so 501 not 401.
    response = keyed_client.get(PRODUCT_ENDPOINT, headers={"X-API-Key": "k-good"})
    assert response.status_code == 501


def test_health_and_metrics_never_require_a_key(keyed_client: TestClient) -> None:
    assert keyed_client.get("/healthz").status_code == 200
    assert keyed_client.get("/readyz").status_code == 200
    assert keyed_client.get("/metrics").status_code == 200


def test_anonymous_allowed_when_no_keys_configured() -> None:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))  # no api_keys
    with TestClient(app) as test_client:
        # No key + no keys configured -> anonymous allowed -> reaches stub (501), not 401.
        assert test_client.get(PRODUCT_ENDPOINT).status_code == 501


@pytest.mark.parametrize("app_env", ["staging", "production"])
def test_deployed_environment_without_keys_fails_closed(app_env: str) -> None:
    app = create_app(Settings(app_env=app_env, rate_limit_enabled=False))
    with TestClient(app) as test_client:
        response = test_client.get(PRODUCT_ENDPOINT)

    assert response.status_code == 401
    assert response.json()["error"]["message"] == "API key authentication is not configured."
    assert response.headers["WWW-Authenticate"] == "X-API-Key"
