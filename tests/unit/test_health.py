"""P0 smoke tests: the app boots and liveness/metrics/docs respond."""

from __future__ import annotations

from fastapi import Request
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_healthz_ok(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "stock-prediction-api"


def test_metrics_exposed(client):
    client.get("/healthz")  # generate at least one sample
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "http_requests_total" in resp.text


def test_readyz_uses_injected_app_state_probes():
    async def ok_probe(_request: Request) -> None:
        return None

    app = create_app(
        Settings(app_env="test", rate_limit_enabled=False),
        readiness_probes=(("database", ok_probe), ("redis", ok_probe)),
    )
    with TestClient(app) as test_client:
        resp = test_client.get("/readyz")

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "checks": [
            {"name": "database", "ok": True, "detail": None},
            {"name": "redis", "ok": True, "detail": None},
        ],
    }


def test_readyz_reports_degraded_probe():
    async def ok_probe(_request: Request) -> None:
        return None

    async def failing_probe(_request: Request) -> None:
        raise RuntimeError("redis unavailable")

    app = create_app(
        Settings(app_env="test", rate_limit_enabled=False),
        readiness_probes=(("database", ok_probe), ("redis", failing_probe)),
    )
    with TestClient(app) as test_client:
        resp = test_client.get("/readyz")

    assert resp.status_code == 503
    assert resp.json()["status"] == "degraded"
    assert resp.json()["checks"][1] == {
        "name": "redis",
        "ok": False,
        "detail": "redis unavailable",
    }


def test_test_factory_disables_default_rate_limit(client):
    for _ in range(130):
        resp = client.get("/healthz")
    assert resp.status_code == 200


def test_metrics_use_route_templates_not_raw_paths(client):
    client.get("/v1/forecast/AAPL")
    client.get("/totally-random-scanner-path")
    resp = client.get("/metrics")

    assert 'path="/v1/forecast/{symbol}"' in resp.text
    assert 'path="/totally-random-scanner-path"' not in resp.text
    assert 'path="<unmatched>"' in resp.text


def test_openapi_lists_surface(client):
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/healthz" in paths
    assert "/v1/prices/{symbol}" in paths
    assert "/v1/forecast" in paths
    assert "/v1/forecast/{symbol}" in paths


def test_placeholder_returns_501_envelope(client):
    resp = client.get("/v1/forecast/AAPL")
    assert resp.status_code == 501
    assert resp.json()["error"]["code"] == "not_implemented"


def test_get_forecast_validates_against_request_contract(client):
    resp = client.get("/v1/forecast/aapl?coverage=0.8&coverage=0.8")
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"

    resp = client.get("/v1/forecast/AAPL?model=experimental")
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"
