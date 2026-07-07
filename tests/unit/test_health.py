"""P0 smoke tests: the app boots and liveness/metrics/docs respond."""

from __future__ import annotations


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
