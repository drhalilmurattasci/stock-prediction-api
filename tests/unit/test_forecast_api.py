"""Forecast route delegation, normalization, idempotency, and fail-closed tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.schemas.forecast import (
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    LookaheadCheck,
)
from app.services.forecasting import get_forecast_service

AS_OF = datetime(2026, 7, 10, 21, tzinfo=UTC)


class FakeForecastService:
    def __init__(self) -> None:
        self.calls: list[tuple[ForecastRequest, str | None, str | None]] = []

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse:
        self.calls.append((request, idempotency_key, principal))
        return _response_for(request)


class ExplodingForecastService(FakeForecastService):
    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse:
        raise AssertionError("forecast service must not run before API-key auth")


def _response_for(request: ForecastRequest) -> ForecastResponse:
    as_of = request.as_of or AS_OF
    steps = [
        ForecastStep(
            step=index + 1,
            target_time=as_of + timedelta(days=index + 1),
            point=101.0 + index,
            quantiles=[
                ForecastQuantile(level=0.1, value=90.0 + index),
                ForecastQuantile(level=0.5, value=101.0 + index),
                ForecastQuantile(level=0.9, value=110.0 + index),
            ],
            intervals=[
                ForecastInterval(
                    coverage=0.8,
                    lower_quantile=0.1,
                    upper_quantile=0.9,
                    lower=90.0 + index,
                    upper=110.0 + index,
                )
            ],
        )
        for index in range(request.horizon)
    ]
    provenance = ForecastProvenance(
        forecast_id=UUID("33333333-3333-3333-3333-333333333333"),
        snapshot_id=request.snapshot_id or "fixture:snapshot:route-v1",
        model_version="baseline-naive-fixture@1",
        series_basis=("split_dividend_adjusted" if request.target == "adjusted_close" else "raw"),
        feature_set_hash="sha256:" + "c" * 64,
        max_available_at=as_of,
        generated_at=as_of + timedelta(minutes=1),
        lookahead_check=LookaheadCheck(
            status="passed",
            checked_at=as_of + timedelta(minutes=1),
            max_feature_available_at=as_of,
        ),
    )
    return ForecastResponse(
        symbol=request.symbol,
        target=request.target,
        horizon=request.horizon,
        horizon_unit=request.horizon_unit,
        as_of=as_of,
        currency=None if request.target in {"return", "log_return"} else "USD",
        forecasts=steps,
        provenance=provenance,
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:fixture",
            method="none",
            sample_count=0,
        ),
    )


def test_get_forecast_delegates_normalized_request() -> None:
    service = FakeForecastService()
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    app.dependency_overrides[get_forecast_service] = lambda: service

    with TestClient(app) as client:
        response = client.get(
            "/v1/forecast/aapl",
            params={
                "horizon": 1,
                "target": "close",
                "model": "baseline_naive",
                "as_of": AS_OF.isoformat(),
                "snapshot_id": "fixture:snapshot:route-v1",
                "coverage": 0.8,
            },
        )

    assert response.status_code == 200
    request, idempotency_key, principal = service.calls[0]
    assert request.symbol == "AAPL"
    assert request.interval_coverages == [0.8]
    assert idempotency_key is None
    assert principal is None
    assert response.json()["provenance"]["model_version"] == "baseline-naive-fixture@1"


def test_post_forecast_forwards_idempotency_key_and_return_currency() -> None:
    service = FakeForecastService()
    app = create_app(
        Settings(
            app_env="test",
            rate_limit_enabled=False,
            api_keys="fixture-api-key",
        )
    )
    app.dependency_overrides[get_forecast_service] = lambda: service
    payload = {
        "symbol": "aapl",
        "horizon": 1,
        "target": "return",
        "as_of": AS_OF.isoformat(),
        "snapshot_id": "fixture:snapshot:route-v1",
        "model": "baseline_naive",
        "interval_coverages": [0.8],
    }

    with TestClient(app) as client:
        response = client.post(
            "/v1/forecast",
            json=payload,
            headers={
                "Idempotency-Key": "fixture-idempotency-key",
                "X-API-Key": "fixture-api-key",
            },
        )

    assert response.status_code == 200
    request, idempotency_key, principal = service.calls[0]
    assert request.symbol == "AAPL"
    assert idempotency_key == "fixture-idempotency-key"
    assert principal == "fixture-api-key"
    assert response.json()["currency"] is None


def test_default_forecast_service_stays_fail_closed() -> None:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    with TestClient(app) as client:
        response = client.get(
            "/v1/forecast/AAPL",
            params={"target": "close", "model": "baseline_naive"},
        )

    assert response.status_code == 501
    body = response.json()["error"]
    assert body["code"] == "not_implemented"
    assert "not enabled" in body["message"]
    assert "pin the code-derived resolution-policy hash" in body["details"]["blockers"]


def test_api_key_auth_short_circuits_forecast_service() -> None:
    service = ExplodingForecastService()
    app = create_app(
        Settings(app_env="test", rate_limit_enabled=False, api_keys="fixture-good-key")
    )
    app.dependency_overrides[get_forecast_service] = lambda: service

    with TestClient(app) as client:
        response = client.get("/v1/forecast/AAPL")

    assert response.status_code == 401
    assert service.calls == []


def test_forecast_validation_and_openapi_use_the_project_error_envelope() -> None:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    with TestClient(app) as client:
        invalid = client.get("/v1/forecast/AAPL", params={"horizon": 0})
        schema = client.get("/openapi.json").json()

    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "validation_error"
    for operation in (
        schema["paths"]["/v1/forecast/{symbol}"]["get"],
        schema["paths"]["/v1/forecast"]["post"],
    ):
        validation_schema = operation["responses"]["422"]["content"]["application/json"]["schema"]
        assert validation_schema["$ref"].endswith("/ErrorResponse")

    get_parameters = schema["paths"]["/v1/forecast/{symbol}"]["get"]["parameters"]
    descriptions = {item["name"]: item["description"] for item in get_parameters}
    assert "baseline_naive" in descriptions["model"]
    post_parameters = schema["paths"]["/v1/forecast"]["post"]["parameters"]
    idempotency = next(item for item in post_parameters if item["name"] == "Idempotency-Key")
    assert "schema-validated stored forecast" in idempotency["description"]
    assert "credential" in idempotency["description"]
