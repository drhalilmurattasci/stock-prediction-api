"""Forecast contract tests.

These tests lock the public shape before Phase 3 implements execution.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.schemas.forecast import (
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    IntervalCalibration,
    LookaheadCheck,
)

GENERATED_AT = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)


def _valid_step() -> ForecastStep:
    return ForecastStep(
        step=1,
        target_time=GENERATED_AT + timedelta(days=1),
        point=215.0,
        quantiles=[
            ForecastQuantile(level=0.1, value=200.0),
            ForecastQuantile(level=0.5, value=215.0),
            ForecastQuantile(level=0.9, value=230.0),
        ],
        intervals=[
            ForecastInterval(
                coverage=0.8,
                lower_quantile=0.1,
                upper_quantile=0.9,
                lower=200.0,
                upper=230.0,
            )
        ],
    )


def _valid_provenance() -> ForecastProvenance:
    return ForecastProvenance(
        forecast_id=UUID("11111111-1111-1111-1111-111111111111"),
        snapshot_id="snap_2026_07_06",
        model_version="baseline-naive@2026-07-06",
        feature_set_hash="sha256:" + "a" * 64,
        max_available_at=GENERATED_AT,
        generated_at=GENERATED_AT,
        lookahead_check=LookaheadCheck(
            status="passed",
            checked_at=GENERATED_AT,
            max_feature_available_at=GENERATED_AT,
        ),
    )


def _valid_calibration() -> ForecastCalibration:
    return ForecastCalibration(
        calibration_set_version="cal_2026_07_06",
        method="adaptive_conformal",
        window_start=date(2025, 7, 1),
        window_end=date(2026, 7, 1),
        sample_count=250,
        by_interval=[
            IntervalCalibration(
                horizon=1,
                nominal_coverage=0.8,
                empirical_coverage=0.79,
                sample_count=250,
            )
        ],
    )


def _valid_response(**overrides) -> ForecastResponse:
    payload = {
        "symbol": "AAPL",
        "target": "adjusted_close",
        "horizon": 1,
        "horizon_unit": "trading_day",
        "as_of": GENERATED_AT,
        "forecasts": [_valid_step()],
        "provenance": _valid_provenance(),
        "calibration": _valid_calibration(),
    }
    payload.update(overrides)
    return ForecastResponse(**payload)


def test_forecast_request_normalizes_symbol_and_coverages():
    request = ForecastRequest(symbol="aapl", interval_coverages=[0.95, 0.5, 0.8])

    assert request.symbol == "AAPL"
    assert request.interval_coverages == [0.5, 0.8, 0.95]


def test_forecast_request_rejects_duplicate_coverages():
    with pytest.raises(ValidationError):
        ForecastRequest(symbol="AAPL", interval_coverages=[0.8, 0.8])


def test_forecast_contract_rejects_timezone_naive_instants():
    with pytest.raises(ValidationError):
        ForecastRequest(symbol="AAPL", as_of=datetime(2026, 7, 6, 12, 0))

    with pytest.raises(ValidationError):
        ForecastStep(step=1, target_time=datetime(2026, 7, 7, 12, 0), point=215.0)


def test_forecast_interval_requires_coverage_to_match_quantile_width():
    with pytest.raises(ValidationError):
        ForecastInterval(
            coverage=0.8,
            lower_quantile=0.2,
            upper_quantile=0.85,
            lower=200.0,
            upper=230.0,
        )


def test_forecast_response_currency_matches_target_semantics():
    response = _valid_response(target="return", currency=None)
    assert response.currency is None

    with pytest.raises(ValidationError):
        _valid_response(target="return")

    with pytest.raises(ValidationError):
        _valid_response(target="adjusted_close", currency=None)


def test_forecast_response_requires_horizon_to_match_payload():
    response = _valid_response(symbol="aapl")
    assert response.symbol == "AAPL"

    with pytest.raises(ValidationError):
        _valid_response(horizon=2)


def test_openapi_exposes_forecast_contract(client):
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()

    assert "/v1/forecast" in spec["paths"]
    get_schema = spec["paths"]["/v1/forecast/{symbol}"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    post_schema = spec["paths"]["/v1/forecast"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]

    assert get_schema["$ref"].endswith("/ForecastResponse")
    assert post_schema["$ref"].endswith("/ForecastRequest")

    response_schema = spec["components"]["schemas"]["ForecastResponse"]
    assert "provenance" in response_schema["properties"]
    assert "calibration" in response_schema["properties"]

    provenance_schema = spec["components"]["schemas"]["ForecastProvenance"]
    provenance_fields = provenance_schema["properties"]
    assert {"snapshot_id", "model_version", "feature_set_hash", "max_available_at"} <= set(
        provenance_fields
    )
    assert "lookahead_check" in provenance_fields

    calibration_schema = spec["components"]["schemas"]["ForecastCalibration"]
    assert "calibration_set_version" in calibration_schema["properties"]
