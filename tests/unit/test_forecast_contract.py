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
        series_basis="split_dividend_adjusted",
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
    request = ForecastRequest(
        symbol="aapl",
        model="baseline_seasonal_naive",
        interval_coverages=[0.95, 0.5, 0.8],
    )

    assert request.symbol == "AAPL"
    assert request.model == "baseline_seasonal_naive"
    assert request.interval_coverages == [0.5, 0.8, 0.95]


def test_forecast_request_rejects_duplicate_coverages():
    with pytest.raises(ValidationError):
        ForecastRequest(symbol="AAPL", interval_coverages=[0.8, 0.8])


@pytest.mark.parametrize("coverage", [1e-13, 0.9999999999999, 0.8001])
def test_forecast_request_rejects_uncanonical_coverage_precision(coverage: float):
    with pytest.raises(ValidationError):
        ForecastRequest(symbol="AAPL", interval_coverages=[coverage])


def test_forecast_request_canonicalizes_float_noise_at_supported_precision():
    request = ForecastRequest(symbol="AAPL", interval_coverages=[0.8000000000001])

    assert request.interval_coverages == [0.8]

    for near_duplicates in (
        [0.3, 0.30000000000000004],
        [0.8, 0.8000000000001],
        [0.7999999999999999, 0.8],
    ):
        with pytest.raises(ValidationError, match="must not contain duplicates"):
            ForecastRequest(symbol="AAPL", interval_coverages=near_duplicates)


def test_forecast_contract_rejects_timezone_naive_instants():
    with pytest.raises(ValidationError):
        ForecastRequest(symbol="AAPL", as_of=datetime(2026, 7, 6, 12, 0))

    step_payload = _valid_step().model_dump()
    step_payload["target_time"] = datetime(2026, 7, 7, 12, 0)
    with pytest.raises(ValidationError):
        ForecastStep.model_validate(step_payload)


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

    raw_provenance = _valid_provenance().model_copy(update={"series_basis": "raw"})
    assert _valid_response(target="close", provenance=raw_provenance).target == "close"
    with pytest.raises(ValidationError, match="adjusted series provenance"):
        _valid_response(provenance=raw_provenance)


def test_forecast_response_requires_horizon_to_match_payload():
    response = _valid_response(symbol="aapl")
    assert response.symbol == "AAPL"

    with pytest.raises(ValidationError):
        _valid_response(horizon=2)


def test_forecast_step_rejects_crossing_or_unreferenced_quantiles():
    payload = _valid_step().model_dump()
    payload["quantiles"][0]["value"] = 220.0
    with pytest.raises(ValidationError, match="nondecreasing"):
        ForecastStep.model_validate(payload)

    payload = _valid_step().model_dump()
    payload["intervals"][0]["lower"] = 199.0
    with pytest.raises(ValidationError, match="referenced quantile"):
        ForecastStep.model_validate(payload)


def test_forecast_step_requires_point_to_equal_median():
    payload = _valid_step().model_dump()
    payload["point"] = 216.0
    with pytest.raises(ValidationError, match="0.5 quantile"):
        ForecastStep.model_validate(payload)


def test_forecast_response_requires_ordered_future_steps_and_pit_cutoff():
    first = _valid_step()
    second = first.model_copy(
        update={"step": 3, "target_time": first.target_time + timedelta(days=1)}
    )
    with pytest.raises(ValidationError, match="contiguous"):
        _valid_response(horizon=2, forecasts=[first, second])

    past_step = first.model_copy(update={"target_time": GENERATED_AT})
    with pytest.raises(ValidationError, match="later than as_of"):
        _valid_response(forecasts=[past_step])

    future_provenance = _valid_provenance().model_copy(
        update={"max_available_at": GENERATED_AT + timedelta(seconds=1)}
    )
    with pytest.raises(ValidationError, match="max_available_at"):
        _valid_response(provenance=future_provenance)


def test_empirical_probability_fields_accept_observed_zero_and_one():
    row = IntervalCalibration(
        horizon=1,
        nominal_coverage=0.8,
        empirical_coverage=0.0,
        sample_count=1,
        confidence_low=0.0,
        confidence_high=1.0,
    )

    assert row.empirical_coverage == 0.0
    assert row.confidence_high == 1.0

    with pytest.raises(ValidationError, match="require empirical_coverage"):
        IntervalCalibration(
            horizon=1,
            nominal_coverage=0.8,
            empirical_coverage=None,
            sample_count=1,
        )

    with pytest.raises(ValidationError, match="supplied together"):
        IntervalCalibration(
            horizon=1,
            nominal_coverage=0.8,
            empirical_coverage=0.8,
            sample_count=10,
            confidence_low=0.7,
        )

    with pytest.raises(ValidationError, match="contain empirical_coverage"):
        IntervalCalibration(
            horizon=1,
            nominal_coverage=0.8,
            empirical_coverage=0.8,
            sample_count=10,
            confidence_low=0.5,
            confidence_high=0.7,
        )


def test_uncalibrated_metadata_cannot_claim_evidence():
    with pytest.raises(ValidationError, match="cannot claim calibration evidence"):
        ForecastCalibration(
            calibration_set_version="uncalibrated:fixture",
            method="none",
            sample_count=1,
        )

    with pytest.raises(ValidationError, match="coherent nonzero evidence"):
        ForecastCalibration(
            calibration_set_version="empty:adaptive",
            method="adaptive_conformal",
            sample_count=0,
        )


def test_calibration_evidence_buckets_are_unique_and_match_response() -> None:
    row = IntervalCalibration(
        horizon=1,
        nominal_coverage=0.8,
        empirical_coverage=0.79,
        sample_count=20,
    )
    with pytest.raises(ValidationError, match="buckets must be unique"):
        ForecastCalibration(
            calibration_set_version="duplicate:fixture",
            method="empirical_residual",
            window_start=date(2026, 1, 1),
            window_end=date(2026, 6, 1),
            sample_count=20,
            by_interval=[row, row],
        )

    wrong_horizon = row.model_copy(update={"horizon": 2})
    mismatched = ForecastCalibration(
        calibration_set_version="mismatch:fixture",
        method="empirical_residual",
        window_start=date(2026, 1, 1),
        window_end=date(2026, 6, 1),
        sample_count=20,
        by_interval=[wrong_horizon],
    )
    with pytest.raises(ValidationError, match="match every emitted interval"):
        _valid_response(calibration=mismatched)


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
    assert {
        "snapshot_id",
        "model_version",
        "series_basis",
        "feature_set_hash",
        "max_available_at",
    } <= set(provenance_fields)
    assert "lookahead_check" in provenance_fields

    calibration_schema = spec["components"]["schemas"]["ForecastCalibration"]
    assert "calibration_set_version" in calibration_schema["properties"]
