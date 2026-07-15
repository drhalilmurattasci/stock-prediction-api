"""Canonical-identity contract for fitted conformal calibration sets."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date

import pytest

from app.services.forecast_calibration_sets import (
    INTERVAL_POLICY_VERSION,
    WINDOW_DATE_POLICY_VERSION,
    FittedCalibrationBucket,
    FittedCalibrationSet,
    ForecastCalibrationSetValidationError,
    calibration_set_version_for,
    canonical_calibration_set,
    parse_calibration_set,
)
from ml.calibration.conformal import (
    ABSOLUTE_RESIDUAL_POLICY_VERSION,
    CQR_POLICY_VERSION,
    FINITE_SAMPLE_POLICY_VERSION,
    fit_absolute_residual,
    fit_cqr,
)

_H = "sha256:" + "0" * 64
_H2 = "sha256:" + "1" * 64
_H3 = "sha256:" + "2" * 64
_H4 = "sha256:" + "3" * 64
_H5 = "sha256:" + "4" * 64
_H6 = "sha256:" + "5" * 64
_H7 = "sha256:" + "6" * 64


def _abs_bucket(horizon: int, coverage: float, *, n: int = 20) -> FittedCalibrationBucket:
    actual = [float(i) for i in range(n)]
    point = [value + 0.5 for value in actual]  # constant residual -> radius 0.5
    return FittedCalibrationBucket(
        horizon=horizon,
        calibration=fit_absolute_residual(actual, point, coverage=coverage),
    )


def _cqr_bucket(horizon: int, coverage: float, *, n: int = 20) -> FittedCalibrationBucket:
    actual = [float(i) for i in range(n)]
    lower = [value - 1.0 for value in actual]
    upper = [value + 1.0 for value in actual]
    return FittedCalibrationBucket(
        horizon=horizon,
        calibration=fit_cqr(actual, lower, upper, coverage=coverage),
    )


def _set(**overrides: object) -> FittedCalibrationSet:
    base: dict[str, object] = {
        "model_version": "baseline-naive@1",
        "symbol": "MSFT",
        "target": "close",
        "series_basis": "raw",
        "horizon_unit": "trading_day",
        "currency": "USD",
        "source_calibration_set_version": "uncalibrated:baseline-naive@1",
        "source_calibration_method": "none",
        "forecast_resolution_policy_hash": _H5,
        "forecast_availability_rule_set_hash": _H6,
        "fit_evidence_digest": _H7,
        "method": "empirical_residual",
        "window_start": date(2026, 1, 5),
        "window_end": date(2026, 3, 10),
        "sample_count": 20,
        "cohort_id": _H,
        "selection_policy_hash": _H2,
        "outcome_resolution_policy_hash": _H3,
        "outcome_availability_rule_set_hash": _H4,
        "buckets": (_abs_bucket(1, 0.8), _abs_bucket(1, 0.5), _abs_bucket(5, 0.8)),
    }
    base.update(overrides)
    return FittedCalibrationSet(**base)  # type: ignore[arg-type]


def _mutate(canonical: bytes, mutate: Callable[[dict[str, object]], None]) -> bytes:
    document = json.loads(canonical)
    mutate(document)
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def test_round_trips_and_is_deterministic() -> None:
    calibration_set = _set()
    canonical = canonical_calibration_set(calibration_set)
    assert canonical_calibration_set(calibration_set) == canonical  # deterministic
    parsed = parse_calibration_set(canonical)
    assert canonical_calibration_set(parsed) == canonical  # round-trip stable
    assert parsed.method == "empirical_residual"
    assert parsed.symbol == "MSFT"
    assert parsed.target == "close"
    assert parsed.series_basis == "raw"
    assert parsed.horizon_unit == "trading_day"
    assert parsed.currency == "USD"
    assert parsed.interval_policy_version == INTERVAL_POLICY_VERSION
    assert parsed.window_date_policy_version == WINDOW_DATE_POLICY_VERSION
    assert len(parsed.buckets) == 3


def test_identity_binds_general_series_semantics_and_bucket_policies() -> None:
    calibration_set = _set(
        symbol="BRK.B",
        target="adjusted_close",
        series_basis="split_dividend_adjusted",
        horizon_unit="trading_session",
        currency="EUR",
    )
    canonical = canonical_calibration_set(calibration_set)
    document = json.loads(canonical)
    assert document["symbol"] == "BRK.B"
    assert document["target"] == "adjusted_close"
    assert document["series_basis"] == "split_dividend_adjusted"
    assert document["horizon_unit"] == "trading_session"
    assert document["currency"] == "EUR"
    assert document["forecast_resolution_policy_hash"] == _H5
    assert document["forecast_availability_rule_set_hash"] == _H6
    assert document["fit_evidence_digest"] == _H7
    assert document["outcome_availability_rule_set_hash"] == _H4
    assert document["interval_policy_version"] == INTERVAL_POLICY_VERSION
    assert document["window_date_policy_version"] == WINDOW_DATE_POLICY_VERSION
    assert document["buckets"][0]["quantile_selection_policy_version"] == (
        FINITE_SAMPLE_POLICY_VERSION
    )
    assert document["buckets"][0]["correction_policy_version"] == (ABSOLUTE_RESIDUAL_POLICY_VERSION)
    parsed = parse_calibration_set(canonical)
    assert parsed.target == "adjusted_close"
    assert parsed.series_basis == "split_dividend_adjusted"
    assert parsed.source_calibration_method == "none"
    assert parsed.source_calibration_set_version == "uncalibrated:baseline-naive@1"


def test_bucket_order_does_not_change_identity() -> None:
    forward = _set(buckets=(_abs_bucket(1, 0.5), _abs_bucket(1, 0.8), _abs_bucket(5, 0.8)))
    shuffled = _set(buckets=(_abs_bucket(5, 0.8), _abs_bucket(1, 0.8), _abs_bucket(1, 0.5)))
    assert canonical_calibration_set(forward) == canonical_calibration_set(shuffled)
    assert calibration_set_version_for(forward) == calibration_set_version_for(shuffled)


def test_version_is_sha256_of_canonical_bytes_and_accepts_bytes() -> None:
    import hashlib

    calibration_set = _set()
    canonical = canonical_calibration_set(calibration_set)
    expected = f"sha256:{hashlib.sha256(canonical).hexdigest()}"
    assert calibration_set_version_for(calibration_set) == expected
    assert calibration_set_version_for(canonical) == expected


def test_cqr_method_round_trips() -> None:
    calibration_set = _set(
        method="conformal_quantile_regression",
        buckets=(_cqr_bucket(1, 0.8), _cqr_bucket(3, 0.9)),
    )
    canonical = canonical_calibration_set(calibration_set)
    document = json.loads(canonical)
    assert document["buckets"][0]["correction_policy_version"] == CQR_POLICY_VERSION
    assert canonical_calibration_set(parse_calibration_set(canonical)) == canonical


def test_method_type_mismatch_is_rejected() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(
            _set(method="conformal_quantile_regression", buckets=(_abs_bucket(1, 0.8),))
        )


def test_duplicate_bucket_is_rejected() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(buckets=(_abs_bucket(1, 0.8), _abs_bucket(1, 0.8))))


def test_empty_buckets_rejected() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(buckets=()))


@pytest.mark.parametrize("horizon", [0, 253])
def test_out_of_range_horizon_rejected(horizon: int) -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(buckets=(_abs_bucket(horizon, 0.8),)))


def test_window_end_before_start_rejected() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(window_start=date(2026, 3, 10), window_end=date(2026, 1, 5)))


def test_bucket_fit_count_cannot_exceed_set_sample_count() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        # bucket fit_sample_count is 20 (n=20) but the set claims only 5 total
        canonical_calibration_set(_set(sample_count=5))


def test_bad_hash_rejected() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(cohort_id="not-a-hash"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", "msft"),
        ("symbol", "MS FT"),
        ("symbol", "A" * 33),
        ("target", ""),
        ("target", "x" * 33),
        ("series_basis", " raw"),
        ("series_basis", "x" * 33),
        ("horizon_unit", ""),
        ("horizon_unit", "x" * 33),
        ("currency", ""),
        ("currency", "x" * 17),
    ],
)
def test_invalid_or_unbounded_series_semantics_are_rejected(field: str, value: str) -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(**{field: value}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_calibration_method", "empirical_residual"),
        ("source_calibration_set_version", "sha256:" + "9" * 64),
    ],
)
def test_source_calibration_identity_must_be_v1_uncalibrated(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ForecastCalibrationSetValidationError, match="uncalibrated"):
        canonical_calibration_set(_set(**{field: value}))


@pytest.mark.parametrize("policy_location", ["quantile", "correction"])
def test_policy_versions_are_revalidated_before_persistence(policy_location: str) -> None:
    bucket = _abs_bucket(1, 0.8)
    if policy_location == "quantile":
        object.__setattr__(bucket.calibration.selection, "policy_version", "tampered-v1")
    else:
        object.__setattr__(bucket.calibration, "policy_version", "tampered-v1")
    with pytest.raises(ForecastCalibrationSetValidationError):
        canonical_calibration_set(_set(buckets=(bucket,)))


def test_parse_rejects_unknown_key() -> None:
    canonical = canonical_calibration_set(_set())
    tampered = _mutate(canonical, lambda doc: doc.__setitem__("surprise", 1))
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(tampered)


def test_parse_rejects_missing_key() -> None:
    canonical = canonical_calibration_set(_set())

    def remove_cohort(doc: dict[str, object]) -> None:
        doc.pop("cohort_id")

    tampered = _mutate(canonical, remove_cohort)
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(tampered)


def test_parse_rejects_noncanonical_bytes() -> None:
    canonical = canonical_calibration_set(_set())
    pretty = json.dumps(json.loads(canonical), indent=2).encode()
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(pretty)


def test_parse_rejects_tampered_rank() -> None:
    canonical = canonical_calibration_set(_set())

    def bump_rank(doc: dict[str, object]) -> None:
        buckets = doc["buckets"]
        assert isinstance(buckets, list)
        bucket = buckets[0]
        assert isinstance(bucket, dict)
        bucket["rank"] = int(bucket["rank"]) + 1

    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(_mutate(canonical, bump_rank))


def test_parse_rejects_tampered_interval_policy_version() -> None:
    canonical = canonical_calibration_set(_set())
    tampered = _mutate(
        canonical,
        lambda doc: doc.__setitem__("interval_policy_version", "one-sided-v1"),
    )
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(tampered)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("quantile_selection_policy_version", "interpolated-v1"),
        ("correction_policy_version", "unsigned-residual-v1"),
    ],
)
def test_parse_rejects_tampered_bucket_policy_version(key: str, value: str) -> None:
    canonical = canonical_calibration_set(_set())

    def tamper_policy(doc: dict[str, object]) -> None:
        buckets = doc["buckets"]
        assert isinstance(buckets, list)
        bucket = buckets[0]
        assert isinstance(bucket, dict)
        bucket[key] = value

    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(_mutate(canonical, tamper_policy))


def test_parse_rejects_duplicate_json_key() -> None:
    raw = b'{"format":"forecast-calibration-set-v2","format":"forecast-calibration-set-v2"}'
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(raw)


def test_parse_rejects_json_constants() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(b'{"value": NaN}')


def test_parse_rejects_empty_and_non_bytes() -> None:
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(b"")
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set("not-bytes")  # type: ignore[arg-type]
