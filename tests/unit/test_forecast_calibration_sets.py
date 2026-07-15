"""Canonical-identity contract for fitted conformal calibration sets."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date

import pytest

from app.services.forecast_calibration_sets import (
    FittedCalibrationBucket,
    FittedCalibrationSet,
    ForecastCalibrationSetValidationError,
    calibration_set_version_for,
    canonical_calibration_set,
    parse_calibration_set,
)
from ml.calibration.conformal import fit_absolute_residual, fit_cqr

_H = "sha256:" + "0" * 64
_H2 = "sha256:" + "1" * 64
_H3 = "sha256:" + "2" * 64
_H4 = "sha256:" + "3" * 64


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
        "method": "empirical_residual",
        "window_start": date(2026, 1, 5),
        "window_end": date(2026, 3, 10),
        "sample_count": 20,
        "cohort_id": _H,
        "selection_policy_hash": _H2,
        "outcome_resolution_policy_hash": _H3,
        "availability_rule_set_hash": _H4,
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
    assert len(parsed.buckets) == 3


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


def test_parse_rejects_unknown_key() -> None:
    canonical = canonical_calibration_set(_set())
    tampered = _mutate(canonical, lambda doc: doc.__setitem__("surprise", 1))
    with pytest.raises(ForecastCalibrationSetValidationError):
        parse_calibration_set(tampered)


def test_parse_rejects_missing_key() -> None:
    canonical = canonical_calibration_set(_set())
    tampered = _mutate(canonical, lambda doc: doc.pop("cohort_id"))
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


def test_parse_rejects_duplicate_json_key() -> None:
    raw = b'{"format":"forecast-calibration-set-v1","format":"forecast-calibration-set-v1"}'
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
