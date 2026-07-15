"""Canonical contract for descriptive-only held-out coverage releases."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
from collections.abc import Callable
from dataclasses import replace
from datetime import date

import pytest

from app.services.forecast_calibration_evidence import (
    WILSON_COVERAGE_POLICY_VERSION,
    HeldoutCoverageBucket,
    HeldoutCoverageEvidence,
    estimate_heldout_coverage,
    wilson_interval,
)
from app.services.forecast_calibration_releases import (
    HELDOUT_COVERAGE_RELEASE_FORMAT,
    HELDOUT_COVERAGE_RELEASE_SCOPE,
    ForecastCalibrationReleaseValidationError,
    build_heldout_coverage_release,
    canonical_heldout_coverage_release,
    heldout_coverage_release_id_for,
    parse_heldout_coverage_release,
)
from app.services.forecast_calibration_sets import (
    FittedCalibrationBucket,
    FittedCalibrationSet,
    calibration_set_version_for,
)
from ml.calibration.conformal import fit_absolute_residual


def _hash(number: int) -> str:
    return f"sha256:{number:064x}"


def _bucket(horizon: int, coverage: float) -> FittedCalibrationBucket:
    actual = [float(index) for index in range(20)]
    point = [value + 0.5 for value in actual]
    return FittedCalibrationBucket(
        horizon=horizon,
        calibration=fit_absolute_residual(actual, point, coverage=coverage),
    )


def _fitted_set(**overrides: object) -> FittedCalibrationSet:
    values: dict[str, object] = {
        "model_version": "baseline-naive@1",
        "symbol": "MSFT",
        "target": "close",
        "series_basis": "raw",
        "horizon_unit": "trading_day",
        "currency": "USD",
        "source_calibration_set_version": "uncalibrated:baseline-naive@1",
        "source_calibration_method": "none",
        "forecast_resolution_policy_hash": _hash(5),
        "forecast_availability_rule_set_hash": _hash(6),
        "fit_evidence_digest": _hash(7),
        "method": "empirical_residual",
        "window_start": date(2026, 1, 5),
        "window_end": date(2026, 3, 10),
        "sample_count": 40,
        "cohort_id": _hash(1),
        "selection_policy_hash": _hash(2),
        "outcome_resolution_policy_hash": _hash(3),
        "outcome_availability_rule_set_hash": _hash(4),
        "buckets": (_bucket(1, 0.8), _bucket(1, 0.5), _bucket(5, 0.8)),
    }
    values.update(overrides)
    return FittedCalibrationSet(**values)  # type: ignore[arg-type]


def _coverage_bucket(
    horizon: int,
    coverage: float,
    *,
    covered_count: int,
    sample_count: int = 20,
    confidence_level: float = 0.95,
) -> HeldoutCoverageBucket:
    low, high = wilson_interval(
        covered_count,
        sample_count,
        confidence_level=confidence_level,
    )
    return HeldoutCoverageBucket(
        horizon=horizon,
        nominal_coverage=coverage,
        covered_count=covered_count,
        sample_count=sample_count,
        empirical_coverage=covered_count / sample_count,
        confidence_low=low,
        confidence_high=high,
    )


def _evidence(
    fitted_set: FittedCalibrationSet,
    **overrides: object,
) -> HeldoutCoverageEvidence:
    values: dict[str, object] = {
        "fitted_calibration_set_version": calibration_set_version_for(fitted_set),
        "method": fitted_set.method,
        "model_version": fitted_set.model_version,
        "symbol": fitted_set.symbol,
        "target": fitted_set.target,
        "series_basis": fitted_set.series_basis,
        "horizon_unit": fitted_set.horizon_unit,
        "currency": fitted_set.currency,
        "fit_cohort_id": fitted_set.cohort_id,
        "fit_selection_policy_hash": fitted_set.selection_policy_hash,
        "heldout_cohort_id": _hash(8),
        "heldout_selection_policy_hash": _hash(9),
        "outcome_resolution_policy_hash": fitted_set.outcome_resolution_policy_hash,
        "outcome_availability_rule_set_hash": (fitted_set.outcome_availability_rule_set_hash),
        "forecast_resolution_policy_hash": fitted_set.forecast_resolution_policy_hash,
        "forecast_availability_rule_set_hash": (fitted_set.forecast_availability_rule_set_hash),
        "fit_evidence_digest": fitted_set.fit_evidence_digest,
        "heldout_evidence_digest": _hash(10),
        "heldout_window_start": date(2026, 4, 1),
        "heldout_window_end": date(2026, 5, 29),
        "heldout_sample_count": 40,
        "confidence_level": 0.95,
        "interval_policy_version": fitted_set.interval_policy_version,
        "window_date_policy_version": fitted_set.window_date_policy_version,
        "estimator_policy_version": WILSON_COVERAGE_POLICY_VERSION,
        "buckets": (
            _coverage_bucket(1, 0.5, covered_count=10),
            _coverage_bucket(1, 0.8, covered_count=16),
            _coverage_bucket(5, 0.8, covered_count=17),
        ),
    }
    values.update(overrides)
    return HeldoutCoverageEvidence(**values)  # type: ignore[arg-type]


def _mutate(canonical: bytes, mutate) -> bytes:
    document = json.loads(canonical)
    mutate(document)
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


def test_round_trip_identity_and_exact_f64_encoding() -> None:
    fitted_set = _fitted_set()
    evidence = _evidence(fitted_set)
    release = build_heldout_coverage_release(fitted_set, evidence)

    expected_id = f"sha256:{hashlib.sha256(release.canonical_release).hexdigest()}"
    assert release.release_id == expected_id
    assert heldout_coverage_release_id_for(release) == expected_id
    assert (
        heldout_coverage_release_id_for(
            release.canonical_release,
            fitted_set=fitted_set,
        )
        == expected_id
    )
    assert (
        parse_heldout_coverage_release(
            release.canonical_release,
            fitted_set=fitted_set,
        )
        == release.evidence
    )
    document = json.loads(release.canonical_release)
    assert document["format"] == HELDOUT_COVERAGE_RELEASE_FORMAT
    assert document["evidence_scope"] == HELDOUT_COVERAGE_RELEASE_SCOPE
    assert document["confidence_level_f64_be"] == "3fee666666666666"


def test_bucket_order_is_normalized_without_changing_identity() -> None:
    fitted_set = _fitted_set()
    forward = _evidence(fitted_set)
    reverse = replace(forward, buckets=tuple(reversed(forward.buckets)))

    assert canonical_heldout_coverage_release(
        fitted_set,
        forward,
    ) == canonical_heldout_coverage_release(fitted_set, reverse)


def test_artifact_structurally_contains_no_acceptance_or_serving_decision() -> None:
    document = json.loads(
        canonical_heldout_coverage_release(_fitted_set(), _evidence(_fitted_set()))
    )
    forbidden = {
        "accepted",
        "approved",
        "minimum_sample_count",
        "multiplicity",
        "promoted",
        "serving_eligible",
        "threshold",
        "tolerance",
    }
    assert forbidden.isdisjoint(document)
    assert all(forbidden.isdisjoint(bucket) for bucket in document["buckets"])


def test_confidence_level_is_an_explicit_estimator_input() -> None:
    parameter = inspect.signature(estimate_heldout_coverage).parameters["confidence_level"]
    assert parameter.default is inspect.Parameter.empty


@pytest.mark.parametrize(
    "change",
    [
        lambda evidence: replace(evidence, fitted_calibration_set_version=_hash(90)),
        lambda evidence: replace(evidence, method="conformal_quantile_regression"),
        lambda evidence: replace(evidence, symbol="AAPL"),
        lambda evidence: replace(evidence, fit_selection_policy_hash=_hash(91)),
        lambda evidence: replace(evidence, outcome_resolution_policy_hash=_hash(92)),
        lambda evidence: replace(
            evidence,
            forecast_availability_rule_set_hash=_hash(93),
        ),
        lambda evidence: replace(evidence, fit_evidence_digest=_hash(94)),
    ],
)
def test_release_must_match_every_fitted_set_binding(
    change: Callable[[HeldoutCoverageEvidence], HeldoutCoverageEvidence],
) -> None:
    fitted_set = _fitted_set()
    with pytest.raises(
        ForecastCalibrationReleaseValidationError,
        match="does not match",
    ):
        build_heldout_coverage_release(fitted_set, change(_evidence(fitted_set)))


def test_release_requires_distinct_canonical_cohorts_and_window() -> None:
    fitted_set = _fitted_set()
    evidence = _evidence(fitted_set)
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="distinct"):
        build_heldout_coverage_release(
            fitted_set,
            replace(evidence, heldout_cohort_id=evidence.fit_cohort_id),
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="window"):
        build_heldout_coverage_release(
            fitted_set,
            replace(
                evidence,
                heldout_window_start=date(2026, 6, 1),
                heldout_window_end=date(2026, 5, 1),
            ),
        )


def test_release_requires_exact_unique_fitted_buckets() -> None:
    fitted_set = _fitted_set()
    evidence = _evidence(fitted_set)
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="exactly match"):
        build_heldout_coverage_release(
            fitted_set,
            replace(evidence, buckets=evidence.buckets[:-1]),
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="duplicate"):
        build_heldout_coverage_release(
            fitted_set,
            replace(evidence, buckets=(*evidence.buckets, evidence.buckets[0])),
        )


def test_release_bucket_populations_exactly_cover_the_heldout_cohort() -> None:
    fitted_set = _fitted_set()
    evidence = _evidence(fitted_set)
    inconsistent_horizon = replace(
        evidence,
        buckets=(
            evidence.buckets[0],
            _coverage_bucket(1, 0.8, covered_count=15, sample_count=19),
            evidence.buckets[2],
        ),
    )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="one horizon"):
        build_heldout_coverage_release(fitted_set, inconsistent_horizon)

    with pytest.raises(ForecastCalibrationReleaseValidationError, match="held-out cohort"):
        build_heldout_coverage_release(
            fitted_set,
            replace(evidence, heldout_sample_count=41),
        )


@pytest.mark.parametrize(
    "bad_bucket",
    [
        lambda row: replace(row, covered_count=-1),
        lambda row: replace(row, covered_count=row.sample_count + 1),
        lambda row: replace(row, sample_count=0),
        lambda row: replace(row, empirical_coverage=0.123),
        lambda row: replace(row, confidence_low=math.nextafter(row.confidence_low, 1.0)),
        lambda row: replace(row, confidence_high=math.nextafter(row.confidence_high, 0.0)),
    ],
)
def test_release_recomputes_counts_empirical_value_and_wilson_bounds(bad_bucket) -> None:
    fitted_set = _fitted_set()
    evidence = _evidence(fitted_set)
    malformed = replace(evidence, buckets=(bad_bucket(evidence.buckets[0]), *evidence.buckets[1:]))
    with pytest.raises(ForecastCalibrationReleaseValidationError):
        build_heldout_coverage_release(fitted_set, malformed)


def test_parse_rejects_unknown_missing_duplicate_and_noncanonical_json() -> None:
    fitted_set = _fitted_set()
    canonical = canonical_heldout_coverage_release(fitted_set, _evidence(fitted_set))
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="unknown or missing"):
        parse_heldout_coverage_release(
            _mutate(canonical, lambda document: document.__setitem__("accepted", True)),
            fitted_set=fitted_set,
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="unknown or missing"):
        parse_heldout_coverage_release(
            _mutate(canonical, lambda document: document.pop("heldout_cohort_id")),
            fitted_set=fitted_set,
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="duplicate"):
        parse_heldout_coverage_release(
            b'{"format":"x","format":"y"}',
            fitted_set=fitted_set,
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="not canonical"):
        parse_heldout_coverage_release(
            json.dumps(json.loads(canonical), indent=2).encode(),
            fitted_set=fitted_set,
        )


def test_parse_rejects_tampered_derived_values_and_float_encodings() -> None:
    fitted_set = _fitted_set()
    canonical = canonical_heldout_coverage_release(fitted_set, _evidence(fitted_set))

    def change_count(document: dict[str, object]) -> None:
        buckets = document["buckets"]
        assert isinstance(buckets, list)
        bucket = buckets[0]
        assert isinstance(bucket, dict)
        bucket["covered_count"] = int(bucket["covered_count"]) + 1

    with pytest.raises(ForecastCalibrationReleaseValidationError):
        parse_heldout_coverage_release(
            _mutate(canonical, change_count),
            fitted_set=fitted_set,
        )
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="lowercase"):
        parse_heldout_coverage_release(
            _mutate(
                canonical,
                lambda document: document.__setitem__("confidence_level_f64_be", "3FEE" * 4),
            ),
            fitted_set=fitted_set,
        )


@pytest.mark.parametrize(
    "payload",
    [b"", b"\xff", b'{"value":NaN}'],
)
def test_parse_rejects_empty_invalid_utf8_and_json_constants(payload: bytes) -> None:
    fitted_set = _fitted_set()
    with pytest.raises(ForecastCalibrationReleaseValidationError):
        parse_heldout_coverage_release(payload, fitted_set=fitted_set)


def test_identity_rejects_forged_release_headers_and_requires_set_for_bytes() -> None:
    fitted_set = _fitted_set()
    release = build_heldout_coverage_release(fitted_set, _evidence(fitted_set))
    with pytest.raises(ForecastCalibrationReleaseValidationError, match="header"):
        heldout_coverage_release_id_for(replace(release, release_id=_hash(99)))
    with pytest.raises(TypeError, match="fitted_set"):
        heldout_coverage_release_id_for(release.canonical_release)
