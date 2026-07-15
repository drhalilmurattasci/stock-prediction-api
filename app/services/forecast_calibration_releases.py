"""Canonical, descriptive-only held-out calibration coverage releases.

This module gives :class:`HeldoutCoverageEvidence` a strict content identity.
It records what a proof-bound held-out estimator observed; it deliberately has
no acceptance threshold, promotion decision, or serving eligibility field.
Those decisions belong to a separate, precommitted policy artifact.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import struct
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

from app.services.forecast_calibration_evidence import (
    WILSON_COVERAGE_POLICY_VERSION,
    FitMethod,
    ForecastCalibrationEvidenceError,
    HeldoutCoverageBucket,
    HeldoutCoverageEvidence,
    wilson_interval,
)
from app.services.forecast_calibration_sets import (
    MAX_CALIBRATION_BUCKETS,
    FittedCalibrationSet,
    ForecastCalibrationSetValidationError,
    calibration_set_version_for,
    canonical_calibration_set,
    parse_calibration_set,
)

HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION = 1
HELDOUT_COVERAGE_RELEASE_FORMAT = "forecast-heldout-coverage-release-v1"
HELDOUT_COVERAGE_RELEASE_SCOPE = "descriptive-only"
MAX_CANONICAL_RELEASE_BYTES = 4 * 1024 * 1024
MAX_HELDOUT_SAMPLE_COUNT = 10_000

_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_FLOAT_BITS_PATTERN = re.compile(r"^[0-9a-f]{16}$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ROOT_KEYS = {
    "buckets",
    "confidence_level_f64_be",
    "currency",
    "estimator_policy_version",
    "evidence_scope",
    "fit_cohort_id",
    "fit_evidence_digest",
    "fit_selection_policy_hash",
    "fitted_calibration_set_version",
    "forecast_availability_rule_set_hash",
    "forecast_resolution_policy_hash",
    "format",
    "heldout_cohort_id",
    "heldout_evidence_digest",
    "heldout_sample_count",
    "heldout_selection_policy_hash",
    "heldout_window_end",
    "heldout_window_start",
    "horizon_unit",
    "interval_policy_version",
    "method",
    "model_version",
    "outcome_availability_rule_set_hash",
    "outcome_resolution_policy_hash",
    "schema_version",
    "series_basis",
    "symbol",
    "target",
    "window_date_policy_version",
}
_BUCKET_KEYS = {
    "confidence_high_f64_be",
    "confidence_low_f64_be",
    "coverage_millis",
    "covered_count",
    "empirical_coverage_f64_be",
    "horizon",
    "sample_count",
}


class ForecastCalibrationReleaseValidationError(ValueError):
    """A descriptive coverage release is malformed, inconsistent, or unsupported."""


@dataclass(frozen=True, slots=True)
class HeldoutCoverageRelease:
    """One content-addressed fitted set plus descriptive held-out evidence."""

    release_id: str
    fitted_set: FittedCalibrationSet
    evidence: HeldoutCoverageEvidence
    canonical_release: bytes


def build_heldout_coverage_release(
    fitted_set: FittedCalibrationSet,
    evidence: HeldoutCoverageEvidence,
) -> HeldoutCoverageRelease:
    """Validate, normalize, and identify one descriptive coverage release."""

    normalized_set = _normalized_set(fitted_set)
    normalized_evidence = _normalized_evidence(normalized_set, evidence)
    canonical = _canonical_from_normalized(normalized_evidence)
    return HeldoutCoverageRelease(
        release_id=_content_id(canonical),
        fitted_set=normalized_set,
        evidence=normalized_evidence,
        canonical_release=canonical,
    )


def canonical_heldout_coverage_release(
    fitted_set: FittedCalibrationSet,
    evidence: HeldoutCoverageEvidence,
) -> bytes:
    """Return strict deterministic UTF-8 JSON for descriptive evidence."""

    return build_heldout_coverage_release(fitted_set, evidence).canonical_release


def parse_heldout_coverage_release(
    canonical_release: bytes,
    *,
    fitted_set: FittedCalibrationSet,
) -> HeldoutCoverageEvidence:
    """Parse exact canonical bytes and validate them against fitted set v2."""

    canonical = _bounded_bytes(canonical_release)
    normalized_set = _normalized_set(fitted_set)
    try:
        document = json.loads(
            canonical.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ForecastCalibrationReleaseValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise ForecastCalibrationReleaseValidationError(
            "canonical coverage release is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise ForecastCalibrationReleaseValidationError("coverage release must be a JSON object")
    if set(document) != _ROOT_KEYS:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release has unknown or missing keys"
        )
    if document["format"] != HELDOUT_COVERAGE_RELEASE_FORMAT:
        raise ForecastCalibrationReleaseValidationError("coverage release format is not supported")
    if document["evidence_scope"] != HELDOUT_COVERAGE_RELEASE_SCOPE:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release is not descriptive-only evidence"
        )
    raw_buckets = document["buckets"]
    if not isinstance(raw_buckets, list):
        raise ForecastCalibrationReleaseValidationError(
            "coverage release buckets must be a JSON array"
        )
    buckets: list[HeldoutCoverageBucket] = []
    for index, raw in enumerate(raw_buckets):
        if not isinstance(raw, dict) or set(raw) != _BUCKET_KEYS:
            raise ForecastCalibrationReleaseValidationError(
                f"coverage release bucket {index} has unknown or missing keys"
            )
        buckets.append(
            HeldoutCoverageBucket(
                horizon=_integer(raw["horizon"], f"buckets[{index}].horizon"),
                nominal_coverage=_coverage_from_millis(
                    raw["coverage_millis"],
                    f"buckets[{index}].coverage_millis",
                ),
                covered_count=_integer(
                    raw["covered_count"],
                    f"buckets[{index}].covered_count",
                ),
                sample_count=_integer(
                    raw["sample_count"],
                    f"buckets[{index}].sample_count",
                ),
                empirical_coverage=_parse_float_bits(
                    raw["empirical_coverage_f64_be"],
                    f"buckets[{index}].empirical_coverage_f64_be",
                ),
                confidence_low=_parse_float_bits(
                    raw["confidence_low_f64_be"],
                    f"buckets[{index}].confidence_low_f64_be",
                ),
                confidence_high=_parse_float_bits(
                    raw["confidence_high_f64_be"],
                    f"buckets[{index}].confidence_high_f64_be",
                ),
            )
        )
    evidence = HeldoutCoverageEvidence(
        fitted_calibration_set_version=_string(
            document["fitted_calibration_set_version"],
            "fitted_calibration_set_version",
        ),
        method=cast(FitMethod, _string(document["method"], "method")),
        model_version=_string(document["model_version"], "model_version"),
        symbol=_string(document["symbol"], "symbol"),
        target=_string(document["target"], "target"),
        series_basis=_string(document["series_basis"], "series_basis"),
        horizon_unit=_string(document["horizon_unit"], "horizon_unit"),
        currency=_string(document["currency"], "currency"),
        fit_cohort_id=_string(document["fit_cohort_id"], "fit_cohort_id"),
        fit_selection_policy_hash=_string(
            document["fit_selection_policy_hash"],
            "fit_selection_policy_hash",
        ),
        heldout_cohort_id=_string(document["heldout_cohort_id"], "heldout_cohort_id"),
        heldout_selection_policy_hash=_string(
            document["heldout_selection_policy_hash"],
            "heldout_selection_policy_hash",
        ),
        outcome_resolution_policy_hash=_string(
            document["outcome_resolution_policy_hash"],
            "outcome_resolution_policy_hash",
        ),
        outcome_availability_rule_set_hash=_string(
            document["outcome_availability_rule_set_hash"],
            "outcome_availability_rule_set_hash",
        ),
        forecast_resolution_policy_hash=_string(
            document["forecast_resolution_policy_hash"],
            "forecast_resolution_policy_hash",
        ),
        forecast_availability_rule_set_hash=_string(
            document["forecast_availability_rule_set_hash"],
            "forecast_availability_rule_set_hash",
        ),
        fit_evidence_digest=_string(
            document["fit_evidence_digest"],
            "fit_evidence_digest",
        ),
        heldout_evidence_digest=_string(
            document["heldout_evidence_digest"],
            "heldout_evidence_digest",
        ),
        heldout_window_start=_parse_date(
            document["heldout_window_start"],
            "heldout_window_start",
        ),
        heldout_window_end=_parse_date(
            document["heldout_window_end"],
            "heldout_window_end",
        ),
        heldout_sample_count=_integer(
            document["heldout_sample_count"],
            "heldout_sample_count",
        ),
        confidence_level=_parse_float_bits(
            document["confidence_level_f64_be"],
            "confidence_level_f64_be",
        ),
        interval_policy_version=_string(
            document["interval_policy_version"],
            "interval_policy_version",
        ),
        window_date_policy_version=_string(
            document["window_date_policy_version"],
            "window_date_policy_version",
        ),
        estimator_policy_version=_string(
            document["estimator_policy_version"],
            "estimator_policy_version",
        ),
        buckets=tuple(buckets),
    )
    normalized = _normalized_evidence(normalized_set, evidence)
    if not hmac.compare_digest(_canonical_from_normalized(normalized), canonical):
        raise ForecastCalibrationReleaseValidationError("coverage release bytes are not canonical")
    return normalized


def heldout_coverage_release_id_for(
    release_or_bytes: HeldoutCoverageRelease | bytes,
    *,
    fitted_set: FittedCalibrationSet | None = None,
) -> str:
    """Return the SHA-256 identity of a fully validated release."""

    if isinstance(release_or_bytes, HeldoutCoverageRelease):
        rebuilt = build_heldout_coverage_release(
            release_or_bytes.fitted_set,
            release_or_bytes.evidence,
        )
        if not hmac.compare_digest(
            rebuilt.release_id, release_or_bytes.release_id
        ) or not hmac.compare_digest(
            rebuilt.canonical_release,
            release_or_bytes.canonical_release,
        ):
            raise ForecastCalibrationReleaseValidationError(
                "coverage release header does not match its canonical evidence"
            )
        return rebuilt.release_id
    if not isinstance(release_or_bytes, bytes):
        raise TypeError("release_or_bytes must be a HeldoutCoverageRelease or bytes")
    if fitted_set is None:
        raise TypeError("fitted_set is required when identifying canonical bytes")
    evidence = parse_heldout_coverage_release(release_or_bytes, fitted_set=fitted_set)
    canonical = canonical_heldout_coverage_release(fitted_set, evidence)
    return _content_id(canonical)


def _normalized_set(fitted_set: FittedCalibrationSet) -> FittedCalibrationSet:
    try:
        return parse_calibration_set(canonical_calibration_set(fitted_set))
    except (ForecastCalibrationSetValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationReleaseValidationError(
            "fitted calibration set is invalid"
        ) from exc


def _normalized_evidence(
    fitted_set: FittedCalibrationSet,
    evidence: HeldoutCoverageEvidence,
) -> HeldoutCoverageEvidence:
    if not isinstance(evidence, HeldoutCoverageEvidence):
        raise TypeError("evidence must be HeldoutCoverageEvidence")
    set_version = calibration_set_version_for(fitted_set)
    expected_bound_fields = (
        set_version,
        fitted_set.method,
        fitted_set.model_version,
        fitted_set.symbol,
        fitted_set.target,
        fitted_set.series_basis,
        fitted_set.horizon_unit,
        fitted_set.currency,
        fitted_set.cohort_id,
        fitted_set.selection_policy_hash,
        fitted_set.outcome_resolution_policy_hash,
        fitted_set.outcome_availability_rule_set_hash,
        fitted_set.forecast_resolution_policy_hash,
        fitted_set.forecast_availability_rule_set_hash,
        fitted_set.fit_evidence_digest,
        fitted_set.interval_policy_version,
        fitted_set.window_date_policy_version,
    )
    actual_bound_fields = (
        evidence.fitted_calibration_set_version,
        evidence.method,
        evidence.model_version,
        evidence.symbol,
        evidence.target,
        evidence.series_basis,
        evidence.horizon_unit,
        evidence.currency,
        evidence.fit_cohort_id,
        evidence.fit_selection_policy_hash,
        evidence.outcome_resolution_policy_hash,
        evidence.outcome_availability_rule_set_hash,
        evidence.forecast_resolution_policy_hash,
        evidence.forecast_availability_rule_set_hash,
        evidence.fit_evidence_digest,
        evidence.interval_policy_version,
        evidence.window_date_policy_version,
    )
    if actual_bound_fields != expected_bound_fields:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release does not match its fitted calibration set"
        )
    fit_cohort_id = _sha256(evidence.fit_cohort_id, "fit_cohort_id")
    heldout_cohort_id = _sha256(evidence.heldout_cohort_id, "heldout_cohort_id")
    if hmac.compare_digest(fit_cohort_id, heldout_cohort_id):
        raise ForecastCalibrationReleaseValidationError("fit and held-out cohorts must be distinct")
    heldout_selection_policy_hash = _sha256(
        evidence.heldout_selection_policy_hash,
        "heldout_selection_policy_hash",
    )
    heldout_evidence_digest = _sha256(
        evidence.heldout_evidence_digest,
        "heldout_evidence_digest",
    )
    window_start = _date(evidence.heldout_window_start, "heldout_window_start")
    window_end = _date(evidence.heldout_window_end, "heldout_window_end")
    if window_end < window_start:
        raise ForecastCalibrationReleaseValidationError(
            "heldout_window_end must be on or after heldout_window_start"
        )
    heldout_sample_count = _positive_integer(
        evidence.heldout_sample_count,
        "heldout_sample_count",
    )
    if heldout_sample_count > MAX_HELDOUT_SAMPLE_COUNT:
        raise ForecastCalibrationReleaseValidationError(
            "heldout_sample_count exceeds the cohort bound"
        )
    confidence_level = _open_probability(evidence.confidence_level, "confidence_level")
    if evidence.estimator_policy_version != WILSON_COVERAGE_POLICY_VERSION:
        raise ForecastCalibrationReleaseValidationError(
            "coverage estimator policy version is not supported"
        )
    if not isinstance(evidence.buckets, tuple):
        raise ForecastCalibrationReleaseValidationError("coverage release buckets must be a tuple")
    if not 1 <= len(evidence.buckets) <= MAX_CALIBRATION_BUCKETS:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release bucket count is outside the supported bound"
        )
    normalized_buckets = tuple(
        _normalized_bucket(
            bucket,
            heldout_sample_count=heldout_sample_count,
            confidence_level=confidence_level,
        )
        for bucket in evidence.buckets
    )
    keys = [
        (bucket.horizon, _coverage_millis(bucket.nominal_coverage)) for bucket in normalized_buckets
    ]
    if len(set(keys)) != len(keys):
        raise ForecastCalibrationReleaseValidationError(
            "coverage release contains a duplicate bucket"
        )
    expected_keys = {
        (bucket.horizon, _coverage_millis(bucket.calibration.selection.coverage))
        for bucket in fitted_set.buckets
    }
    if set(keys) != expected_keys:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release buckets do not exactly match the fitted set"
        )
    samples_by_horizon: dict[int, int] = {}
    for bucket in normalized_buckets:
        prior = samples_by_horizon.setdefault(bucket.horizon, bucket.sample_count)
        if prior != bucket.sample_count:
            raise ForecastCalibrationReleaseValidationError(
                "coverage buckets for one horizon must share a population"
            )
    if sum(samples_by_horizon.values()) != heldout_sample_count:
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket populations do not cover the held-out cohort"
        )
    ordered = tuple(
        sorted(
            normalized_buckets,
            key=lambda bucket: (bucket.horizon, _coverage_millis(bucket.nominal_coverage)),
        )
    )
    return HeldoutCoverageEvidence(
        fitted_calibration_set_version=set_version,
        method=fitted_set.method,
        model_version=fitted_set.model_version,
        symbol=fitted_set.symbol,
        target=fitted_set.target,
        series_basis=fitted_set.series_basis,
        horizon_unit=fitted_set.horizon_unit,
        currency=fitted_set.currency,
        fit_cohort_id=fit_cohort_id,
        fit_selection_policy_hash=fitted_set.selection_policy_hash,
        heldout_cohort_id=heldout_cohort_id,
        heldout_selection_policy_hash=heldout_selection_policy_hash,
        outcome_resolution_policy_hash=fitted_set.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=(fitted_set.outcome_availability_rule_set_hash),
        forecast_resolution_policy_hash=fitted_set.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=(fitted_set.forecast_availability_rule_set_hash),
        fit_evidence_digest=fitted_set.fit_evidence_digest,
        heldout_evidence_digest=heldout_evidence_digest,
        heldout_window_start=window_start,
        heldout_window_end=window_end,
        heldout_sample_count=heldout_sample_count,
        confidence_level=confidence_level,
        interval_policy_version=fitted_set.interval_policy_version,
        window_date_policy_version=fitted_set.window_date_policy_version,
        estimator_policy_version=WILSON_COVERAGE_POLICY_VERSION,
        buckets=ordered,
    )


def _normalized_bucket(
    bucket: HeldoutCoverageBucket,
    *,
    heldout_sample_count: int,
    confidence_level: float,
) -> HeldoutCoverageBucket:
    if not isinstance(bucket, HeldoutCoverageBucket):
        raise ForecastCalibrationReleaseValidationError(
            "coverage release buckets have the wrong type"
        )
    horizon = _integer(bucket.horizon, "bucket.horizon")
    if not 1 <= horizon <= 252:
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket horizon must be within 1..252"
        )
    coverage = _canonical_coverage(bucket.nominal_coverage)
    covered_count = _integer(bucket.covered_count, "bucket.covered_count")
    sample_count = _positive_integer(bucket.sample_count, "bucket.sample_count")
    if not 0 <= covered_count <= sample_count <= heldout_sample_count:
        raise ForecastCalibrationReleaseValidationError("coverage bucket counts are incoherent")
    expected_empirical = covered_count / sample_count
    empirical = _finite(bucket.empirical_coverage, "bucket.empirical_coverage")
    low = _finite(bucket.confidence_low, "bucket.confidence_low")
    high = _finite(bucket.confidence_high, "bucket.confidence_high")
    if _float_bits(empirical, "bucket.empirical_coverage") != _float_bits(
        expected_empirical,
        "expected empirical coverage",
    ):
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket empirical value does not match its counts"
        )
    try:
        expected_low, expected_high = wilson_interval(
            covered_count,
            sample_count,
            confidence_level=confidence_level,
        )
    except ForecastCalibrationEvidenceError as exc:
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket Wilson interval cannot be reproduced"
        ) from exc
    if _float_bits(low, "bucket.confidence_low") != _float_bits(
        expected_low, "expected confidence_low"
    ) or _float_bits(high, "bucket.confidence_high") != _float_bits(
        expected_high, "expected confidence_high"
    ):
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket confidence bounds do not match the estimator"
        )
    if not 0.0 <= low <= empirical <= high <= 1.0:
        raise ForecastCalibrationReleaseValidationError(
            "coverage bucket confidence bounds are incoherent"
        )
    return HeldoutCoverageBucket(
        horizon=horizon,
        nominal_coverage=coverage,
        covered_count=covered_count,
        sample_count=sample_count,
        empirical_coverage=expected_empirical,
        confidence_low=expected_low,
        confidence_high=expected_high,
    )


def _canonical_from_normalized(evidence: HeldoutCoverageEvidence) -> bytes:
    document = {
        "buckets": [
            {
                "confidence_high_f64_be": _float_bits(
                    bucket.confidence_high,
                    "bucket.confidence_high",
                ),
                "confidence_low_f64_be": _float_bits(
                    bucket.confidence_low,
                    "bucket.confidence_low",
                ),
                "coverage_millis": _coverage_millis(bucket.nominal_coverage),
                "covered_count": bucket.covered_count,
                "empirical_coverage_f64_be": _float_bits(
                    bucket.empirical_coverage,
                    "bucket.empirical_coverage",
                ),
                "horizon": bucket.horizon,
                "sample_count": bucket.sample_count,
            }
            for bucket in evidence.buckets
        ],
        "confidence_level_f64_be": _float_bits(
            evidence.confidence_level,
            "confidence_level",
        ),
        "currency": evidence.currency,
        "estimator_policy_version": evidence.estimator_policy_version,
        "evidence_scope": HELDOUT_COVERAGE_RELEASE_SCOPE,
        "fit_cohort_id": evidence.fit_cohort_id,
        "fit_evidence_digest": evidence.fit_evidence_digest,
        "fit_selection_policy_hash": evidence.fit_selection_policy_hash,
        "fitted_calibration_set_version": evidence.fitted_calibration_set_version,
        "forecast_availability_rule_set_hash": (evidence.forecast_availability_rule_set_hash),
        "forecast_resolution_policy_hash": evidence.forecast_resolution_policy_hash,
        "format": HELDOUT_COVERAGE_RELEASE_FORMAT,
        "heldout_cohort_id": evidence.heldout_cohort_id,
        "heldout_evidence_digest": evidence.heldout_evidence_digest,
        "heldout_sample_count": evidence.heldout_sample_count,
        "heldout_selection_policy_hash": evidence.heldout_selection_policy_hash,
        "heldout_window_end": evidence.heldout_window_end.isoformat(),
        "heldout_window_start": evidence.heldout_window_start.isoformat(),
        "horizon_unit": evidence.horizon_unit,
        "interval_policy_version": evidence.interval_policy_version,
        "method": evidence.method,
        "model_version": evidence.model_version,
        "outcome_availability_rule_set_hash": (evidence.outcome_availability_rule_set_hash),
        "outcome_resolution_policy_hash": evidence.outcome_resolution_policy_hash,
        "schema_version": HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION,
        "series_basis": evidence.series_basis,
        "symbol": evidence.symbol,
        "target": evidence.target,
        "window_date_policy_version": evidence.window_date_policy_version,
    }
    try:
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise ForecastCalibrationReleaseValidationError(
            "coverage release cannot be encoded canonically"
        ) from exc
    if not canonical or len(canonical) > MAX_CANONICAL_RELEASE_BYTES:
        raise ForecastCalibrationReleaseValidationError(
            "canonical coverage release exceeds the storage limit"
        )
    return canonical


def _content_id(canonical: bytes) -> str:
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _coverage_millis(value: object) -> int:
    coverage = _finite(value, "coverage")
    millis = round(coverage * 1000)
    if not 1 <= millis <= 999 or abs(coverage - millis / 1000) > 1e-12:
        raise ForecastCalibrationReleaseValidationError("coverage must be a canonical thousandth")
    return millis


def _canonical_coverage(value: object) -> float:
    return _coverage_millis(value) / 1000


def _coverage_from_millis(value: object, label: str) -> float:
    millis = _integer(value, label)
    if not 1 <= millis <= 999:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be within 1..999")
    return millis / 1000


def _float_bits(value: object, label: str) -> str:
    return struct.pack(">d", _finite(value, label)).hex()


def _parse_float_bits(value: object, label: str) -> float:
    if not isinstance(value, str) or _FLOAT_BITS_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationReleaseValidationError(
            f"{label} must be 16 lowercase hexadecimal digits"
        )
    return _finite(struct.unpack(">d", bytes.fromhex(value))[0], label)


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a finite real number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ForecastCalibrationReleaseValidationError(f"{label} must be finite")
    return 0.0 if converted == 0.0 else converted


def _open_probability(value: object, label: str) -> float:
    converted = _finite(value, label)
    if not 0.0 < converted < 1.0:
        raise ForecastCalibrationReleaseValidationError(
            f"{label} must be strictly between zero and one"
        )
    return converted


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _positive_integer(value: object, label: str) -> int:
    integer = _integer(value, label)
    if integer <= 0:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a positive integer")
    return integer


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be an integer")
    return value


def _date(value: object, label: str) -> date:
    if type(value) is not date or isinstance(value, datetime):
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a calendar date")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str) or _DATE_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ForecastCalibrationReleaseValidationError(f"{label} is not a valid date") from exc
    if parsed.isoformat() != value:
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a canonical ISO date")
    return parsed


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ForecastCalibrationReleaseValidationError(f"{label} must be a string")
    return value


def _bounded_bytes(value: object) -> bytes:
    if not isinstance(value, bytes) or not value or len(value) > MAX_CANONICAL_RELEASE_BYTES:
        raise ForecastCalibrationReleaseValidationError(
            "canonical coverage release must be non-empty bounded bytes"
        )
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForecastCalibrationReleaseValidationError(
                "canonical coverage release contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ForecastCalibrationReleaseValidationError(f"JSON constant {value!r} is not permitted")


__all__ = [
    "HELDOUT_COVERAGE_RELEASE_FORMAT",
    "HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION",
    "HELDOUT_COVERAGE_RELEASE_SCOPE",
    "ForecastCalibrationReleaseValidationError",
    "HeldoutCoverageRelease",
    "build_heldout_coverage_release",
    "canonical_heldout_coverage_release",
    "heldout_coverage_release_id_for",
    "parse_heldout_coverage_release",
]
