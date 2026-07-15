"""Pure, canonical identity for a *fitted* conformal calibration set.

A fitted calibration set is the content-addressed output of running a
precommitted calibration cohort's realized residuals through the conformal
kernels (:mod:`ml.calibration.conformal`).  It binds, into one SHA-256 version:

* the model whose intervals it calibrates and the fit method;
* the fit window and total fit sample count;
* the exact provenance the fit was drawn from -- the source ``cohort_id`` and the
  ``selection`` / ``outcome_resolution`` / ``availability`` policy hashes; and
* one fitted conformal correction per ``(horizon, coverage)`` bucket.

It deliberately carries **no empirical-coverage claim**.  Held-out coverage is a
separate, disjoint-cohort measurement (see the calibration-coverage estimator);
keeping it out of the fitted artifact makes an in-sample coverage number
*structurally impossible* to smuggle into a fit.  This module is independent of
SQL and of the serving schema so it can be validated on both sides of any
boundary, mirroring :mod:`app.services.forecast_runs` and
:mod:`app.services.forecast_cohorts`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from ml.calibration.conformal import (
    AbsoluteResidualCalibration,
    ConformalValidationError,
    CQRCalibration,
    QuantileSelection,
)

CALIBRATION_SET_SCHEMA_VERSION = 1
CALIBRATION_SET_FORMAT = "forecast-calibration-set-v1"
MAX_CALIBRATION_BUCKETS = 10_000
MODEL_VERSION_MAX_LENGTH = 128
MAX_CANONICAL_BYTES = 4 * 1024 * 1024

type CalibrationMethod = Literal["empirical_residual", "conformal_quantile_regression"]

_FittedCorrection = AbsoluteResidualCalibration | CQRCalibration
_METHOD_TYPES: dict[str, type[_FittedCorrection]] = {
    "empirical_residual": AbsoluteResidualCalibration,
    "conformal_quantile_regression": CQRCalibration,
}
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ForecastCalibrationSetValidationError(ValueError):
    """A fitted calibration-set artifact is malformed, tampered with, or unsupported."""


@dataclass(frozen=True)
class FittedCalibrationBucket:
    """One fitted conformal correction for a single ``(horizon, coverage)`` bucket."""

    horizon: int
    calibration: _FittedCorrection


@dataclass(frozen=True)
class FittedCalibrationSet:
    """Content-addressable fitted calibration for one model, over one cohort."""

    model_version: str
    method: CalibrationMethod
    window_start: date
    window_end: date
    sample_count: int
    cohort_id: str
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    buckets: tuple[FittedCalibrationBucket, ...]
    schema_version: int = CALIBRATION_SET_SCHEMA_VERSION


def canonical_calibration_set(calibration_set: FittedCalibrationSet) -> bytes:
    """Return strict, deterministic UTF-8 JSON for one fitted calibration set."""

    normalized = _normalized_set(calibration_set)
    document = {
        "availability_rule_set_hash": normalized.availability_rule_set_hash,
        "buckets": [
            {
                "coverage_millis": _coverage_millis(bucket.calibration.selection.coverage),
                "fit_sample_count": bucket.calibration.selection.sample_count,
                "horizon": bucket.horizon,
                "rank": bucket.calibration.selection.rank,
                "value": _canonical_float(bucket.calibration.selection.value, "bucket value"),
            }
            for bucket in normalized.buckets
        ],
        "cohort_id": normalized.cohort_id,
        "format": CALIBRATION_SET_FORMAT,
        "method": normalized.method,
        "model_version": normalized.model_version,
        "outcome_resolution_policy_hash": normalized.outcome_resolution_policy_hash,
        "sample_count": normalized.sample_count,
        "schema_version": normalized.schema_version,
        "selection_policy_hash": normalized.selection_policy_hash,
        "window_end": normalized.window_end.isoformat(),
        "window_start": normalized.window_start.isoformat(),
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
        raise ForecastCalibrationSetValidationError(
            "fitted calibration set cannot be encoded canonically"
        ) from exc
    if not canonical or len(canonical) > MAX_CANONICAL_BYTES:
        raise ForecastCalibrationSetValidationError(
            "canonical fitted calibration set exceeds the storage limit"
        )
    return canonical


def parse_calibration_set(canonical_set: bytes) -> FittedCalibrationSet:
    """Parse and recanonicalize bytes, rejecting duplicate or unknown JSON keys."""

    _bounded_bytes(canonical_set)
    try:
        document = json.loads(
            canonical_set.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ForecastCalibrationSetValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise ForecastCalibrationSetValidationError(
            "canonical fitted calibration set is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise ForecastCalibrationSetValidationError("fitted calibration set must be a JSON object")
    expected_keys = {
        "availability_rule_set_hash",
        "buckets",
        "cohort_id",
        "format",
        "method",
        "model_version",
        "outcome_resolution_policy_hash",
        "sample_count",
        "schema_version",
        "selection_policy_hash",
        "window_end",
        "window_start",
    }
    if set(document) != expected_keys:
        raise ForecastCalibrationSetValidationError(
            "fitted calibration set has unknown or missing keys"
        )
    if document["format"] != CALIBRATION_SET_FORMAT:
        raise ForecastCalibrationSetValidationError(
            "fitted calibration set format is not supported"
        )
    method = _method(document["method"])
    raw_buckets = document["buckets"]
    if not isinstance(raw_buckets, list):
        raise ForecastCalibrationSetValidationError("calibration buckets must be a JSON array")
    bucket_keys = {"coverage_millis", "fit_sample_count", "horizon", "rank", "value"}
    buckets: list[FittedCalibrationBucket] = []
    for index, raw in enumerate(raw_buckets):
        if not isinstance(raw, dict) or set(raw) != bucket_keys:
            raise ForecastCalibrationSetValidationError(
                f"calibration bucket {index} has unknown or missing keys"
            )
        buckets.append(
            _bucket(
                horizon=raw["horizon"],
                coverage_millis=raw["coverage_millis"],
                fit_sample_count=raw["fit_sample_count"],
                rank=raw["rank"],
                value=raw["value"],
                method=method,
                label=f"buckets[{index}]",
            )
        )
    calibration_set = FittedCalibrationSet(
        schema_version=_integer(document["schema_version"], "schema_version"),
        model_version=_model_version(document["model_version"]),
        method=method,
        window_start=_parse_date(document["window_start"], "window_start"),
        window_end=_parse_date(document["window_end"], "window_end"),
        sample_count=_positive_integer(document["sample_count"], "sample_count"),
        cohort_id=_sha256(document["cohort_id"], "cohort_id"),
        selection_policy_hash=_sha256(document["selection_policy_hash"], "selection_policy_hash"),
        outcome_resolution_policy_hash=_sha256(
            document["outcome_resolution_policy_hash"],
            "outcome_resolution_policy_hash",
        ),
        availability_rule_set_hash=_sha256(
            document["availability_rule_set_hash"],
            "availability_rule_set_hash",
        ),
        buckets=tuple(buckets),
    )
    normalized = _normalized_set(calibration_set)
    if canonical_calibration_set(normalized) != canonical_set:
        raise ForecastCalibrationSetValidationError(
            "fitted calibration set bytes are not canonical"
        )
    return normalized


def calibration_set_version_for(set_or_bytes: FittedCalibrationSet | bytes) -> str:
    """Return the SHA-256 identity of validated canonical fitted-set bytes."""

    if isinstance(set_or_bytes, FittedCalibrationSet):
        canonical = canonical_calibration_set(set_or_bytes)
    else:
        canonical = canonical_calibration_set(parse_calibration_set(set_or_bytes))
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _normalized_set(calibration_set: FittedCalibrationSet) -> FittedCalibrationSet:
    if not isinstance(calibration_set, FittedCalibrationSet):
        raise TypeError("calibration_set must be a FittedCalibrationSet")
    if (
        type(calibration_set.schema_version) is not int
        or calibration_set.schema_version != CALIBRATION_SET_SCHEMA_VERSION
    ):
        raise ForecastCalibrationSetValidationError(
            "calibration set schema_version is not supported"
        )
    method = _method(calibration_set.method)
    model_version = _model_version(calibration_set.model_version)
    window_start = _date(calibration_set.window_start, "window_start")
    window_end = _date(calibration_set.window_end, "window_end")
    if window_end < window_start:
        raise ForecastCalibrationSetValidationError("window_end must be on or after window_start")
    sample_count = _positive_integer(calibration_set.sample_count, "sample_count")
    if not isinstance(calibration_set.buckets, tuple):
        raise ForecastCalibrationSetValidationError("calibration buckets must be a tuple")
    if not 1 <= len(calibration_set.buckets) <= MAX_CALIBRATION_BUCKETS:
        raise ForecastCalibrationSetValidationError(
            "calibration bucket count is outside the supported bounds"
        )
    normalized_buckets = tuple(
        _normalized_bucket(bucket, method=method, sample_count=sample_count)
        for bucket in calibration_set.buckets
    )
    keys = [
        (bucket.horizon, _coverage_millis(bucket.calibration.selection.coverage))
        for bucket in normalized_buckets
    ]
    if len(set(keys)) != len(keys):
        raise ForecastCalibrationSetValidationError(
            "calibration set contains a duplicate horizon/coverage bucket"
        )
    ordered = tuple(
        sorted(
            normalized_buckets,
            key=lambda bucket: (
                bucket.horizon,
                _coverage_millis(bucket.calibration.selection.coverage),
            ),
        )
    )
    return FittedCalibrationSet(
        schema_version=calibration_set.schema_version,
        model_version=model_version,
        method=method,
        window_start=window_start,
        window_end=window_end,
        sample_count=sample_count,
        cohort_id=_sha256(calibration_set.cohort_id, "cohort_id"),
        selection_policy_hash=_sha256(
            calibration_set.selection_policy_hash,
            "selection_policy_hash",
        ),
        outcome_resolution_policy_hash=_sha256(
            calibration_set.outcome_resolution_policy_hash,
            "outcome_resolution_policy_hash",
        ),
        availability_rule_set_hash=_sha256(
            calibration_set.availability_rule_set_hash,
            "availability_rule_set_hash",
        ),
        buckets=ordered,
    )


def _normalized_bucket(
    bucket: FittedCalibrationBucket,
    *,
    method: CalibrationMethod,
    sample_count: int,
) -> FittedCalibrationBucket:
    if not isinstance(bucket, FittedCalibrationBucket):
        raise ForecastCalibrationSetValidationError("calibration buckets have the wrong type")
    horizon = bucket.horizon
    if type(horizon) is not int or not 1 <= horizon <= 252:
        raise ForecastCalibrationSetValidationError(
            "calibration bucket horizon must be within 1..252"
        )
    expected_type = _METHOD_TYPES[method]
    if not isinstance(bucket.calibration, expected_type):
        raise ForecastCalibrationSetValidationError(
            "calibration bucket type does not match the set method"
        )
    selection = bucket.calibration.selection
    if not isinstance(selection, QuantileSelection):
        raise ForecastCalibrationSetValidationError("calibration bucket has no fitted selection")
    if not 1 <= selection.sample_count <= sample_count:
        raise ForecastCalibrationSetValidationError(
            "calibration bucket fit sample count is outside the set sample count"
        )
    return FittedCalibrationBucket(horizon=horizon, calibration=bucket.calibration)


def _bucket(
    *,
    horizon: object,
    coverage_millis: object,
    fit_sample_count: object,
    rank: object,
    value: object,
    method: CalibrationMethod,
    label: str,
) -> FittedCalibrationBucket:
    millis = _integer(coverage_millis, f"{label}.coverage_millis")
    if not 1 <= millis <= 999:
        raise ForecastCalibrationSetValidationError(
            f"{label}.coverage_millis must be within 1..999"
        )
    correction_type = _METHOD_TYPES[method]
    try:
        selection = QuantileSelection(
            coverage=millis / 1000,
            sample_count=_positive_integer(fit_sample_count, f"{label}.fit_sample_count"),
            rank=_positive_integer(rank, f"{label}.rank"),
            value=_number(value, f"{label}.value"),
        )
        correction = correction_type(selection=selection)
    except (ConformalValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationSetValidationError(
            f"{label} is not a valid fitted conformal correction"
        ) from exc
    return FittedCalibrationBucket(
        horizon=_integer(horizon, f"{label}.horizon"),
        calibration=correction,
    )


def _method(value: object) -> CalibrationMethod:
    if not isinstance(value, str) or value not in _METHOD_TYPES:
        raise ForecastCalibrationSetValidationError("calibration method is not supported")
    return value  # type: ignore[return-value]


def _model_version(value: object) -> str:
    if not isinstance(value, str):
        raise ForecastCalibrationSetValidationError("model_version must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or len(normalized) > MODEL_VERSION_MAX_LENGTH
        or any(0xD800 <= ord(character) <= 0xDFFF for character in normalized)
    ):
        raise ForecastCalibrationSetValidationError("model_version is empty, too long, or invalid")
    return normalized


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationSetValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ForecastCalibrationSetValidationError(f"{label} must be an integer")
    return value


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ForecastCalibrationSetValidationError(f"{label} must be a positive integer")
    return value


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastCalibrationSetValidationError(f"{label} must be numeric")
    converted = float(value)
    if not math.isfinite(converted):
        raise ForecastCalibrationSetValidationError(f"{label} must be finite")
    return converted


def _canonical_float(value: object, label: str) -> float:
    converted = _number(value, label)
    return 0.0 if converted == 0.0 else converted


def _coverage_millis(value: float) -> int:
    converted = round(value * 1000)
    if abs(value * 1000 - converted) > 1e-9 or not 1 <= converted <= 999:
        raise ForecastCalibrationSetValidationError("coverage cannot be represented canonically")
    return converted


def _date(value: object, label: str) -> date:
    # ``datetime`` subclasses ``date``; a fitted window is a pure calendar date.
    if type(value) is not date or isinstance(value, datetime):
        raise ForecastCalibrationSetValidationError(f"{label} must be a calendar date")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str) or _DATE_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationSetValidationError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ForecastCalibrationSetValidationError(f"{label} is not a valid date") from exc
    if parsed.isoformat() != value:
        raise ForecastCalibrationSetValidationError(f"{label} must be a canonical ISO date")
    return parsed


def _bounded_bytes(value: object) -> bytes:
    if not isinstance(value, bytes) or not value or len(value) > MAX_CANONICAL_BYTES:
        raise ForecastCalibrationSetValidationError(
            "canonical fitted calibration set must be non-empty bounded bytes"
        )
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForecastCalibrationSetValidationError(
                "canonical fitted calibration set contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ForecastCalibrationSetValidationError(f"JSON constant {value!r} is not permitted")


__all__ = [
    "CALIBRATION_SET_FORMAT",
    "CALIBRATION_SET_SCHEMA_VERSION",
    "CalibrationMethod",
    "FittedCalibrationBucket",
    "FittedCalibrationSet",
    "ForecastCalibrationSetValidationError",
    "canonical_calibration_set",
    "calibration_set_version_for",
    "parse_calibration_set",
]
