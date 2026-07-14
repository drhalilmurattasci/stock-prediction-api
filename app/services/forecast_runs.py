"""Pure canonical identities for persisted forecast runs.

The run store must be able to prove three different facts without trusting a
database row's denormalized columns:

* which normalized request was accepted;
* which complete, schema-valid response was returned; and
* which value-free forecast opportunity the response belongs to.

Each identity uses its own versioned JSON format.  Parsers reject duplicate
keys, unknown fields, non-finite numbers, and non-canonical encodings before a
payload can be replayed.  This module is deliberately independent of SQL so it
can be used on both sides of a repository boundary.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from uuid import UUID

from pydantic import ValidationError

from app.schemas.forecast import ForecastRequest, ForecastResponse

RUN_SCHEMA_VERSION = 1
REQUEST_FORMAT = "forecast-run-request-v1"
OUTPUT_FORMAT = "forecast-run-output-v1"
OPPORTUNITY_FORMAT = "forecast-opportunity-v1"
MAX_CANONICAL_BYTES = 4 * 1024 * 1024

_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_ORIGIN_KIND_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_IDEMPOTENCY_DOMAIN = b"stockapi.forecast-run.idempotency.v1\x00"

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


class ForecastRunValidationError(ValueError):
    """Canonical run material is malformed, tampered with, or unsupported."""


def canonical_request(request: ForecastRequest) -> bytes:
    """Return normalized, versioned canonical bytes for ``ForecastRequest``."""

    normalized = _revalidate_request(request)
    payload = normalized.model_dump(mode="python", round_trip=True)
    payload["interval_coverages"] = sorted(payload["interval_coverages"])
    return _canonical_document(REQUEST_FORMAT, payload)


def parse_request(canonical_payload: bytes) -> ForecastRequest:
    """Parse canonical request bytes and independently rerun schema validation."""

    payload = _parse_document(canonical_payload, REQUEST_FORMAT)
    try:
        request = ForecastRequest.model_validate(payload)
    except ValidationError as exc:
        raise ForecastRunValidationError(
            "canonical request payload fails ForecastRequest validation"
        ) from exc
    if canonical_request(request) != canonical_payload:
        raise ForecastRunValidationError("request payload bytes are not canonical")
    return request


def request_hash(request_or_payload: ForecastRequest | bytes) -> str:
    """Hash a request model or already-canonical, strictly revalidated bytes."""

    canonical = (
        canonical_request(request_or_payload)
        if isinstance(request_or_payload, ForecastRequest)
        else canonical_request(parse_request(request_or_payload))
    )
    return _sha256(canonical)


def canonical_output(response: ForecastResponse) -> bytes:
    """Return normalized, versioned canonical bytes for ``ForecastResponse``."""

    normalized = _revalidate_response(response)
    payload = normalized.model_dump(mode="python", round_trip=True)
    _normalize_output_collections(payload)
    return _canonical_document(OUTPUT_FORMAT, payload)


def parse_output(canonical_payload: bytes) -> ForecastResponse:
    """Parse canonical output bytes and independently rerun all response invariants."""

    payload = _parse_document(canonical_payload, OUTPUT_FORMAT)
    try:
        response = ForecastResponse.model_validate(payload)
    except ValidationError as exc:
        raise ForecastRunValidationError(
            "canonical output payload fails ForecastResponse validation"
        ) from exc
    if canonical_output(response) != canonical_payload:
        raise ForecastRunValidationError("output payload bytes are not canonical")
    return response


def output_hash(response_or_payload: ForecastResponse | bytes) -> str:
    """Hash a response model or already-canonical, strictly revalidated bytes."""

    canonical = (
        canonical_output(response_or_payload)
        if isinstance(response_or_payload, ForecastResponse)
        else canonical_output(parse_output(response_or_payload))
    )
    return _sha256(canonical)


def opportunity_manifest(
    response: ForecastResponse,
    *,
    resolution_policy_hash: str,
    availability_rule_set_hash: str,
    origin_kind: str,
) -> bytes:
    """Build the canonical value-free identity of one forecast opportunity.

    The manifest binds the resolved snapshot, model/code identity, policy
    versions, calibration identity, target timestamps, and emitted interval
    coverages.  It intentionally excludes the forecast UUID, generation/check
    timestamps, point/quantile/bound values, and empirical calibration values.
    Those remain covered by :func:`output_hash`, so divergent results for the
    same opportunity are detectable without minting a new opportunity.
    """

    normalized = _revalidate_response(response)
    policy_hash = _canonical_sha256(resolution_policy_hash, "resolution_policy_hash")
    rule_set_hash = _canonical_sha256(
        availability_rule_set_hash,
        "availability_rule_set_hash",
    )
    origin = _canonical_origin_kind(origin_kind)
    provenance = normalized.provenance
    calibration = normalized.calibration

    targets = [
        {
            "interval_coverages_millis": sorted(
                _coverage_millis(interval.coverage) for interval in step.intervals
            ),
            "step": step.step,
            "target_time": step.target_time,
        }
        for step in normalized.forecasts
    ]
    calibration_buckets = sorted(
        (
            {
                "horizon": row.horizon,
                "nominal_coverage_millis": _coverage_millis(row.nominal_coverage),
                "sample_count": row.sample_count,
            }
            for row in calibration.by_interval
        ),
        key=lambda row: (row["horizon"], row["nominal_coverage_millis"]),
    )
    payload: dict[str, object] = {
        "as_of": normalized.as_of,
        "availability_rule_set_hash": rule_set_hash,
        "calibration": {
            "buckets": calibration_buckets,
            "calibration_set_version": calibration.calibration_set_version,
            "method": calibration.method,
            "sample_count": calibration.sample_count,
            "window_end": calibration.window_end,
            "window_start": calibration.window_start,
        },
        "code_version": provenance.code_version,
        "currency": normalized.currency,
        "feature_set_hash": _normalized_feature_hash(provenance.feature_set_hash),
        "horizon": normalized.horizon,
        "horizon_unit": normalized.horizon_unit,
        "lookahead_status": provenance.lookahead_check.status,
        "max_available_at": provenance.max_available_at,
        "model_version": provenance.model_version,
        "origin_kind": origin,
        "resolution_policy_hash": policy_hash,
        "series_basis": provenance.series_basis,
        "snapshot_id": provenance.snapshot_id,
        "symbol": normalized.symbol,
        "target": normalized.target,
        "targets": targets,
    }
    return _canonical_document(OPPORTUNITY_FORMAT, payload)


def opportunity_hash(
    response: ForecastResponse,
    *,
    resolution_policy_hash: str,
    availability_rule_set_hash: str,
    origin_kind: str,
) -> str:
    """Return the SHA-256 identity for :func:`opportunity_manifest`."""

    return _sha256(
        opportunity_manifest(
            response,
            resolution_policy_hash=resolution_policy_hash,
            availability_rule_set_hash=availability_rule_set_hash,
            origin_kind=origin_kind,
        )
    )


def idempotency_digest(
    *,
    principal: str,
    idempotency_key: str,
    secret: str | bytes,
) -> str:
    """HMAC a credential identity and retry key without retaining either.

    The two UTF-8 values are independently length-framed, preventing ambiguous
    concatenations, and the format-specific domain prevents cross-protocol use
    of the same application secret. The resulting namespace is intentionally
    bound to that credential and secret epoch; durable key-rotation aliases
    require the planned stable API-principal registry.
    """

    principal_bytes = _identity_bytes(principal, "principal", max_length=512)
    key_bytes = _identity_bytes(idempotency_key, "idempotency_key", max_length=128)
    secret_bytes = _secret_bytes(secret)
    message = b"".join(
        (
            _IDEMPOTENCY_DOMAIN,
            len(principal_bytes).to_bytes(4, "big"),
            principal_bytes,
            len(key_bytes).to_bytes(4, "big"),
            key_bytes,
        )
    )
    digest = hmac.new(secret_bytes, message, hashlib.sha256).hexdigest()
    return f"hmac-sha256:{digest}"


def _revalidate_request(request: ForecastRequest) -> ForecastRequest:
    if not isinstance(request, ForecastRequest):
        raise TypeError("request must be a ForecastRequest")
    try:
        return ForecastRequest.model_validate(request.model_dump(mode="python", round_trip=True))
    except (ValidationError, ValueError, TypeError) as exc:
        raise ForecastRunValidationError("ForecastRequest fails revalidation") from exc


def _revalidate_response(response: ForecastResponse) -> ForecastResponse:
    if not isinstance(response, ForecastResponse):
        raise TypeError("response must be a ForecastResponse")
    try:
        return ForecastResponse.model_validate(response.model_dump(mode="python", round_trip=True))
    except (ValidationError, ValueError, TypeError) as exc:
        raise ForecastRunValidationError("ForecastResponse fails revalidation") from exc


def _normalize_output_collections(payload: dict[str, object]) -> None:
    forecasts = _dict_list(payload["forecasts"], "forecasts")
    for step in forecasts:
        step["quantiles"] = sorted(
            _dict_list(step["quantiles"], "quantiles"),
            key=lambda row: _number_key(row["level"], "quantile level"),
        )
        step["intervals"] = sorted(
            _dict_list(step["intervals"], "intervals"),
            key=lambda row: (
                _number_key(row["coverage"], "interval coverage"),
                _number_key(row["lower_quantile"], "lower quantile"),
                _number_key(row["upper_quantile"], "upper quantile"),
            ),
        )

    provenance = _dict_value(payload["provenance"], "provenance")
    provenance["feature_set_hash"] = _normalized_feature_hash(
        _string_value(provenance["feature_set_hash"], "feature_set_hash")
    )
    sources = _dict_list(provenance["data_sources"], "data_sources")
    for source in sources:
        source["fields"] = sorted(_string_list(source["fields"], "source fields"))
    provenance["data_sources"] = sorted(
        sources,
        key=lambda row: (
            _string_value(row["name"], "source name"),
            _string_value(row["snapshot_id"], "source snapshot_id"),
            _timestamp_key(row["max_available_at"]),
            tuple(_string_list(row["fields"], "source fields")),
        ),
    )
    lookahead = _dict_value(provenance["lookahead_check"], "lookahead_check")
    lookahead["violations"] = sorted(_string_list(lookahead["violations"], "lookahead violations"))

    calibration = _dict_value(payload["calibration"], "calibration")
    calibration["by_interval"] = sorted(
        _dict_list(calibration["by_interval"], "calibration by_interval"),
        key=lambda row: (
            _number_key(row["horizon"], "calibration horizon"),
            _number_key(row["nominal_coverage"], "calibration coverage"),
        ),
    )


def _canonical_document(format_name: str, payload: Mapping[str, object]) -> bytes:
    document = {
        "format": format_name,
        "payload": dict(payload),
        "schema_version": RUN_SCHEMA_VERSION,
    }
    normalized = _json_value(document)
    try:
        canonical = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise ForecastRunValidationError("run payload cannot be encoded canonically") from exc
    if not canonical or len(canonical) > MAX_CANONICAL_BYTES:
        raise ForecastRunValidationError("canonical run payload exceeds the storage limit")
    return canonical


def _parse_document(canonical_payload: bytes, expected_format: str) -> dict[str, object]:
    if (
        not isinstance(canonical_payload, bytes)
        or not canonical_payload
        or len(canonical_payload) > MAX_CANONICAL_BYTES
    ):
        raise ForecastRunValidationError("canonical payload must be non-empty bounded bytes")
    try:
        document = json.loads(
            canonical_payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ForecastRunValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise ForecastRunValidationError("canonical payload is not strict UTF-8 JSON") from exc
    root = _dict_value(document, "document")
    if set(root) != {"format", "payload", "schema_version"}:
        raise ForecastRunValidationError("canonical document has unknown or missing keys")
    if root["format"] != expected_format:
        raise ForecastRunValidationError("canonical document format is not supported")
    if type(root["schema_version"]) is not int or root["schema_version"] != RUN_SCHEMA_VERSION:
        raise ForecastRunValidationError("canonical document schema_version is not supported")
    return _dict_value(root["payload"], "payload")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForecastRunValidationError("canonical payload contains a duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ForecastRunValidationError(f"JSON constant {value!r} is not permitted")


def _json_value(value: object) -> JsonValue:
    if value is None or type(value) in {bool, int}:
        return value  # type: ignore[return-value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ForecastRunValidationError("canonical payload numbers must be finite")
        return 0.0 if value == 0.0 else value
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value)
        if any(0xD800 <= ord(character) <= 0xDFFF for character in normalized):
            raise ForecastRunValidationError("canonical payload contains invalid Unicode")
        return normalized
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ForecastRunValidationError("canonical object keys must be strings")
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in result:
                raise ForecastRunValidationError(
                    "canonical object keys collide after normalization"
                )
            result[normalized_key] = _json_value(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    raise ForecastRunValidationError(f"unsupported canonical value type: {type(value).__name__}")


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ForecastRunValidationError("canonical timestamps must be timezone-aware")
    try:
        utc = value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise ForecastRunValidationError("timestamp cannot be normalized to UTC") from exc
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _timestamp_key(value: object) -> str:
    if not isinstance(value, datetime):
        raise ForecastRunValidationError("timestamp collection member is invalid")
    return _timestamp(value)


def _dict_value(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ForecastRunValidationError(f"{label} must be a JSON object")
    return value


def _dict_list(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ForecastRunValidationError(f"{label} must be a list of objects")
    return value


def _string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ForecastRunValidationError(f"{label} must be a list of strings")
    return value


def _string_value(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ForecastRunValidationError(f"{label} must be a string")
    return value


def _number_key(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastRunValidationError(f"{label} must be numeric")
    converted = float(value)
    if not math.isfinite(converted):
        raise ForecastRunValidationError(f"{label} must be finite")
    return converted


def _canonical_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastRunValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _normalized_feature_hash(value: str) -> str:
    prefixed = value if value.lower().startswith("sha256:") else f"sha256:{value}"
    normalized = prefixed.lower()
    if _SHA256_PATTERN.fullmatch(normalized) is None:
        raise ForecastRunValidationError("feature_set_hash is not a SHA-256 identity")
    return normalized


def _canonical_origin_kind(value: object) -> str:
    if not isinstance(value, str) or _ORIGIN_KIND_PATTERN.fullmatch(value) is None:
        raise ForecastRunValidationError("origin_kind is not canonical")
    return value


def _coverage_millis(value: float) -> int:
    converted = round(value * 1000)
    if abs(value * 1000 - converted) > 1e-9 or not 1 <= converted <= 999:
        raise ForecastRunValidationError("coverage cannot be represented canonically")
    return converted


def _identity_bytes(value: object, label: str, *, max_length: int) -> bytes:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if (
        not value
        or len(value) > max_length
        or any(0xD800 <= ord(character) <= 0xDFFF for character in value)
    ):
        raise ForecastRunValidationError(f"{label} is empty, too long, or invalid")
    # These are opaque identities, not semantic document text. Preserve their
    # exact code-point sequence so independently configured credentials cannot
    # collapse through Unicode normalization.
    return value.encode("utf-8")


def _secret_bytes(value: object) -> bytes:
    if isinstance(value, str):
        try:
            encoded = value.encode("utf-8")
        except UnicodeError as exc:
            raise ForecastRunValidationError("HMAC secret is not valid UTF-8") from exc
    elif isinstance(value, bytes):
        encoded = value
    else:
        raise TypeError("secret must be str or bytes")
    if not encoded:
        raise ForecastRunValidationError("HMAC secret must not be empty")
    return encoded


def _sha256(canonical_payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(canonical_payload).hexdigest()}"


__all__ = [
    "ForecastRunValidationError",
    "MAX_CANONICAL_BYTES",
    "OPPORTUNITY_FORMAT",
    "OUTPUT_FORMAT",
    "REQUEST_FORMAT",
    "RUN_SCHEMA_VERSION",
    "canonical_output",
    "canonical_request",
    "idempotency_digest",
    "opportunity_hash",
    "opportunity_manifest",
    "output_hash",
    "parse_output",
    "parse_request",
    "request_hash",
]
