"""Canonical realized-outcome evidence for archived forecasts.

An outcome is not an assertion that one mutable bar is permanently "truth".
It is a content-addressed statement that an explicitly identified resolution
policy selected one exact, post-commit-visible bar version at one explicit
cutoff.  Policy selection and SQL resolution live outside this module.

Version 1 is intentionally narrow: a nonnegative raw daily close backed by one
exact ``polygon_open_close`` bar version and its availability receipt.  That
source contract is what makes its stored session-close timestamp equal the
forecast target time.  A broader target, source, or derived value needs a new
format rather than a weaker interpretation of these bytes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import struct
import unicodedata
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from datetime import UTC, datetime
from numbers import Real

OUTCOME_SCHEMA_VERSION = 1
OUTCOME_FORMAT = "forecast-realized-outcome-v1"
MAX_CANONICAL_BYTES = 256 * 1024

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
_FLOAT_BITS_PATTERN = re.compile(r"^[0-9a-f]{16}$")

_ROOT_KEYS = {"format", "payload", "schema_version"}
_PAYLOAD_KEYS = {
    "availability_rule_set_hash",
    "currency",
    "outcome_resolution_policy_hash",
    "realized_value_f64",
    "resolution_cutoff",
    "series_basis",
    "source_version",
    "symbol",
    "target",
    "target_time",
}
_SOURCE_KEYS = {
    "adjustment_basis",
    "available_at",
    "fetched_at",
    "field",
    "multiplier",
    "observed_at",
    "source",
    "source_as_of",
    "symbol",
    "timespan",
    "value_f64",
    "version_recorded_at",
}


class OutcomeValidationError(ValueError):
    """Outcome material is malformed, tampered with, or unsupported."""


@dataclass(frozen=True)
class BarVersionEvidence:
    """One exact bar version plus its post-commit availability receipt."""

    symbol: str
    timespan: str
    multiplier: int
    observed_at: datetime
    source: str
    adjustment_basis: str
    fetched_at: datetime
    source_as_of: datetime
    version_recorded_at: datetime
    available_at: datetime
    field: str
    value: float


@dataclass(frozen=True)
class RealizedOutcomePayload:
    """Semantic evidence covered by ``outcome_id``.

    Both policy hashes and ``resolution_cutoff`` are mandatory.  This boundary
    therefore cannot silently choose a truth lag or trust an availability rule.
    """

    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    resolution_cutoff: datetime
    symbol: str
    target: str
    series_basis: str
    target_time: datetime
    currency: str
    realized_value: float
    source_version: BarVersionEvidence
    schema_version: int = OUTCOME_SCHEMA_VERSION


@dataclass(frozen=True)
class RealizedOutcomeRecord:
    """Storage row shape derived exactly from one canonical payload."""

    outcome_id: str
    schema_version: int
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    symbol: str
    target: str
    series_basis: str
    target_time: datetime
    currency: str
    resolution_cutoff: datetime
    bar_timespan: str
    bar_multiplier: int
    bar_observed_at: datetime
    bar_source: str
    bar_adjustment_basis: str
    bar_version_recorded_at: datetime
    bar_fetched_at: datetime
    bar_source_as_of: datetime
    bar_available_at: datetime
    bar_field: str
    bar_value: float
    realized_value: float
    sealed_at: datetime
    canonical_evidence: bytes


def canonical_outcome_payload(payload: RealizedOutcomePayload) -> bytes:
    """Return strict, versioned canonical UTF-8 JSON for one outcome."""

    normalized = _normalized_payload(payload)
    source = normalized.source_version
    document = {
        "format": OUTCOME_FORMAT,
        "payload": {
            "availability_rule_set_hash": normalized.availability_rule_set_hash,
            "currency": normalized.currency,
            "outcome_resolution_policy_hash": (normalized.outcome_resolution_policy_hash),
            "realized_value_f64": _float_bits(normalized.realized_value),
            "resolution_cutoff": _timestamp(normalized.resolution_cutoff),
            "series_basis": normalized.series_basis,
            "source_version": {
                "adjustment_basis": source.adjustment_basis,
                "available_at": _timestamp(source.available_at),
                "fetched_at": _timestamp(source.fetched_at),
                "field": source.field,
                "multiplier": source.multiplier,
                "observed_at": _timestamp(source.observed_at),
                "source": source.source,
                "source_as_of": _timestamp(source.source_as_of),
                "symbol": source.symbol,
                "timespan": source.timespan,
                "value_f64": _float_bits(source.value),
                "version_recorded_at": _timestamp(source.version_recorded_at),
            },
            "symbol": normalized.symbol,
            "target": normalized.target,
            "target_time": _timestamp(normalized.target_time),
        },
        "schema_version": normalized.schema_version,
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
        raise OutcomeValidationError("outcome payload cannot be encoded canonically") from exc
    if not canonical or len(canonical) > MAX_CANONICAL_BYTES:
        raise OutcomeValidationError("canonical outcome payload exceeds the storage limit")
    return canonical


def outcome_id_for_payload(canonical_payload: bytes) -> str:
    """Return the content identity of strictly validated canonical bytes."""

    canonical = canonical_outcome_payload(parse_outcome_payload(canonical_payload))
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def build_outcome_record(
    payload: RealizedOutcomePayload,
    *,
    sealed_at: datetime,
) -> RealizedOutcomeRecord:
    """Build the exact immutable row header and canonical payload."""

    normalized = _normalized_payload(payload)
    sealed = _utc(sealed_at, "sealed_at")
    if sealed < normalized.resolution_cutoff:
        raise OutcomeValidationError("sealed_at must not be earlier than resolution_cutoff")
    canonical = canonical_outcome_payload(normalized)
    source = normalized.source_version
    return RealizedOutcomeRecord(
        outcome_id=f"sha256:{hashlib.sha256(canonical).hexdigest()}",
        schema_version=normalized.schema_version,
        outcome_resolution_policy_hash=normalized.outcome_resolution_policy_hash,
        availability_rule_set_hash=normalized.availability_rule_set_hash,
        symbol=normalized.symbol,
        target=normalized.target,
        series_basis=normalized.series_basis,
        target_time=normalized.target_time,
        currency=normalized.currency,
        resolution_cutoff=normalized.resolution_cutoff,
        bar_timespan=source.timespan,
        bar_multiplier=source.multiplier,
        bar_observed_at=source.observed_at,
        bar_source=source.source,
        bar_adjustment_basis=source.adjustment_basis,
        bar_version_recorded_at=source.version_recorded_at,
        bar_fetched_at=source.fetched_at,
        bar_source_as_of=source.source_as_of,
        bar_available_at=source.available_at,
        bar_field=source.field,
        bar_value=source.value,
        realized_value=normalized.realized_value,
        sealed_at=sealed,
        canonical_evidence=canonical,
    )


def parse_outcome_payload(canonical_payload: bytes) -> RealizedOutcomePayload:
    """Parse only exact canonical bytes, rejecting duplicate or unknown keys."""

    if (
        not isinstance(canonical_payload, bytes)
        or not canonical_payload
        or len(canonical_payload) > MAX_CANONICAL_BYTES
    ):
        raise OutcomeValidationError("canonical payload must be non-empty bounded bytes")
    try:
        document = json.loads(
            canonical_payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except OutcomeValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise OutcomeValidationError("canonical payload is not strict UTF-8 JSON") from exc

    root = _object(document, "document")
    _exact_keys(root, _ROOT_KEYS, "document")
    if _string(root["format"], "format") != OUTCOME_FORMAT:
        raise OutcomeValidationError("outcome format is not supported")
    schema_version = _integer(root["schema_version"], "schema_version")

    row = _object(root["payload"], "payload")
    _exact_keys(row, _PAYLOAD_KEYS, "payload")
    source_row = _object(row["source_version"], "source_version")
    _exact_keys(source_row, _SOURCE_KEYS, "source_version")
    source = BarVersionEvidence(
        symbol=_string(source_row["symbol"], "source_version.symbol"),
        timespan=_string(source_row["timespan"], "source_version.timespan"),
        multiplier=_integer(source_row["multiplier"], "source_version.multiplier"),
        observed_at=_parse_timestamp(
            source_row["observed_at"],
            "source_version.observed_at",
        ),
        source=_string(source_row["source"], "source_version.source"),
        adjustment_basis=_string(
            source_row["adjustment_basis"],
            "source_version.adjustment_basis",
        ),
        fetched_at=_parse_timestamp(
            source_row["fetched_at"],
            "source_version.fetched_at",
        ),
        source_as_of=_parse_timestamp(
            source_row["source_as_of"],
            "source_version.source_as_of",
        ),
        version_recorded_at=_parse_timestamp(
            source_row["version_recorded_at"],
            "source_version.version_recorded_at",
        ),
        available_at=_parse_timestamp(
            source_row["available_at"],
            "source_version.available_at",
        ),
        field=_string(source_row["field"], "source_version.field"),
        value=_parse_float_bits(source_row["value_f64"], "source_version.value_f64"),
    )
    payload = RealizedOutcomePayload(
        schema_version=schema_version,
        outcome_resolution_policy_hash=_string(
            row["outcome_resolution_policy_hash"],
            "outcome_resolution_policy_hash",
        ),
        availability_rule_set_hash=_string(
            row["availability_rule_set_hash"],
            "availability_rule_set_hash",
        ),
        resolution_cutoff=_parse_timestamp(
            row["resolution_cutoff"],
            "resolution_cutoff",
        ),
        symbol=_string(row["symbol"], "symbol"),
        target=_string(row["target"], "target"),
        series_basis=_string(row["series_basis"], "series_basis"),
        target_time=_parse_timestamp(row["target_time"], "target_time"),
        currency=_string(row["currency"], "currency"),
        realized_value=_parse_float_bits(row["realized_value_f64"], "realized_value_f64"),
        source_version=source,
    )
    normalized = _normalized_payload(payload)
    if canonical_outcome_payload(normalized) != canonical_payload:
        raise OutcomeValidationError("outcome payload bytes are not canonical")
    return normalized


def validate_outcome_record(
    record: RealizedOutcomeRecord,
    *,
    expected_outcome_resolution_policy_hash: str,
    expected_availability_rule_set_hash: str,
) -> RealizedOutcomePayload:
    """Verify bytes, digest, denormalized header, and explicit policy trust."""

    if not isinstance(record, RealizedOutcomeRecord):
        raise TypeError("record must be a RealizedOutcomeRecord")
    expected_policy = _hash(
        expected_outcome_resolution_policy_hash,
        "expected_outcome_resolution_policy_hash",
    )
    expected_rules = _hash(
        expected_availability_rule_set_hash,
        "expected_availability_rule_set_hash",
    )
    payload = parse_outcome_payload(record.canonical_evidence)
    actual_id = f"sha256:{hashlib.sha256(record.canonical_evidence).hexdigest()}"
    _hash(record.outcome_id, "outcome_id")
    if not hmac.compare_digest(record.outcome_id, actual_id):
        raise OutcomeValidationError("outcome_id does not match canonical_payload")
    expected_record = build_outcome_record(payload, sealed_at=record.sealed_at)
    if not _strict_record_equal(record, expected_record):
        raise OutcomeValidationError("outcome header does not match canonical_payload")
    if not hmac.compare_digest(payload.outcome_resolution_policy_hash, expected_policy):
        raise OutcomeValidationError("outcome resolution policy is not trusted")
    if not hmac.compare_digest(payload.availability_rule_set_hash, expected_rules):
        raise OutcomeValidationError("outcome availability rule set is not trusted")
    return payload


def _normalized_payload(payload: RealizedOutcomePayload) -> RealizedOutcomePayload:
    if not isinstance(payload, RealizedOutcomePayload):
        raise TypeError("payload must be a RealizedOutcomePayload")
    if type(payload.schema_version) is not int or payload.schema_version != OUTCOME_SCHEMA_VERSION:
        raise OutcomeValidationError("outcome schema_version is not supported")
    policy_hash = _hash(
        payload.outcome_resolution_policy_hash,
        "outcome_resolution_policy_hash",
    )
    rule_set_hash = _hash(payload.availability_rule_set_hash, "availability_rule_set_hash")
    symbol = _text(payload.symbol, "symbol", max_length=32)
    if symbol != symbol.upper() or _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise OutcomeValidationError("symbol must be uppercase and canonical")
    if payload.target != "close":
        raise OutcomeValidationError("outcome v1 supports raw close only")
    if payload.series_basis != "raw":
        raise OutcomeValidationError("outcome v1 supports raw close only")
    currency = _text(payload.currency, "currency", max_length=3)
    if _CURRENCY_PATTERN.fullmatch(currency) is None or currency != "USD":
        raise OutcomeValidationError("outcome v1 currency must be USD")
    target_time = _utc(payload.target_time, "target_time")
    cutoff = _utc(payload.resolution_cutoff, "resolution_cutoff")
    realized = _finite(payload.realized_value, "realized_value")
    if realized < 0.0:
        raise OutcomeValidationError("realized close must be nonnegative")

    source = _normalized_source(payload.source_version)
    if source.symbol != symbol:
        raise OutcomeValidationError("source version symbol does not match outcome")
    if source.observed_at != target_time:
        raise OutcomeValidationError("source version timestamp does not match target_time")
    if source.adjustment_basis != payload.series_basis:
        raise OutcomeValidationError("source version basis does not match outcome")
    if _float_bits(source.value) != _float_bits(realized):
        raise OutcomeValidationError("source version value does not match realized_value")
    if source.available_at > cutoff:
        raise OutcomeValidationError("source version was not available by resolution_cutoff")

    return RealizedOutcomePayload(
        schema_version=OUTCOME_SCHEMA_VERSION,
        outcome_resolution_policy_hash=policy_hash,
        availability_rule_set_hash=rule_set_hash,
        resolution_cutoff=cutoff,
        symbol=symbol,
        target="close",
        series_basis="raw",
        target_time=target_time,
        currency=currency,
        realized_value=realized,
        source_version=source,
    )


def _normalized_source(source: BarVersionEvidence) -> BarVersionEvidence:
    if not isinstance(source, BarVersionEvidence):
        raise OutcomeValidationError("source_version has the wrong type")
    symbol = _text(source.symbol, "source_version.symbol", max_length=32)
    if symbol != symbol.upper() or _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise OutcomeValidationError("source version symbol must be uppercase and canonical")
    if source.timespan != "day" or type(source.multiplier) is not int or source.multiplier != 1:
        raise OutcomeValidationError("outcome v1 requires one-day source bars")
    if source.adjustment_basis != "raw" or source.field != "close":
        raise OutcomeValidationError("outcome v1 requires the raw close source field")
    source_name = _text(source.source, "source_version.source", max_length=64)
    if source_name != "polygon_open_close":
        raise OutcomeValidationError("outcome v1 requires the polygon_open_close source contract")
    observed_at = _utc(source.observed_at, "source_version.observed_at")
    fetched_at = _utc(source.fetched_at, "source_version.fetched_at")
    source_as_of = _utc(source.source_as_of, "source_version.source_as_of")
    version_recorded_at = _utc(
        source.version_recorded_at,
        "source_version.version_recorded_at",
    )
    available_at = _utc(source.available_at, "source_version.available_at")
    if not (observed_at <= fetched_at <= source_as_of <= version_recorded_at <= available_at):
        raise OutcomeValidationError("source version availability timestamps are out of order")
    value = _finite(source.value, "source_version.value")
    if value < 0.0:
        raise OutcomeValidationError("source close must be nonnegative")
    return BarVersionEvidence(
        symbol=symbol,
        timespan="day",
        multiplier=1,
        observed_at=observed_at,
        source=source_name,
        adjustment_basis="raw",
        fetched_at=fetched_at,
        source_as_of=source_as_of,
        version_recorded_at=version_recorded_at,
        available_at=available_at,
        field="close",
        value=value,
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise OutcomeValidationError("canonical payload contains a duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise OutcomeValidationError(f"JSON constant {value!r} is not permitted")


def _exact_keys(value: dict[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise OutcomeValidationError(f"{label} has missing or unknown keys")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise OutcomeValidationError(f"{label} must be a JSON object")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise OutcomeValidationError(f"{label} must be a string")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise OutcomeValidationError(f"{label} must be an integer")
    return value


def _parse_timestamp(value: object, label: str) -> datetime:
    raw = _string(value, label)
    try:
        iso_value = raw.removesuffix("Z") + "+00:00" if raw.endswith("Z") else raw
        parsed = datetime.fromisoformat(iso_value)
    except (ValueError, OverflowError) as exc:
        raise OutcomeValidationError(f"{label} must be an ISO timestamp") from exc
    return _utc(parsed, label)


def _parse_float_bits(value: object, label: str) -> float:
    raw = _string(value, label)
    if _FLOAT_BITS_PATTERN.fullmatch(raw) is None:
        raise OutcomeValidationError(f"{label} must be 16 lowercase hexadecimal digits")
    number = struct.unpack(">d", bytes.fromhex(raw))[0]
    return _finite(number, label)


def _strict_record_equal(
    actual: RealizedOutcomeRecord,
    expected: RealizedOutcomeRecord,
) -> bool:
    return all(
        type(getattr(actual, field.name)) is type(getattr(expected, field.name))
        and getattr(actual, field.name) == getattr(expected, field.name)
        for field in dataclass_fields(RealizedOutcomeRecord)
    )


def _hash(value: object, label: str) -> str:
    if not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None:
        raise OutcomeValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _text(value: object, label: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise OutcomeValidationError(f"{label} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized.strip() != normalized
        or len(normalized) > max_length
        or any(0xD800 <= ord(character) <= 0xDFFF for character in normalized)
    ):
        raise OutcomeValidationError(f"{label} must be a non-empty trimmed string")
    return normalized


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OutcomeValidationError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise OutcomeValidationError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise OutcomeValidationError(f"{label} cannot be normalized to UTC") from exc


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise OutcomeValidationError(f"{label} must be a finite real number")
    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OutcomeValidationError(f"{label} must be a finite real number") from exc
    if not math.isfinite(converted):
        raise OutcomeValidationError(f"{label} must be finite")
    return 0.0 if converted == 0.0 else converted


def _float_bits(value: float) -> str:
    return struct.pack(">d", _finite(value, "value")).hex()


__all__ = [
    "BarVersionEvidence",
    "MAX_CANONICAL_BYTES",
    "OUTCOME_FORMAT",
    "OUTCOME_SCHEMA_VERSION",
    "OutcomeValidationError",
    "RealizedOutcomePayload",
    "RealizedOutcomeRecord",
    "build_outcome_record",
    "canonical_outcome_payload",
    "outcome_id_for_payload",
    "parse_outcome_payload",
    "validate_outcome_record",
]
