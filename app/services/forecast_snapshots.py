"""Canonical, fail-closed forecast-input snapshot boundary.

The database row stores the exact canonical bytes hashed by ``snapshot_id``.
This module independently parses, canonicalizes, hashes, and binds those bytes
to a request before producing :class:`ResolvedForecastInput`. Timestamp-shape
validation alone is not treated as proof that upstream availability semantics
were correct: ``availability_verified`` is true only for a persisted rule-set
hash explicitly trusted by the caller.
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
from typing import Literal, Protocol, runtime_checkable

from app.schemas.forecast import DataSourceLineage, ForecastRequest
from app.services.forecasting import (
    ForecastObservation,
    ResolvedForecastInput,
)

SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_FORMAT = "forecast-input-snapshot-v1"
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_FLOAT_BITS_PATTERN = re.compile(r"^[0-9a-f]{16}$")
_TARGETS = frozenset({"close", "adjusted_close", "return", "log_return"})
_PRICE_TARGETS = frozenset({"close", "adjusted_close"})
_HORIZON_UNITS = frozenset({"trading_day", "calendar_day", "minute", "hour", "week"})
_SERIES_BASES = frozenset({"raw", "split_adjusted", "split_dividend_adjusted"})
_INPUT_TIMESPANS = frozenset({"minute", "hour", "day", "week"})
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
_MAX_OBSERVATIONS = 10_000
_MAX_TARGET_TIMES = 252
_MAX_SOURCES = 64
_MAX_SOURCE_FIELDS = 128
_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024


class SnapshotValidationError(ValueError):
    """A snapshot is malformed, tampered with, or mismatched to its request."""


@dataclass(frozen=True)
class SnapshotObservation:
    observed_at: datetime
    available_at: datetime
    value: float


@dataclass(frozen=True)
class SnapshotSourceLineage:
    name: str
    snapshot_id: str
    max_available_at: datetime
    fields: tuple[str, ...]


@dataclass(frozen=True)
class SnapshotAvailabilityEvidence:
    """Persisted proof identity; ``checked_at`` must be stable across retries."""

    status: Literal["not_run", "passed"]
    rule_set_hash: str | None = None
    checked_at: datetime | None = None


@dataclass(frozen=True)
class ForecastInputSnapshotPayload:
    """Semantic inputs covered by the content hash.

    ``resolution_policy_hash`` identifies every builder rule not otherwise a
    scalar field, including source-selection, calendar version, and whether an
    availability verifier/rule set is required. A retry reuses the persisted
    availability proof identity and timestamp; it does not mint a new clock
    value for the same semantic snapshot.
    """

    resolution_policy_hash: str
    symbol: str
    target: str
    horizon_unit: str
    series_basis: str
    input_timespan: str
    input_multiplier: int
    as_of: datetime
    currency: str | None
    observations: tuple[SnapshotObservation, ...]
    target_times: tuple[datetime, ...]
    data_sources: tuple[SnapshotSourceLineage, ...]
    availability: SnapshotAvailabilityEvidence
    schema_version: int = SNAPSHOT_SCHEMA_VERSION


@dataclass(frozen=True)
class ForecastInputSnapshotRecord:
    """Database row shape used at the pure validation boundary."""

    snapshot_id: str
    schema_version: int
    resolution_policy_hash: str
    symbol: str
    target: str
    horizon_unit: str
    series_basis: str
    input_timespan: str
    input_multiplier: int
    as_of: datetime
    sealed_at: datetime
    currency: str | None
    observation_count: int
    target_time_count: int
    first_observed_at: datetime
    last_observed_at: datetime
    max_available_at: datetime
    availability_status: str
    availability_rule_set_hash: str | None
    availability_checked_at: datetime | None
    canonical_payload: bytes


@dataclass(frozen=True)
class ForecastInputSnapshotSelector:
    resolution_policy_hash: str
    symbol: str
    target: str
    horizon_unit: str
    series_basis: str
    input_timespan: str
    input_multiplier: int
    cutoff: datetime | None


@runtime_checkable
class ForecastInputSnapshotRepository(Protocol):
    """Read-only store seam; snapshot creation is a separate privileged lane."""

    async def get(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None: ...

    async def latest(
        self,
        selector: ForecastInputSnapshotSelector,
    ) -> ForecastInputSnapshotRecord | None: ...


@runtime_checkable
class ForecastInputResolver(Protocol):
    async def resolve(self, request: ForecastRequest) -> ResolvedForecastInput: ...


def canonical_snapshot_payload(payload: ForecastInputSnapshotPayload) -> bytes:
    """Return versioned canonical UTF-8 JSON bytes for a semantic payload."""

    normalized = _normalized_payload(payload)
    document = {
        "as_of": _timestamp(normalized.as_of),
        "availability": {
            "checked_at": (
                _timestamp(normalized.availability.checked_at)
                if normalized.availability.checked_at is not None
                else None
            ),
            "rule_set_hash": normalized.availability.rule_set_hash,
            "status": normalized.availability.status,
        },
        "currency": normalized.currency,
        "data_sources": [
            {
                "fields": list(source.fields),
                "max_available_at": _timestamp(source.max_available_at),
                "name": source.name,
                "snapshot_id": source.snapshot_id,
            }
            for source in normalized.data_sources
        ],
        "format": SNAPSHOT_FORMAT,
        "horizon_unit": normalized.horizon_unit,
        "input_multiplier": normalized.input_multiplier,
        "input_timespan": normalized.input_timespan,
        "observations": [
            {
                "available_at": _timestamp(observation.available_at),
                "observed_at": _timestamp(observation.observed_at),
                "value_f64": _float_bits(observation.value),
            }
            for observation in normalized.observations
        ],
        "resolution_policy_hash": normalized.resolution_policy_hash,
        "schema_version": normalized.schema_version,
        "series_basis": normalized.series_basis,
        "symbol": normalized.symbol,
        "target": normalized.target,
        "target_times": [_timestamp(value) for value in normalized.target_times],
    }
    try:
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise SnapshotValidationError("snapshot payload cannot be encoded canonically") from exc
    if len(canonical) > _MAX_PAYLOAD_BYTES:
        raise SnapshotValidationError("canonical_payload exceeds the storage limit")
    return canonical


def snapshot_id_for_payload(canonical_payload: bytes) -> str:
    if (
        not isinstance(canonical_payload, bytes)
        or not canonical_payload
        or len(canonical_payload) > _MAX_PAYLOAD_BYTES
    ):
        raise SnapshotValidationError("canonical_payload must be non-empty bytes")
    return f"sha256:{hashlib.sha256(canonical_payload).hexdigest()}"


def build_snapshot_record(
    payload: ForecastInputSnapshotPayload,
    *,
    sealed_at: datetime,
) -> ForecastInputSnapshotRecord:
    """Build the exact insert/read header from one canonical semantic payload."""

    normalized = _normalized_payload(payload)
    sealed = _utc(sealed_at, "sealed_at")
    if sealed < normalized.as_of:
        raise SnapshotValidationError("sealed_at must not be earlier than as_of")
    if (
        normalized.availability.checked_at is not None
        and normalized.availability.checked_at > sealed
    ):
        raise SnapshotValidationError("availability checked_at must not be later than sealed_at")
    canonical = canonical_snapshot_payload(normalized)
    max_available_at = max(
        [item.available_at for item in normalized.observations]
        + [source.max_available_at for source in normalized.data_sources]
    )
    return ForecastInputSnapshotRecord(
        snapshot_id=snapshot_id_for_payload(canonical),
        schema_version=normalized.schema_version,
        resolution_policy_hash=normalized.resolution_policy_hash,
        symbol=normalized.symbol,
        target=normalized.target,
        horizon_unit=normalized.horizon_unit,
        series_basis=normalized.series_basis,
        input_timespan=normalized.input_timespan,
        input_multiplier=normalized.input_multiplier,
        as_of=normalized.as_of,
        sealed_at=sealed,
        currency=normalized.currency,
        observation_count=len(normalized.observations),
        target_time_count=len(normalized.target_times),
        first_observed_at=normalized.observations[0].observed_at,
        last_observed_at=normalized.observations[-1].observed_at,
        max_available_at=max_available_at,
        availability_status=normalized.availability.status,
        availability_rule_set_hash=normalized.availability.rule_set_hash,
        availability_checked_at=normalized.availability.checked_at,
        canonical_payload=canonical,
    )


def parse_snapshot_payload(canonical_payload: bytes) -> ForecastInputSnapshotPayload:
    """Strictly parse canonical bytes, rejecting duplicate and unknown JSON keys."""

    if (
        not isinstance(canonical_payload, bytes)
        or not canonical_payload
        or len(canonical_payload) > _MAX_PAYLOAD_BYTES
    ):
        raise SnapshotValidationError("canonical_payload must be non-empty bytes")
    try:
        document = json.loads(
            canonical_payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise SnapshotValidationError("canonical_payload is not strict UTF-8 JSON") from exc
    root = _object(document, "payload")
    _exact_keys(
        root,
        {
            "as_of",
            "availability",
            "currency",
            "data_sources",
            "format",
            "horizon_unit",
            "input_multiplier",
            "input_timespan",
            "observations",
            "resolution_policy_hash",
            "schema_version",
            "series_basis",
            "symbol",
            "target",
            "target_times",
        },
        "payload",
    )
    if _string(root["format"], "format") != SNAPSHOT_FORMAT:
        raise SnapshotValidationError("snapshot format is not supported")
    schema_version = _integer(root["schema_version"], "schema_version")

    availability_row = _object(root["availability"], "availability")
    _exact_keys(
        availability_row,
        {"checked_at", "rule_set_hash", "status"},
        "availability",
    )
    status = _string(availability_row["status"], "availability status")
    if status not in {"not_run", "passed"}:
        raise SnapshotValidationError("availability status is not supported")
    rule_set_hash = _optional_string(
        availability_row["rule_set_hash"],
        "availability rule_set_hash",
    )
    checked_at = _optional_timestamp(
        availability_row["checked_at"],
        "availability checked_at",
    )

    observations_raw = _array(root["observations"], "observations")
    observations: list[SnapshotObservation] = []
    for index, value in enumerate(observations_raw):
        row = _object(value, f"observations[{index}]")
        _exact_keys(
            row,
            {"available_at", "observed_at", "value_f64"},
            f"observations[{index}]",
        )
        observations.append(
            SnapshotObservation(
                observed_at=_parse_timestamp(row["observed_at"], "observed_at"),
                available_at=_parse_timestamp(row["available_at"], "available_at"),
                value=_parse_float_bits(row["value_f64"]),
            )
        )

    sources_raw = _array(root["data_sources"], "data_sources")
    sources: list[SnapshotSourceLineage] = []
    for index, value in enumerate(sources_raw):
        row = _object(value, f"data_sources[{index}]")
        _exact_keys(
            row,
            {"fields", "max_available_at", "name", "snapshot_id"},
            f"data_sources[{index}]",
        )
        fields = tuple(
            _string(item, f"data_sources[{index}].fields")
            for item in _array(row["fields"], f"data_sources[{index}].fields")
        )
        sources.append(
            SnapshotSourceLineage(
                name=_string(row["name"], "source name"),
                snapshot_id=_string(row["snapshot_id"], "source snapshot_id"),
                max_available_at=_parse_timestamp(
                    row["max_available_at"],
                    "source max_available_at",
                ),
                fields=fields,
            )
        )

    target_times = tuple(
        _parse_timestamp(value, "target_time")
        for value in _array(root["target_times"], "target_times")
    )
    target = _string(root["target"], "target")
    payload = ForecastInputSnapshotPayload(
        schema_version=schema_version,
        resolution_policy_hash=_string(
            root["resolution_policy_hash"],
            "resolution_policy_hash",
        ),
        symbol=_string(root["symbol"], "symbol"),
        target=target,
        horizon_unit=_string(root["horizon_unit"], "horizon_unit"),
        series_basis=_string(root["series_basis"], "series_basis"),
        input_timespan=_string(root["input_timespan"], "input_timespan"),
        input_multiplier=_integer(root["input_multiplier"], "input_multiplier"),
        as_of=_parse_timestamp(root["as_of"], "as_of"),
        currency=_optional_string(root["currency"], "currency"),
        observations=tuple(observations),
        target_times=target_times,
        data_sources=tuple(sources),
        availability=SnapshotAvailabilityEvidence(
            status=status,  # type: ignore[arg-type]
            rule_set_hash=rule_set_hash,
            checked_at=checked_at,
        ),
    )
    return _normalized_payload(payload)


def validate_and_resolve_snapshot(
    record: ForecastInputSnapshotRecord,
    request: ForecastRequest,
    *,
    expected_series_basis: str,
    expected_resolution_policy_hash: str,
    expected_input_timespan: str = "day",
    expected_input_multiplier: int = 1,
    trusted_availability_rule_set_hash: str | None = None,
) -> ResolvedForecastInput:
    """Verify bytes, headers, request binding, and explicit availability trust."""

    if not isinstance(record, ForecastInputSnapshotRecord):
        raise TypeError("record must be a ForecastInputSnapshotRecord")
    if not isinstance(request, ForecastRequest):
        raise TypeError("request must be a ForecastRequest")
    _hash(expected_resolution_policy_hash, "expected_resolution_policy_hash")
    if expected_series_basis not in _SERIES_BASES:
        raise SnapshotValidationError("expected_series_basis is not supported")
    if expected_input_timespan not in _INPUT_TIMESPANS:
        raise SnapshotValidationError("expected_input_timespan is not supported")
    _positive_integer(expected_input_multiplier, "expected_input_multiplier")
    if trusted_availability_rule_set_hash is not None:
        _hash(
            trusted_availability_rule_set_hash,
            "trusted_availability_rule_set_hash",
        )

    payload = parse_snapshot_payload(record.canonical_payload)
    if canonical_snapshot_payload(payload) != record.canonical_payload:
        raise SnapshotValidationError("canonical_payload bytes are not canonical")
    _hash(record.snapshot_id, "snapshot_id")
    actual_snapshot_id = snapshot_id_for_payload(record.canonical_payload)
    if not hmac.compare_digest(record.snapshot_id, actual_snapshot_id):
        raise SnapshotValidationError("snapshot_id does not match canonical_payload")
    expected_record = build_snapshot_record(payload, sealed_at=record.sealed_at)
    if not _strict_record_equal(record, expected_record):
        raise SnapshotValidationError("snapshot header does not match canonical_payload")

    if payload.resolution_policy_hash != expected_resolution_policy_hash:
        raise SnapshotValidationError("snapshot resolution policy does not match")
    if payload.series_basis != expected_series_basis:
        raise SnapshotValidationError("snapshot series basis does not match")
    if payload.input_timespan != expected_input_timespan:
        raise SnapshotValidationError("snapshot input_timespan does not match")
    if payload.input_multiplier != expected_input_multiplier:
        raise SnapshotValidationError("snapshot input_multiplier does not match")
    if payload.symbol != request.symbol:
        raise SnapshotValidationError("snapshot symbol does not match the request")
    if payload.target != request.target:
        raise SnapshotValidationError("snapshot target does not match the request")
    if payload.horizon_unit != request.horizon_unit:
        raise SnapshotValidationError("snapshot horizon unit does not match the request")
    if request.snapshot_id is not None:
        if request.snapshot_id != record.snapshot_id:
            raise SnapshotValidationError("pinned snapshot_id does not match the loaded record")
    elif request.as_of is not None and payload.as_of > request.as_of.astimezone(UTC):
        raise SnapshotValidationError("snapshot is later than the requested as_of cutoff")
    if len(payload.target_times) < request.horizon:
        raise SnapshotValidationError("snapshot does not contain the requested target horizon")

    availability_verified = (
        payload.availability.status == "passed"
        and trusted_availability_rule_set_hash is not None
        and payload.availability.rule_set_hash is not None
        and hmac.compare_digest(
            payload.availability.rule_set_hash,
            trusted_availability_rule_set_hash,
        )
    )
    return ResolvedForecastInput(
        symbol=payload.symbol,
        target=payload.target,
        horizon_unit=payload.horizon_unit,
        series_basis=payload.series_basis,
        snapshot_id=record.snapshot_id,
        as_of=payload.as_of,
        observations=tuple(
            ForecastObservation(
                observed_at=item.observed_at,
                available_at=item.available_at,
                value=item.value,
            )
            for item in payload.observations
        ),
        target_times=payload.target_times[: request.horizon],
        data_sources=tuple(
            DataSourceLineage(
                name=source.name,
                snapshot_id=source.snapshot_id,
                max_available_at=source.max_available_at,
                fields=list(source.fields),
            )
            for source in payload.data_sources
        ),
        currency=payload.currency,
        availability_verified=availability_verified,
    )


def _normalized_payload(payload: ForecastInputSnapshotPayload) -> ForecastInputSnapshotPayload:
    if not isinstance(payload, ForecastInputSnapshotPayload):
        raise TypeError("payload must be a ForecastInputSnapshotPayload")
    if type(payload.schema_version) is not int or payload.schema_version != SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotValidationError("snapshot schema_version is not supported")
    resolution_policy_hash = _hash(payload.resolution_policy_hash, "resolution_policy_hash")
    symbol = _text(payload.symbol, "symbol", max_length=32)
    if symbol != symbol.upper() or _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise SnapshotValidationError("symbol must be uppercase and canonical")
    target = _text(payload.target, "target", max_length=32)
    if target not in _TARGETS:
        raise SnapshotValidationError("target is not supported")
    horizon_unit = _text(payload.horizon_unit, "horizon_unit", max_length=32)
    if horizon_unit not in _HORIZON_UNITS:
        raise SnapshotValidationError("horizon_unit is not supported")
    series_basis = _text(payload.series_basis, "series_basis", max_length=32)
    if series_basis not in _SERIES_BASES:
        raise SnapshotValidationError("series_basis is not supported")
    if target == "close" and series_basis != "raw":
        raise SnapshotValidationError("close snapshots require raw series basis")
    if target == "adjusted_close" and series_basis == "raw":
        raise SnapshotValidationError("adjusted_close snapshots require adjusted series basis")
    input_timespan = _text(payload.input_timespan, "input_timespan", max_length=16)
    if input_timespan not in _INPUT_TIMESPANS:
        raise SnapshotValidationError("input_timespan is not supported")
    input_multiplier = _positive_integer(payload.input_multiplier, "input_multiplier")
    currency = _optional_text(payload.currency, "currency", max_length=3)
    if target in _PRICE_TARGETS:
        if currency is None or _CURRENCY_PATTERN.fullmatch(currency) is None:
            raise SnapshotValidationError("price snapshots require uppercase ISO currency")
    elif currency is not None:
        raise SnapshotValidationError("return snapshots must not specify currency")
    as_of = _utc(payload.as_of, "as_of")

    if not isinstance(payload.observations, tuple):
        raise SnapshotValidationError("snapshot observations must be a tuple")
    if not 1 <= len(payload.observations) <= _MAX_OBSERVATIONS:
        raise SnapshotValidationError("snapshot observations must be within the size limit")
    if any(not isinstance(item, SnapshotObservation) for item in payload.observations):
        raise SnapshotValidationError("snapshot observations have the wrong type")
    observations = sorted(
        (
            SnapshotObservation(
                observed_at=_utc(item.observed_at, "observation observed_at"),
                available_at=_utc(item.available_at, "observation available_at"),
                value=_finite(item.value, "observation value"),
            )
            for item in payload.observations
        ),
        key=lambda item: item.observed_at,
    )
    if len({item.observed_at for item in observations}) != len(observations):
        raise SnapshotValidationError("observation timestamps must be unique")
    for item in observations:
        if item.observed_at > as_of:
            raise SnapshotValidationError("observation time must not be later than as_of")
        if item.available_at < item.observed_at or item.available_at > as_of:
            raise SnapshotValidationError("observation availability must be within its cutoff")
        if target in _PRICE_TARGETS and item.value < 0.0:
            raise SnapshotValidationError("price snapshot values must be nonnegative")

    if not isinstance(payload.target_times, tuple):
        raise SnapshotValidationError("snapshot target_times must be a tuple")
    if not 1 <= len(payload.target_times) <= _MAX_TARGET_TIMES:
        raise SnapshotValidationError("snapshot target_times must be within the size limit")
    target_times = sorted(_utc(value, "target_time") for value in payload.target_times)
    if len(set(target_times)) != len(target_times):
        raise SnapshotValidationError("target_times must be unique")
    if target_times[0] <= as_of:
        raise SnapshotValidationError("target_times must be later than as_of")

    if not isinstance(payload.data_sources, tuple):
        raise SnapshotValidationError("snapshot data_sources must be a tuple")
    if not 1 <= len(payload.data_sources) <= _MAX_SOURCES:
        raise SnapshotValidationError("snapshot data_sources must be within the size limit")
    if any(not isinstance(source, SnapshotSourceLineage) for source in payload.data_sources):
        raise SnapshotValidationError("snapshot data_sources have the wrong type")
    grouped_sources: dict[tuple[str, str, datetime], set[str]] = {}
    for source in payload.data_sources:
        name = _text(source.name, "source name", max_length=64)
        source_snapshot_id = _text(
            source.snapshot_id,
            "source snapshot_id",
            max_length=128,
        )
        max_available_at = _utc(source.max_available_at, "source max_available_at")
        if max_available_at > as_of:
            raise SnapshotValidationError("source availability must not be later than as_of")
        if not isinstance(source.fields, tuple):
            raise SnapshotValidationError("source fields must be a tuple")
        if not 1 <= len(source.fields) <= _MAX_SOURCE_FIELDS:
            raise SnapshotValidationError("source fields must be within the size limit")
        fields = {_text(field, "source field", max_length=128) for field in source.fields}
        if not 1 <= len(fields) <= _MAX_SOURCE_FIELDS:
            raise SnapshotValidationError("source fields must be within the size limit")
        grouped_sources.setdefault((name, source_snapshot_id, max_available_at), set()).update(
            fields
        )
    data_sources = tuple(
        SnapshotSourceLineage(
            name=name,
            snapshot_id=source_snapshot_id,
            max_available_at=max_available_at,
            fields=tuple(sorted(grouped_sources[(name, source_snapshot_id, max_available_at)])),
        )
        for name, source_snapshot_id, max_available_at in sorted(grouped_sources)
    )

    if not isinstance(payload.availability, SnapshotAvailabilityEvidence):
        raise SnapshotValidationError("availability evidence has the wrong type")
    if payload.availability.status == "not_run":
        if (
            payload.availability.rule_set_hash is not None
            or payload.availability.checked_at is not None
        ):
            raise SnapshotValidationError("not_run availability cannot include proof fields")
        availability = SnapshotAvailabilityEvidence(status="not_run")
    elif payload.availability.status == "passed":
        if payload.availability.rule_set_hash is None or payload.availability.checked_at is None:
            raise SnapshotValidationError("passed availability requires proof fields")
        rule_set_hash = _hash(
            payload.availability.rule_set_hash,
            "availability rule_set_hash",
        )
        checked_at = _utc(payload.availability.checked_at, "availability checked_at")
        max_available_at = max(
            [item.available_at for item in observations]
            + [source.max_available_at for source in data_sources]
        )
        if checked_at < max(max_available_at, as_of):
            raise SnapshotValidationError("availability proof predates its snapshot cutoff")
        availability = SnapshotAvailabilityEvidence(
            status="passed",
            rule_set_hash=rule_set_hash,
            checked_at=checked_at,
        )
    else:
        raise SnapshotValidationError("availability status is not supported")

    return ForecastInputSnapshotPayload(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        resolution_policy_hash=resolution_policy_hash,
        symbol=symbol,
        target=target,
        horizon_unit=horizon_unit,
        series_basis=series_basis,
        input_timespan=input_timespan,
        input_multiplier=input_multiplier,
        as_of=as_of,
        currency=currency,
        observations=tuple(observations),
        target_times=tuple(target_times),
        data_sources=data_sources,
        availability=availability,
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise SnapshotValidationError(f"non-finite JSON constant is forbidden: {value}")


def _exact_keys(value: dict[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise SnapshotValidationError(f"{label} has missing or unknown keys")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise SnapshotValidationError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise SnapshotValidationError(f"{label} must be an array")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise SnapshotValidationError(f"{label} must be a string")
    return value


def _optional_string(value: object, label: str) -> str | None:
    return None if value is None else _string(value, label)


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise SnapshotValidationError(f"{label} must be an integer")
    return value


def _parse_timestamp(value: object, label: str) -> datetime:
    raw = _string(value, label)
    try:
        iso_value = raw.removesuffix("Z") + "+00:00" if raw.endswith("Z") else raw
        parsed = datetime.fromisoformat(iso_value)
    except (ValueError, OverflowError) as exc:
        raise SnapshotValidationError(f"{label} must be an ISO timestamp") from exc
    return _utc(parsed, label)


def _optional_timestamp(value: object, label: str) -> datetime | None:
    return None if value is None else _parse_timestamp(value, label)


def _parse_float_bits(value: object) -> float:
    raw = _string(value, "value_f64")
    if _FLOAT_BITS_PATTERN.fullmatch(raw) is None:
        raise SnapshotValidationError("value_f64 must be 16 lowercase hexadecimal digits")
    number = struct.unpack(">d", bytes.fromhex(raw))[0]
    return _finite(number, "value_f64")


def _strict_record_equal(
    actual: ForecastInputSnapshotRecord,
    expected: ForecastInputSnapshotRecord,
) -> bool:
    return all(
        type(getattr(actual, field.name)) is type(getattr(expected, field.name))
        and getattr(actual, field.name) == getattr(expected, field.name)
        for field in dataclass_fields(ForecastInputSnapshotRecord)
    )


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or not 1 <= value <= 10_000:
        raise SnapshotValidationError(f"{label} must be a positive integer")
    return value


def _hash(value: object, label: str) -> str:
    if not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None:
        raise SnapshotValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _text(value: object, label: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise SnapshotValidationError(f"{label} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized.strip() != normalized
        or len(normalized) > max_length
        or any(0xD800 <= ord(character) <= 0xDFFF for character in normalized)
    ):
        raise SnapshotValidationError(f"{label} must be a non-empty trimmed string")
    return normalized


def _optional_text(value: object, label: str, *, max_length: int) -> str | None:
    return None if value is None else _text(value, label, max_length=max_length)


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SnapshotValidationError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise SnapshotValidationError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise SnapshotValidationError(f"{label} cannot be normalized to UTC") from exc


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise SnapshotValidationError(f"{label} must be a finite real number")
    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError(f"{label} must be a finite real number") from exc
    if not math.isfinite(converted):
        raise SnapshotValidationError(f"{label} must be finite")
    return 0.0 if converted == 0.0 else converted


def _float_bits(value: float) -> str:
    return struct.pack(">d", _finite(value, "value")).hex()


__all__ = [
    "ForecastInputResolver",
    "ForecastInputSnapshotPayload",
    "ForecastInputSnapshotRecord",
    "ForecastInputSnapshotRepository",
    "ForecastInputSnapshotSelector",
    "SNAPSHOT_FORMAT",
    "SNAPSHOT_SCHEMA_VERSION",
    "SnapshotAvailabilityEvidence",
    "SnapshotObservation",
    "SnapshotSourceLineage",
    "SnapshotValidationError",
    "build_snapshot_record",
    "canonical_snapshot_payload",
    "parse_snapshot_payload",
    "snapshot_id_for_payload",
    "validate_and_resolve_snapshot",
]
