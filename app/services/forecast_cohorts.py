"""Pure, canonical evidence for pre-outcome forecast cohorts.

A cohort is a policy-bound, immutable list of exact scheduled forecast steps.
The canonical manifest deliberately contains no database clock: a database row
records the manifest first, and a separate post-commit receipt proves that the
row became visible before its earliest target.  Keeping those facts separate
prevents a long-running insert transaction from manufacturing a false
"pre-outcome" timestamp.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from app.services.forecast_runs import (
    ForecastRunValidationError,
    output_hash,
    parse_output,
)

COHORT_SCHEMA_VERSION = 1
COHORT_FORMAT = "forecast-outcome-cohort-v1"
MAX_COHORT_MEMBERS = 10_000
MAX_CANONICAL_BYTES = 4 * 1024 * 1024

type CohortPurpose = Literal["calibration_fit", "heldout_evaluation"]

_PURPOSES = frozenset({"calibration_fit", "heldout_evaluation"})
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIMESTAMP_PATTERN = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\."
    r"(?P<microsecond>\d{6})Z$"
)


class ForecastCohortValidationError(ValueError):
    """A cohort artifact is malformed, tampered with, or not pre-outcome."""


@runtime_checkable
class ScheduledForecastRunSource(Protocol):
    """Read shape required to derive a cohort member from ``forecast_runs``."""

    forecast_id: UUID
    origin_kind: str
    opportunity_hash: str
    output_hash: str
    canonical_output: bytes


@dataclass(frozen=True)
class ForecastCohortMember:
    """One exact forecast step precommitted for later outcome evaluation."""

    forecast_id: UUID
    step: int
    target_time: datetime
    opportunity_hash: str
    output_hash: str


@dataclass(frozen=True)
class ForecastCohortManifest:
    """Semantic cohort membership covered by the content hash."""

    purpose: CohortPurpose
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    members: tuple[ForecastCohortMember, ...]
    schema_version: int = COHORT_SCHEMA_VERSION


@dataclass(frozen=True)
class ForecastCohortRecord:
    """Immutable manifest-row shape at the pure persistence boundary."""

    cohort_id: str
    schema_version: int
    purpose: CohortPurpose
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    member_count: int
    earliest_target_time: datetime
    latest_target_time: datetime
    recorded_at: datetime
    creator_xid: int
    canonical_manifest: bytes


@dataclass(frozen=True)
class ForecastCohortSeal:
    """Second-transaction receipt proving pre-outcome manifest visibility."""

    cohort_id: str
    manifest_recorded_at: datetime
    sealed_at: datetime
    sealer_xid: int


def member_from_scheduled_run(
    source: ScheduledForecastRunSource,
    *,
    step: int,
) -> ForecastCohortMember:
    """Derive one member only from a validated scheduled forecast archive row."""

    if not isinstance(source, ScheduledForecastRunSource):
        raise TypeError("source must provide the scheduled ForecastRun read shape")
    if source.origin_kind != "scheduled_evaluation":
        raise ForecastCohortValidationError(
            "cohort members must come from scheduled_evaluation forecast runs"
        )
    if type(step) is not int or not 1 <= step <= 252:
        raise ForecastCohortValidationError("cohort member step must be within 1..252")
    forecast_id = _uuid(source.forecast_id, "forecast_id")
    opportunity = _sha256(source.opportunity_hash, "opportunity_hash")
    stored_output_hash = _sha256(source.output_hash, "output_hash")
    if not isinstance(source.canonical_output, bytes):
        raise ForecastCohortValidationError("canonical_output must be bytes")
    try:
        response = parse_output(source.canonical_output)
        derived_output_hash = output_hash(response)
    except (ForecastRunValidationError, TypeError, ValueError) as exc:
        raise ForecastCohortValidationError(
            "scheduled forecast output is not valid canonical evidence"
        ) from exc
    if derived_output_hash != stored_output_hash:
        raise ForecastCohortValidationError(
            "scheduled forecast output hash does not match its canonical bytes"
        )
    if response.provenance.forecast_id != forecast_id:
        raise ForecastCohortValidationError(
            "scheduled forecast identifier does not match its canonical output"
        )
    selected = next((item for item in response.forecasts if item.step == step), None)
    if selected is None:
        raise ForecastCohortValidationError("scheduled forecast does not contain the selected step")
    return ForecastCohortMember(
        forecast_id=forecast_id,
        step=step,
        target_time=_utc(selected.target_time, "target_time"),
        opportunity_hash=opportunity,
        output_hash=derived_output_hash,
    )


def canonical_cohort_manifest(manifest: ForecastCohortManifest) -> bytes:
    """Return strict, deterministic UTF-8 JSON for one semantic cohort."""

    normalized = _normalized_manifest(manifest)
    document = {
        "availability_rule_set_hash": normalized.availability_rule_set_hash,
        "format": COHORT_FORMAT,
        "members": [
            {
                "forecast_id": str(member.forecast_id),
                "opportunity_hash": member.opportunity_hash,
                "output_hash": member.output_hash,
                "step": member.step,
                "target_time": _timestamp(member.target_time),
            }
            for member in normalized.members
        ],
        "outcome_resolution_policy_hash": normalized.outcome_resolution_policy_hash,
        "purpose": normalized.purpose,
        "schema_version": normalized.schema_version,
        "selection_policy_hash": normalized.selection_policy_hash,
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
        raise ForecastCohortValidationError(
            "cohort manifest cannot be encoded canonically"
        ) from exc
    if not canonical or len(canonical) > MAX_CANONICAL_BYTES:
        raise ForecastCohortValidationError("canonical cohort manifest exceeds the storage limit")
    return canonical


def parse_cohort_manifest(canonical_manifest: bytes) -> ForecastCohortManifest:
    """Parse and recanonicalize bytes, rejecting duplicate or unknown JSON keys."""

    _bounded_bytes(canonical_manifest)
    try:
        document = json.loads(
            canonical_manifest.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ForecastCohortValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise ForecastCohortValidationError(
            "canonical cohort manifest is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise ForecastCohortValidationError("cohort manifest must be a JSON object")
    expected_keys = {
        "availability_rule_set_hash",
        "format",
        "members",
        "outcome_resolution_policy_hash",
        "purpose",
        "schema_version",
        "selection_policy_hash",
    }
    if set(document) != expected_keys:
        raise ForecastCohortValidationError("cohort manifest has unknown or missing keys")
    if document["format"] != COHORT_FORMAT:
        raise ForecastCohortValidationError("cohort manifest format is not supported")
    if type(document["schema_version"]) is not int:
        raise ForecastCohortValidationError("cohort schema_version must be an integer")
    raw_members = document["members"]
    if not isinstance(raw_members, list):
        raise ForecastCohortValidationError("cohort members must be a JSON array")
    members: list[ForecastCohortMember] = []
    member_keys = {"forecast_id", "opportunity_hash", "output_hash", "step", "target_time"}
    for index, raw in enumerate(raw_members):
        if not isinstance(raw, dict) or set(raw) != member_keys:
            raise ForecastCohortValidationError(
                f"cohort member {index} has unknown or missing keys"
            )
        members.append(
            ForecastCohortMember(
                forecast_id=_parse_uuid(raw["forecast_id"], f"members[{index}].forecast_id"),
                step=_integer(raw["step"], f"members[{index}].step"),
                target_time=_parse_timestamp(
                    raw["target_time"],
                    f"members[{index}].target_time",
                ),
                opportunity_hash=_sha256(
                    raw["opportunity_hash"],
                    f"members[{index}].opportunity_hash",
                ),
                output_hash=_sha256(raw["output_hash"], f"members[{index}].output_hash"),
            )
        )
    purpose = _purpose(document["purpose"])
    manifest = ForecastCohortManifest(
        schema_version=_integer(document["schema_version"], "schema_version"),
        purpose=purpose,
        selection_policy_hash=_sha256(
            document["selection_policy_hash"],
            "selection_policy_hash",
        ),
        outcome_resolution_policy_hash=_sha256(
            document["outcome_resolution_policy_hash"],
            "outcome_resolution_policy_hash",
        ),
        availability_rule_set_hash=_sha256(
            document["availability_rule_set_hash"],
            "availability_rule_set_hash",
        ),
        members=tuple(members),
    )
    normalized = _normalized_manifest(manifest)
    if canonical_cohort_manifest(normalized) != canonical_manifest:
        raise ForecastCohortValidationError("cohort manifest bytes are not canonical")
    return normalized


def cohort_id_for_manifest(manifest_or_bytes: ForecastCohortManifest | bytes) -> str:
    """Return the SHA-256 identity of validated canonical cohort bytes."""

    if isinstance(manifest_or_bytes, ForecastCohortManifest):
        canonical = canonical_cohort_manifest(manifest_or_bytes)
    else:
        canonical = canonical_cohort_manifest(parse_cohort_manifest(manifest_or_bytes))
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def build_cohort_record(
    manifest: ForecastCohortManifest,
    *,
    recorded_at: datetime,
    creator_xid: int,
) -> ForecastCohortRecord:
    """Build an exact manifest-row DTO from semantic content and DB evidence."""

    normalized = _normalized_manifest(manifest)
    recorded = _utc(recorded_at, "recorded_at")
    xid = _xid(creator_xid, "creator_xid")
    earliest = normalized.members[0].target_time
    latest = max(member.target_time for member in normalized.members)
    if recorded >= earliest:
        raise ForecastCohortValidationError(
            "cohort manifest must be recorded before its earliest target"
        )
    canonical = canonical_cohort_manifest(normalized)
    return ForecastCohortRecord(
        cohort_id=cohort_id_for_manifest(canonical),
        schema_version=normalized.schema_version,
        purpose=normalized.purpose,
        selection_policy_hash=normalized.selection_policy_hash,
        outcome_resolution_policy_hash=normalized.outcome_resolution_policy_hash,
        availability_rule_set_hash=normalized.availability_rule_set_hash,
        member_count=len(normalized.members),
        earliest_target_time=earliest,
        latest_target_time=latest,
        recorded_at=recorded,
        creator_xid=xid,
        canonical_manifest=canonical,
    )


def validate_cohort_record(record: ForecastCohortRecord) -> ForecastCohortManifest:
    """Revalidate canonical bytes and every denormalized manifest-row header."""

    if not isinstance(record, ForecastCohortRecord):
        raise TypeError("record must be a ForecastCohortRecord")
    manifest = parse_cohort_manifest(record.canonical_manifest)
    expected = build_cohort_record(
        manifest,
        recorded_at=record.recorded_at,
        creator_xid=record.creator_xid,
    )
    if record != expected:
        raise ForecastCohortValidationError(
            "cohort record headers do not match the canonical manifest"
        )
    return manifest


def validate_cohort_seal(
    record: ForecastCohortRecord,
    seal: ForecastCohortSeal,
) -> ForecastCohortManifest:
    """Validate a distinct post-commit receipt sealed before all outcomes."""

    manifest = validate_cohort_record(record)
    if not isinstance(seal, ForecastCohortSeal):
        raise TypeError("seal must be a ForecastCohortSeal")
    if _sha256(seal.cohort_id, "seal cohort_id") != record.cohort_id:
        raise ForecastCohortValidationError("cohort seal identifies a different manifest")
    manifest_recorded = _utc(
        seal.manifest_recorded_at,
        "manifest_recorded_at",
    )
    if manifest_recorded != record.recorded_at:
        raise ForecastCohortValidationError(
            "cohort seal does not bind the manifest recording receipt"
        )
    sealed = _utc(seal.sealed_at, "sealed_at")
    sealer_xid = _xid(seal.sealer_xid, "sealer_xid")
    if sealer_xid == record.creator_xid:
        raise ForecastCohortValidationError(
            "cohort seal must be recorded in a transaction after the manifest commits"
        )
    if sealed < record.recorded_at:
        raise ForecastCohortValidationError("cohort seal predates the manifest record")
    if sealed >= record.earliest_target_time:
        raise ForecastCohortValidationError(
            "cohort must be sealed strictly before its earliest target"
        )
    return manifest


def _normalized_manifest(manifest: ForecastCohortManifest) -> ForecastCohortManifest:
    if not isinstance(manifest, ForecastCohortManifest):
        raise TypeError("manifest must be a ForecastCohortManifest")
    if type(manifest.schema_version) is not int or manifest.schema_version != COHORT_SCHEMA_VERSION:
        raise ForecastCohortValidationError("cohort schema_version is not supported")
    purpose = _purpose(manifest.purpose)
    if not isinstance(manifest.members, tuple):
        raise ForecastCohortValidationError("cohort members must be a tuple")
    if not 1 <= len(manifest.members) <= MAX_COHORT_MEMBERS:
        raise ForecastCohortValidationError("cohort member count is outside the supported bounds")
    members = tuple(_normalized_member(member) for member in manifest.members)
    forecast_steps = [(member.forecast_id, member.step) for member in members]
    if len(set(forecast_steps)) != len(forecast_steps):
        raise ForecastCohortValidationError("cohort contains a duplicate forecast step")
    opportunity_steps = [(member.opportunity_hash, member.step) for member in members]
    if len(set(opportunity_steps)) != len(opportunity_steps):
        raise ForecastCohortValidationError("cohort contains a duplicate forecast opportunity step")
    ordered = tuple(
        sorted(
            members,
            key=lambda member: (
                member.target_time,
                str(member.forecast_id),
                member.step,
            ),
        )
    )
    return ForecastCohortManifest(
        schema_version=manifest.schema_version,
        purpose=purpose,
        selection_policy_hash=_sha256(
            manifest.selection_policy_hash,
            "selection_policy_hash",
        ),
        outcome_resolution_policy_hash=_sha256(
            manifest.outcome_resolution_policy_hash,
            "outcome_resolution_policy_hash",
        ),
        availability_rule_set_hash=_sha256(
            manifest.availability_rule_set_hash,
            "availability_rule_set_hash",
        ),
        members=ordered,
    )


def _normalized_member(member: ForecastCohortMember) -> ForecastCohortMember:
    if not isinstance(member, ForecastCohortMember):
        raise ForecastCohortValidationError("cohort members have the wrong type")
    if type(member.step) is not int or not 1 <= member.step <= 252:
        raise ForecastCohortValidationError("cohort member step must be within 1..252")
    return ForecastCohortMember(
        forecast_id=_uuid(member.forecast_id, "forecast_id"),
        step=member.step,
        target_time=_utc(member.target_time, "target_time"),
        opportunity_hash=_sha256(member.opportunity_hash, "opportunity_hash"),
        output_hash=_sha256(member.output_hash, "output_hash"),
    )


def _bounded_bytes(value: object) -> bytes:
    if not isinstance(value, bytes) or not value or len(value) > MAX_CANONICAL_BYTES:
        raise ForecastCohortValidationError(
            "canonical cohort manifest must be non-empty bounded bytes"
        )
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForecastCohortValidationError(
                "canonical cohort manifest contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ForecastCohortValidationError(f"JSON constant {value!r} is not permitted")


def _purpose(value: object) -> CohortPurpose:
    if not isinstance(value, str) or value not in _PURPOSES:
        raise ForecastCohortValidationError("cohort purpose is not supported")
    return value  # type: ignore[return-value]


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastCohortValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _uuid(value: object, label: str) -> UUID:
    if not isinstance(value, UUID):
        raise ForecastCohortValidationError(f"{label} must be a UUID")
    return value


def _parse_uuid(value: object, label: str) -> UUID:
    if not isinstance(value, str):
        raise ForecastCohortValidationError(f"{label} must be a canonical UUID")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ForecastCohortValidationError(f"{label} must be a canonical UUID") from exc
    if str(parsed) != value:
        raise ForecastCohortValidationError(f"{label} must be a canonical UUID")
    return parsed


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ForecastCohortValidationError(f"{label} must be an integer")
    return value


def _xid(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ForecastCohortValidationError(f"{label} must be a positive transaction identity")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ForecastCohortValidationError(f"{label} must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise ForecastCohortValidationError(f"{label} cannot be normalized to UTC") from exc


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ForecastCohortValidationError(f"{label} must be a canonical UTC timestamp")
    match = _TIMESTAMP_PATTERN.fullmatch(value)
    if match is None:
        raise ForecastCohortValidationError(f"{label} must be a canonical UTC timestamp")
    try:
        parsed = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
            microsecond=int(match.group("microsecond")),
            tzinfo=UTC,
        )
    except ValueError as exc:
        raise ForecastCohortValidationError(f"{label} is not a valid timestamp") from exc
    if _timestamp(parsed) != value:
        raise ForecastCohortValidationError(f"{label} must be a canonical UTC timestamp")
    return parsed


__all__ = [
    "COHORT_FORMAT",
    "COHORT_SCHEMA_VERSION",
    "CohortPurpose",
    "ForecastCohortManifest",
    "ForecastCohortMember",
    "ForecastCohortRecord",
    "ForecastCohortSeal",
    "ForecastCohortValidationError",
    "ScheduledForecastRunSource",
    "build_cohort_record",
    "canonical_cohort_manifest",
    "cohort_id_for_manifest",
    "member_from_scheduled_run",
    "parse_cohort_manifest",
    "validate_cohort_record",
    "validate_cohort_seal",
]
