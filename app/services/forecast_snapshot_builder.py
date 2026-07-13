"""Privileged, fail-closed creation of verified forecast-input snapshots.

The public API never calls this module. A dedicated worker reconstructs one
Massive/Polygon regular-session raw-close series at a database cutoff, proves
that every retained bar came from a completed session and was available,
resolves future XNYS session closes, and inserts
one immutable canonical payload.  The deliberately narrow first policy serves
honest raw-close forecasts while corporate-action and security-master data are
still absent; it does not relabel vendor-adjusted history as locally reproducible
adjusted prices.
"""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime, time, timedelta
from functools import lru_cache
from importlib.metadata import version as package_version
from typing import Any, Literal, cast

import exchange_calendars
import pandas as pd
from sqlalchemy import Select, and_, func, literal, select, union
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.services.forecast_snapshots import (
    SNAPSHOT_SCHEMA_VERSION,
    ForecastInputSnapshotPayload,
    ForecastInputSnapshotRecord,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    SnapshotValidationError,
    build_snapshot_record,
    canonical_snapshot_payload,
    parse_snapshot_payload,
    snapshot_id_for_payload,
)
from ingestion.locks import (
    acquire_advisory_xact_lock,
    bar_series_lock_id,
    stable_lock_id,
)

SnapshotTarget = Literal["close"]
SnapshotHorizonUnit = Literal["trading_day"]

_CALENDAR_ENGINE_VERSION = "4.13.2"
_CALENDAR_TZDATA_VERSION = package_version("tzdata")
_PANDAS_VERSION = package_version("pandas")
_HASH_PREFIX = "sha256:"
_CALENDAR_START = "1990-01-01"
_CALENDAR_END = "2100-12-31"


class SnapshotBuildError(ValueError):
    """A snapshot could not be built without weakening its policy."""


class SnapshotBuildMisconfigured(SnapshotBuildError):
    """Configured trust identities do not match the code-defined policy."""


class SnapshotInputUnavailable(SnapshotBuildError):
    """The required complete, contiguous point-in-time input is unavailable."""


class SnapshotAvailabilityError(SnapshotBuildError):
    """Stored source evidence failed the pinned availability rules."""


class SnapshotSemanticConflict(SnapshotBuildError):
    """An immutable row occupies a semantic key with incompatible bytes."""


@dataclass(frozen=True)
class SnapshotBuildSpec:
    symbol: str
    target: SnapshotTarget
    horizon_unit: SnapshotHorizonUnit
    as_of: datetime


@dataclass(frozen=True)
class PointInTimeBar:
    symbol: str
    timespan: str
    multiplier: int
    source: str
    adjustment_basis: str
    observed_at: datetime
    close: float
    fetched_at: datetime
    source_as_of: datetime
    recorded_at: datetime
    available_at: datetime
    active_version_count: int = 1


@dataclass(frozen=True)
class SnapshotBuildResult:
    snapshot_id: str
    as_of: datetime
    availability_checked_at: datetime
    observation_count: int
    target_time_count: int
    created: bool


def _hash_document(document: Mapping[str, object]) -> str:
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return _HASH_PREFIX + hashlib.sha256(canonical).hexdigest()


AVAILABILITY_RULE_SET_DOCUMENT: dict[str, object] = {
    "format": "forecast-snapshot-availability-rules-v1",
    "rules": [
        "exact_source_key",
        "post_commit_receipt_for_exact_bar_version",
        "newest_finalized_exact_version_per_bar_at_cutoff",
        "observed_at<=session_close<=fetched_at<=source_as_of<=recorded_at"
        "<=post_commit_available_at<=cutoff",
        "finite_nonnegative_raw_close",
        "xnys_session_label",
        "source_fetch_not_before_regular_session_close",
        "newest_contiguous_finalized_suffix_through_completed_session",
        "newest_completed_session_present",
        "db_clock_checked_at_not_before_cutoff",
    ],
    "version_visibility": {
        "selection": (
            "maximum version_recorded_at whose exact-version receipt.available_at<=cutoff"
        ),
        "receipt_order": "irrelevant_to_version_selection",
        "same_transaction_receipt": "rejected_by_database",
        "legacy_or_unfinalized_version": "not_visible",
    },
}
DEFAULT_AVAILABILITY_RULE_SET_HASH = _hash_document(AVAILABILITY_RULE_SET_DOCUMENT)


@dataclass(frozen=True)
class ForecastSnapshotBuildPolicy:
    source: str = "polygon_open_close"
    input_timespan: str = "day"
    input_multiplier: int = 1
    series_basis: str = "raw"
    currency: str = "USD"
    calendar_name: str = "XNYS"
    observation_limit: int = 512
    minimum_observations: int = 258
    target_time_count: int = 252
    allowed_symbols: tuple[str, ...] = ("AAPL", "MSFT", "NVDA", "QQQ", "SPY")

    @property
    def resolution_policy_hash(self) -> str:
        return _hash_document(self.resolution_policy_document)

    @property
    def resolution_policy_document(self) -> dict[str, object]:
        """Every configurable resolution choice is covered by this identity."""

        return {
            "format": "forecast-snapshot-resolution-policy-v1",
            "availability_rule_set_hash": self.availability_rule_set_hash,
            "calendar": {
                "engine": "exchange_calendars",
                "engine_version": _CALENDAR_ENGINE_VERSION,
                "name": self.calendar_name,
                "pandas_version": _PANDAS_VERSION,
                "schedule_start": _CALENDAR_START,
                "schedule_end": _CALENDAR_END,
                "target_timestamp": "session_close_utc",
                "tzdata_version": _CALENDAR_TZDATA_VERSION,
            },
            "currency": {
                "resolver": "fixed_us_equity_universe_v1",
                "value": self.currency,
            },
            "cutoff": {
                "admissible": "operator_selected_utc_instant_not_after_database_clock",
                "scheduled_default": "most_recent_daily_17:00:00Z",
            },
            "horizon_units": ["trading_day"],
            "input": {
                "adjustment_basis": self.series_basis,
                "field": "close",
                "multiplier": self.input_multiplier,
                "provider_endpoint": "/v1/open-close/{ticker}/{date}?adjusted=false",
                "provider_semantics": (
                    "regular_session_close; preMarket and afterHours are separate fields"
                ),
                "source": self.source,
                "timespan": self.input_timespan,
            },
            "minimum_observations": self.minimum_observations,
            "observation_limit": self.observation_limit,
            "symbols": list(self.allowed_symbols),
            "targets": {"close": {"series_basis": self.series_basis, "transform": "identity"}},
            "target_time_count": self.target_time_count,
        }

    @property
    def availability_rule_set_hash(self) -> str:
        return DEFAULT_AVAILABILITY_RULE_SET_HASH

    def validate_spec(self, spec: SnapshotBuildSpec) -> SnapshotBuildSpec:
        symbol = spec.symbol.strip().upper() if isinstance(spec.symbol, str) else ""
        if symbol not in self.allowed_symbols:
            raise SnapshotBuildError(
                f"symbol {symbol!r} is outside the pinned US-equity snapshot universe"
            )
        if spec.target != "close":
            raise SnapshotBuildError("only raw close snapshots are supported by policy v1")
        if spec.horizon_unit != "trading_day":
            raise SnapshotBuildError("only trading_day horizons are supported by policy v1")
        return SnapshotBuildSpec(
            symbol=symbol,
            target="close",
            horizon_unit="trading_day",
            as_of=_utc(spec.as_of, "as_of"),
        )

    def validate_configured_hashes(
        self,
        resolution_policy_hash: str | None,
        availability_rule_set_hash: str | None,
    ) -> None:
        if resolution_policy_hash != self.resolution_policy_hash:
            raise SnapshotBuildMisconfigured(
                "configured resolution-policy hash does not match the snapshot builder"
            )
        if availability_rule_set_hash != self.availability_rule_set_hash:
            raise SnapshotBuildMisconfigured(
                "configured availability rule-set hash does not match the snapshot builder"
            )


DEFAULT_SNAPSHOT_BUILD_POLICY = ForecastSnapshotBuildPolicy()
RESOLUTION_POLICY_DOCUMENT = DEFAULT_SNAPSHOT_BUILD_POLICY.resolution_policy_document
DEFAULT_RESOLUTION_POLICY_HASH = DEFAULT_SNAPSHOT_BUILD_POLICY.resolution_policy_hash


def build_point_in_time_bars_statement(
    spec: SnapshotBuildSpec,
    policy: ForecastSnapshotBuildPolicy = DEFAULT_SNAPSHOT_BUILD_POLICY,
) -> Select[Any]:
    """Select the newest finalized version at ``spec.as_of`` for each bar."""

    spec = policy.validate_spec(spec)
    current_key_filters = (
        Bar.symbol == spec.symbol,
        Bar.timespan == policy.input_timespan,
        Bar.multiplier == policy.input_multiplier,
        Bar.source == policy.source,
        Bar.adjustment_basis == policy.series_basis,
    )
    revision_key_filters = (
        BarRevision.symbol == spec.symbol,
        BarRevision.timespan == policy.input_timespan,
        BarRevision.multiplier == policy.input_multiplier,
        BarRevision.source == policy.source,
        BarRevision.adjustment_basis == policy.series_basis,
    )
    current = select(
        Bar.symbol.label("symbol"),
        Bar.timespan.label("timespan"),
        Bar.multiplier.label("multiplier"),
        Bar.source.label("source"),
        Bar.adjustment_basis.label("adjustment_basis"),
        Bar.ts.label("observed_at"),
        Bar.close.label("close"),
        Bar.fetched_at.label("fetched_at"),
        Bar.as_of.label("source_as_of"),
        Bar.recorded_at.label("recorded_at"),
    ).where(
        *current_key_filters,
        Bar.ts <= spec.as_of,
        Bar.as_of <= spec.as_of,
    )
    previous = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.ts.label("observed_at"),
        BarRevision.previous_close.label("close"),
        BarRevision.previous_fetched_at.label("fetched_at"),
        BarRevision.previous_as_of.label("source_as_of"),
        BarRevision.previous_recorded_at.label("recorded_at"),
    ).where(
        *revision_key_filters,
        BarRevision.ts <= spec.as_of,
        BarRevision.previous_as_of <= spec.as_of,
        BarRevision.previous_recorded_at.is_not(None),
    )
    incoming = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.ts.label("observed_at"),
        BarRevision.incoming_close.label("close"),
        BarRevision.incoming_fetched_at.label("fetched_at"),
        BarRevision.incoming_as_of.label("source_as_of"),
        BarRevision.incoming_recorded_at.label("recorded_at"),
    ).where(
        *revision_key_filters,
        BarRevision.ts <= spec.as_of,
        BarRevision.incoming_as_of <= spec.as_of,
        BarRevision.incoming_recorded_at.is_not(None),
    )
    versions = union(current, previous, incoming).subquery("stored_bar_versions")
    receipt = BarVersionAvailability
    finalized = (
        select(
            *versions.c,
            receipt.available_at.label("available_at"),
            func.row_number()
            .over(
                partition_by=versions.c.observed_at,
                order_by=versions.c.recorded_at.desc(),
            )
            .label("version_rank"),
        )
        .join(
            receipt,
            and_(
                receipt.symbol == versions.c.symbol,
                receipt.timespan == versions.c.timespan,
                receipt.multiplier == versions.c.multiplier,
                receipt.ts == versions.c.observed_at,
                receipt.source == versions.c.source,
                receipt.adjustment_basis == versions.c.adjustment_basis,
                receipt.version_recorded_at == versions.c.recorded_at,
            ),
        )
        .where(receipt.available_at <= spec.as_of)
        .subquery("finalized_bar_versions")
    )
    return (
        select(
            finalized.c.symbol,
            finalized.c.timespan,
            finalized.c.multiplier,
            finalized.c.source,
            finalized.c.adjustment_basis,
            finalized.c.observed_at,
            finalized.c.close,
            finalized.c.fetched_at,
            finalized.c.source_as_of,
            finalized.c.recorded_at,
            finalized.c.available_at,
            literal(1).label("active_version_count"),
        )
        .where(finalized.c.version_rank == 1)
        .order_by(finalized.c.observed_at.desc())
        .limit(policy.observation_limit)
    )


@lru_cache(maxsize=8)
def _calendar(policy: ForecastSnapshotBuildPolicy):
    if exchange_calendars.__version__ != _CALENDAR_ENGINE_VERSION:
        raise SnapshotBuildMisconfigured(
            "exchange_calendars version differs from the hashed resolution policy"
        )
    try:
        return exchange_calendars.get_calendar(
            policy.calendar_name,
            start=_CALENDAR_START,
            end=_CALENDAR_END,
        )
    except (KeyError, ValueError) as exc:
        raise SnapshotBuildMisconfigured("the pinned exchange calendar is unavailable") from exc


def _latest_completed_session(calendar: Any, cutoff: datetime) -> pd.Timestamp:
    label = calendar.date_to_session(pd.Timestamp(cutoff.date()), direction="previous")
    if _utc(calendar.session_close(label).to_pydatetime(), "session close") > cutoff:
        label = calendar.previous_session(label)
    return cast(pd.Timestamp, label)


def _target_times(
    cutoff: datetime,
    policy: ForecastSnapshotBuildPolicy,
    calendar: Any,
) -> tuple[datetime, ...]:
    date_label = calendar.date_to_session(pd.Timestamp(cutoff.date()), direction="next")
    if _utc(calendar.session_close(date_label).to_pydatetime(), "session close") <= cutoff:
        date_label = calendar.next_session(date_label)
    sessions = calendar.sessions_window(date_label, policy.target_time_count)
    closes = tuple(
        _utc(calendar.session_close(label).to_pydatetime(), "target session close")
        for label in sessions
    )
    if len(closes) != policy.target_time_count or closes[0] <= cutoff:
        raise SnapshotAvailabilityError("exchange calendar did not yield the full future horizon")
    return closes


def _verify_and_order_bars(
    rows: tuple[PointInTimeBar, ...],
    spec: SnapshotBuildSpec,
    policy: ForecastSnapshotBuildPolicy,
    calendar: Any,
) -> tuple[PointInTimeBar, ...]:
    ordered = tuple(sorted(rows, key=lambda row: row.observed_at))
    latest = _latest_completed_session(calendar, spec.as_of)
    expected = latest
    suffix_reversed: list[PointInTimeBar] = []
    for row in reversed(ordered):
        observed_at = _utc(row.observed_at, "bar observed_at")
        label = pd.Timestamp(observed_at.date())
        if not calendar.is_session(label):
            if not suffix_reversed or label >= expected:
                raise SnapshotAvailabilityError("daily bar timestamp is not an XNYS session")
            break
        if label < expected:
            break
        if label > expected:
            raise SnapshotAvailabilityError(
                "point-in-time reconstruction returned duplicate or future bars"
            )
        suffix_reversed.append(row)
        expected = calendar.previous_session(expected)

    selected = tuple(reversed(suffix_reversed))
    if len(selected) < policy.minimum_observations:
        raise SnapshotInputUnavailable(
            "newest contiguous suffix requires at least "
            f"{policy.minimum_observations} complete observations; found {len(selected)}"
        )

    for row in selected:
        observed_at = _utc(row.observed_at, "bar observed_at")
        fetched_at = _utc(row.fetched_at, "bar fetched_at")
        source_as_of = _utc(row.source_as_of, "bar as_of")
        recorded_at = _utc(row.recorded_at, "bar recorded_at")
        available_at = _utc(row.available_at, "bar available_at")
        if (
            row.symbol != spec.symbol
            or row.timespan != policy.input_timespan
            or row.multiplier != policy.input_multiplier
            or row.source != policy.source
            or row.adjustment_basis != policy.series_basis
        ):
            raise SnapshotAvailabilityError("bar does not match the pinned source key")
        if row.active_version_count != 1:
            raise SnapshotAvailabilityError("bar has multiple active versions at the cutoff")
        close = float(row.close)
        if not math.isfinite(close) or close < 0.0:
            raise SnapshotAvailabilityError("bar close must be finite and nonnegative")
        label = pd.Timestamp(observed_at.date())
        session_close = _utc(calendar.session_close(label).to_pydatetime(), "session close")
        if not (
            observed_at
            <= session_close
            <= fetched_at
            <= source_as_of
            <= recorded_at
            <= available_at
            <= spec.as_of
        ):
            raise SnapshotAvailabilityError("bar availability timestamps violate the rule set")
    return selected


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _source_snapshot_id(
    rows: tuple[PointInTimeBar, ...],
    spec: SnapshotBuildSpec,
    policy: ForecastSnapshotBuildPolicy,
) -> str:
    manifest: dict[str, object] = {
        "format": "forecast-source-bar-manifest-v1",
        "selector": {
            "adjustment_basis": policy.series_basis,
            "cutoff": _timestamp(spec.as_of),
            "field": "close",
            "multiplier": policy.input_multiplier,
            "source": policy.source,
            "symbol": spec.symbol,
            "timespan": policy.input_timespan,
        },
        "versions": [
            {
                "close_f64": struct.pack(">d", float(row.close)).hex(),
                "available_at": _timestamp(row.available_at),
                "fetched_at": _timestamp(row.fetched_at),
                "observed_at": _timestamp(row.observed_at),
                "recorded_at": _timestamp(row.recorded_at),
                "source_as_of": _timestamp(row.source_as_of),
            }
            for row in rows
        ],
    }
    return _hash_document(manifest)


def assemble_verified_snapshot_payload(
    rows: tuple[PointInTimeBar, ...],
    spec: SnapshotBuildSpec,
    *,
    checked_at: datetime,
    policy: ForecastSnapshotBuildPolicy = DEFAULT_SNAPSHOT_BUILD_POLICY,
) -> ForecastInputSnapshotPayload:
    """Verify source rows and assemble the exact trusted canonical payload."""

    spec = policy.validate_spec(spec)
    checked_at = _utc(checked_at, "availability checked_at")
    if checked_at < spec.as_of:
        raise SnapshotAvailabilityError("availability check predates the snapshot cutoff")
    calendar = _calendar(policy)
    ordered = _verify_and_order_bars(rows, spec, policy, calendar)
    target_times = _target_times(spec.as_of, policy, calendar)
    max_available_at = max(row.available_at for row in ordered)
    source_id = _source_snapshot_id(ordered, spec, policy)
    return ForecastInputSnapshotPayload(
        resolution_policy_hash=policy.resolution_policy_hash,
        symbol=spec.symbol,
        target=spec.target,
        horizon_unit=spec.horizon_unit,
        series_basis=policy.series_basis,
        input_timespan=policy.input_timespan,
        input_multiplier=policy.input_multiplier,
        as_of=spec.as_of,
        currency=policy.currency,
        observations=tuple(
            SnapshotObservation(
                observed_at=row.observed_at,
                available_at=row.available_at,
                value=row.close,
            )
            for row in ordered
        ),
        target_times=target_times,
        data_sources=(
            SnapshotSourceLineage(
                name=policy.source,
                snapshot_id=source_id,
                max_available_at=max_available_at,
                fields=("close",),
            ),
        ),
        availability=SnapshotAvailabilityEvidence(
            status="passed",
            rule_set_hash=policy.availability_rule_set_hash,
            checked_at=checked_at,
        ),
    )


def _record_from_row(row: ForecastInputSnapshot) -> ForecastInputSnapshotRecord:
    return ForecastInputSnapshotRecord(
        snapshot_id=row.snapshot_id,
        schema_version=row.schema_version,
        resolution_policy_hash=row.resolution_policy_hash,
        symbol=row.symbol,
        target=row.target,
        horizon_unit=row.horizon_unit,
        series_basis=row.series_basis,
        input_timespan=row.input_timespan,
        input_multiplier=row.input_multiplier,
        as_of=row.as_of,
        sealed_at=row.sealed_at,
        currency=row.currency,
        observation_count=row.observation_count,
        target_time_count=row.target_time_count,
        first_observed_at=row.first_observed_at,
        last_observed_at=row.last_observed_at,
        max_available_at=row.max_available_at,
        availability_status=row.availability_status,
        availability_rule_set_hash=row.availability_rule_set_hash,
        availability_checked_at=row.availability_checked_at,
        canonical_payload=bytes(row.canonical_payload),
    )


def _validate_existing_record(
    record: ForecastInputSnapshotRecord,
    spec: SnapshotBuildSpec,
    policy: ForecastSnapshotBuildPolicy,
) -> ForecastInputSnapshotPayload:
    try:
        payload = parse_snapshot_payload(record.canonical_payload)
        if canonical_snapshot_payload(payload) != record.canonical_payload:
            raise SnapshotValidationError("payload bytes are not canonical")
        if snapshot_id_for_payload(record.canonical_payload) != record.snapshot_id:
            raise SnapshotValidationError("payload hash does not match snapshot_id")
        if build_snapshot_record(payload, sealed_at=record.sealed_at) != record:
            raise SnapshotValidationError("snapshot header does not match its payload")
    except SnapshotValidationError as exc:
        raise SnapshotSemanticConflict("existing semantic snapshot is invalid") from exc
    if (
        payload.resolution_policy_hash != policy.resolution_policy_hash
        or payload.symbol != spec.symbol
        or payload.target != spec.target
        or payload.horizon_unit != spec.horizon_unit
        or payload.series_basis != policy.series_basis
        or payload.input_timespan != policy.input_timespan
        or payload.input_multiplier != policy.input_multiplier
        or payload.as_of != spec.as_of
        or payload.currency != policy.currency
        or payload.availability.status != "passed"
        or payload.availability.rule_set_hash != policy.availability_rule_set_hash
        or payload.availability.checked_at is None
    ):
        raise SnapshotSemanticConflict("existing snapshot conflicts with the builder policy")
    return payload


def _semantic_statement(
    spec: SnapshotBuildSpec,
    policy: ForecastSnapshotBuildPolicy,
) -> Select[tuple[ForecastInputSnapshot]]:
    return select(ForecastInputSnapshot).where(
        ForecastInputSnapshot.schema_version == SNAPSHOT_SCHEMA_VERSION,
        ForecastInputSnapshot.resolution_policy_hash == policy.resolution_policy_hash,
        ForecastInputSnapshot.symbol == spec.symbol,
        ForecastInputSnapshot.target == spec.target,
        ForecastInputSnapshot.horizon_unit == spec.horizon_unit,
        ForecastInputSnapshot.series_basis == policy.series_basis,
        ForecastInputSnapshot.input_timespan == policy.input_timespan,
        ForecastInputSnapshot.input_multiplier == policy.input_multiplier,
        ForecastInputSnapshot.as_of == spec.as_of,
    )


@dataclass(frozen=True)
class ForecastSnapshotBuilder:
    sessionmaker: async_sessionmaker[AsyncSession]
    policy: ForecastSnapshotBuildPolicy = DEFAULT_SNAPSHOT_BUILD_POLICY

    async def build(self, spec: SnapshotBuildSpec) -> SnapshotBuildResult:
        spec = self.policy.validate_spec(spec)
        async with self.sessionmaker() as session, session.begin():
            # READ COMMITTED is intentional: a second builder can begin while
            # the first holds the advisory lock and must see the winner after
            # it wakes. The PIT bar reconstruction itself is one SQL statement,
            # so it still observes one MVCC snapshot without a stale-transaction
            # uniqueness race on replay.
            await acquire_advisory_xact_lock(
                session,
                bar_series_lock_id(spec.symbol, self.policy.source, self.policy.input_timespan),
            )
            await acquire_advisory_xact_lock(
                session,
                stable_lock_id(
                    "forecast-snapshot",
                    self.policy.resolution_policy_hash,
                    spec.symbol,
                    spec.target,
                    spec.horizon_unit,
                    self.policy.series_basis,
                    self.policy.input_timespan,
                    str(self.policy.input_multiplier),
                    _timestamp(spec.as_of),
                ),
            )
            database_now = _utc(
                (await session.execute(select(func.clock_timestamp()))).scalar_one(),
                "database clock",
            )
            if spec.as_of > database_now:
                raise SnapshotAvailabilityError("snapshot cutoff is later than the database clock")

            existing = (
                await session.execute(_semantic_statement(spec, self.policy))
            ).scalar_one_or_none()
            if existing is not None:
                record = _record_from_row(existing)
                payload = _validate_existing_record(record, spec, self.policy)
                checked_at = cast(datetime, payload.availability.checked_at)
                result = await session.execute(
                    build_point_in_time_bars_statement(spec, self.policy)
                )
                rows = tuple(PointInTimeBar(**dict(row)) for row in result.mappings())
                try:
                    reconstructed = assemble_verified_snapshot_payload(
                        rows,
                        spec,
                        checked_at=checked_at,
                        policy=self.policy,
                    )
                except SnapshotBuildError as exc:
                    raise SnapshotSemanticConflict(
                        "existing snapshot cannot be reproduced from finalized source versions"
                    ) from exc
                if canonical_snapshot_payload(reconstructed) != record.canonical_payload:
                    raise SnapshotSemanticConflict(
                        "existing snapshot bytes differ from reconstructed policy inputs"
                    )
                return SnapshotBuildResult(
                    snapshot_id=record.snapshot_id,
                    as_of=spec.as_of,
                    availability_checked_at=checked_at,
                    observation_count=record.observation_count,
                    target_time_count=record.target_time_count,
                    created=False,
                )

            result = await session.execute(build_point_in_time_bars_statement(spec, self.policy))
            rows = tuple(PointInTimeBar(**dict(row)) for row in result.mappings())
            checked_at = _utc(
                (await session.execute(select(func.clock_timestamp()))).scalar_one(),
                "availability checked_at",
            )
            payload = assemble_verified_snapshot_payload(
                rows,
                spec,
                checked_at=checked_at,
                policy=self.policy,
            )
            record = build_snapshot_record(payload, sealed_at=checked_at)
            row = ForecastInputSnapshot(
                **{field.name: getattr(record, field.name) for field in fields(record)}
            )
            session.add(row)
            await session.flush()
            await session.refresh(row)
            persisted = _record_from_row(row)
            persisted_payload = _validate_existing_record(persisted, spec, self.policy)
            persisted_checked_at = cast(datetime, persisted_payload.availability.checked_at)
            return SnapshotBuildResult(
                snapshot_id=persisted.snapshot_id,
                as_of=spec.as_of,
                availability_checked_at=persisted_checked_at,
                observation_count=persisted.observation_count,
                target_time_count=persisted.target_time_count,
                created=True,
            )


async def database_snapshot_cutoff(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> datetime:
    """Read the database clock used to freeze one retry-safe build batch."""

    async with sessionmaker() as session:
        value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    return _utc(value, "database snapshot cutoff")


def scheduled_snapshot_cutoff(database_now: datetime) -> datetime:
    """Return the stable daily cutoff used across retry and late redelivery."""

    database_now = _utc(database_now, "database clock")
    candidate = datetime.combine(database_now.date(), time(17, tzinfo=UTC))
    return candidate if candidate <= database_now else candidate - timedelta(days=1)


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SnapshotAvailabilityError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise SnapshotAvailabilityError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise SnapshotAvailabilityError(f"{label} cannot be normalized to UTC") from exc
