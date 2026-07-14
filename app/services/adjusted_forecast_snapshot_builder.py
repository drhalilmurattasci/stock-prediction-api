"""Privileged creation of factor-backed adjusted-close forecast snapshots.

This lane is deliberately separate from the raw-close snapshot builder.  It
selects one deterministic, already-published adjustment-factor set at an exact
cutoff, reconstructs and validates every immutable input through the adjusted
price kernel, and only then opens a short transaction to insert or replay one
content-addressed forecast snapshot.

The factor artifact's later post-commit receipt is the availability boundary
for every derived observation.  A factor cutoff is therefore allowed (and
expected) to precede the forecast snapshot cutoff; requiring them to be equal
would make a correctly receipted factor artifact impossible to consume.
"""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, date, datetime
from functools import lru_cache
from importlib.metadata import version as package_version
from typing import Any, Literal, cast

import exchange_calendars
import pandas as pd
from sqlalchemy import Select, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.adjustment_factors import (
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.services.adjusted_price_store import (
    AdjustedPriceEvidenceUnavailable,
    AdjustedPriceFactorSetNotFound,
    LoadedAdjustedPriceEvidence,
    load_adjusted_price_evidence,
)
from app.services.adjusted_prices import (
    ADJUSTED_PRICE_BASIS,
    AdjustedPriceError,
    AdjustedPriceWindow,
    adjust_ohlcv_window,
)
from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_POLICY_HASH,
    ADJUSTMENT_FACTOR_POLICY_VERSION,
    ADJUSTMENT_FACTOR_SET_FORMAT,
)
from app.services.corporate_actions import CORPORATE_ACTION_QUERY_POLICY_HASH
from app.services.forecast_snapshot_builder import (
    SnapshotAvailabilityError,
    SnapshotBuildError,
    SnapshotBuildMisconfigured,
    SnapshotBuildResult,
    SnapshotInputUnavailable,
    SnapshotSemanticConflict,
)
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
from ingestion.locks import acquire_advisory_xact_lock, stable_lock_id

AdjustedSnapshotTarget = Literal["adjusted_close"]
AdjustedSnapshotHorizonUnit = Literal["trading_day"]

_CALENDAR_ENGINE_VERSION = "4.13.2"
_CALENDAR_TZDATA_VERSION = package_version("tzdata")
_PANDAS_VERSION = package_version("pandas")
_HASH_PREFIX = "sha256:"
_CALENDAR_START = "1990-01-01"
_CALENDAR_END = "2100-12-31"


def _hash_document(document: Mapping[str, object]) -> str:
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return _HASH_PREFIX + hashlib.sha256(canonical).hexdigest()


ADJUSTED_AVAILABILITY_RULE_SET_DOCUMENT: dict[str, object] = {
    "factor_evidence": {
        "action_query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
        "factor_format": ADJUSTMENT_FACTOR_SET_FORMAT,
        "factor_policy_hash": ADJUSTMENT_FACTOR_POLICY_HASH,
        "factor_policy_version": ADJUSTMENT_FACTOR_POLICY_VERSION,
    },
    "format": "forecast-adjusted-snapshot-availability-rules-v1",
    "rules": [
        "factor_set_content_address_matches_exact_canonical_payload",
        "factor_set_has_distinct_post_commit_receipt",
        "factor_receipt_available_at<=snapshot_cutoff",
        "exact_split_and_dividend_collection_receipts_bound_by_factor_set",
        "exact_raw_bar_version_receipt_per_factor_ordinal",
        "persisted_factor_projection_matches_canonical_payload",
        "complete_factor_window_validated_before_observation_slice",
        "published_ieee754_binary64_factor_bits_applied_without_decimal_reparse",
        "derived_observation_available_at=factor_set_receipt_available_at",
        "exact_contiguous_xnys_inputs_through_latest_completed_session",
        "minimum_258_complete_observations",
        "finite_nonnegative_adjusted_close",
        "db_clock_checked_at_not_before_snapshot_cutoff",
    ],
}
ADJUSTED_AVAILABILITY_RULE_SET_HASH = _hash_document(ADJUSTED_AVAILABILITY_RULE_SET_DOCUMENT)


@dataclass(frozen=True)
class AdjustedSnapshotBuildSpec:
    """One exact adjusted-close snapshot request."""

    symbol: str
    target: AdjustedSnapshotTarget
    horizon_unit: AdjustedSnapshotHorizonUnit
    as_of: datetime


@dataclass(frozen=True)
class AdjustedForecastSnapshotBuildPolicy:
    """All adjusted snapshot resolution choices covered by one public hash."""

    input_timespan: str = "day"
    input_multiplier: int = 1
    series_basis: str = ADJUSTED_PRICE_BASIS
    currency: str = "USD"
    calendar_name: str = "XNYS"
    observation_limit: int = 512
    minimum_observations: int = 258
    target_time_count: int = 252
    allowed_symbols: tuple[str, ...] = ("AAPL", "MSFT", "NVDA", "QQQ", "SPY")

    @property
    def availability_rule_set_hash(self) -> str:
        return ADJUSTED_AVAILABILITY_RULE_SET_HASH

    @property
    def resolution_policy_hash(self) -> str:
        return _hash_document(self.resolution_policy_document)

    @property
    def resolution_policy_document(self) -> dict[str, object]:
        """Every selection, transform, and lineage choice covered by the hash."""

        return {
            "availability_rule_set_hash": self.availability_rule_set_hash,
            "calendar": {
                "engine": "exchange_calendars",
                "engine_version": _CALENDAR_ENGINE_VERSION,
                "name": self.calendar_name,
                "pandas_version": _PANDAS_VERSION,
                "schedule_end": _CALENDAR_END,
                "schedule_start": _CALENDAR_START,
                "target_timestamp": "session_close_utc",
                "tzdata_version": _CALENDAR_TZDATA_VERSION,
            },
            "currency": {
                "resolver": "fixed_us_equity_universe_v1",
                "value": self.currency,
            },
            "cutoff": {
                "admissible": "operator_selected_utc_instant_not_after_database_clock",
            },
            "factor_input": {
                "action_query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
                "basis": self.series_basis,
                "factor_format": ADJUSTMENT_FACTOR_SET_FORMAT,
                "factor_policy_hash": ADJUSTMENT_FACTOR_POLICY_HASH,
                "factor_policy_version": ADJUSTMENT_FACTOR_POLICY_VERSION,
                "input_multiplier": self.input_multiplier,
                "input_timespan": self.input_timespan,
                "receipt": "distinct_post_commit_receipt_available_not_after_cutoff",
                "selection": {
                    "eligible": [
                        "symbol_matches",
                        "factor_policy_matches",
                        "factor_cutoff<=snapshot_cutoff",
                        "factor_receipt_available_at<=snapshot_cutoff",
                        "anchor=latest_completed_xnys_session",
                        "input_count>=minimum_observations",
                    ],
                    "order_descending": [
                        "factor_cutoff",
                        "factor_receipt_available_at",
                        "factor_recorded_at",
                        "factor_set_id",
                    ],
                },
                "source": "persisted_stockapi_adjustment_factor_set",
                "transform": "raw_close_f64*published_price_factor_f64",
            },
            "format": "forecast-adjusted-snapshot-resolution-policy-v1",
            "horizon_units": ["trading_day"],
            "lineage_sources": [
                "polygon_open_close_exact_raw_receipt_manifest",
                "polygon_split_collection",
                "polygon_dividend_collection",
                "stockapi_adjustment_factor_set",
            ],
            "minimum_observations": self.minimum_observations,
            "observation_limit": self.observation_limit,
            "symbols": list(self.allowed_symbols),
            "targets": {
                "adjusted_close": {
                    "series_basis": self.series_basis,
                    "transform": "identity_of_factor_adjusted_close",
                }
            },
            "target_time_count": self.target_time_count,
        }

    def validate_spec(self, spec: AdjustedSnapshotBuildSpec) -> AdjustedSnapshotBuildSpec:
        symbol = spec.symbol.strip().upper() if isinstance(spec.symbol, str) else ""
        if symbol not in self.allowed_symbols:
            raise SnapshotBuildError(
                f"symbol {symbol!r} is outside the pinned US-equity snapshot universe"
            )
        if spec.target != "adjusted_close":
            raise SnapshotBuildError(
                "only split/dividend-adjusted close snapshots are supported by this policy"
            )
        if spec.horizon_unit != "trading_day":
            raise SnapshotBuildError("only trading_day horizons are supported by this policy")
        if (
            self.series_basis != ADJUSTED_PRICE_BASIS
            or self.input_timespan != "day"
            or self.input_multiplier != 1
        ):
            raise SnapshotBuildMisconfigured(
                "adjusted snapshot input source contract is unsupported"
            )
        if not (
            1 <= self.minimum_observations <= self.observation_limit <= 10_000
            and 1 <= self.target_time_count <= 252
        ):
            raise SnapshotBuildMisconfigured("adjusted snapshot window bounds are invalid")
        return AdjustedSnapshotBuildSpec(
            symbol=symbol,
            target="adjusted_close",
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
                "configured adjusted resolution-policy hash does not match the builder"
            )
        if availability_rule_set_hash != self.availability_rule_set_hash:
            raise SnapshotBuildMisconfigured(
                "configured adjusted availability rule-set hash does not match the builder"
            )


DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY = AdjustedForecastSnapshotBuildPolicy()
ADJUSTED_RESOLUTION_POLICY_DOCUMENT = (
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.resolution_policy_document
)
ADJUSTED_RESOLUTION_POLICY_HASH = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.resolution_policy_hash


def build_eligible_adjustment_factor_statement(
    spec: AdjustedSnapshotBuildSpec,
    *,
    anchor_date: date,
    policy: AdjustedForecastSnapshotBuildPolicy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
) -> Select[tuple[str]]:
    """Select the deterministic newest eligible, fully receipted factor set."""

    normalized = policy.validate_spec(spec)
    return (
        select(AdjustmentFactorSetRecord.factor_set_id)
        .join(
            AdjustmentFactorSetAvailability,
            and_(
                AdjustmentFactorSetAvailability.factor_set_id
                == AdjustmentFactorSetRecord.factor_set_id,
                AdjustmentFactorSetAvailability.factor_set_recorded_at
                == AdjustmentFactorSetRecord.recorded_at,
            ),
        )
        .where(
            AdjustmentFactorSetRecord.symbol == normalized.symbol,
            AdjustmentFactorSetRecord.format == ADJUSTMENT_FACTOR_SET_FORMAT,
            AdjustmentFactorSetRecord.policy_version == ADJUSTMENT_FACTOR_POLICY_VERSION,
            AdjustmentFactorSetRecord.policy_hash == ADJUSTMENT_FACTOR_POLICY_HASH,
            AdjustmentFactorSetRecord.cutoff <= normalized.as_of,
            AdjustmentFactorSetAvailability.available_at <= normalized.as_of,
            AdjustmentFactorSetRecord.anchor_date == anchor_date,
            AdjustmentFactorSetRecord.coverage_end == anchor_date,
            AdjustmentFactorSetRecord.input_count >= policy.minimum_observations,
        )
        .order_by(
            AdjustmentFactorSetRecord.cutoff.desc(),
            AdjustmentFactorSetAvailability.available_at.desc(),
            AdjustmentFactorSetRecord.recorded_at.desc(),
            AdjustmentFactorSetRecord.factor_set_id.desc(),
        )
        .limit(1)
    )


@lru_cache(maxsize=8)
def _calendar(policy: AdjustedForecastSnapshotBuildPolicy):
    if exchange_calendars.__version__ != _CALENDAR_ENGINE_VERSION:
        raise SnapshotBuildMisconfigured(
            "exchange_calendars version differs from the adjusted resolution policy"
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
    policy: AdjustedForecastSnapshotBuildPolicy,
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


def verify_adjusted_snapshot_evidence(
    evidence: LoadedAdjustedPriceEvidence,
    spec: AdjustedSnapshotBuildSpec,
    *,
    policy: AdjustedForecastSnapshotBuildPolicy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
) -> AdjustedPriceWindow:
    """Validate the complete factor artifact before any observation slicing."""

    normalized = policy.validate_spec(spec)
    try:
        window = adjust_ohlcv_window(
            factor_set=evidence.factor_set,
            raw_rows=evidence.raw_rows,
            split_collection_receipt=evidence.split_collection_receipt,
            dividend_collection_receipt=evidence.dividend_collection_receipt,
            factor_set_receipt=evidence.factor_set_receipt,
        )
    except AdjustedPriceError as exc:
        raise SnapshotAvailabilityError(
            "persisted adjustment-factor evidence failed full-window validation"
        ) from exc

    calendar = _calendar(policy)
    latest = _latest_completed_session(calendar, normalized.as_of)
    latest_close = _utc(calendar.session_close(latest).to_pydatetime(), "latest session close")
    rows = window.rows
    lineage = window.lineage
    if (
        evidence.factor_set.symbol != normalized.symbol
        or lineage.factor_set_id != evidence.factor_set.factor_set_id
        or lineage.policy_hash != ADJUSTMENT_FACTOR_POLICY_HASH
        or lineage.policy_version != ADJUSTMENT_FACTOR_POLICY_VERSION
        or lineage.adjustment_basis != policy.series_basis
        or lineage.cutoff > normalized.as_of
        or lineage.factor_set_available_at > normalized.as_of
        or lineage.anchor_date != latest.date()
        or len(rows) != lineage.raw_input_count
    ):
        raise SnapshotAvailabilityError(
            "adjustment-factor evidence does not match the adjusted snapshot policy"
        )
    if len(rows) < policy.minimum_observations:
        raise SnapshotInputUnavailable(
            "adjusted snapshot requires at least "
            f"{policy.minimum_observations} complete observations; found {len(rows)}"
        )
    if rows[-1].timestamp != latest_close:
        raise SnapshotInputUnavailable(
            "adjustment-factor inputs do not end at the latest completed XNYS session"
        )

    expected_labels = calendar.sessions_in_range(
        pd.Timestamp(rows[0].timestamp.date()),
        pd.Timestamp(rows[-1].timestamp.date()),
    )
    expected_closes = tuple(
        _utc(calendar.session_close(label).to_pydatetime(), "factor input session close")
        for label in expected_labels
    )
    if tuple(row.timestamp for row in rows) != expected_closes:
        raise SnapshotAvailabilityError(
            "adjustment-factor inputs are not exact contiguous XNYS session closes"
        )
    if any(
        row.available_at != lineage.factor_set_available_at
        or row.available_at > normalized.as_of
        or not math.isfinite(float(row.close))
        or float(row.close) < 0.0
        for row in rows
    ):
        raise SnapshotAvailabilityError(
            "adjusted observations violate value or factor-receipt availability rules"
        )
    return window


def _raw_source_snapshot_id(evidence: LoadedAdjustedPriceEvidence) -> str:
    artifact = evidence.factor_set
    manifest: dict[str, object] = {
        "factor_set_id": artifact.factor_set_id,
        "format": "forecast-adjusted-raw-source-manifest-v1",
        "selector": {
            "adjustment_basis": "raw",
            "field": "close",
            "multiplier": 1,
            "source": "polygon_open_close",
            "symbol": artifact.symbol,
            "timespan": "day",
        },
        "versions": [
            {
                "available_at": _timestamp(row.available_at),
                "close_f64": struct.pack(">d", float(row.close)).hex(),
                "observed_at": _timestamp(row.observed_at),
                "version_recorded_at": _timestamp(row.version_recorded_at),
            }
            for row in artifact.raw_inputs
        ],
    }
    return _hash_document(manifest)


def assemble_verified_adjusted_snapshot_payload(
    evidence: LoadedAdjustedPriceEvidence,
    window: AdjustedPriceWindow,
    spec: AdjustedSnapshotBuildSpec,
    *,
    checked_at: datetime,
    policy: AdjustedForecastSnapshotBuildPolicy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
) -> ForecastInputSnapshotPayload:
    """Assemble a canonical adjusted snapshot from one verified full window."""

    normalized = policy.validate_spec(spec)
    checked = _utc(checked_at, "availability checked_at")
    if checked < normalized.as_of:
        raise SnapshotAvailabilityError("availability check predates the snapshot cutoff")
    if window.lineage.factor_set_id != evidence.factor_set.factor_set_id:
        raise SnapshotAvailabilityError("adjusted window and factor evidence identities differ")
    rows = window.rows[-policy.observation_limit :]
    if len(rows) < policy.minimum_observations:
        raise SnapshotInputUnavailable(
            "adjusted observation slice is shorter than the pinned minimum"
        )

    lineage = window.lineage
    raw_max_available_at = max(row.raw_available_at for row in window.rows)
    return ForecastInputSnapshotPayload(
        resolution_policy_hash=policy.resolution_policy_hash,
        symbol=normalized.symbol,
        target=normalized.target,
        horizon_unit=normalized.horizon_unit,
        series_basis=policy.series_basis,
        input_timespan=policy.input_timespan,
        input_multiplier=policy.input_multiplier,
        as_of=normalized.as_of,
        currency=policy.currency,
        observations=tuple(
            SnapshotObservation(
                observed_at=row.timestamp,
                available_at=lineage.factor_set_available_at,
                value=row.close,
            )
            for row in rows
        ),
        target_times=_target_times(normalized.as_of, policy, _calendar(policy)),
        data_sources=(
            SnapshotSourceLineage(
                name="polygon_open_close",
                snapshot_id=_raw_source_snapshot_id(evidence),
                max_available_at=raw_max_available_at,
                fields=("close",),
            ),
            SnapshotSourceLineage(
                name="polygon_splits",
                snapshot_id=lineage.split_collection_id,
                max_available_at=lineage.split_collection_available_at,
                fields=("split_ratio",),
            ),
            SnapshotSourceLineage(
                name="polygon_dividends",
                snapshot_id=lineage.dividend_collection_id,
                max_available_at=lineage.dividend_collection_available_at,
                fields=("cash_dividend",),
            ),
            SnapshotSourceLineage(
                name="stockapi_adjustment_factors",
                snapshot_id=lineage.factor_set_id,
                max_available_at=lineage.factor_set_available_at,
                fields=("adjusted_close", "price_factor_f64"),
            ),
        ),
        availability=SnapshotAvailabilityEvidence(
            status="passed",
            rule_set_hash=policy.availability_rule_set_hash,
            checked_at=checked,
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
    spec: AdjustedSnapshotBuildSpec,
    policy: AdjustedForecastSnapshotBuildPolicy,
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
        raise SnapshotSemanticConflict("existing adjusted semantic snapshot is invalid") from exc
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
        raise SnapshotSemanticConflict(
            "existing adjusted snapshot conflicts with the builder policy"
        )
    return payload


def _semantic_statement(
    spec: AdjustedSnapshotBuildSpec,
    policy: AdjustedForecastSnapshotBuildPolicy,
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
class AdjustedForecastSnapshotBuilder:
    """Two-phase adjusted snapshot builder with a short final transaction."""

    sessionmaker: async_sessionmaker[AsyncSession]
    policy: AdjustedForecastSnapshotBuildPolicy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY

    async def build(self, spec: AdjustedSnapshotBuildSpec) -> SnapshotBuildResult:
        normalized = self.policy.validate_spec(spec)
        calendar = _calendar(self.policy)
        anchor = _latest_completed_session(calendar, normalized.as_of).date()

        async with self.sessionmaker() as read_session:
            database_now = _utc(
                (await read_session.execute(select(func.clock_timestamp()))).scalar_one(),
                "database clock",
            )
            if normalized.as_of > database_now:
                raise SnapshotAvailabilityError(
                    "adjusted snapshot cutoff is later than the database clock"
                )
            factor_set_id = (
                await read_session.execute(
                    build_eligible_adjustment_factor_statement(
                        normalized,
                        anchor_date=anchor,
                        policy=self.policy,
                    )
                )
            ).scalar_one_or_none()
            if factor_set_id is None:
                raise SnapshotInputUnavailable(
                    "no eligible receipted adjustment-factor set is visible at the cutoff"
                )
            try:
                evidence = await load_adjusted_price_evidence(read_session, factor_set_id)
            except (AdjustedPriceEvidenceUnavailable, AdjustedPriceFactorSetNotFound) as exc:
                raise SnapshotAvailabilityError(
                    "selected adjustment-factor evidence is incomplete or invalid"
                ) from exc

        # The factor, action, receipt, and exact raw-version rows are immutable.
        # Full validation and floating-point work therefore need not hold a DB
        # connection or an idle transaction.
        window = verify_adjusted_snapshot_evidence(evidence, normalized, policy=self.policy)

        async with self.sessionmaker() as write_session, write_session.begin():
            await acquire_advisory_xact_lock(
                write_session,
                stable_lock_id(
                    "forecast-snapshot",
                    self.policy.resolution_policy_hash,
                    normalized.symbol,
                    normalized.target,
                    normalized.horizon_unit,
                    self.policy.series_basis,
                    self.policy.input_timespan,
                    str(self.policy.input_multiplier),
                    _timestamp(normalized.as_of),
                ),
            )
            database_now = _utc(
                (await write_session.execute(select(func.clock_timestamp()))).scalar_one(),
                "availability checked_at",
            )
            if database_now < normalized.as_of:
                raise SnapshotAvailabilityError(
                    "availability check predates the adjusted snapshot cutoff"
                )
            existing = (
                await write_session.execute(_semantic_statement(normalized, self.policy))
            ).scalar_one_or_none()
            if existing is not None:
                record = _record_from_row(existing)
                existing_payload = _validate_existing_record(
                    record,
                    normalized,
                    self.policy,
                )
                checked_at = cast(datetime, existing_payload.availability.checked_at)
                reconstructed = assemble_verified_adjusted_snapshot_payload(
                    evidence,
                    window,
                    normalized,
                    checked_at=checked_at,
                    policy=self.policy,
                )
                if canonical_snapshot_payload(reconstructed) != record.canonical_payload:
                    raise SnapshotSemanticConflict(
                        "existing adjusted snapshot bytes differ from reconstructed factor evidence"
                    )
                return SnapshotBuildResult(
                    snapshot_id=record.snapshot_id,
                    as_of=normalized.as_of,
                    availability_checked_at=checked_at,
                    observation_count=record.observation_count,
                    target_time_count=record.target_time_count,
                    created=False,
                )

            payload = assemble_verified_adjusted_snapshot_payload(
                evidence,
                window,
                normalized,
                checked_at=database_now,
                policy=self.policy,
            )
            record = build_snapshot_record(payload, sealed_at=database_now)
            row = ForecastInputSnapshot(
                **{field.name: getattr(record, field.name) for field in fields(record)}
            )
            write_session.add(row)
            await write_session.flush()
            await write_session.refresh(row)
            persisted = _record_from_row(row)
            persisted_payload = _validate_existing_record(
                persisted,
                normalized,
                self.policy,
            )
            persisted_checked_at = cast(
                datetime,
                persisted_payload.availability.checked_at,
            )
            return SnapshotBuildResult(
                snapshot_id=persisted.snapshot_id,
                as_of=normalized.as_of,
                availability_checked_at=persisted_checked_at,
                observation_count=persisted.observation_count,
                target_time_count=persisted.target_time_count,
                created=True,
            )


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SnapshotAvailabilityError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise SnapshotAvailabilityError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise SnapshotAvailabilityError(f"{label} cannot be normalized to UTC") from exc


__all__ = [
    "ADJUSTED_AVAILABILITY_RULE_SET_DOCUMENT",
    "ADJUSTED_AVAILABILITY_RULE_SET_HASH",
    "ADJUSTED_RESOLUTION_POLICY_DOCUMENT",
    "ADJUSTED_RESOLUTION_POLICY_HASH",
    "DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY",
    "AdjustedForecastSnapshotBuildPolicy",
    "AdjustedForecastSnapshotBuilder",
    "AdjustedSnapshotBuildSpec",
    "assemble_verified_adjusted_snapshot_payload",
    "build_eligible_adjustment_factor_statement",
    "verify_adjusted_snapshot_evidence",
]
