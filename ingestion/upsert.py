"""Idempotent upsert planning and DB sink for OHLCV bars.

The ingestion boundary stores current bar values in ``bars``. A PostgreSQL
trigger stamps database-recorded version time and atomically captures every
restatement in append-only ``bars_revisions``. The upsert uses
``IS DISTINCT FROM`` so unchanged replays are no-ops while real value changes
require strictly newer availability evidence.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC
from typing import Any, Self, cast

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator
from sqlalchemy import Select, and_, func, select, tuple_, union
from sqlalchemy.dialects.postgresql import Insert, insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from data_sources.base import AdjustmentBasis, OHLCVBar, Timespan

#: Columns forming the unique conflict key for an OHLCV upsert.
BAR_CONFLICT_KEY: tuple[str, ...] = (
    "symbol",
    "timespan",
    "multiplier",
    "ts",
    "source",
    "adjustment_basis",
)
BAR_VERSION_LANE_KEY: tuple[str, ...] = tuple(
    column for column in BAR_CONFLICT_KEY if column != "ts"
)
BAR_VERSION_KEY: tuple[str, ...] = (*BAR_CONFLICT_KEY, "version_recorded_at")
BAR_VALUE_COLUMNS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)


class BarVersionConflictError(ValueError):
    """A changed value did not carry strictly newer availability evidence."""


async def finalize_bar_version_availability(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
) -> int:
    """Publish receipts for every committed version in the supplied rows' lanes.

    This must run in a transaction that begins after the bar-upsert transaction
    commits. The database trigger rejects a same-transaction attempt and stamps
    ``available_at`` itself. Finalizing every still-unreceipted version across
    each full symbol/timespan/multiplier/source/adjustment lane also repairs the
    safe failure mode where a worker committed a bootstrap batch and died
    before writing its receipts, even when a later retry only refetches a
    trailing window.
    """

    row_list = _dedupe_rows(rows)
    if not row_list:
        return 0
    statement = build_bar_version_availability_statement(row_list)
    raw_result = await session.execute(statement)
    # Lightweight unit fakes do not emulate a CursorResult; production async
    # sessions always return one for this INSERT.
    return 0 if raw_result is None else cast(CursorResult[Any], raw_result).rowcount


def build_bar_version_availability_statement(rows: Iterable[BarUpsertRow]) -> Insert:
    """Build a lane-wide, concurrency-safe missing-receipt reconciliation."""

    row_list = _dedupe_rows(rows)
    if not row_list:
        raise ValueError("at least one bar row is required")
    lanes = sorted(
        {tuple(getattr(row, column) for column in BAR_VERSION_LANE_KEY) for row in row_list}
    )
    current = select(
        Bar.symbol,
        Bar.timespan,
        Bar.multiplier,
        Bar.ts,
        Bar.source,
        Bar.adjustment_basis,
        Bar.recorded_at.label("version_recorded_at"),
    ).where(tuple_(*(getattr(Bar, column) for column in BAR_VERSION_LANE_KEY)).in_(lanes))
    previous = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts,
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.previous_recorded_at.label("version_recorded_at"),
    ).where(
        tuple_(*(getattr(BarRevision, column) for column in BAR_VERSION_LANE_KEY)).in_(lanes),
        BarRevision.previous_recorded_at.is_not(None),
    )
    incoming = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts,
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.incoming_recorded_at.label("version_recorded_at"),
    ).where(
        tuple_(*(getattr(BarRevision, column) for column in BAR_VERSION_LANE_KEY)).in_(lanes),
        BarRevision.incoming_recorded_at.is_not(None),
    )
    versions = union(current, previous, incoming).subquery("bar_versions")
    receipt_exists = (
        select(1)
        .select_from(BarVersionAvailability)
        .where(
            and_(
                *(
                    BarVersionAvailability.__table__.c[column] == versions.c[column]
                    for column in BAR_VERSION_KEY
                )
            )
        )
        .exists()
    )
    missing_versions = select(*(versions.c[column] for column in BAR_VERSION_KEY)).where(
        ~receipt_exists
    )
    return (
        insert(BarVersionAvailability)
        .from_select(BAR_VERSION_KEY, missing_versions)
        .on_conflict_do_nothing()
    )


class BarUpsertRow(BaseModel):
    """One row ready for an idempotent OHLCV upsert."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    symbol: str
    ts: AwareDatetime
    timespan: Timespan
    multiplier: int
    source: str
    adjustment_basis: AdjustmentBasis
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int | None
    fetched_at: AwareDatetime
    as_of: AwareDatetime

    @model_validator(mode="after")
    def timestamps_follow_data_availability_order(self) -> Self:
        if self.fetched_at < self.ts:
            raise ValueError("fetched_at must not be earlier than the bar timestamp")
        if self.as_of < self.fetched_at:
            raise ValueError("as_of must not be earlier than fetched_at")
        return self

    @property
    def conflict_key(self) -> tuple[object, ...]:
        return tuple(getattr(self, column) for column in BAR_CONFLICT_KEY)

    def value_tuple(self) -> tuple[object, ...]:
        return tuple(getattr(self, column) for column in BAR_VALUE_COLUMNS)


class BarRevisionRow(BaseModel):
    """Expected revision shape used only for planning and ingest counters."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    conflict_key: tuple[object, ...]
    previous: BarUpsertRow
    incoming: BarUpsertRow


class BarUpsertPlan(BaseModel):
    """Rows to write plus expected database-triggered revisions."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    rows: list[BarUpsertRow]
    revisions: list[BarRevisionRow]


def build_bar_upserts(
    bars: Iterable[OHLCVBar], *, as_of: AwareDatetime | None = None
) -> list[BarUpsertRow]:
    """Dedupe identical bars by conflict key and stamp ``as_of``.

    Pure and idempotent: the same input always yields the same output. ``as_of``
    defaults to each bar's ``fetched_at`` when not supplied. Rows are returned
    sorted by the full conflict key for stable locking and reviewable output.
    """
    return _dedupe_rows(_to_bar_upsert_row(bar, as_of=as_of) for bar in bars)


def build_bar_upsert_plan(
    bars: Iterable[OHLCVBar],
    *,
    existing_rows: Iterable[BarUpsertRow] = (),
    as_of: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Build rows to upsert and revision rows for changed existing values."""
    return build_bar_upsert_plan_from_rows(
        build_bar_upserts(bars, as_of=as_of),
        existing_rows=existing_rows,
    )


def build_bar_upsert_plan_from_rows(
    rows: Iterable[BarUpsertRow],
    *,
    existing_rows: Iterable[BarUpsertRow] = (),
) -> BarUpsertPlan:
    """Build rows to upsert and revision rows for already-normalized rows."""
    row_list = _dedupe_rows(rows)
    existing_by_key = {row.conflict_key: row for row in existing_rows}
    revisions: list[BarRevisionRow] = []
    for row in row_list:
        previous = existing_by_key.get(row.conflict_key)
        if previous is None or previous.value_tuple() == row.value_tuple():
            continue
        if row.as_of <= previous.as_of or row.fetched_at <= previous.fetched_at:
            raise BarVersionConflictError(
                "changed bar values require strictly newer fetched_at and as_of timestamps"
            )
        revisions.append(
            BarRevisionRow(
                conflict_key=row.conflict_key,
                previous=previous,
                incoming=row,
            )
        )
    return BarUpsertPlan(rows=row_list, revisions=revisions)


async def upsert_bars(
    session: AsyncSession,
    bars: Iterable[OHLCVBar],
    *,
    as_of: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Upsert provider DTOs; PostgreSQL atomically appends revision rows."""
    return await upsert_bar_rows(
        session,
        build_bar_upserts(bars, as_of=as_of),
    )


async def upsert_bar_rows(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
) -> BarUpsertPlan:
    """Upsert normalized rows into ``bars`` and append changed values to revisions.

    The caller owns transaction boundaries. Ingestion tasks should call this
    inside ``async with session.begin()`` so revision capture and current-row
    update commit atomically.
    """
    row_list = _dedupe_rows(rows)
    if not row_list:
        return BarUpsertPlan(rows=[], revisions=[])

    await _lock_bar_keys(session, row_list)
    existing_rows = await load_existing_bar_rows(session, row_list)
    plan = build_bar_upsert_plan_from_rows(
        row_list,
        existing_rows=existing_rows,
    )

    write_started_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    # The database trigger appends revision rows and stamps recorded_at in the
    # same statement. Keeping capture in PostgreSQL prevents direct writers or
    # an absent-key race from bypassing history.
    await session.execute(build_bar_upsert_statement(plan.rows))
    await _verify_persisted_rows(session, plan.rows)
    persisted_revisions = await _load_persisted_revisions(
        session,
        plan.rows,
        recorded_not_before=write_started_at,
    )
    return BarUpsertPlan(rows=plan.rows, revisions=persisted_revisions)


async def load_existing_bar_rows(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
) -> list[BarUpsertRow]:
    """Load current ``bars`` rows for the incoming conflict keys."""
    row_list = _dedupe_rows(rows)
    if not row_list:
        return []
    result = await session.execute(build_existing_bars_select(row_list))
    return [_orm_bar_to_upsert_row(bar) for bar in result.scalars()]


def build_existing_bars_select(rows: Iterable[BarUpsertRow]) -> Select[tuple[Bar]]:
    """Build the SELECT used to fetch current rows for incoming conflict keys."""
    row_list = _dedupe_rows(rows)
    if not row_list:
        raise ValueError("at least one bar row is required")
    key_expr = tuple_(*(Bar.__table__.c[column] for column in BAR_CONFLICT_KEY))
    return select(Bar).where(key_expr.in_([row.conflict_key for row in row_list])).with_for_update()


def build_bar_upsert_statement(rows: Iterable[BarUpsertRow]) -> Insert:
    """Build the PostgreSQL upsert statement for current bar rows."""
    row_list = _dedupe_rows(rows)
    if not row_list:
        raise ValueError("at least one bar row is required")

    stmt = insert(Bar).values([_bar_to_insert_dict(row) for row in row_list])
    excluded = stmt.excluded
    value_changed = tuple_(
        *(Bar.__table__.c[column] for column in BAR_VALUE_COLUMNS)
    ).is_distinct_from(tuple_(*(excluded[column] for column in BAR_VALUE_COLUMNS)))
    update_columns = (*BAR_VALUE_COLUMNS, "fetched_at", "as_of")
    return stmt.on_conflict_do_update(
        index_elements=[Bar.__table__.c[column] for column in BAR_CONFLICT_KEY],
        set_={column: excluded[column] for column in update_columns},
        where=value_changed,
    )


def _to_bar_upsert_row(bar: OHLCVBar, *, as_of: AwareDatetime | None = None) -> BarUpsertRow:
    return BarUpsertRow(
        symbol=bar.symbol,
        ts=bar.timestamp,
        timespan=bar.timespan,
        multiplier=bar.multiplier,
        source=bar.source,
        adjustment_basis=bar.adjustment_basis,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        vwap=bar.vwap,
        trade_count=bar.trade_count,
        fetched_at=bar.fetched_at,
        as_of=as_of or bar.fetched_at,
    )


def _dedupe_rows(rows: Iterable[BarUpsertRow]) -> list[BarUpsertRow]:
    by_key: dict[tuple[object, ...], BarUpsertRow] = {}
    for row in rows:
        previous = by_key.get(row.conflict_key)
        if previous is None:
            by_key[row.conflict_key] = row
        elif previous.value_tuple() != row.value_tuple():
            raise BarVersionConflictError(
                "one ingest batch cannot contain conflicting values for the same bar key"
            )
        elif (row.as_of, row.fetched_at) < (previous.as_of, previous.fetched_at):
            # Preserve the earliest known availability for identical values;
            # input order must not rewrite historical visibility.
            by_key[row.conflict_key] = row
    return sorted(
        by_key.values(),
        key=lambda row: (
            row.symbol,
            row.timespan,
            row.multiplier,
            row.ts,
            row.source,
            row.adjustment_basis,
        ),
    )


async def _lock_bar_keys(session: AsyncSession, rows: Iterable[BarUpsertRow]) -> None:
    """Serialize even absent conflict keys for plan/trigger agreement."""

    for row in rows:
        lock_key = json.dumps(
            [
                row.symbol,
                row.timespan,
                row.multiplier,
                row.ts.astimezone(UTC).isoformat(),
                row.source,
                row.adjustment_basis,
            ],
            separators=(",", ":"),
        )
        await session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(lock_key, 0)))
        )


async def _verify_persisted_rows(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
) -> None:
    """Fail if a non-cooperating concurrent writer won an absent-key race."""

    row_list = _dedupe_rows(rows)
    persisted = {row.conflict_key: row for row in await load_existing_bar_rows(session, row_list)}
    for incoming in row_list:
        current = persisted.get(incoming.conflict_key)
        if current is None or current.value_tuple() != incoming.value_tuple():
            raise BarVersionConflictError("database retained a different bar version")


async def _load_persisted_revisions(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
    *,
    recorded_not_before: AwareDatetime,
) -> list[BarRevisionRow]:
    """Hydrate revisions actually emitted by this DB write window."""

    row_list = _dedupe_rows(rows)
    version_columns = (
        *(BarRevision.__table__.c[column] for column in BAR_CONFLICT_KEY),
        BarRevision.incoming_fetched_at,
        BarRevision.incoming_as_of,
    )
    version_keys = [(*row.conflict_key, row.fetched_at, row.as_of) for row in row_list]
    result = await session.execute(
        select(BarRevision).where(
            tuple_(*version_columns).in_(version_keys),
            BarRevision.incoming_recorded_at >= recorded_not_before,
        )
    )
    revisions = [_orm_revision_to_plan(row) for row in result.scalars()]
    if len({revision.conflict_key for revision in revisions}) != len(revisions):
        raise RuntimeError("database emitted duplicate revisions for one incoming bar version")
    return sorted(revisions, key=lambda revision: revision.conflict_key)


def _bar_to_insert_dict(row: BarUpsertRow) -> dict[str, Any]:
    columns = (*BAR_CONFLICT_KEY, *BAR_VALUE_COLUMNS, "fetched_at", "as_of")
    return {column: getattr(row, column) for column in columns}


def _orm_bar_to_upsert_row(bar: Bar) -> BarUpsertRow:
    return BarUpsertRow(
        symbol=bar.symbol,
        ts=bar.ts,
        timespan=cast(Timespan, bar.timespan),
        multiplier=bar.multiplier,
        source=bar.source,
        adjustment_basis=cast(AdjustmentBasis, bar.adjustment_basis),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        vwap=bar.vwap,
        trade_count=bar.trade_count,
        fetched_at=bar.fetched_at,
        as_of=bar.as_of,
    )


def _orm_revision_to_plan(revision: BarRevision) -> BarRevisionRow:
    common = {
        "symbol": revision.symbol,
        "ts": revision.ts,
        "timespan": cast(Timespan, revision.timespan),
        "multiplier": revision.multiplier,
        "source": revision.source,
        "adjustment_basis": cast(AdjustmentBasis, revision.adjustment_basis),
    }
    previous = BarUpsertRow(
        **common,
        open=revision.previous_open,
        high=revision.previous_high,
        low=revision.previous_low,
        close=revision.previous_close,
        volume=revision.previous_volume,
        vwap=revision.previous_vwap,
        trade_count=revision.previous_trade_count,
        fetched_at=revision.previous_fetched_at,
        as_of=revision.previous_as_of,
    )
    incoming = BarUpsertRow(
        **common,
        open=revision.incoming_open,
        high=revision.incoming_high,
        low=revision.incoming_low,
        close=revision.incoming_close,
        volume=revision.incoming_volume,
        vwap=revision.incoming_vwap,
        trade_count=revision.incoming_trade_count,
        fetched_at=revision.incoming_fetched_at,
        as_of=revision.incoming_as_of,
    )
    return BarRevisionRow(
        conflict_key=incoming.conflict_key,
        previous=previous,
        incoming=incoming,
    )
