"""Idempotent upsert planning and DB sink for OHLCV bars.

The ingestion boundary stores raw/current bar values in ``bars`` and preserves
vendor restatements in ``bars_revisions`` before updating the current row. The
database write uses ``IS DISTINCT FROM`` so unchanged replays are no-ops while
real value changes are explicit and auditable.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import Select, select, tuple_
from sqlalchemy.dialects.postgresql import Insert, insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.bars import Bar, BarRevision
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
BAR_VALUE_COLUMNS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)


class BarUpsertRow(BaseModel):
    """One row ready for an idempotent OHLCV upsert."""

    model_config = ConfigDict(frozen=True, extra="forbid")

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

    @property
    def conflict_key(self) -> tuple[object, ...]:
        return tuple(getattr(self, column) for column in BAR_CONFLICT_KEY)

    def value_tuple(self) -> tuple[object, ...]:
        return tuple(getattr(self, column) for column in BAR_VALUE_COLUMNS)


class BarRevisionRow(BaseModel):
    """One captured prior value for an incoming bar restatement/correction."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    conflict_key: tuple[object, ...]
    previous: BarUpsertRow
    incoming: BarUpsertRow
    revised_at: AwareDatetime


class BarUpsertPlan(BaseModel):
    """Rows to write plus append-only revisions to insert first."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: list[BarUpsertRow]
    revisions: list[BarRevisionRow]


def build_bar_upserts(
    bars: Iterable[OHLCVBar], *, as_of: AwareDatetime | None = None
) -> list[BarUpsertRow]:
    """Dedupe bars by conflict key (last write wins) and stamp ``as_of``.

    Pure and idempotent: the same input always yields the same output. ``as_of``
    defaults to each bar's ``fetched_at`` when not supplied. Rows are returned
    sorted by (symbol, ts, source) for stable, reviewable output.
    """
    by_key: dict[tuple[object, ...], BarUpsertRow] = {}
    for bar in bars:
        row = _to_bar_upsert_row(bar, as_of=as_of)
        by_key[row.conflict_key] = row
    return sorted(by_key.values(), key=lambda r: (r.symbol, r.ts, r.source))


def build_bar_upsert_plan(
    bars: Iterable[OHLCVBar],
    *,
    existing_rows: Iterable[BarUpsertRow] = (),
    as_of: AwareDatetime | None = None,
    revised_at: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Build rows to upsert and revision rows for changed existing values."""
    return build_bar_upsert_plan_from_rows(
        build_bar_upserts(bars, as_of=as_of),
        existing_rows=existing_rows,
        revised_at=revised_at,
    )


def build_bar_upsert_plan_from_rows(
    rows: Iterable[BarUpsertRow],
    *,
    existing_rows: Iterable[BarUpsertRow] = (),
    revised_at: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Build rows to upsert and revision rows for already-normalized rows."""
    row_list = _dedupe_rows(rows)
    existing_by_key = {row.conflict_key: row for row in existing_rows}
    revisions: list[BarRevisionRow] = []
    for row in row_list:
        previous = existing_by_key.get(row.conflict_key)
        if previous is None or previous.value_tuple() == row.value_tuple():
            continue
        revisions.append(
            BarRevisionRow(
                conflict_key=row.conflict_key,
                previous=previous,
                incoming=row,
                revised_at=revised_at or row.as_of,
            )
        )
    return BarUpsertPlan(rows=row_list, revisions=revisions)


async def upsert_bars(
    session: AsyncSession,
    bars: Iterable[OHLCVBar],
    *,
    as_of: AwareDatetime | None = None,
    revised_at: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Upsert provider DTOs into TimescaleDB and append revision rows first."""
    return await upsert_bar_rows(
        session,
        build_bar_upserts(bars, as_of=as_of),
        revised_at=revised_at,
    )


async def upsert_bar_rows(
    session: AsyncSession,
    rows: Iterable[BarUpsertRow],
    *,
    revised_at: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Upsert normalized rows into ``bars`` and append changed values to revisions.

    The caller owns transaction boundaries. Ingestion tasks should call this
    inside ``async with session.begin()`` so revision capture and current-row
    update commit atomically.
    """
    row_list = _dedupe_rows(rows)
    if not row_list:
        return BarUpsertPlan(rows=[], revisions=[])

    existing_rows = await load_existing_bar_rows(session, row_list)
    plan = build_bar_upsert_plan_from_rows(
        row_list,
        existing_rows=existing_rows,
        revised_at=revised_at,
    )

    if plan.revisions:
        revision_dicts = [_revision_to_insert_dict(r) for r in plan.revisions]
        await session.execute(insert(BarRevision), revision_dicts)
    await session.execute(build_bar_upsert_statement(plan.rows))
    return plan


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
        by_key[row.conflict_key] = row
    return sorted(by_key.values(), key=lambda r: (r.symbol, r.ts, r.source))


def _bar_to_insert_dict(row: BarUpsertRow) -> dict[str, Any]:
    columns = (*BAR_CONFLICT_KEY, *BAR_VALUE_COLUMNS, "fetched_at", "as_of")
    return {column: getattr(row, column) for column in columns}


def _revision_to_insert_dict(revision: BarRevisionRow) -> dict[str, Any]:
    previous = revision.previous
    incoming = revision.incoming
    values: dict[str, Any] = {column: getattr(previous, column) for column in BAR_CONFLICT_KEY}
    for column in BAR_VALUE_COLUMNS:
        values[f"previous_{column}"] = getattr(previous, column)
        values[f"incoming_{column}"] = getattr(incoming, column)
    values["previous_fetched_at"] = previous.fetched_at
    values["previous_as_of"] = previous.as_of
    values["incoming_fetched_at"] = incoming.fetched_at
    values["incoming_as_of"] = incoming.as_of
    values["revised_at"] = revision.revised_at
    return values


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
