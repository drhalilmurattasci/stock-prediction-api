"""Idempotent upsert shape for OHLCV bars (DB-agnostic).

Defines the canonical row a future TimescaleDB upsert will write, plus a pure
helper that dedupes provider bars by their conflict key. There is no database
access yet — the ORM/hypertable layer is not implemented. The intended DB
semantics for the eventual ingestion job are::

    WITH incoming AS (...), changed AS (
      SELECT existing.*, incoming.*
      FROM bars existing
      JOIN incoming USING (symbol, timespan, multiplier, ts, source, adjustment_basis)
      WHERE (existing.open, existing.high, existing.low, existing.close,
             existing.volume, existing.vwap, existing.trade_count)
        IS DISTINCT FROM
            (incoming.open, incoming.high, incoming.low, incoming.close,
             incoming.volume, incoming.vwap, incoming.trade_count)
    )
    INSERT INTO bars_revisions (...) SELECT ... FROM changed;

    INSERT INTO bars (...)
    VALUES (...)
    ON CONFLICT (symbol, timespan, multiplier, ts, source, adjustment_basis)
    DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                  close = EXCLUDED.close, volume = EXCLUDED.volume,
                  vwap = EXCLUDED.vwap, trade_count = EXCLUDED.trade_count,
                  fetched_at = EXCLUDED.fetched_at, as_of = EXCLUDED.as_of
    WHERE (bars.open, bars.high, bars.low, bars.close,
           bars.volume, bars.vwap, bars.trade_count)
      IS DISTINCT FROM
          (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close,
           EXCLUDED.volume, EXCLUDED.vwap, EXCLUDED.trade_count);

Keeping raw + adjustment basis in the key lets raw and adjusted series coexist.
Revision capture preserves the pre-restatement row instead of silently vaporizing
history, matching the immutable-data doctrine without implementing full bitemporal
storage in P1.
"""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import AwareDatetime, BaseModel, ConfigDict

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
    """One row ready for an idempotent OHLCV upsert (no DB dependency)."""

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
    """Pure upsert plan: rows to write plus revisions to append first."""

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
        by_key[row.conflict_key] = row  # last write wins
    return sorted(by_key.values(), key=lambda r: (r.symbol, r.ts, r.source))


def build_bar_upsert_plan(
    bars: Iterable[OHLCVBar],
    *,
    existing_rows: Iterable[BarUpsertRow] = (),
    as_of: AwareDatetime | None = None,
    revised_at: AwareDatetime | None = None,
) -> BarUpsertPlan:
    """Build rows to upsert and revision rows for changed existing values.

    ``existing_rows`` is supplied by the future DB layer after loading current
    rows for the incoming conflict keys. When a row already exists and any value
    column differs, the previous row is emitted into ``revisions`` before the
    incoming row overwrites it.
    """
    rows = build_bar_upserts(bars, as_of=as_of)
    existing_by_key = {row.conflict_key: row for row in existing_rows}
    revisions: list[BarRevisionRow] = []
    for row in rows:
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
    return BarUpsertPlan(rows=rows, revisions=revisions)


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
