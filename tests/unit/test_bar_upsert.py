"""Unit tests for the idempotent OHLCV upsert shape (no DB)."""

from __future__ import annotations

from datetime import UTC, datetime

from data_sources.base import OHLCVBar
from ingestion.upsert import BAR_CONFLICT_KEY, build_bar_upsert_plan, build_bar_upserts

TS = datetime(2026, 7, 6, tzinfo=UTC)


def _bar(close: float, fetched_at: datetime, **overrides) -> OHLCVBar:
    fields = {
        "symbol": "AAPL",
        "timestamp": TS,
        "timespan": "day",
        "multiplier": 1,
        "open": close,
        "high": close + 1.0,
        "low": max(close - 1.0, 0.0),
        "close": close,
        "volume": 100.0,
        "source": "polygon",
        "adjustment_basis": "raw",
        "fetched_at": fetched_at,
    }
    fields.update(overrides)
    return OHLCVBar(**fields)


def test_same_key_last_write_wins():
    earlier = datetime(2026, 7, 6, 10, tzinfo=UTC)
    later = datetime(2026, 7, 6, 11, tzinfo=UTC)
    rows = build_bar_upserts([_bar(100.0, earlier), _bar(101.0, later)])

    assert len(rows) == 1
    assert rows[0].close == 101.0
    assert rows[0].as_of == later


def test_distinct_keys_are_kept_and_sorted():
    fetched = datetime(2026, 7, 6, 10, tzinfo=UTC)
    rows = build_bar_upserts(
        [
            _bar(2.0, fetched, symbol="MSFT"),
            _bar(3.0, fetched, adjustment_basis="split_dividend_adjusted"),
            _bar(1.0, fetched, symbol="AAPL"),
        ]
    )
    assert len(rows) == 3
    assert rows[0].symbol == "AAPL"  # sorted by (symbol, ts, source)


def test_conflict_key_shape():
    row = build_bar_upserts([_bar(1.0, TS)])[0]
    assert row.conflict_key == ("AAPL", "day", 1, TS, "polygon", "raw")
    assert row.conflict_key == tuple(getattr(row, column) for column in BAR_CONFLICT_KEY)


def test_explicit_as_of_overrides_fetched_at():
    fetched = datetime(2026, 7, 6, 10, tzinfo=UTC)
    as_of = datetime(2026, 7, 7, tzinfo=UTC)
    rows = build_bar_upserts([_bar(1.0, fetched)], as_of=as_of)
    assert rows[0].as_of == as_of
    assert rows[0].fetched_at == fetched


def test_idempotent_repeated_calls_identical():
    fetched = datetime(2026, 7, 6, 10, tzinfo=UTC)
    bars = [_bar(1.0, fetched), _bar(2.0, fetched, symbol="MSFT")]
    assert build_bar_upserts(bars) == build_bar_upserts(bars)


def test_revision_plan_captures_changed_existing_row():
    old_fetch = datetime(2026, 7, 6, 10, tzinfo=UTC)
    new_fetch = datetime(2026, 7, 6, 11, tzinfo=UTC)
    existing = build_bar_upserts([_bar(100.0, old_fetch)])[0]
    plan = build_bar_upsert_plan([_bar(101.0, new_fetch)], existing_rows=[existing])

    assert plan.rows[0].close == 101.0
    assert len(plan.revisions) == 1
    assert plan.revisions[0].previous.close == 100.0
    assert plan.revisions[0].incoming.close == 101.0
    assert plan.revisions[0].revised_at == new_fetch


def test_revision_plan_skips_unchanged_existing_row():
    fetched = datetime(2026, 7, 6, 10, tzinfo=UTC)
    existing = build_bar_upserts([_bar(100.0, fetched)])[0]
    plan = build_bar_upsert_plan([_bar(100.0, fetched)], existing_rows=[existing])

    assert plan.rows == [existing]
    assert plan.revisions == []
