"""SQL construction for the OHLCV DB sink."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.dialects import postgresql

from data_sources.base import OHLCVBar
from ingestion.upsert import (
    build_bar_upsert_plan,
    build_bar_upsert_statement,
    build_bar_upserts,
)

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


def test_bar_upsert_statement_uses_conflict_key_and_is_distinct_from():
    row = build_bar_upserts([_bar(100.0, TS)])[0]
    statement = build_bar_upsert_statement([row])
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT (symbol, timespan, multiplier, ts, source, adjustment_basis)" in sql
    assert "DO UPDATE SET" in sql
    assert "IS DISTINCT FROM" in sql
    assert "excluded.close" in sql
    assert "fetched_at = excluded.fetched_at" in sql
    assert "as_of = excluded.as_of" in sql


def test_bar_upsert_statement_rejects_empty_rows():
    with pytest.raises(ValueError, match="at least one bar row is required"):
        build_bar_upsert_statement([])


def test_revision_plan_maps_previous_and_incoming_values_for_sink():
    old_fetch = datetime(2026, 7, 6, 10, tzinfo=UTC)
    new_fetch = datetime(2026, 7, 6, 11, tzinfo=UTC)
    existing = build_bar_upserts([_bar(100.0, old_fetch)])[0]
    plan = build_bar_upsert_plan([_bar(101.0, new_fetch)], existing_rows=[existing])
    revision = plan.revisions[0]

    assert revision.conflict_key == existing.conflict_key
    assert revision.previous.close == 100.0
    assert revision.incoming.close == 101.0
    assert revision.revised_at == new_fetch
