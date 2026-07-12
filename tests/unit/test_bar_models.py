"""ORM and migration shape for OHLCV bars."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import DateTime

from app.db.base import Base
from app.db.models import Bar, BarRevision
from ingestion.upsert import BAR_CONFLICT_KEY


def test_bar_models_register_with_base_metadata():
    assert Base.metadata.tables["bars"] is Bar.__table__
    assert Base.metadata.tables["bars_revisions"] is BarRevision.__table__


def test_bar_primary_key_matches_upsert_conflict_key():
    primary_key = tuple(column.name for column in Bar.__table__.primary_key.columns)

    assert primary_key == BAR_CONFLICT_KEY


def test_bar_timestamps_are_timestamptz():
    for column_name in ("ts", "fetched_at", "as_of"):
        column_type = Bar.__table__.c[column_name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True

    for column_name in (
        "ts",
        "previous_fetched_at",
        "previous_as_of",
        "incoming_fetched_at",
        "incoming_as_of",
        "revised_at",
    ):
        column_type = BarRevision.__table__.c[column_name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True


def test_bars_migration_creates_hypertable_and_revision_ledger():
    migration = Path("migrations/versions/0002_bars.py").read_text(encoding="utf-8")

    assert "create_hypertable('bars', 'ts', if_not_exists => TRUE)" in migration
    assert '"bars_revisions"' in migration
    assert "previous_close" in migration
    assert "incoming_close" in migration


def test_bars_series_index_puts_every_equality_column_before_ts():
    # The /v1/prices read filters these five columns by equality and orders by
    # ts; with source/adjustment_basis after ts (as in the PK) a sparse series
    # degrades to an unbounded prefix scan, so LIMIT stops bounding work.
    index = next(idx for idx in Bar.__table__.indexes if idx.name == "ix_bars_series_ts")
    assert tuple(column.name for column in index.columns) == (
        "symbol",
        "timespan",
        "multiplier",
        "source",
        "adjustment_basis",
        "ts",
    )

    migration = Path("migrations/versions/0003_bars_series_index.py").read_text(encoding="utf-8")
    assert '"ix_bars_series_ts"' in migration
    assert '"0002_bars"' in migration  # chains onto the bars migration
    assert (
        'BAR_SERIES_INDEX = ("symbol", "timespan", "multiplier", "source", '
        '"adjustment_basis", "ts")'
    ) in migration
