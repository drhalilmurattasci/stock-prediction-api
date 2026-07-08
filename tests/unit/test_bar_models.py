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
