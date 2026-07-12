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
    for column_name in ("ts", "fetched_at", "as_of", "recorded_at"):
        column_type = Bar.__table__.c[column_name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True

    for column_name in (
        "ts",
        "previous_fetched_at",
        "previous_as_of",
        "incoming_fetched_at",
        "incoming_as_of",
        "previous_recorded_at",
        "incoming_recorded_at",
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


def test_bars_reject_non_finite_values_at_the_storage_layer():
    # Postgres treats NaN as greater than everything (NaN >= 0 is TRUE), so the
    # nonnegativity CHECKs cannot exclude NaN/+Infinity; `col < 'Infinity'` is
    # FALSE for both. A stored non-finite value would 500 every finite-only
    # /v1/prices read of its page, so storage must refuse it outright.
    constraint_names = {str(c.name) for c in Bar.__table__.constraints}
    assert {"ck_bars_ohlcv_finite", "ck_bars_vwap_finite"} <= constraint_names
    finite = next(c for c in Bar.__table__.constraints if str(c.name) == "ck_bars_ohlcv_finite")
    for column in ("open", "high", "low", "close", "volume"):
        assert f"{column} < 'Infinity'::float8" in str(finite.sqltext)

    migration = Path("migrations/versions/0004_bars_finiteness.py").read_text(encoding="utf-8")
    assert '"0003_bars_series_index"' in migration  # chains onto the index migration
    assert "ck_bars_ohlcv_finite" in migration
    assert "ck_bars_vwap_finite" in migration
    assert "volume < 'Infinity'::float8" in migration
    assert "vwap IS NULL OR vwap < 'Infinity'::float8" in migration


def test_bar_version_history_is_db_recorded_append_only_and_fully_indexed():
    bar_constraints = {str(constraint.name) for constraint in Bar.__table__.constraints}
    revision_constraints = {
        str(constraint.name) for constraint in BarRevision.__table__.constraints
    }
    assert {
        "ck_bars_fetched_not_before_bar",
        "ck_bars_as_of_not_before_fetch",
    } <= bar_constraints
    assert {
        "ck_bars_revisions_revision_version_evidence",
        "ck_bars_revisions_revision_ohlcv_finite",
    } <= revision_constraints

    index = next(
        value
        for value in BarRevision.__table__.indexes
        if value.name == "ix_bars_revisions_series_version"
    )
    assert tuple(column.name for column in index.columns) == (
        "symbol",
        "timespan",
        "multiplier",
        "source",
        "adjustment_basis",
        "ts",
        "incoming_recorded_at",
    )

    migration = Path("migrations/versions/0006_bar_version_history.py").read_text(encoding="utf-8")
    assert 'down_revision: str | None = "0005_forecast_input_snapshots"' in migration
    assert "NEW.recorded_at := recorded" in migration
    assert "SECURITY DEFINER" in migration
    assert "OLD.recorded_at + interval '1 microsecond'" in migration
    assert "INSERT INTO public.bars_revisions" in migration
    assert "required runtime role stockapi_app is missing" in migration
    assert "GRANT USAGE, SELECT ON SEQUENCE public.bars_revisions_id_seq" in migration
    assert "REVOKE ALL PRIVILEGES ON TABLE public.bars FROM stockapi_app" in migration
    assert "BEFORE UPDATE OR DELETE ON bars_revisions" in migration
    assert "BEFORE TRUNCATE ON bars_revisions" in migration

    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    role_init = Path("scripts/db-init/02-runtime-role.sh").read_text(encoding="utf-8")
    assert "timescale/timescaledb:2.28.2-pg17" in compose
    assert "stockapi_app:${POSTGRES_APP_URL_PASSWORD}" in compose
    app_tier = compose.split("\n  api:", maxsplit=1)[1]
    assert "env_file:" not in app_tier
    assert "MIGRATION_DATABASE_URL" not in app_tier
    assert "POSTGRES_PASSWORD:" not in app_tier
    assert '"--concurrency=1"' in app_tier
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" in role_init
    assert "ALTER ROLE stockapi_app RESET ALL" in role_init
