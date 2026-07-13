"""Live-database gate: migrations, writes, reads, and immutable snapshots.

Skipped unless ``TEST_DATABASE_URL`` points at a **throwaway** TimescaleDB
through an owner/admin account. The fixture RESETS the target database (bars
tables and alembic_version are dropped) before applying migrations. It never
creates or mutates the cluster-global runtime roles: bootstrap them first and
provide both least-privilege URLs. Never point this gate at shared data. Run with::

    docker compose up -d timescaledb          # needs .env credentials
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://<user>:<pass>@localhost:5432/<db>"
    # Optional when .env DATABASE_URL does not target the same throwaway DB:
    $env:TEST_RUNTIME_DATABASE_URL = "postgresql+asyncpg://stockapi_app:<pass>@localhost:5432/<db>"
    $env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = "postgresql+asyncpg://stockapi_snapshot_builder:<pass>@localhost:5432/<db>"
    $env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = "stockapi-test-only"
    uv run pytest tests/integration -v

This is the empirical proof the unit suite cannot give: the Alembic chain
applies against a real hypertable, ``upsert_bars`` replay is a no-op while a
restatement writes a revision row, a seeded two-page ``/v1/prices`` read has
no gaps or duplicates with TIMESTAMPTZ round-tripping, the finiteness CHECKs
actually reject NaN/Infinity under Postgres NaN ordering, and the API
statement-timeout cancels a pathological statement.
The same command also proves forecast-input snapshot SHA-256 enforcement,
idempotent insertion, semantic collision rejection, pure resolution, and
database-level UPDATE/DELETE/TRUNCATE refusal.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import exchange_calendars
import pytest
from sqlalchemy import create_engine, func, make_url, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings
from app.db.models.bars import Bar, BarRevision
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.db.session import build_engine, build_sessionmaker
from app.schemas.forecast import ForecastRequest
from app.schemas.prices import PriceFilters
from app.services.forecast_serving import (
    ForecastServingPolicy,
    SnapshotForecastService,
    SqlForecastInputSnapshotRepository,
)
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    ForecastSnapshotBuilder,
    SnapshotBuildSpec,
    database_snapshot_cutoff,
)
from app.services.forecast_snapshots import (
    ForecastInputSnapshotPayload,
    ForecastInputSnapshotRecord,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    build_snapshot_record,
    parse_snapshot_payload,
    validate_and_resolve_snapshot,
)
from app.services.prices import read_prices
from data_sources.base import OHLCVBar
from ingestion.upsert import (
    BarVersionConflictError,
    finalize_bar_version_availability,
    upsert_bars,
)

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL not set - live-DB gate skipped", allow_module_level=True)
if os.environ.get("TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET") != "stockapi-test-only":
    raise RuntimeError(
        "set TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET=stockapi-test-only only for the "
        "owner-designated throwaway database"
    )

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class LiveDatabaseUrls:
    owner: str
    runtime: str
    snapshot_builder: str


def _async_url(url: str) -> str:
    if "+asyncpg" in url:
        return url
    return url.replace("postgresql://", "postgresql+asyncpg://")


def _sync_url(url: str) -> str:
    return _async_url(url).replace("+asyncpg", "+psycopg")


def _runtime_url_for(owner_url: str) -> str:
    configured = os.environ.get("TEST_RUNTIME_DATABASE_URL") or Settings().database_url
    runtime_url = make_url(_async_url(configured))
    owner = make_url(owner_url)
    if runtime_url.username != "stockapi_app":
        raise ValueError("live gate runtime URL must use the non-owner stockapi_app role")
    owner_target = (owner.host, owner.port or 5432, owner.database)
    runtime_target = (runtime_url.host, runtime_url.port or 5432, runtime_url.database)
    if runtime_target != owner_target:
        raise ValueError("live gate owner and runtime URLs must target the same throwaway database")
    if owner.username == runtime_url.username:
        raise ValueError("TEST_DATABASE_URL must use a distinct owner/admin account")
    return runtime_url.render_as_string(hide_password=False)


def _snapshot_builder_url_for(owner_url: str) -> str:
    configured = os.environ.get("TEST_SNAPSHOT_BUILDER_DATABASE_URL", "")
    if not configured:
        raise ValueError(
            "TEST_SNAPSHOT_BUILDER_DATABASE_URL must name the dedicated throwaway-DB role"
        )
    builder_url = make_url(_async_url(configured))
    owner = make_url(owner_url)
    if builder_url.username != "stockapi_snapshot_builder":
        raise ValueError("live gate builder URL must use stockapi_snapshot_builder")
    owner_target = (owner.host, owner.port or 5432, owner.database)
    builder_target = (builder_url.host, builder_url.port or 5432, builder_url.database)
    if builder_target != owner_target:
        raise ValueError("live gate owner and builder URLs must target the same throwaway database")
    return builder_url.render_as_string(hide_password=False)


@pytest.fixture(scope="module")
def migrated_database_url() -> LiveDatabaseUrls:
    """Reset the throwaway database, then apply the full migration chain."""
    url = _async_url(TEST_DATABASE_URL)
    runtime_url = _runtime_url_for(url)
    snapshot_builder_url = _snapshot_builder_url_for(url)
    sync_engine = create_engine(_sync_url(url))
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "DROP TABLE IF EXISTS forecast_input_snapshots, "
                "bar_version_availability, bars_revisions, bars, alembic_version CASCADE"
            )
        )
        conn.execute(text("DROP FUNCTION IF EXISTS reject_forecast_input_snapshot_mutation()"))
        conn.execute(text("DROP FUNCTION IF EXISTS stamp_forecast_input_snapshot_sealed_at()"))
        conn.execute(text("DROP FUNCTION IF EXISTS reject_bar_history_mutation()"))
        conn.execute(text("DROP FUNCTION IF EXISTS require_bar_revision_version_evidence()"))
        conn.execute(text("DROP FUNCTION IF EXISTS version_bar_write()"))
        conn.execute(text("DROP FUNCTION IF EXISTS stamp_bar_version_availability()"))
    sync_engine.dispose()

    result = subprocess.run(  # fresh process: env.py resolves DATABASE_URL uncached
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=REPO_ROOT,
        env={**os.environ, "DATABASE_URL": url, "MIGRATION_DATABASE_URL": url},
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}"
    for direction, target in (
        ("downgrade", "0007_snapshot_builder_privileges"),
        ("upgrade", "head"),
    ):
        result = subprocess.run(
            [sys.executable, "-m", "alembic", direction, target],
            cwd=REPO_ROOT,
            env={**os.environ, "DATABASE_URL": url, "MIGRATION_DATABASE_URL": url},
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            f"alembic {direction} {target} failed:\n{result.stdout}\n{result.stderr}"
        )
    return LiveDatabaseUrls(
        owner=url,
        runtime=runtime_url,
        snapshot_builder=snapshot_builder_url,
    )


@pytest.fixture
async def engine(migrated_database_url: LiveDatabaseUrls) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.runtime)
    yield engine
    await engine.dispose()


@pytest.fixture
async def owner_engine(migrated_database_url: LiveDatabaseUrls) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.owner)
    yield engine
    await engine.dispose()


@pytest.fixture
async def snapshot_builder_engine(
    migrated_database_url: LiveDatabaseUrls,
) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.snapshot_builder)
    yield engine
    await engine.dispose()


def _bar(
    day: int,
    *,
    symbol: str = "AAPL",
    close: float = 101.0,
    fetched_hour: int = 1,
) -> OHLCVBar:
    ts = datetime(2026, 7, day, tzinfo=UTC)
    return OHLCVBar(
        symbol=symbol,
        timestamp=ts,
        timespan="day",
        multiplier=1,
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=1_000.0 + day,
        vwap=close - 0.5,
        trade_count=10 + day,
        source="polygon",
        fetched_at=ts + timedelta(hours=fetched_hour),
    )


async def test_migration_chain_applies_and_bars_is_a_hypertable(
    owner_engine: AsyncEngine,
) -> None:
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        assert version == "0008_bar_version_availability"
        hypertables = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM timescaledb_information.hypertables "
                    "WHERE hypertable_name = 'bars'"
                )
            )
        ).scalar_one()
        assert hypertables == 1
        series_index = (
            await conn.execute(
                text("SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_bars_series_ts'")
            )
        ).scalar_one()
        assert series_index == 1
        snapshot_index = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_indexes "
                    "WHERE indexname = 'ix_forecast_input_snapshots_resolve'"
                )
            )
        ).scalar_one()
        assert snapshot_index == 1
        pgcrypto = (
            await conn.execute(text("SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto'"))
        ).scalar_one()
        assert pgcrypto == 1
        runtime_role = (
            await conn.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
                    "rolbypassrls, rolinherit, rolcanlogin "
                    "FROM pg_roles WHERE rolname = 'stockapi_app'"
                )
            )
        ).one()
        assert tuple(runtime_role) == (False, False, False, False, False, False, True)
        memberships = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_auth_members AS membership "
                    "JOIN pg_roles AS role ON role.oid = membership.member "
                    "WHERE role.rolname = 'stockapi_app'"
                )
            )
        ).scalar_one()
        assert memberships == 0
        role_settings = (
            await conn.execute(
                text(
                    "SELECT setting.setdatabase, setting.setconfig "
                    "FROM pg_db_role_setting AS setting "
                    "JOIN pg_roles AS role ON role.oid = setting.setrole "
                    "WHERE role.rolname = 'stockapi_app'"
                )
            )
        ).one()
        assert role_settings.setdatabase == 0
        assert role_settings.setconfig == ["search_path=pg_catalog, public"]
        privileges = {
            name: (
                await conn.execute(
                    text("SELECT has_table_privilege('stockapi_app', :table_name, :privilege)"),
                    {"table_name": table_name, "privilege": privilege},
                )
            ).scalar_one()
            for name, table_name, privilege in (
                ("bars_select", "bars", "SELECT"),
                ("bars_insert", "bars", "INSERT"),
                ("bars_update", "bars", "UPDATE"),
                ("bars_delete", "bars", "DELETE"),
                ("bars_truncate", "bars", "TRUNCATE"),
                ("bars_references", "bars", "REFERENCES"),
                ("bars_trigger", "bars", "TRIGGER"),
                ("bars_maintain", "bars", "MAINTAIN"),
                ("revision_select", "bars_revisions", "SELECT"),
                ("revision_insert", "bars_revisions", "INSERT"),
                ("revision_update", "bars_revisions", "UPDATE"),
                ("revision_delete", "bars_revisions", "DELETE"),
                ("revision_truncate", "bars_revisions", "TRUNCATE"),
                ("availability_select", "bar_version_availability", "SELECT"),
                ("availability_insert", "bar_version_availability", "INSERT"),
                ("availability_update", "bar_version_availability", "UPDATE"),
                ("availability_delete", "bar_version_availability", "DELETE"),
                ("availability_references", "bar_version_availability", "REFERENCES"),
                ("availability_trigger", "bar_version_availability", "TRIGGER"),
                ("availability_maintain", "bar_version_availability", "MAINTAIN"),
                ("snapshot_select", "forecast_input_snapshots", "SELECT"),
                ("snapshot_insert", "forecast_input_snapshots", "INSERT"),
                ("snapshot_update", "forecast_input_snapshots", "UPDATE"),
                ("snapshot_delete", "forecast_input_snapshots", "DELETE"),
                ("snapshot_truncate", "forecast_input_snapshots", "TRUNCATE"),
            )
        }
        assert privileges == {
            "bars_select": True,
            "bars_insert": True,
            "bars_update": True,
            "bars_delete": False,
            "bars_truncate": False,
            "bars_references": False,
            "bars_trigger": False,
            "bars_maintain": False,
            "revision_select": True,
            "revision_insert": False,
            "revision_update": False,
            "revision_delete": False,
            "revision_truncate": False,
            "availability_select": True,
            "availability_insert": True,
            "availability_update": False,
            "availability_delete": False,
            "availability_references": False,
            "availability_trigger": False,
            "availability_maintain": False,
            "snapshot_select": True,
            "snapshot_insert": False,
            "snapshot_update": False,
            "snapshot_delete": False,
            "snapshot_truncate": False,
        }
        sequence_usage = (
            await conn.execute(
                text(
                    "SELECT has_sequence_privilege("
                    "'stockapi_app', 'bars_revisions_id_seq', 'USAGE')"
                )
            )
        ).scalar_one()
        assert sequence_usage is False
        schema_privileges = (
            await conn.execute(
                text(
                    "SELECT has_schema_privilege('stockapi_app', 'public', 'USAGE'), "
                    "has_schema_privilege('stockapi_app', 'public', 'CREATE')"
                )
            )
        ).one()
        assert tuple(schema_privileges) == (True, False)
        can_execute_version_trigger = (
            await conn.execute(
                text(
                    "SELECT has_function_privilege("
                    "'stockapi_app', 'version_bar_write()', 'EXECUTE')"
                )
            )
        ).scalar_one()
        assert can_execute_version_trigger is False

        builder_role = (
            await conn.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
                    "rolbypassrls, rolinherit, rolcanlogin "
                    "FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).one()
        assert tuple(builder_role) == (False, False, False, False, False, False, True)
        builder_memberships = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_auth_members AS membership "
                    "JOIN pg_roles AS member_role ON member_role.oid = membership.member "
                    "JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid "
                    "WHERE member_role.rolname = 'stockapi_snapshot_builder' "
                    "OR granted_role.rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).scalar_one()
        assert builder_memberships == 0
        builder_settings = (
            await conn.execute(
                text(
                    "SELECT setting.setdatabase, setting.setconfig "
                    "FROM pg_db_role_setting AS setting "
                    "JOIN pg_roles AS role ON role.oid = setting.setrole "
                    "WHERE role.rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).one()
        assert builder_settings.setdatabase == 0
        assert builder_settings.setconfig == ["search_path=pg_catalog, public"]
        builder_privileges = {
            name: (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, :privilege)"
                    ),
                    {"table_name": table_name, "privilege": privilege},
                )
            ).scalar_one()
            for name, table_name, privilege in (
                ("bars_select", "bars", "SELECT"),
                ("bars_insert", "bars", "INSERT"),
                ("bars_update", "bars", "UPDATE"),
                ("revision_select", "bars_revisions", "SELECT"),
                ("revision_insert", "bars_revisions", "INSERT"),
                ("availability_select", "bar_version_availability", "SELECT"),
                ("availability_insert", "bar_version_availability", "INSERT"),
                ("availability_references", "bar_version_availability", "REFERENCES"),
                ("availability_trigger", "bar_version_availability", "TRIGGER"),
                ("availability_maintain", "bar_version_availability", "MAINTAIN"),
                ("snapshot_select", "forecast_input_snapshots", "SELECT"),
                ("snapshot_insert", "forecast_input_snapshots", "INSERT"),
                ("snapshot_update", "forecast_input_snapshots", "UPDATE"),
                ("snapshot_delete", "forecast_input_snapshots", "DELETE"),
                ("snapshot_truncate", "forecast_input_snapshots", "TRUNCATE"),
            )
        }
        assert builder_privileges == {
            "bars_select": True,
            "bars_insert": False,
            "bars_update": False,
            "revision_select": True,
            "revision_insert": False,
            "availability_select": True,
            "availability_insert": False,
            "availability_references": False,
            "availability_trigger": False,
            "availability_maintain": False,
            "snapshot_select": True,
            "snapshot_insert": True,
            "snapshot_update": False,
            "snapshot_delete": False,
            "snapshot_truncate": False,
        }
        for role in ("stockapi_app", "stockapi_snapshot_builder"):
            can_execute_receipt_trigger = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        ":role, 'stamp_bar_version_availability()', 'EXECUTE')"
                    ),
                    {"role": role},
                )
            ).scalar_one()
            assert can_execute_receipt_trigger is False


async def test_upsert_replay_is_noop_and_restatement_writes_revision(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)
    symbol = "UPST"

    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=101.0)])

    # Identical values with a later fetched_at: the IS DISTINCT FROM guard must
    # leave the row byte-identical and write no revision.
    async with maker() as session:
        async with session.begin():
            replay = await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=101.0, fetched_hour=9)],
            )
        assert replay.revisions == []

    async with maker() as session:
        row = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        assert row.close == 101.0
        assert row.fetched_at == datetime(2026, 7, 1, 1, tzinfo=UTC)  # replay did not touch it
        revision_count = (
            await session.execute(
                select(func.count()).select_from(BarRevision).where(BarRevision.symbol == symbol)
            )
        ).scalar_one()
        assert revision_count == 0

    # A restated close must update the current row AND append the prior value.
    async with maker() as session:
        async with session.begin():
            restated = await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=102.5, fetched_hour=12)],
            )
        assert len(restated.revisions) == 1

    async with maker() as session:
        row = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        assert row.close == 102.5
        revision = (
            await session.execute(select(BarRevision).where(BarRevision.symbol == symbol))
        ).scalar_one()
        assert revision.previous_close == 101.0
        assert revision.incoming_close == 102.5
        assert revision.previous_recorded_at is not None
        assert revision.previous_recorded_at < row.recorded_at
    assert revision.incoming_recorded_at == revision.revised_at == row.recorded_at


async def test_receipt_rejects_version_written_in_same_outer_transaction_savepoint(
    engine: AsyncEngine,
) -> None:
    """A subxid must not bypass the top-level post-commit boundary."""

    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        async with session.begin_nested():
            plan = await upsert_bars(
                session,
                [_bar(day, symbol="RCPT", close=100.0 + day) for day in (1, 2, 3)],
            )
        with pytest.raises(DBAPIError, match="finalized after its write commits"):
            async with session.begin_nested():
                await finalize_bar_version_availability(session, plan.rows[-1:])

    async with maker() as session, session.begin():
        # A trailing-only retry reconciles the two earlier versions whose
        # hypothetical first finalizer never committed.
        assert await finalize_bar_version_availability(session, plan.rows[-1:]) == 3


async def test_stale_conflicts_and_history_mutation_fail_closed(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    maker = build_sessionmaker(engine)
    symbol = "GUARD"
    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=101.0, fetched_hour=1)])
    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=102.0, fetched_hour=2)])

    async with engine.begin() as conn:
        raw_update = await conn.execute(
            text(
                "UPDATE bars SET open = 102.0, high = 104.0, low = 101.0, close = 103.0, "
                "fetched_at = fetched_at + interval '1 hour', "
                "as_of = as_of + interval '1 hour', "
                "recorded_at = '2099-01-01T00:00:00+00' "
                "WHERE symbol = :symbol"
            ),
            {"symbol": symbol},
        )
        assert raw_update.rowcount == 1
        identical = await conn.execute(
            text("UPDATE bars SET close = close WHERE symbol = :symbol"),
            {"symbol": symbol},
        )
        assert identical.rowcount == 0

    async with maker() as session, session.begin():
        with pytest.raises(BarVersionConflictError, match="strictly newer"):
            await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=104.0, fetched_hour=3)],
            )

    direct_mutations = (
        (
            "UPDATE bars SET close = 104.0 WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "UPDATE bars SET close = 104.0, fetched_at = fetched_at + interval '1 hour' "
            "WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "UPDATE bars SET close = 104.0, as_of = as_of + interval '1 hour' "
            "WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "DELETE FROM bars WHERE symbol = :symbol",
            "append-only",
        ),
        (
            "UPDATE bars_revisions SET incoming_close = 103.0 WHERE symbol = :symbol",
            "append-only",
        ),
        (
            "DELETE FROM bars_revisions WHERE symbol = :symbol",
            "append-only",
        ),
        ("TRUNCATE bars_revisions", "append-only"),
        ("TRUNCATE bars", "append-only"),
    )
    for mutation, message in direct_mutations:
        mutation_engine = engine if mutation.startswith("UPDATE bars SET") else owner_engine
        async with mutation_engine.connect() as conn:
            transaction = await conn.begin()
            parameters = {"symbol": symbol} if ":symbol" in mutation else {}
            with pytest.raises(DBAPIError, match=message):
                await conn.execute(text(mutation), parameters)
            await transaction.rollback()

    async with maker() as session:
        current = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        revision_count = (
            await session.execute(
                select(func.count()).select_from(BarRevision).where(BarRevision.symbol == symbol)
            )
        ).scalar_one()
    assert current.close == 103.0
    assert current.recorded_at.year != 2099
    assert revision_count == 2


async def test_two_page_read_covers_all_bars_without_gap_or_dup(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)
    symbol = "PAGE"
    async with maker() as session, session.begin():
        await upsert_bars(
            session,
            [_bar(day, symbol=symbol, close=100.0 + day) for day in (1, 2, 3, 4, 5)],
        )

    async with maker() as session:
        page_one = await read_prices(session, symbol, PriceFilters(limit=3))
        assert [bar.timestamp for bar in page_one.bars] == [
            datetime(2026, 7, day, tzinfo=UTC) for day in (3, 4, 5)
        ]
        assert page_one.page.has_more is True
        assert page_one.page.next_end == datetime(2026, 7, 3, tzinfo=UTC)

        page_two = await read_prices(
            session, symbol, PriceFilters(limit=3, end=page_one.page.next_end)
        )
        assert [bar.timestamp for bar in page_two.bars] == [
            datetime(2026, 7, day, tzinfo=UTC) for day in (1, 2)
        ]
        assert page_two.page.has_more is False
        assert page_two.page.next_end is None

    combined = [bar.timestamp for bar in (*page_two.bars, *page_one.bars)]
    assert combined == sorted(combined)  # chronological across pages
    assert len(set(combined)) == 5  # no duplicates, no gaps
    assert all(ts.utcoffset() == timedelta(0) for ts in combined)  # TIMESTAMPTZ round-trip


_RAW_INSERT = text(
    "INSERT INTO bars (symbol, timespan, multiplier, ts, source, adjustment_basis, "
    "open, high, low, close, volume, vwap, trade_count, fetched_at, as_of) "
    "VALUES ('EVIL', 'day', 1, '2026-07-01T00:00:00+00', 'polygon', 'raw', "
    "1.0, 2.0, 0.5, 1.5, :volume, :vwap, 1, now(), now())"
)


@pytest.mark.parametrize("volume", [float("nan"), float("inf")])
async def test_non_finite_ohlcv_rejected_by_storage_check(
    engine: AsyncEngine, volume: float
) -> None:
    # Bypasses the DTO on purpose: proves the DB CHECK itself rejects what the
    # nonnegativity constraints cannot (NaN orders greater than 0 in Postgres).
    async with engine.connect() as conn:
        with pytest.raises(IntegrityError, match="ck_bars_ohlcv_finite"):
            await conn.execute(_RAW_INSERT, {"volume": volume, "vwap": None})


async def test_non_finite_vwap_rejected_by_storage_check(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        with pytest.raises(IntegrityError, match="ck_bars_vwap_finite"):
            await conn.execute(_RAW_INSERT, {"volume": 3.0, "vwap": float("inf")})


async def test_api_statement_timeout_cancels_pathological_statement(
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    settings = Settings(app_env="test", database_url=migrated_database_url.runtime)
    capped = build_engine(settings, statement_timeout_ms=150)
    try:
        async with capped.connect() as conn:
            with pytest.raises(DBAPIError):
                await conn.execute(text("SELECT pg_sleep(2)"))
    finally:
        await capped.dispose()


def _forecast_snapshot_record(
    *,
    final_value: float = 102.0,
    policy_hash: str = "sha256:" + "a" * 64,
) -> ForecastInputSnapshotRecord:
    as_of = datetime(2026, 7, 10, 21, tzinfo=UTC)
    return build_snapshot_record(
        ForecastInputSnapshotPayload(
            resolution_policy_hash=policy_hash,
            symbol="AAPL",
            target="close",
            horizon_unit="calendar_day",
            series_basis="raw",
            input_timespan="day",
            input_multiplier=1,
            as_of=as_of,
            currency="USD",
            observations=(
                SnapshotObservation(
                    observed_at=as_of - timedelta(days=2, hours=1),
                    available_at=as_of - timedelta(days=2),
                    value=100.0,
                ),
                SnapshotObservation(
                    observed_at=as_of - timedelta(days=1, hours=1),
                    available_at=as_of - timedelta(days=1),
                    value=final_value,
                ),
            ),
            target_times=(as_of + timedelta(days=1), as_of + timedelta(days=2)),
            data_sources=(
                SnapshotSourceLineage(
                    name="live-gate",
                    snapshot_id="live-gate-source-v1",
                    max_available_at=as_of - timedelta(hours=1),
                    fields=("close",),
                ),
            ),
            availability=SnapshotAvailabilityEvidence(status="not_run"),
        ),
        sealed_at=as_of + timedelta(minutes=1),
    )


def _record_values(record: ForecastInputSnapshotRecord) -> dict[str, object]:
    return {field.name: getattr(record, field.name) for field in fields(record)}


async def test_snapshot_insert_hash_resolution_and_immutability(
    owner_engine: AsyncEngine,
) -> None:
    record = _forecast_snapshot_record()
    maker = build_sessionmaker(owner_engine)
    statement = (
        pg_insert(ForecastInputSnapshot)
        .values(**_record_values(record))
        .on_conflict_do_nothing(index_elements=["snapshot_id"])
    )
    async with maker() as session:
        async with session.begin():
            insert_before = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            first = await session.execute(statement)
            insert_after = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        assert first.rowcount == 1
        async with session.begin():
            replay = await session.execute(statement)
        assert replay.rowcount == 0

    async with maker() as session:
        stored = (
            await session.execute(
                select(ForecastInputSnapshot).where(
                    ForecastInputSnapshot.snapshot_id == record.snapshot_id
                )
            )
        ).scalar_one()
        loaded = ForecastInputSnapshotRecord(
            **{
                field.name: getattr(stored, field.name)
                for field in fields(ForecastInputSnapshotRecord)
            }
        )
    assert loaded.sealed_at != record.sealed_at  # caller value was overwritten by the DB trigger
    assert insert_before <= loaded.sealed_at <= insert_after
    request = ForecastRequest(
        symbol="AAPL",
        horizon=2,
        horizon_unit="calendar_day",
        target="close",
        snapshot_id=record.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    resolved = validate_and_resolve_snapshot(
        loaded,
        request,
        expected_series_basis="raw",
        expected_resolution_policy_hash=record.resolution_policy_hash,
    )
    assert [item.value for item in resolved.observations] == [100.0, 102.0]
    assert resolved.availability_verified is False

    tamper_base = _forecast_snapshot_record(policy_hash="sha256:" + "c" * 64)
    tampered = replace(tamper_base, snapshot_id="sha256:" + "0" * 64)
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(
            IntegrityError, match="ck_forecast_input_snapshots_payload_hash_matches_id"
        ):
            await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(tampered)))
        await transaction.rollback()

    # Same semantic selector/cutoff with different bytes is ambiguous and must
    # fail instead of silently becoming another "latest" snapshot.
    collision = _forecast_snapshot_record(final_value=103.0)
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(IntegrityError, match="uq_forecast_input_snapshots_semantic_key"):
            await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(collision)))
        await transaction.rollback()

    for mutation in (
        "UPDATE forecast_input_snapshots SET symbol = 'MSFT' WHERE snapshot_id = :snapshot_id",
        "DELETE FROM forecast_input_snapshots WHERE snapshot_id = :snapshot_id",
        "TRUNCATE forecast_input_snapshots",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="insert-only"):
                parameters = (
                    {"snapshot_id": record.snapshot_id} if ":snapshot_id" in mutation else {}
                )
                await conn.execute(text(mutation), parameters)
            await transaction.rollback()


async def test_bars_to_verified_snapshot_to_served_forecast(
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    """Prove the complete privileged-write/read-only-serving chain."""

    runtime_maker = build_sessionmaker(engine)
    builder_maker = build_sessionmaker(snapshot_builder_engine)
    initial_cutoff = await database_snapshot_cutoff(builder_maker)
    calendar = exchange_calendars.get_calendar("XNYS", start="1990-01-01", end="2100-12-31")
    latest = calendar.date_to_session(initial_cutoff.date(), direction="previous")
    if calendar.session_close(latest).to_pydatetime() > initial_cutoff:
        latest = calendar.previous_session(latest)
    sessions = calendar.sessions_window(latest, -258)
    bars = []
    for index, label in enumerate(sessions):
        timestamp = label.to_pydatetime().replace(tzinfo=UTC)
        fetched_at = calendar.session_close(label).to_pydatetime()
        close = 100.0 + index * 0.05 + (index % 7) * 0.2
        bars.append(
            OHLCVBar(
                symbol="AAPL",
                timestamp=timestamp,
                timespan="day",
                multiplier=1,
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=10_000.0 + index,
                source="polygon_open_close",
                adjustment_basis="raw",
                fetched_at=fetched_at,
            )
        )
    async with runtime_maker() as session, session.begin():
        plan = await upsert_bars(session, bars)
    async with runtime_maker() as session, session.begin():
        await finalize_bar_version_availability(session, plan.rows)

    historical_cutoff = await database_snapshot_cutoff(builder_maker)
    historical_spec = SnapshotBuildSpec(
        symbol="AAPL",
        target="close",
        horizon_unit="trading_day",
        as_of=historical_cutoff,
    )
    builder = ForecastSnapshotBuilder(builder_maker)
    historical_created = await builder.build(historical_spec)

    # Freeze the historical cutoff above, then publish and finalize a newer
    # version. Replaying the old semantic key must reconstruct the prior close
    # from bars_revisions, while a later cutoff must select the restatement.
    previous = bars[-1]
    restated_close = previous.close + 7.0
    restatement = OHLCVBar(
        symbol=previous.symbol,
        timestamp=previous.timestamp,
        timespan=previous.timespan,
        multiplier=previous.multiplier,
        open=restated_close - 0.5,
        high=restated_close + 1.0,
        low=restated_close - 1.0,
        close=restated_close,
        volume=previous.volume,
        source=previous.source,
        adjustment_basis=previous.adjustment_basis,
        fetched_at=previous.fetched_at + timedelta(minutes=1),
    )
    async with runtime_maker() as session, session.begin():
        restated_plan = await upsert_bars(session, [restatement])
    assert len(restated_plan.revisions) == 1
    async with runtime_maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, restated_plan.rows) == 1

    historical_replayed = await builder.build(historical_spec)
    current_cutoff = await database_snapshot_cutoff(builder_maker)
    current_created = await builder.build(
        SnapshotBuildSpec(
            symbol="AAPL",
            target="close",
            horizon_unit="trading_day",
            as_of=current_cutoff,
        )
    )
    assert historical_created.created is True
    assert historical_replayed.created is False
    assert historical_replayed.snapshot_id == historical_created.snapshot_id
    assert historical_replayed.availability_checked_at == historical_created.availability_checked_at
    assert current_created.created is True
    assert current_created.snapshot_id != historical_created.snapshot_id

    async with builder_maker() as session:
        stored_snapshots = (
            (
                await session.execute(
                    select(ForecastInputSnapshot).where(
                        ForecastInputSnapshot.snapshot_id.in_(
                            (
                                historical_created.snapshot_id,
                                current_created.snapshot_id,
                            )
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
    payloads = {
        row.snapshot_id: parse_snapshot_payload(bytes(row.canonical_payload))
        for row in stored_snapshots
    }
    assert payloads[historical_created.snapshot_id].observations[-1].value == previous.close
    assert payloads[current_created.snapshot_id].observations[-1].value == restated_close

    service = SnapshotForecastService(
        repository=SqlForecastInputSnapshotRepository(
            runtime_maker,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
    )
    response = await service.forecast(
        ForecastRequest(
            symbol="AAPL",
            horizon=2,
            horizon_unit="trading_day",
            target="close",
            snapshot_id=current_created.snapshot_id,
            model="baseline_naive",
            interval_coverages=[0.8],
        )
    )
    assert response.symbol == "AAPL"
    assert response.provenance.snapshot_id == current_created.snapshot_id
    assert response.provenance.lookahead_check.status == "passed"
    assert len(response.forecasts) == 2
