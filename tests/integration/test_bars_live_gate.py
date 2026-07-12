"""Live-database gate: migrations, upsert semantics, pagination, finiteness.

Skipped unless ``TEST_DATABASE_URL`` points at a **throwaway** TimescaleDB —
the target database is RESET (bars tables and alembic_version are dropped)
before the migration chain is applied. Run it with::

    docker compose up -d timescaledb          # needs .env credentials
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://<user>:<pass>@localhost:5432/<db>"
    uv run pytest tests/integration -v

This is the empirical proof the unit suite cannot give: the Alembic chain
applies against a real hypertable, ``upsert_bars`` replay is a no-op while a
restatement writes a revision row, a seeded two-page ``/v1/prices`` read has
no gaps or duplicates with TIMESTAMPTZ round-tripping, the finiteness CHECKs
actually reject NaN/Infinity under Postgres NaN ordering, and the API
statement-timeout cancels a pathological statement.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings
from app.db.models.bars import Bar, BarRevision
from app.db.session import build_engine, build_sessionmaker
from app.schemas.prices import PriceFilters
from app.services.prices import read_prices
from data_sources.base import OHLCVBar
from ingestion.upsert import upsert_bars

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL not set - live-DB gate skipped", allow_module_level=True)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _async_url(url: str) -> str:
    if "+asyncpg" in url:
        return url
    return url.replace("postgresql://", "postgresql+asyncpg://")


def _sync_url(url: str) -> str:
    return _async_url(url).replace("+asyncpg", "+psycopg")


@pytest.fixture(scope="module")
def migrated_database_url() -> str:
    """Reset the throwaway database, then apply the full migration chain."""
    url = _async_url(TEST_DATABASE_URL)
    sync_engine = create_engine(_sync_url(url))
    with sync_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS bars_revisions, bars, alembic_version CASCADE"))
    sync_engine.dispose()

    result = subprocess.run(  # fresh process: env.py resolves DATABASE_URL uncached
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=REPO_ROOT,
        env={**os.environ, "DATABASE_URL": url},
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}"
    return url


@pytest.fixture
async def engine(migrated_database_url: str) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE bars, bars_revisions"))
    yield engine
    await engine.dispose()


def _bar(day: int, *, close: float = 101.0, fetched_hour: int = 1) -> OHLCVBar:
    ts = datetime(2026, 7, day, tzinfo=UTC)
    return OHLCVBar(
        symbol="AAPL",
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


async def test_migration_chain_applies_and_bars_is_a_hypertable(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        assert version == "0004_bars_finiteness"
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


async def test_upsert_replay_is_noop_and_restatement_writes_revision(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)

    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, close=101.0)])

    # Identical values with a later fetched_at: the IS DISTINCT FROM guard must
    # leave the row byte-identical and write no revision.
    async with maker() as session:
        async with session.begin():
            replay = await upsert_bars(session, [_bar(1, close=101.0, fetched_hour=9)])
        assert replay.revisions == []

    async with maker() as session:
        row = (await session.execute(select(Bar))).scalar_one()
        assert row.close == 101.0
        assert row.fetched_at == datetime(2026, 7, 1, 1, tzinfo=UTC)  # replay did not touch it
        revision_count = (
            await session.execute(select(func.count()).select_from(BarRevision))
        ).scalar_one()
        assert revision_count == 0

    # A restated close must update the current row AND append the prior value.
    async with maker() as session:
        async with session.begin():
            restated = await upsert_bars(session, [_bar(1, close=102.5, fetched_hour=12)])
        assert len(restated.revisions) == 1

    async with maker() as session:
        row = (await session.execute(select(Bar))).scalar_one()
        assert row.close == 102.5
        revision = (await session.execute(select(BarRevision))).scalar_one()
        assert revision.previous_close == 101.0
        assert revision.incoming_close == 102.5


async def test_two_page_read_covers_all_bars_without_gap_or_dup(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(day, close=100.0 + day) for day in (1, 2, 3, 4, 5)])

    async with maker() as session:
        page_one = await read_prices(session, "AAPL", PriceFilters(limit=3))
        assert [bar.timestamp for bar in page_one.bars] == [
            datetime(2026, 7, day, tzinfo=UTC) for day in (3, 4, 5)
        ]
        assert page_one.page.has_more is True
        assert page_one.page.next_end == datetime(2026, 7, 3, tzinfo=UTC)

        page_two = await read_prices(
            session, "AAPL", PriceFilters(limit=3, end=page_one.page.next_end)
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
    migrated_database_url: str,
) -> None:
    settings = Settings(app_env="test", database_url=migrated_database_url)
    capped = build_engine(settings, statement_timeout_ms=150)
    try:
        async with capped.connect() as conn:
            with pytest.raises(DBAPIError):
                await conn.execute(text("SELECT pg_sleep(2)"))
    finally:
        await capped.dispose()
