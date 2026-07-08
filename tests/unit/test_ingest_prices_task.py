"""Price ingestion task wiring tests."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from app.config import Settings
from data_sources.base import OHLCVBar
from ingestion.tasks.ingest_prices import ingest_prices_async
from ingestion.upsert import BarUpsertPlan, build_bar_upserts, build_existing_bars_select

TS = datetime(2026, 7, 6, tzinfo=UTC)


class FakeProvider:
    name = "fake"

    def __init__(self) -> None:
        self.calls: list[tuple[str, date, date, bool]] = []
        self.closed = False

    async def __aenter__(self) -> FakeProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def get_daily_bars(
        self, symbol: str, start: date, end: date, *, adjusted: bool = False
    ) -> list[OHLCVBar]:
        self.calls.append((symbol, start, end, adjusted))
        return [_bar(symbol)]


class FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeScalarResult:
    def __init__(self, value: datetime | None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> datetime | None:
        return self._value


class FakeSession:
    def __init__(self, latest_ts: datetime | None = None) -> None:
        self._latest_ts = latest_ts
        self.executed: list[tuple[str, dict[str, Any]]] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    def begin(self) -> FakeTransaction:
        return FakeTransaction()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        statement_text = str(statement)
        self.executed.append((statement_text, params or {}))
        if "max(bars.ts)" in statement_text:
            return FakeScalarResult(self._latest_ts)
        return None


class FakeSessionmaker:
    def __init__(self, latest_ts: datetime | None = None) -> None:
        self._latest_ts = latest_ts
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession(self._latest_ts)
        self.sessions.append(session)
        return session


def _bar(symbol: str) -> OHLCVBar:
    return OHLCVBar(
        symbol=symbol,
        timestamp=TS,
        timespan="day",
        multiplier=1,
        open=100,
        high=101,
        low=99,
        close=100,
        volume=1000,
        source="fake",
        fetched_at=TS,
    )


@pytest.mark.asyncio
async def test_ingest_prices_fetches_locks_and_upserts_per_symbol():
    provider = FakeProvider()
    sessionmaker = FakeSessionmaker()
    upsert_calls: list[list[str]] = []

    async def fake_upsert(session: Any, bars: Sequence[OHLCVBar]) -> BarUpsertPlan:
        del session
        upsert_calls.append([bar.symbol for bar in bars])
        rows = build_bar_upserts(bars)
        return BarUpsertPlan(rows=rows, revisions=[])

    result = await ingest_prices_async(
        symbols=["msft", "AAPL", "aapl"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 6),
        adjusted=True,
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
        upsert_fn=fake_upsert,  # type: ignore[arg-type]
    )

    assert provider.closed is True
    assert provider.calls == [
        ("AAPL", date(2026, 7, 1), date(2026, 7, 6), True),
        ("MSFT", date(2026, 7, 1), date(2026, 7, 6), True),
    ]
    assert upsert_calls == [["AAPL"], ["MSFT"]]
    assert result["status"] == "ok"
    assert result["symbols"] == ["AAPL", "MSFT"]
    assert result["rows_upserted"] == 2
    assert len(sessionmaker.sessions) == 2
    assert all(
        "pg_advisory_xact_lock" in session.executed[0][0] for session in sessionmaker.sessions
    )


@pytest.mark.asyncio
async def test_ingest_prices_uses_watermark_when_start_is_not_explicit():
    provider = FakeProvider()
    latest_ts = datetime(2026, 7, 4, tzinfo=UTC)
    sessionmaker = FakeSessionmaker(latest_ts=latest_ts)

    async def fake_upsert(session: Any, bars: Sequence[OHLCVBar]) -> BarUpsertPlan:
        del session
        rows = build_bar_upserts(bars)
        return BarUpsertPlan(rows=rows, revisions=[])

    result = await ingest_prices_async(
        symbols=["AAPL"],
        end=date(2026, 7, 6),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
        upsert_fn=fake_upsert,  # type: ignore[arg-type]
    )

    assert provider.calls == [("AAPL", date(2026, 7, 5), date(2026, 7, 6), False)]
    assert result["requested_start"] == "2026-06-29"
    assert result["watermark_enabled"] is True
    assert result["per_symbol"][0]["fetch_start"] == "2026-07-05"


@pytest.mark.asyncio
async def test_ingest_prices_skips_symbol_when_watermark_is_current():
    provider = FakeProvider()
    sessionmaker = FakeSessionmaker(latest_ts=datetime(2026, 7, 6, tzinfo=UTC))

    result = await ingest_prices_async(
        symbols=["AAPL"],
        end=date(2026, 7, 6),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
    )

    assert provider.calls == []
    assert result["rows_upserted"] == 0
    assert result["per_symbol"][0]["fetch_start"] is None


@pytest.mark.asyncio
async def test_ingest_prices_requires_nonempty_symbol_list():
    with pytest.raises(ValueError, match="at least one symbol"):
        await ingest_prices_async(
            symbols=["", "  "],
            settings=Settings(app_env="test", polygon_api_key="unused"),
            provider_factory=lambda _settings: FakeProvider(),
            sessionmaker=FakeSessionmaker(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_ingest_prices_requires_valid_date_window():
    with pytest.raises(ValueError, match="start must be on or before end"):
        await ingest_prices_async(
            symbols=["AAPL"],
            start=date(2026, 7, 7),
            end=date(2026, 7, 6),
            settings=Settings(app_env="test", polygon_api_key="unused"),
            provider_factory=lambda _settings: FakeProvider(),
            sessionmaker=FakeSessionmaker(),  # type: ignore[arg-type]
        )


def test_existing_bar_select_locks_loaded_rows_for_update():
    row = build_bar_upserts([_bar("AAPL")])[0]
    sql = str(build_existing_bars_select([row]).compile(dialect=postgresql.dialect()))

    assert "FOR UPDATE" in sql
