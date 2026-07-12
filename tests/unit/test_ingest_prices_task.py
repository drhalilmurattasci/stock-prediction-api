"""Price ingestion task wiring tests."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from app.config import Settings
from data_sources.base import OHLCVBar, SymbolNotFoundError
from data_sources.guards import InMemoryCostRateGuard
from data_sources.polygon import PolygonProvider
from ingestion.tasks.ingest_prices import (
    _build_provider,
    _latest_bar_ts_statement,
    _polygon_guard,
    ingest_prices_async,
)
from ingestion.upsert import BarUpsertPlan, build_bar_upserts, build_existing_bars_select

TS = datetime(2026, 7, 6, tzinfo=UTC)


class FakeProvider:
    name = "fake"

    def __init__(
        self,
        *,
        sessionmaker: FakeSessionmaker | None = None,
        fail_symbols: set[str] | None = None,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._fail_symbols = fail_symbols or set()
        self.calls: list[tuple[str, date, date, bool]] = []
        self.closed = False

    async def __aenter__(self) -> FakeProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def get_daily_bars(
        self, symbol: str, start: date, end: date, *, adjusted: bool = False
    ) -> list[OHLCVBar]:
        if self._sessionmaker is not None:
            assert self._sessionmaker.active_transactions == 0
        self.calls.append((symbol, start, end, adjusted))
        if symbol in self._fail_symbols:
            raise SymbolNotFoundError(f"{symbol} missing")
        return [_bar(symbol)]


class FakeTransaction:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> None:
        self._session.sessionmaker.active_transactions += 1
        return None

    async def __aexit__(self, *exc_info: object) -> None:
        self._session.sessionmaker.active_transactions -= 1
        return None


class FakeScalarResult:
    def __init__(self, value: datetime | None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> datetime | None:
        return self._value


class FakeSession:
    def __init__(self, sessionmaker: FakeSessionmaker, latest_ts: datetime | None = None) -> None:
        self.sessionmaker = sessionmaker
        self._latest_ts = latest_ts
        self.executed: list[tuple[str, dict[str, Any]]] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    def begin(self) -> FakeTransaction:
        return FakeTransaction(self)

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
        self.active_transactions = 0

    def __call__(self) -> FakeSession:
        session = FakeSession(self, self._latest_ts)
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


async def _fake_upsert(session: Any, bars: Sequence[OHLCVBar]) -> BarUpsertPlan:
    del session
    rows = build_bar_upserts(bars)
    return BarUpsertPlan(rows=rows, revisions=[])


async def test_default_polygon_provider_has_shared_fail_closed_guard():
    _polygon_guard.cache_clear()
    settings = Settings(
        app_env="test",
        polygon_api_key="test-key",
    )
    first = _build_provider(settings, None)
    second = _build_provider(settings, None)
    try:
        assert isinstance(first, PolygonProvider)
        assert isinstance(second, PolygonProvider)
        assert isinstance(first._guard, InMemoryCostRateGuard)
        assert first._guard is second._guard
        assert first._guard.max_calls == 5
        assert first._guard.window == 60
        assert first._guard.total_budget is None
    finally:
        await first.aclose()
        await second.aclose()
        _polygon_guard.cache_clear()


@pytest.mark.asyncio
async def test_ingest_prices_fetches_locks_and_upserts_per_symbol():
    sessionmaker = FakeSessionmaker()
    provider = FakeProvider(sessionmaker=sessionmaker)
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
    assert len(sessionmaker.sessions) == 4
    assert all(
        "pg_advisory_xact_lock" in session.executed[0][0] for session in sessionmaker.sessions
    )


@pytest.mark.asyncio
async def test_ingest_prices_refetches_trailing_overlap_when_watermark_is_recent():
    latest_ts = datetime(2026, 7, 4, tzinfo=UTC)
    sessionmaker = FakeSessionmaker(latest_ts=latest_ts)
    provider = FakeProvider(sessionmaker=sessionmaker)

    result = await ingest_prices_async(
        symbols=["AAPL"],
        end=date(2026, 7, 6),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
        upsert_fn=_fake_upsert,  # type: ignore[arg-type]
    )

    assert provider.calls == [("AAPL", date(2026, 6, 29), date(2026, 7, 6), False)]
    assert result["requested_start"] == "2026-06-29"
    assert result["watermark_enabled"] is True
    assert result["per_symbol"][0]["fetch_start"] == "2026-06-29"


@pytest.mark.asyncio
async def test_ingest_prices_fetches_from_next_missing_day_after_long_outage():
    sessionmaker = FakeSessionmaker(latest_ts=datetime(2026, 7, 6, tzinfo=UTC))
    provider = FakeProvider(sessionmaker=sessionmaker)

    result = await ingest_prices_async(
        symbols=["AAPL"],
        end=date(2026, 7, 20),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
        upsert_fn=_fake_upsert,  # type: ignore[arg-type]
    )

    assert provider.calls == [("AAPL", date(2026, 7, 7), date(2026, 7, 20), False)]
    assert result["per_symbol"][0]["fetch_start"] == "2026-07-07"


@pytest.mark.asyncio
async def test_ingest_prices_continues_after_one_symbol_failure():
    provider = FakeProvider(fail_symbols={"BAD"})
    sessionmaker = FakeSessionmaker()

    result = await ingest_prices_async(
        symbols=["AAPL", "BAD", "MSFT"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 6),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,
        sessionmaker=sessionmaker,  # type: ignore[arg-type]
        upsert_fn=_fake_upsert,  # type: ignore[arg-type]
    )

    assert result["status"] == "degraded"
    assert result["rows_upserted"] == 2
    assert result["failures"] == 1
    assert result["retryable_failures"] == 0
    assert [(entry["symbol"], entry["status"]) for entry in result["per_symbol"]] == [
        ("AAPL", "ok"),
        ("BAD", "failed"),
        ("MSFT", "ok"),
    ]


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


def test_latest_bar_ts_statement_is_adjustment_basis_specific():
    sql = str(
        _latest_bar_ts_statement("AAPL", "polygon", "day", "raw").compile(
            dialect=postgresql.dialect()
        )
    )

    assert "bars.adjustment_basis" in sql
