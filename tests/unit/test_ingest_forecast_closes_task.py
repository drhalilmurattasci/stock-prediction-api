"""Dedicated forecast-close ingestion task tests."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from app.config import Settings
from data_sources.base import OHLCVBar
from data_sources.guards import AsyncPacingCostRateGuard
from data_sources.polygon_open_close import PolygonOpenCloseProvider
from ingestion.tasks import ingest_forecast_closes as close_task
from ingestion.upsert import BarUpsertPlan, build_bar_upserts


class FakeProvider:
    name = "polygon_open_close"

    def __init__(self) -> None:
        self.calls: list[tuple[str, date, date, bool]] = []
        self.closed = False

    async def __aenter__(self) -> FakeProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]:
        self.calls.append((symbol, start, end, adjusted))
        return [
            OHLCVBar(
                symbol=symbol,
                timestamp=datetime(2026, 7, 10, 20, tzinfo=UTC),
                timespan="day",
                multiplier=1,
                open=100,
                high=101,
                low=99,
                close=100,
                volume=1000,
                source=self.name,
                fetched_at=datetime(2026, 7, 10, 21, tzinfo=UTC),
            )
        ]


async def test_ingests_latest_completed_session_with_distinct_source(
    monkeypatch: Any,
) -> None:
    provider = FakeProvider()
    resolution_calls: list[dict[str, Any]] = []
    upsert_calls: list[dict[str, Any]] = []

    async def resolve_fetch_start(sessionmaker: Any, **kwargs: Any) -> date:
        del sessionmaker
        resolution_calls.append(kwargs)
        return kwargs["requested_start"]

    async def upsert_symbol_bars(sessionmaker: Any, **kwargs: Any) -> BarUpsertPlan:
        del sessionmaker
        upsert_calls.append(kwargs)
        return BarUpsertPlan(rows=build_bar_upserts(kwargs["bars"]), revisions=[])

    monkeypatch.setattr(close_task, "_resolve_fetch_start", resolve_fetch_start)
    monkeypatch.setattr(close_task, "_upsert_symbol_bars", upsert_symbol_bars)

    fixed_now = datetime(2026, 7, 13, 16, tzinfo=UTC)
    result = await close_task.ingest_forecast_closes_async(
        symbols=["msft", "AAPL", "aapl"],
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,  # type: ignore[return-value]
        sessionmaker=object(),  # type: ignore[arg-type]
        clock=lambda: fixed_now,
    )

    assert provider.closed is True
    assert provider.calls == [
        ("AAPL", date(2025, 5, 16), date(2026, 7, 10), False),
        ("MSFT", date(2025, 5, 16), date(2026, 7, 10), False),
    ]
    assert result["status"] == "ok"
    assert result["provider"] == "polygon_open_close"
    assert result["symbols"] == ["AAPL", "MSFT"]
    assert result["requested_start"] == "2025-05-16"
    assert result["end"] == "2026-07-10"
    assert result["rows_upserted"] == 2
    assert all(call["source"] == "polygon_open_close" for call in resolution_calls)
    assert all(call["adjustment_basis"] == "raw" for call in resolution_calls)
    assert all(call["source"] == "polygon_open_close" for call in upsert_calls)


def test_latest_completed_session_never_selects_an_open_session() -> None:
    assert close_task.latest_completed_xnys_session(datetime(2026, 7, 13, 16, tzinfo=UTC)) == date(
        2026, 7, 10
    )


def test_gap_audit_returns_the_earliest_missing_xnys_session() -> None:
    existing = {
        date(2026, 7, 1),
        date(2026, 7, 2),
        date(2026, 7, 7),
    }
    # July 3 is a holiday and July 4-5 are a weekend; July 6 is the real gap.
    assert close_task._earliest_missing_xnys_session(
        existing,
        date(2026, 7, 1),
        date(2026, 7, 7),
    ) == date(2026, 7, 6)
    assert close_task.latest_completed_xnys_session(datetime(2026, 7, 13, 21, tzinfo=UTC)) == date(
        2026, 7, 13
    )
    assert close_task.latest_completed_xnys_session(datetime(2026, 11, 27, 19, tzinfo=UTC)) == date(
        2026, 11, 27
    )


async def test_default_provider_paces_per_session_vendor_calls() -> None:
    provider = close_task._build_provider(
        Settings(
            app_env="test",
            polygon_api_key="test-key",
            polygon_max_calls_per_window=7,
            polygon_rate_window_seconds=30,
        ),
        None,
    )
    try:
        assert isinstance(provider, PolygonOpenCloseProvider)
        assert isinstance(provider._guard, AsyncPacingCostRateGuard)  # noqa: SLF001
        assert provider._guard.max_calls == 7  # noqa: SLF001
        assert provider._guard.window == 30  # noqa: SLF001
    finally:
        await provider.aclose()


async def test_explicit_window_disables_automatic_watermark(monkeypatch: Any) -> None:
    provider = FakeProvider()
    watermark_values: list[bool] = []

    async def resolve_fetch_start(sessionmaker: Any, **kwargs: Any) -> date:
        del sessionmaker
        watermark_values.append(kwargs["use_watermark"])
        return kwargs["requested_start"]

    async def upsert_symbol_bars(
        sessionmaker: Any,
        *,
        bars: Sequence[OHLCVBar],
        **kwargs: Any,
    ) -> BarUpsertPlan:
        del sessionmaker, kwargs
        return BarUpsertPlan(rows=build_bar_upserts(bars), revisions=[])

    monkeypatch.setattr(close_task, "_resolve_fetch_start", resolve_fetch_start)
    monkeypatch.setattr(close_task, "_upsert_symbol_bars", upsert_symbol_bars)

    result = await close_task.ingest_forecast_closes_async(
        symbols=["AAPL"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 10),
        settings=Settings(app_env="test", polygon_api_key="unused"),
        provider_factory=lambda _settings: provider,  # type: ignore[return-value]
        sessionmaker=object(),  # type: ignore[arg-type]
        clock=lambda: datetime(2026, 7, 13, 21, tzinfo=UTC),
    )

    assert result["watermark_enabled"] is False
    assert watermark_values == [False]
