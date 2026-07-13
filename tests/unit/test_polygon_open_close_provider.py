"""Official regular-session daily open/close adapter tests."""

from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pytest

from data_sources.guards import AsyncPacingCostRateGuard
from data_sources.polygon_open_close import (
    OpenClosePayloadError,
    PolygonOpenCloseProvider,
)


async def test_fetches_only_xnys_sessions_and_uses_regular_close() -> None:
    response_count = 0
    requested_dates: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal response_count
        response_count += 1
        requested_date = request.url.path.rsplit("/", maxsplit=1)[-1]
        requested_dates.append(requested_date)
        assert request.url.params["adjusted"] == "false"
        assert request.headers["Authorization"] == "Bearer test-key"
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "symbol": "AAPL",
                "from": requested_date,
                "open": 100.0,
                "high": 104.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 1_000_000,
                "afterHours": 999.0,
                "preMarket": 1.0,
            },
        )

    clock_calls = 0

    def completion_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        assert response_count == clock_calls
        return datetime(2026, 7, 6, 21, clock_calls, tzinfo=UTC)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = PolygonOpenCloseProvider(
        "test-key",
        client=client,
        clock=completion_clock,
        retry_wait=0.0,
    )

    bars = await provider.get_daily_bars("aapl", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    # July 3 was an XNYS holiday; the weekend is excluded as well.
    assert requested_dates == ["2026-07-01", "2026-07-02", "2026-07-06"]
    assert [bar.timestamp for bar in bars] == [
        datetime(2026, 7, 1, 20, tzinfo=UTC),
        datetime(2026, 7, 2, 20, tzinfo=UTC),
        datetime(2026, 7, 6, 20, tzinfo=UTC),
    ]
    assert all(bar.close == 103.0 for bar in bars)
    assert all(bar.close != 999.0 for bar in bars)
    assert all(bar.source == "polygon_open_close" for bar in bars)
    assert all(bar.adjustment_basis == "raw" for bar in bars)
    assert [bar.fetched_at.minute for bar in bars] == [1, 2, 3]


async def test_rejects_adjusted_requests_without_calling_vendor() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    provider = PolygonOpenCloseProvider(
        "test-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(ValueError, match="must be unadjusted"):
        await provider.get_daily_bars(
            "AAPL",
            date(2026, 7, 2),
            date(2026, 7, 2),
            adjusted=True,
        )
    await provider.aclose()

    assert calls == 0


async def test_rejects_response_received_before_official_session_close() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "symbol": "AAPL",
                "from": "2026-07-06",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
            },
        )

    provider = PolygonOpenCloseProvider(
        "test-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        clock=lambda: datetime(2026, 7, 6, 19, 59, tzinfo=UTC),
    )
    with pytest.raises(OpenClosePayloadError, match="before the session closed"):
        await provider.get_daily_bars("AAPL", date(2026, 7, 6), date(2026, 7, 6))
    await provider.aclose()


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"status": "DELAYED"}, "status"),
        ({"symbol": "MSFT"}, "symbol"),
        ({"from": "2026-07-01"}, "date"),
        ({"close": None}, "missing fields"),
    ],
)
async def test_rejects_mismatched_or_incomplete_payload(
    override: dict[str, object],
    message: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload: dict[str, object] = {
            "status": "OK",
            "symbol": "AAPL",
            "from": "2026-07-06",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 10,
        }
        payload.update(override)
        return httpx.Response(200, json=payload)

    provider = PolygonOpenCloseProvider(
        "test-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        clock=lambda: datetime(2026, 7, 6, 21, tzinfo=UTC),
    )
    with pytest.raises(OpenClosePayloadError, match=message):
        await provider.get_daily_bars("AAPL", date(2026, 7, 6), date(2026, 7, 6))
    await provider.aclose()


async def test_pacing_guard_waits_for_next_window_without_dropping_work() -> None:
    now = [0.0]
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)
        now[0] += seconds

    guard = AsyncPacingCostRateGuard(
        max_calls_per_window=2,
        window_seconds=10,
        clock=lambda: now[0],
        sleep=sleep,
    )

    await guard.acquire("polygon_open_close")
    await guard.acquire("polygon_open_close")
    await guard.acquire("polygon_open_close")

    assert waits == [10.0]
    assert guard.snapshot("polygon_open_close") == {"window_count": 1, "spent": 3}
