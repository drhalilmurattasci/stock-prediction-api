"""Unit tests for the Polygon provider using a mocked httpx transport.

No real network calls: every test injects an ``httpx.MockTransport`` whose handler
returns canned Polygon-shaped JSON. ``clock`` and ``retry_wait`` are pinned so the
tests are deterministic and fast.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pytest
import tenacity

from data_sources.base import (
    OHLCVBar,
    ProviderHTTPError,
    SymbolNotFoundError,
    VendorRateLimitError,
)
from data_sources.guards import InMemoryCostRateGuard
from data_sources.polygon import PolygonProvider

FIXED_NOW = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)


def _provider(handler, **kwargs) -> PolygonProvider:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    kwargs.setdefault("retry_wait", 0.0)
    return PolygonProvider("test-key", client=client, clock=lambda: FIXED_NOW, **kwargs)


async def test_daily_bars_parse_and_stamp():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/v2/aggs/ticker/AAPL/range/1/day/" in request.url.path
        assert request.url.params["adjusted"] == "false"
        assert request.headers["Authorization"] == "Bearer test-key"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "t": 1751803200000,
                        "o": 210.0,
                        "h": 215.0,
                        "l": 209.0,
                        "c": 214.0,
                        "v": 1_000_000,
                        "vw": 213.2,
                        "n": 5000,
                    }
                ]
            },
        )

    provider = _provider(handler)
    bars = await provider.get_daily_bars("aapl", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert len(bars) == 1
    bar = bars[0]
    assert isinstance(bar, OHLCVBar)
    assert bar.symbol == "AAPL"
    assert bar.timespan == "day" and bar.multiplier == 1
    assert bar.close == 214.0 and bar.vwap == 213.2 and bar.trade_count == 5000
    assert bar.adjustment_basis == "raw"
    assert bar.source == "polygon" and bar.fetched_at == FIXED_NOW
    assert bar.timestamp.tzinfo is not None


async def test_adjusted_flag_sets_basis():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["adjusted"] == "true"
        return httpx.Response(
            200,
            json={"results": [{"t": 1751803200000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10}]},
        )

    provider = _provider(handler)
    bars = await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6), adjusted=True)
    await provider.aclose()
    assert bars[0].adjustment_basis == "split_dividend_adjusted"


async def test_aggregates_follow_next_url_pagination():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "cursor" not in request.url.query.decode():
            return httpx.Response(
                200,
                json={
                    "results": [{"t": 1751803200000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}],
                    "next_url": "https://api.polygon.io/v2/aggs/next?cursor=abc",
                },
            )
        return httpx.Response(
            200,
            json={"results": [{"t": 1751889600000, "o": 2, "h": 2, "l": 2, "c": 2, "v": 2}]},
        )

    provider = _provider(handler)
    bars = await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert [b.close for b in bars] == [1, 2]
    assert len(calls) == 2


async def test_transient_500_is_retried_then_succeeds():
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(200, json={"results": []})

    provider = _provider(handler, max_attempts=4)
    bars = await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert bars == []
    assert attempts["n"] == 3  # two failures, then success


async def test_429_honors_retry_after_without_sleeping():
    waits: list[float] = []
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "2"}, json={"error": "slow down"})

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    provider = _provider(handler, max_attempts=2, retry_wait=0.1)
    provider._retrying = lambda: tenacity.AsyncRetrying(  # noqa: SLF001
        stop=tenacity.stop_after_attempt(provider._max_attempts),  # noqa: SLF001
        wait=provider._wait_seconds,  # noqa: SLF001
        retry=tenacity.retry_if_exception(provider._is_retryable),  # noqa: SLF001
        sleep=sleep,
        reraise=True,
    )
    with pytest.raises(ProviderHTTPError) as exc:
        await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert exc.value.status_code == 429
    assert waits == [2.0]
    assert attempts["n"] == 2


async def test_non_retryable_4xx_raises_immediately():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": "bad request"})

    provider = _provider(handler, max_attempts=4)
    with pytest.raises(ProviderHTTPError) as exc:
        await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert exc.value.status_code == 400
    assert calls["n"] == 1  # not retried


async def test_404_maps_to_symbol_not_found():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "not found"})

    provider = _provider(handler)
    with pytest.raises(SymbolNotFoundError):
        await provider.get_security("NOPE")
    await provider.aclose()


async def test_get_security_parses_reference():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v3/reference/tickers/AAPL"
        return httpx.Response(
            200,
            json={
                "results": {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "currency_name": "usd",
                    "active": True,
                    "cik": "0000320193",
                    "composite_figi": "BBG000B9XRY4",
                }
            },
        )

    provider = _provider(handler)
    sec = await provider.get_security("aapl")
    await provider.aclose()
    assert sec.symbol == "AAPL" and sec.name == "Apple Inc."
    assert sec.security_type == "CS" and sec.active is True


async def test_splits_and_dividends_parse():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v3/reference/splits":
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "ticker": "AAPL",
                            "execution_date": "2020-08-31",
                            "split_from": 1,
                            "split_to": 4,
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "ticker": "AAPL",
                        "ex_dividend_date": "2026-05-10",
                        "pay_date": "2026-05-15",
                        "cash_amount": 0.25,
                        "currency": "USD",
                        "frequency": 4,
                        "dividend_type": "CD",
                    }
                ]
            },
        )

    provider = _provider(handler)
    splits = await provider.get_splits("AAPL")
    dividends = await provider.get_dividends("AAPL")
    await provider.aclose()

    assert len(splits) == 1
    assert splits[0].split_to == 4 and splits[0].execution_date == date(2020, 8, 31)
    assert len(dividends) == 1
    assert dividends[0].cash_amount == 0.25
    assert dividends[0].ex_dividend_date == date(2026, 5, 10)
    assert dividends[0].pay_date == date(2026, 5, 15)


async def test_guard_is_invoked_per_request():
    guard = InMemoryCostRateGuard(max_calls_per_window=1, window_seconds=60, clock=lambda: 0.0)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"results": []})

    provider = _provider(handler, guard=guard)
    await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    # second call exceeds the 1-call window budget -> guard raises

    with pytest.raises(VendorRateLimitError):
        await provider.get_daily_bars("MSFT", date(2026, 7, 1), date(2026, 7, 6))
    assert calls == 1  # the rejected call never reaches the network
    await provider.aclose()


async def test_requires_api_key():
    with pytest.raises(ValueError, match="API key"):
        PolygonProvider("")


async def test_intraday_rejects_bad_timespan():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": []})

    provider = _provider(handler)
    with pytest.raises(ValueError, match="intraday timespan"):
        await provider.get_intraday_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6), timespan="day")
    await provider.aclose()
