"""Unit tests for the Polygon provider using a mocked httpx transport.

No real network calls: every test injects an ``httpx.MockTransport`` whose handler
returns canned Polygon-shaped JSON. ``clock`` and ``retry_wait`` are pinned so the
tests are deterministic and fast.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest
import tenacity

from data_sources.base import (
    CostBudgetExceeded,
    DividendPage,
    OHLCVBar,
    ProviderHTTPError,
    SplitPage,
    SymbolNotFoundError,
    VendorRateLimitError,
)
from data_sources.guards import AsyncPacingCostRateGuard, InMemoryCostRateGuard
from data_sources.polygon import CorporateActionPayloadError, PolygonProvider

FIXED_NOW = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)


def _provider(handler, **kwargs) -> PolygonProvider:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    kwargs.setdefault("retry_wait", 0.0)
    kwargs.setdefault("clock", lambda: FIXED_NOW)
    return PolygonProvider("test-key", client=client, **kwargs)


async def test_provider_owned_client_ignores_ambient_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:18080")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:18443")
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:19050")

    provider = PolygonProvider("test-key")
    try:
        assert provider._client._trust_env is False  # noqa: SLF001
    finally:
        await provider.aclose()


async def test_injected_client_preserves_its_proxy_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:18443")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(204)),
        trust_env=True,
    )
    provider = PolygonProvider("test-key", client=client)
    try:
        assert provider._client is client  # noqa: SLF001
        assert provider._client._trust_env is True  # noqa: SLF001
        await provider.aclose()
        assert client.is_closed is False
    finally:
        await client.aclose()


ACTION_START = date(2025, 7, 2)
ACTION_END = date(2026, 7, 13)


def _split_row(**updates):
    row = {
        "id": "split-1",
        "ticker": "MSFT",
        "execution_date": "2026-01-15",
        "split_from": 1,
        "split_to": 2,
        "adjustment_type": "forward_split",
        "historical_adjustment_factor": 0.5,
    }
    row.update(updates)
    return row


def _dividend_row(**updates):
    row = {
        "id": "dividend-1",
        "ticker": "MSFT",
        "ex_dividend_date": "2026-05-10",
        "pay_date": "2026-05-15",
        "record_date": "2026-05-11",
        "declaration_date": "2026-03-10",
        "cash_amount": 0.83,
        "split_adjusted_cash_amount": 0.83,
        "historical_adjustment_factor": 0.996,
        "currency": "USD",
        "frequency": 4,
        "distribution_type": "recurring",
    }
    row.update(updates)
    return row


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


async def test_total_budget_one_blocks_retry_before_a_second_http_attempt():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    guard = AsyncPacingCostRateGuard(
        max_calls_per_window=1,
        window_seconds=60,
        total_budget=1,
    )
    provider = _provider(handler, guard=guard, max_attempts=4)
    with pytest.raises(CostBudgetExceeded):
        await provider.get_daily_bars("AAPL", date(2026, 7, 1), date(2026, 7, 6))
    await provider.aclose()

    assert attempts == 1
    assert guard.snapshot("polygon") == {"window_count": 1, "spent": 1}


async def test_429_honors_retry_after_without_sleeping():
    waits: list[float] = []
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "2"}, json={"error": "slow down"})

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    provider = _provider(handler, max_attempts=2, retry_wait=0.1)
    provider._retrying = lambda: tenacity.AsyncRetrying(  # type: ignore[method-assign]  # noqa: SLF001
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


async def test_splits_and_dividends_use_modern_bounded_one_page_contract():
    calls: list[str] = []
    response_seen = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal response_seen
        calls.append(request.url.path)
        response_seen = True
        assert request.url.host == "api.massive.com"
        assert request.url.params["ticker"] == "MSFT"
        assert request.url.params["limit"] == "5000"
        assert request.headers["Authorization"] == "Bearer test-key"
        if request.url.path == "/stocks/v1/splits":
            assert request.url.params["execution_date.gte"] == ACTION_START.isoformat()
            assert request.url.params["execution_date.lte"] == ACTION_END.isoformat()
            assert request.url.params["sort"] == "execution_date.asc"
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "request_id": 101,
                    "results": [_split_row()],
                },
            )
        assert request.url.path == "/stocks/v1/dividends"
        assert request.url.params["ex_dividend_date.gte"] == ACTION_START.isoformat()
        assert request.url.params["ex_dividend_date.lte"] == ACTION_END.isoformat()
        assert request.url.params["sort"] == "ex_dividend_date.asc"
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "request_id": "request-dividend",
                "results": [_dividend_row(id="dividend-2"), _dividend_row()],
            },
        )

    def clock() -> datetime:
        assert response_seen, "fetched_at must be sampled after the response"
        return FIXED_NOW

    provider = _provider(handler, clock=clock)
    splits = await provider.get_splits(" msft ", start=ACTION_START, end=ACTION_END)
    dividends = await provider.get_dividends("msft", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    assert isinstance(splits, SplitPage)
    assert splits.provider_request_id == "101"
    assert splits.provider_origin == "https://api.massive.com"
    assert splits.endpoint == "/stocks/v1/splits"
    assert splits.action_kind == "splits"
    assert splits.symbol == "MSFT"
    assert splits.start == ACTION_START and splits.end == ACTION_END
    assert splits.source == "polygon" and splits.fetched_at == FIXED_NOW
    assert len(splits.results) == 1
    split = splits.results[0]
    assert split.provider_event_id == "split-1"
    assert split.adjustment_type == "forward_split"
    assert split.split_from == Decimal("1")
    assert split.split_to == Decimal("2")
    assert split.historical_adjustment_factor == Decimal("0.5")
    assert split.source == "polygon" and split.fetched_at == FIXED_NOW

    assert isinstance(dividends, DividendPage)
    assert dividends.provider_request_id == "request-dividend"
    assert dividends.endpoint == "/stocks/v1/dividends"
    assert dividends.action_kind == "dividends"
    assert len(dividends.results) == 2
    dividend = dividends.results[0]
    assert dividend.provider_event_id == "dividend-1"
    assert dividend.cash_amount == Decimal("0.83")
    assert dividend.split_adjusted_cash_amount == Decimal("0.83")
    assert dividend.historical_adjustment_factor == Decimal("0.996")
    assert dividend.distribution_type == "recurring"
    assert dividend.ex_dividend_date == date(2026, 5, 10)
    assert dividend.pay_date == date(2026, 5, 15)
    assert [value.provider_event_id for value in dividends.results] == [
        "dividend-1",
        "dividend-2",
    ]
    assert calls == ["/stocks/v1/splits", "/stocks/v1/dividends"]


async def test_empty_corporate_action_page_retains_request_provenance():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "request_id": "empty-1", "results": []})

    provider = _provider(handler)
    page = await provider.get_splits("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    assert page.results == ()
    assert page.provider_request_id == "empty-1"
    assert page.fetched_at == FIXED_NOW
    assert page.start == ACTION_START and page.end == ACTION_END


async def test_corporate_action_decimal_lexemes_are_preserved_exactly():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=(
                '{"status":"OK","request_id":"decimal-1","results":[{'
                '"id":"dividend-decimal","ticker":"MSFT",'
                '"ex_dividend_date":"2026-05-10",'
                '"cash_amount":0.10000000000000000001,'
                '"split_adjusted_cash_amount":0.10000000000000000002,'
                '"historical_adjustment_factor":0.99999999999999999999,'
                '"distribution_type":"recurring"}]}'
            ),
        )

    provider = _provider(handler)
    page = await provider.get_dividends("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    result = page.results[0]
    assert str(result.cash_amount) == "0.10000000000000000001"
    assert str(result.split_adjusted_cash_amount) == "0.10000000000000000002"
    assert str(result.historical_adjustment_factor) == "0.99999999999999999999"
    assert '"cash_amount":"0.10000000000000000001"' in page.model_dump_json()


@pytest.mark.parametrize(
    "next_url",
    [None, "", "https://api.massive.com/stocks/v1/splits?cursor=next"],
)
async def test_corporate_actions_reject_any_present_next_url_without_following_it(
    next_url: str | None,
):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "request_id": "page-1",
                "results": [_split_row()],
                "next_url": next_url,
            },
        )

    provider = _provider(handler)
    with pytest.raises(CorporateActionPayloadError, match="one-page scope"):
        await provider.get_splits("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    assert calls == 1


async def test_corporate_actions_never_retry_an_http_failure():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    provider = _provider(handler, max_attempts=4)
    with pytest.raises(ProviderHTTPError) as exc:
        await provider.get_dividends("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    assert exc.value.status_code == 503
    assert calls == 1


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (["not", "an", "object"], "response must be an object"),
        ({"status": "ERROR", "request_id": "x", "results": []}, "status is not OK"),
        ({"status": "OK", "request_id": "x"}, "array of objects"),
        ({"status": "OK", "request_id": "", "results": []}, "request_id"),
        ({"status": "OK", "request_id": "bad, id", "results": []}, "request_id"),
        ({"status": "OK", "request_id": " x ", "results": []}, "request_id"),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(ticker="AAPL")],
            },
            "ticker does not match",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(execution_date="2024-01-01")],
            },
            "escaped the requested date window",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(execution_date="20260710")],
            },
            "event date",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(), _split_row()],
            },
            "duplicate event id",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(id="bad, id")],
            },
            "event id",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(id=" split-1 ")],
            },
            "event id",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(historical_adjustment_factor="NaN")],
            },
            "numeric field",
        ),
        (
            {
                "status": "OK",
                "request_id": "x",
                "results": [_split_row(adjustment_type="mystery")],
            },
            "split is invalid",
        ),
    ],
)
async def test_splits_fail_closed_on_unprovable_payload(payload, message):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    provider = _provider(handler)
    with pytest.raises(CorporateActionPayloadError, match=message):
        await provider.get_splits("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()


@pytest.mark.parametrize(
    "updates",
    [
        {"ticker": "AAPL"},
        {"ex_dividend_date": "2024-01-01"},
        {"historical_adjustment_factor": "Infinity"},
        {"cash_amount": -1},
        {"frequency": 4.5},
        {"pay_date": "20260710"},
        {"record_date": 20260710},
        {"distribution_type": "mystery"},
        {"id": ""},
    ],
)
async def test_dividends_fail_closed_on_invalid_scope_identity_or_numeric_fields(updates):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "request_id": "x",
                "results": [_dividend_row(**updates)],
            },
        )

    provider = _provider(handler)
    with pytest.raises(CorporateActionPayloadError):
        await provider.get_dividends("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()


async def test_corporate_action_bounds_refuse_before_http():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    provider = _provider(handler)
    for symbol in ("  ", "MS FT", "../MSFT", "M" * 33):
        with pytest.raises(ValueError, match="symbol"):
            await provider.get_splits(symbol, start=ACTION_START, end=ACTION_END)
    with pytest.raises(ValueError, match="start"):
        await provider.get_dividends("MSFT", start=ACTION_END, end=ACTION_START)
    await provider.aclose()

    assert calls == 0


@pytest.mark.parametrize(
    "origin",
    [
        "http://api.massive.com",
        "https://user:secret@api.massive.com",
        "https://api.massive.com/path",
        "https://api.massive.com?query=1",
    ],
)
def test_corporate_action_origin_must_be_one_https_origin(origin):
    with pytest.raises(ValueError, match="one HTTPS origin"):
        PolygonProvider("test-key", corporate_actions_base_url=origin)


async def test_corporate_action_guard_receives_stable_endpoint_identity():
    calls: list[tuple[str, int, str | None]] = []

    class Guard:
        async def acquire(
            self,
            vendor: str,
            *,
            cost: int = 1,
            endpoint: str | None = None,
        ) -> None:
            calls.append((vendor, cost, endpoint))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "request_id": "x", "results": []})

    provider = _provider(handler, guard=Guard())
    await provider.get_splits("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.get_dividends("MSFT", start=ACTION_START, end=ACTION_END)
    await provider.aclose()

    assert calls == [
        ("polygon", 1, "/stocks/v1/splits"),
        ("polygon", 1, "/stocks/v1/dividends"),
    ]


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
