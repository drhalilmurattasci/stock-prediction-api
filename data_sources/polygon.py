"""Polygon.io / Massive market-data provider.

Implements ``MarketDataProvider`` over Polygon's REST API using async ``httpx``
with ``tenacity`` retries and an injectable cost/rate guard. Returns normalized,
provenance-stamped DTOs — raw (unadjusted) bars by default; corporate actions are
fetched separately so adjusted prices can be computed on read.

The ``httpx.AsyncClient`` is injectable so tests drive it with a mock transport
(no real network calls). ``clock`` is injectable so ``fetched_at`` is deterministic
in tests.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import httpx
import structlog
import tenacity

from data_sources.base import (
    AdjustmentBasis,
    CostRateGuard,
    Dividend,
    OHLCVBar,
    ProviderHTTPError,
    SecurityRef,
    Split,
    SymbolNotFoundError,
    Timespan,
)
from data_sources.guards import NullCostRateGuard

log = structlog.get_logger(__name__)

DEFAULT_BASE_URL = "https://api.polygon.io"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_INTRADAY_TIMESPANS = frozenset({"minute", "hour"})
_MAX_RETRY_WAIT = 30.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _opt_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


class PolygonProvider:
    """Polygon/Massive adapter. Implements the ``MarketDataProvider`` protocol."""

    name = "polygon"

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        client: httpx.AsyncClient | None = None,
        guard: CostRateGuard | None = None,
        timeout: float = 10.0,
        max_attempts: int = 4,
        retry_wait: float = 0.5,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        if not api_key:
            raise ValueError("PolygonProvider requires a non-empty API key.")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._guard: CostRateGuard = guard or NullCostRateGuard()
        self._clock = clock
        self._max_attempts = max_attempts
        self._retry_wait = retry_wait
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        """Close the underlying client if this provider created it."""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> PolygonProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    # --- HTTP core ----------------------------------------------------------
    @staticmethod
    def _is_retryable(exc: BaseException) -> bool:
        if isinstance(exc, httpx.TransportError):
            return True
        return isinstance(exc, ProviderHTTPError) and exc.status_code in _RETRYABLE_STATUS

    def _wait_seconds(self, retry_state: tenacity.RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, ProviderHTTPError) and exc.retry_after is not None:
            return min(exc.retry_after, _MAX_RETRY_WAIT)
        return min(
            self._retry_wait * (2 ** max(retry_state.attempt_number - 1, 0)), _MAX_RETRY_WAIT
        )

    def _retrying(self) -> tenacity.AsyncRetrying:
        return tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(self._max_attempts),
            wait=self._wait_seconds,
            retry=tenacity.retry_if_exception(self._is_retryable),
            reraise=True,
        )

    @staticmethod
    def _retry_after_seconds(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            delay = float(value)
        except ValueError:
            return None
        return max(delay, 0.0)

    async def _request(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async for attempt in self._retrying():
            with attempt:
                await self._guard.acquire(self.name)
                resp = await self._client.get(url, params=params, headers=headers)
                if resp.status_code == 404:
                    raise SymbolNotFoundError(f"Polygon 404 for {url}")
                if resp.status_code >= 400:
                    raise ProviderHTTPError(
                        f"Polygon returned {resp.status_code} for {url}",
                        status_code=resp.status_code,
                        url=str(resp.request.url),
                        retry_after=self._retry_after_seconds(resp.headers.get("Retry-After")),
                    )
                return resp.json()
        raise ProviderHTTPError("retry loop exhausted", url=url)  # pragma: no cover

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request(f"{self._base_url}{path}", params)

    # --- aggregates ---------------------------------------------------------
    async def _fetch_aggs(
        self,
        symbol: str,
        multiplier: int,
        timespan: Timespan,
        start: date,
        end: date,
        *,
        adjusted: bool,
    ) -> list[OHLCVBar]:
        sym = symbol.upper()
        fetched_at = self._clock()
        basis: AdjustmentBasis = "split_dividend_adjusted" if adjusted else "raw"
        path = (
            f"/v2/aggs/ticker/{sym}/range/{multiplier}/{timespan}"
            f"/{start.isoformat()}/{end.isoformat()}"
        )
        payload = await self._get(
            path,
            {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000},
        )
        bars: list[OHLCVBar] = []
        while True:
            for row in payload.get("results") or []:
                bars.append(self._to_bar(sym, multiplier, timespan, row, basis, fetched_at))
            next_url = payload.get("next_url")
            if not next_url:
                break
            payload = await self._request(next_url)
        return bars

    def _to_bar(
        self,
        symbol: str,
        multiplier: int,
        timespan: Timespan,
        row: dict[str, Any],
        basis: AdjustmentBasis,
        fetched_at: datetime,
    ) -> OHLCVBar:
        return OHLCVBar(
            symbol=symbol,
            timestamp=datetime.fromtimestamp(row["t"] / 1000, tz=UTC),
            timespan=timespan,
            multiplier=multiplier,
            open=row["o"],
            high=row["h"],
            low=row["l"],
            close=row["c"],
            volume=row.get("v", 0.0),
            vwap=row.get("vw"),
            trade_count=row.get("n"),
            adjustment_basis=basis,
            source=self.name,
            fetched_at=fetched_at,
        )

    async def get_daily_bars(
        self, symbol: str, start: date, end: date, *, adjusted: bool = False
    ) -> list[OHLCVBar]:
        return await self._fetch_aggs(symbol, 1, "day", start, end, adjusted=adjusted)

    async def get_intraday_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        multiplier: int = 1,
        timespan: Timespan = "minute",
        adjusted: bool = False,
    ) -> list[OHLCVBar]:
        if timespan not in _INTRADAY_TIMESPANS:
            raise ValueError("intraday timespan must be 'minute' or 'hour'")
        return await self._fetch_aggs(symbol, multiplier, timespan, start, end, adjusted=adjusted)

    # --- reference ----------------------------------------------------------
    def _to_security(self, r: dict[str, Any]) -> SecurityRef:
        return SecurityRef(
            symbol=r["ticker"],
            name=r.get("name"),
            primary_exchange=r.get("primary_exchange"),
            security_type=r.get("type"),
            currency=r.get("currency_name"),
            active=r.get("active"),
            cik=r.get("cik"),
            composite_figi=r.get("composite_figi"),
            source=self.name,
            fetched_at=self._clock(),
        )

    async def get_security(self, symbol: str) -> SecurityRef:
        payload = await self._get(f"/v3/reference/tickers/{symbol.upper()}")
        result = payload.get("results")
        if not result:
            raise SymbolNotFoundError(f"No reference data for {symbol.upper()}")
        return self._to_security(result)

    async def search_securities(self, query: str, *, limit: int = 20) -> list[SecurityRef]:
        payload = await self._get(
            "/v3/reference/tickers",
            {"search": query, "active": "true", "limit": limit},
        )
        return [self._to_security(r) for r in payload.get("results") or []]

    # --- corporate actions --------------------------------------------------
    async def get_splits(self, symbol: str) -> list[Split]:
        sym = symbol.upper()
        fetched_at = self._clock()
        payload = await self._get("/v3/reference/splits", {"ticker": sym, "limit": 1000})
        return [
            Split(
                symbol=r.get("ticker", sym),
                execution_date=date.fromisoformat(r["execution_date"]),
                split_from=r["split_from"],
                split_to=r["split_to"],
                source=self.name,
                fetched_at=fetched_at,
            )
            for r in payload.get("results") or []
        ]

    async def get_dividends(self, symbol: str) -> list[Dividend]:
        sym = symbol.upper()
        fetched_at = self._clock()
        payload = await self._get("/v3/reference/dividends", {"ticker": sym, "limit": 1000})
        return [
            Dividend(
                symbol=r.get("ticker", sym),
                ex_dividend_date=date.fromisoformat(r["ex_dividend_date"]),
                cash_amount=r["cash_amount"],
                currency=r.get("currency"),
                pay_date=_opt_date(r.get("pay_date")),
                record_date=_opt_date(r.get("record_date")),
                declaration_date=_opt_date(r.get("declaration_date")),
                frequency=r.get("frequency"),
                dividend_type=r.get("dividend_type"),
                source=self.name,
                fetched_at=fetched_at,
            )
            for r in payload.get("results") or []
        ]
