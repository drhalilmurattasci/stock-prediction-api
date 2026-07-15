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

import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog
import tenacity

from data_sources.base import (
    AdjustmentBasis,
    CostRateGuard,
    Dividend,
    DividendPage,
    OHLCVBar,
    ProviderError,
    ProviderHTTPError,
    SecurityRef,
    Split,
    SplitPage,
    SymbolNotFoundError,
    Timespan,
)
from data_sources.guards import NullCostRateGuard

log = structlog.get_logger(__name__)

DEFAULT_BASE_URL = "https://api.polygon.io"
DEFAULT_CORPORATE_ACTIONS_BASE_URL = "https://api.massive.com"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_INTRADAY_TIMESPANS = frozenset({"minute", "hour"})
_MAX_RETRY_WAIT = 30.0
_CORPORATE_ACTION_LIMIT = 5000
_SPLITS_ENDPOINT = "/stocks/v1/splits"
_DIVIDENDS_ENDPOINT = "/stocks/v1/dividends"
_CORPORATE_ACTION_SOURCE = "polygon"
_CORPORATE_ACTION_SYMBOL = re.compile(r"^[A-Z0-9.\-_:]{1,32}$")
_CORPORATE_ACTION_ID = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _opt_date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CorporateActionPayloadError("corporate-action optional date is invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise CorporateActionPayloadError("corporate-action optional date is invalid") from None
    if parsed.isoformat() != value:
        raise CorporateActionPayloadError("corporate-action optional date is invalid")
    return parsed


class CorporateActionPayloadError(ProviderError):
    """A corporate-action response cannot prove one complete bounded page."""


class PolygonProvider:
    """Polygon/Massive adapter. Implements the ``MarketDataProvider`` protocol."""

    name = "polygon"

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        corporate_actions_base_url: str = DEFAULT_CORPORATE_ACTIONS_BASE_URL,
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
        corporate_origin = httpx.URL(corporate_actions_base_url)
        if (
            corporate_origin.scheme != "https"
            or not corporate_origin.host
            or corporate_origin.username
            or corporate_origin.password
            or corporate_origin.query
            or corporate_origin.fragment
            or corporate_origin.path not in {"", "/"}
        ):
            raise ValueError("corporate_actions_base_url must be one HTTPS origin")
        port = (
            f":{corporate_origin.port}"
            if corporate_origin.port is not None and corporate_origin.port != 443
            else ""
        )
        self._corporate_actions_base_url = (
            f"{corporate_origin.scheme}://{corporate_origin.host}{port}"
        )
        self._guard: CostRateGuard = guard or NullCostRateGuard()
        self._clock = clock
        self._max_attempts = max_attempts
        self._retry_wait = retry_wait
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout, trust_env=False)

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

    async def _request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        endpoint: str | None = None,
    ) -> dict[str, Any]:
        async for attempt in self._retrying():
            with attempt:
                return await self._request_once(url, params, endpoint=endpoint)
        raise ProviderHTTPError("retry loop exhausted", url=url)  # pragma: no cover

    async def _request_once(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        endpoint: str | None = None,
        decimal_numbers: bool = False,
    ) -> dict[str, Any]:
        """Send exactly one guarded HTTP attempt with no internal retry."""

        headers = {"Authorization": f"Bearer {self._api_key}"}
        await self._guard.acquire(self.name, endpoint=endpoint)
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
        if decimal_numbers:
            return resp.json(parse_float=Decimal, parse_constant=Decimal)
        return resp.json()

    async def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        endpoint: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            f"{self._base_url}{path}",
            params,
            endpoint=endpoint or path,
        )

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
            payload = await self._request(next_url, endpoint=path)
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
    @staticmethod
    def _bounded_symbol(symbol: str, start: date, end: date) -> str:
        sym = symbol.strip().upper()
        if _CORPORATE_ACTION_SYMBOL.fullmatch(sym) is None:
            raise ValueError("symbol must be a canonical bounded market identifier")
        if start > end:
            raise ValueError("start must be on or before end")
        return sym

    async def _corporate_action_page(
        self,
        endpoint: str,
        params: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], str, datetime]:
        raw_payload: object = await self._request_once(
            f"{self._corporate_actions_base_url}{endpoint}",
            params,
            endpoint=endpoint,
            decimal_numbers=True,
        )
        # Availability provenance is sampled only after the complete response
        # has arrived and JSON decoding has succeeded.
        fetched_at = self._clock()
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise CorporateActionPayloadError("corporate-action fetched_at must be timezone-aware")
        if not isinstance(raw_payload, dict):
            raise CorporateActionPayloadError("corporate-action response must be an object")
        payload: dict[str, Any] = raw_payload
        if payload.get("status") != "OK":
            raise CorporateActionPayloadError("corporate-action response status is not OK")
        if "next_url" in payload:
            raise CorporateActionPayloadError(
                "corporate-action response exceeds the authorized one-page scope"
            )
        results = payload.get("results")
        if not isinstance(results, list) or any(not isinstance(row, dict) for row in results):
            raise CorporateActionPayloadError(
                "corporate-action results must be an array of objects"
            )
        if len(results) > _CORPORATE_ACTION_LIMIT:
            raise CorporateActionPayloadError(
                "corporate-action response exceeds the requested page limit"
            )
        raw_request_id = payload.get("request_id")
        if isinstance(raw_request_id, bool) or not isinstance(raw_request_id, (int, str)):
            raise CorporateActionPayloadError("corporate-action request_id is missing or invalid")
        request_id = str(raw_request_id)
        if isinstance(raw_request_id, str) and request_id != request_id.strip():
            raise CorporateActionPayloadError("corporate-action request_id is missing or invalid")
        if _CORPORATE_ACTION_ID.fullmatch(request_id) is None:
            raise CorporateActionPayloadError("corporate-action request_id is missing or invalid")
        return results, request_id, fetched_at

    @staticmethod
    def _event_id(row: dict[str, Any], seen: set[str]) -> str:
        raw_event_id = row.get("id")
        if not isinstance(raw_event_id, str):
            raise CorporateActionPayloadError("corporate-action event id is missing or invalid")
        event_id = raw_event_id
        if event_id != event_id.strip():
            raise CorporateActionPayloadError("corporate-action event id is missing or invalid")
        if _CORPORATE_ACTION_ID.fullmatch(event_id) is None:
            raise CorporateActionPayloadError("corporate-action event id is missing or invalid")
        if event_id in seen:
            raise CorporateActionPayloadError(
                "corporate-action response contains a duplicate event id"
            )
        seen.add(event_id)
        return event_id

    @staticmethod
    def _event_date(
        row: dict[str, Any],
        field: str,
        *,
        start: date,
        end: date,
    ) -> date:
        raw_value = row.get(field)
        if not isinstance(raw_value, str):
            raise CorporateActionPayloadError("corporate-action event date is missing or invalid")
        try:
            value = date.fromisoformat(raw_value)
        except ValueError:
            raise CorporateActionPayloadError(
                "corporate-action event date is missing or invalid"
            ) from None
        if value.isoformat() != raw_value:
            raise CorporateActionPayloadError("corporate-action event date is missing or invalid")
        if not start <= value <= end:
            raise CorporateActionPayloadError(
                "corporate-action event escaped the requested date window"
            )
        return value

    @staticmethod
    def _finite_decimal(row: dict[str, Any], field: str) -> Decimal:
        value = row.get(field)
        if isinstance(value, bool):
            raise CorporateActionPayloadError(
                "corporate-action numeric field is missing or invalid"
            )
        if isinstance(value, int):
            decimal_value = Decimal(value)
        elif isinstance(value, Decimal):
            decimal_value = value
        else:
            raise CorporateActionPayloadError(
                "corporate-action numeric field is missing or invalid"
            )
        if not decimal_value.is_finite():
            raise CorporateActionPayloadError(
                "corporate-action numeric field is missing or invalid"
            )
        return decimal_value

    @staticmethod
    def _optional_nonnegative_int(row: dict[str, Any], field: str) -> int | None:
        value = row.get(field)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CorporateActionPayloadError("corporate-action integer field is invalid")
        return value

    async def get_splits(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> SplitPage:
        sym = self._bounded_symbol(symbol, start, end)
        rows, request_id, fetched_at = await self._corporate_action_page(
            _SPLITS_ENDPOINT,
            {
                "ticker": sym,
                "execution_date.gte": start.isoformat(),
                "execution_date.lte": end.isoformat(),
                "limit": _CORPORATE_ACTION_LIMIT,
                "sort": "execution_date.asc",
            },
        )
        splits: list[Split] = []
        seen: set[str] = set()
        for row in rows:
            if row.get("ticker") != sym:
                raise CorporateActionPayloadError(
                    "corporate-action response ticker does not match request"
                )
            try:
                splits.append(
                    Split(
                        provider_event_id=self._event_id(row, seen),
                        symbol=sym,
                        execution_date=self._event_date(
                            row,
                            "execution_date",
                            start=start,
                            end=end,
                        ),
                        split_from=self._finite_decimal(row, "split_from"),
                        split_to=self._finite_decimal(row, "split_to"),
                        adjustment_type=row["adjustment_type"],
                        historical_adjustment_factor=self._finite_decimal(
                            row, "historical_adjustment_factor"
                        ),
                        source=_CORPORATE_ACTION_SOURCE,
                        fetched_at=fetched_at,
                    )
                )
            except CorporateActionPayloadError:
                raise
            except (KeyError, TypeError, ValueError) as exc:
                raise CorporateActionPayloadError("corporate-action split is invalid") from exc
        return SplitPage(
            provider_request_id=request_id,
            provider_origin=self._corporate_actions_base_url,
            endpoint=_SPLITS_ENDPOINT,
            symbol=sym,
            start=start,
            end=end,
            source=_CORPORATE_ACTION_SOURCE,
            fetched_at=fetched_at,
            results=tuple(
                sorted(splits, key=lambda value: (value.execution_date, value.provider_event_id))
            ),
        )

    async def get_dividends(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> DividendPage:
        sym = self._bounded_symbol(symbol, start, end)
        rows, request_id, fetched_at = await self._corporate_action_page(
            _DIVIDENDS_ENDPOINT,
            {
                "ticker": sym,
                "ex_dividend_date.gte": start.isoformat(),
                "ex_dividend_date.lte": end.isoformat(),
                "limit": _CORPORATE_ACTION_LIMIT,
                "sort": "ex_dividend_date.asc",
            },
        )
        dividends: list[Dividend] = []
        seen: set[str] = set()
        for row in rows:
            if row.get("ticker") != sym:
                raise CorporateActionPayloadError(
                    "corporate-action response ticker does not match request"
                )
            try:
                dividends.append(
                    Dividend(
                        provider_event_id=self._event_id(row, seen),
                        symbol=sym,
                        ex_dividend_date=self._event_date(
                            row,
                            "ex_dividend_date",
                            start=start,
                            end=end,
                        ),
                        cash_amount=self._finite_decimal(row, "cash_amount"),
                        split_adjusted_cash_amount=self._finite_decimal(
                            row, "split_adjusted_cash_amount"
                        ),
                        historical_adjustment_factor=self._finite_decimal(
                            row, "historical_adjustment_factor"
                        ),
                        currency=row.get("currency"),
                        pay_date=_opt_date(row.get("pay_date")),
                        record_date=_opt_date(row.get("record_date")),
                        declaration_date=_opt_date(row.get("declaration_date")),
                        frequency=self._optional_nonnegative_int(row, "frequency"),
                        distribution_type=row["distribution_type"],
                        source=_CORPORATE_ACTION_SOURCE,
                        fetched_at=fetched_at,
                    )
                )
            except CorporateActionPayloadError:
                raise
            except (KeyError, TypeError, ValueError) as exc:
                raise CorporateActionPayloadError("corporate-action dividend is invalid") from exc
        return DividendPage(
            provider_request_id=request_id,
            provider_origin=self._corporate_actions_base_url,
            endpoint=_DIVIDENDS_ENDPOINT,
            symbol=sym,
            start=start,
            end=end,
            source=_CORPORATE_ACTION_SOURCE,
            fetched_at=fetched_at,
            results=tuple(
                sorted(
                    dividends,
                    key=lambda value: (value.ex_dividend_date, value.provider_event_id),
                )
            ),
        )
