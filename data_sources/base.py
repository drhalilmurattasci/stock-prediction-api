"""Market-data provider protocol, DTOs, and error types.

Vendor adapters (e.g. ``data_sources/polygon.py``) implement ``MarketDataProvider``
and return these normalized, provenance-stamped DTOs. Per the master plan we store
raw (unadjusted) prices plus a separate corporate-action series and compute adjusted
prices on read — so bars default to ``adjustment_basis="raw"`` and every record
carries its ``source`` and the ``fetched_at`` time it was retrieved.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal, Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

# --- literals ---------------------------------------------------------------
Timespan = Literal["minute", "hour", "day", "week", "month"]
AdjustmentBasis = Literal["raw", "split_adjusted", "split_dividend_adjusted"]
SplitAdjustmentType = Literal["forward_split", "reverse_split", "stock_dividend"]
DividendDistributionType = Literal[
    "recurring",
    "special",
    "supplemental",
    "irregular",
    "unknown",
]


# --- errors -----------------------------------------------------------------
class ProviderError(Exception):
    """Base class for market-data provider failures."""


class ProviderHTTPError(ProviderError):
    """A non-success HTTP response from a provider."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.retry_after = retry_after


class SymbolNotFoundError(ProviderError):
    """The requested symbol/resource does not exist at the provider."""


class VendorRateLimitError(ProviderError):
    """A cost/rate guard rejected the call: too many requests in the window."""


class CostBudgetExceeded(ProviderError):
    """A cost/rate guard rejected the call: the vendor call budget is exhausted."""


# --- DTOs -------------------------------------------------------------------
class _DTO(BaseModel):
    # allow_inf_nan=False: a NaN/Infinity from a vendor payload must be rejected
    # at ingestion. Postgres CHECK constraints alone cannot be trusted for this
    # (NaN compares greater-than-everything there), and a stored non-finite
    # value would 500 every read of its page (the read contract is finite-only).
    model_config = ConfigDict(
        frozen=True, extra="forbid", str_strip_whitespace=True, allow_inf_nan=False
    )


class OHLCVBar(_DTO):
    """One normalized OHLCV bar (raw/unadjusted by default)."""

    symbol: str
    timestamp: AwareDatetime = Field(description="Bar start, timezone-aware (UTC).")
    timespan: Timespan
    multiplier: int = Field(ge=1, description="Bar size = multiplier × timespan.")
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(ge=0)
    vwap: float | None = Field(default=None, ge=0)
    trade_count: int | None = Field(default=None, ge=0)
    adjustment_basis: AdjustmentBasis = "raw"
    source: str
    fetched_at: AwareDatetime

    @model_validator(mode="after")
    def _validate_ohlc(self) -> OHLCVBar:
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= open and close")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= open and close")
        return self


class SecurityRef(_DTO):
    """Reference/metadata for a security."""

    symbol: str
    name: str | None = None
    primary_exchange: str | None = None
    security_type: str | None = None
    currency: str | None = None
    active: bool | None = None
    cik: str | None = None
    composite_figi: str | None = None
    source: str
    fetched_at: AwareDatetime


class Split(_DTO):
    """A stock split (ratio = split_to / split_from)."""

    provider_event_id: str = Field(min_length=1)
    symbol: str
    execution_date: date
    split_from: Decimal = Field(gt=Decimal("0"), strict=True)
    split_to: Decimal = Field(gt=Decimal("0"), strict=True)
    adjustment_type: SplitAdjustmentType
    historical_adjustment_factor: Decimal = Field(gt=Decimal("0"), strict=True)
    source: str = Field(min_length=1)
    fetched_at: AwareDatetime


class Dividend(_DTO):
    """A cash dividend / distribution."""

    provider_event_id: str = Field(min_length=1)
    symbol: str
    ex_dividend_date: date
    cash_amount: Decimal = Field(gt=Decimal("0"), strict=True)
    split_adjusted_cash_amount: Decimal = Field(gt=Decimal("0"), strict=True)
    historical_adjustment_factor: Decimal = Field(gt=Decimal("0"), strict=True)
    currency: str | None = None
    pay_date: date | None = None
    record_date: date | None = None
    declaration_date: date | None = None
    frequency: int | None = Field(default=None, ge=0)
    distribution_type: DividendDistributionType
    source: str = Field(min_length=1)
    fetched_at: AwareDatetime


class CorporateActionPage(_DTO):
    """One complete, bounded provider page plus empty-page provenance."""

    provider_request_id: str = Field(min_length=1)
    provider_origin: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    start: date
    end: date
    source: str = Field(min_length=1)
    fetched_at: AwareDatetime

    @model_validator(mode="after")
    def _validate_window(self) -> CorporateActionPage:
        if self.start > self.end:
            raise ValueError("start must be on or before end")
        return self


class SplitPage(CorporateActionPage):
    """A complete one-page split response for an inclusive date window."""

    action_kind: Literal["splits"] = "splits"
    results: tuple[Split, ...]

    @model_validator(mode="after")
    def _validate_results(self) -> SplitPage:
        for result in self.results:
            if (
                result.symbol != self.symbol
                or result.source != self.source
                or result.fetched_at != self.fetched_at
                or not self.start <= result.execution_date <= self.end
            ):
                raise ValueError("split result does not match its page provenance")
        return self


class DividendPage(CorporateActionPage):
    """A complete one-page dividend response for an inclusive date window."""

    action_kind: Literal["dividends"] = "dividends"
    results: tuple[Dividend, ...]

    @model_validator(mode="after")
    def _validate_results(self) -> DividendPage:
        for result in self.results:
            if (
                result.symbol != self.symbol
                or result.source != self.source
                or result.fetched_at != self.fetched_at
                or not self.start <= result.ex_dividend_date <= self.end
            ):
                raise ValueError("dividend result does not match its page provenance")
        return self


# --- protocols --------------------------------------------------------------
@runtime_checkable
class CostRateGuard(Protocol):
    """Gate a vendor call against rate/cost budgets before it is made.

    Implementations raise ``VendorRateLimitError`` or ``CostBudgetExceeded`` when the
    call would breach a limit; otherwise they record the spend and return.
    """

    async def acquire(self, vendor: str, *, cost: int = 1, endpoint: str | None = None) -> None: ...


@runtime_checkable
class MarketDataProvider(Protocol):
    """Common interface every price/reference vendor adapter implements."""

    name: str

    async def __aenter__(self) -> MarketDataProvider: ...

    async def __aexit__(self, *exc_info: object) -> None: ...

    async def get_daily_bars(
        self, symbol: str, start: date, end: date, *, adjusted: bool = False
    ) -> list[OHLCVBar]: ...

    async def get_intraday_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        multiplier: int = 1,
        timespan: Timespan = "minute",
        adjusted: bool = False,
    ) -> list[OHLCVBar]: ...

    async def get_security(self, symbol: str) -> SecurityRef: ...

    async def search_securities(self, query: str, *, limit: int = 20) -> list[SecurityRef]: ...

    async def get_splits(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> SplitPage: ...

    async def get_dividends(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> DividendPage: ...
