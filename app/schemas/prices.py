"""Validated request and response contracts for current OHLCV prices."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from data_sources.base import AdjustmentBasis, Timespan


class _PriceModel(BaseModel):
    """Strict, immutable base for the public prices contract."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )


class PriceFilters(_PriceModel):
    """Filters selecting one exact stored price series and a half-open time range."""

    start: AwareDatetime | None = Field(
        default=None,
        description="Inclusive UTC bar-start bound.",
    )
    end: AwareDatetime | None = Field(
        default=None,
        description="Exclusive UTC bar-start bound; also used for pagination.",
    )
    timespan: Timespan = Field(default="day", description="Stored bar interval unit.")
    multiplier: int = Field(
        default=1,
        ge=1,
        le=10_000,
        description="Stored bar interval multiplier.",
    )
    source: str = Field(
        default="polygon",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_.-]+$",
        description="Normalized market-data source identity.",
    )
    adjustment_basis: AdjustmentBasis = Field(
        default="raw",
        description="Adjustment basis already stored with the requested series.",
    )
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        return value.lower()

    @field_validator("start", "end")
    @classmethod
    def normalize_bounds_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_time_range(self) -> PriceFilters:
        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("start must be earlier than end")
        return self


class PriceBar(_PriceModel):
    """One current-snapshot OHLCV bar with its observation provenance."""

    timestamp: AwareDatetime = Field(
        description="UTC bar-start time; this is not the bar's completion or availability time."
    )
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(ge=0)
    vwap: float | None = Field(default=None, ge=0)
    trade_count: int | None = Field(default=None, ge=0)
    fetched_at: AwareDatetime = Field(description="UTC time when the source payload was fetched.")
    as_of: AwareDatetime = Field(
        description=(
            "System observation/version time for this current row; not a historical "
            "point-in-time query cutoff."
        )
    )

    @field_validator("timestamp", "fetched_at", "as_of")
    @classmethod
    def normalize_datetimes_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_ohlc(self) -> PriceBar:
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be greater than or equal to open and close")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be less than or equal to open and close")
        return self


class PricePage(_PriceModel):
    """Pagination metadata for descending-keyset reads returned chronologically."""

    limit: int = Field(ge=1)
    has_more: bool
    next_end: AwareDatetime | None = Field(
        default=None,
        description="Exclusive end bound to request the next, older page when has_more is true.",
    )

    @field_validator("next_end")
    @classmethod
    def normalize_next_end_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_next_end(self) -> PricePage:
        if self.has_more != (self.next_end is not None):
            raise ValueError("next_end must be set if and only if has_more is true")
        return self


class PricesResponse(_PriceModel):
    """Current-snapshot response for one homogeneous stored price series."""

    symbol: str = Field(min_length=1, max_length=32)
    source: str = Field(min_length=1, max_length=64)
    timespan: Timespan
    multiplier: int = Field(ge=1)
    adjustment_basis: AdjustmentBasis
    data_as_of: AwareDatetime | None = Field(
        description="Newest system observation time among bars on this response page."
    )
    count: int = Field(ge=0)
    page: PricePage
    bars: list[PriceBar]

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("source")
    @classmethod
    def normalize_response_source(cls, value: str) -> str:
        return value.lower()

    @field_validator("data_as_of")
    @classmethod
    def normalize_data_as_of_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_response(self) -> PricesResponse:
        if self.count != len(self.bars):
            raise ValueError("count must equal the number of bars")
        return self
