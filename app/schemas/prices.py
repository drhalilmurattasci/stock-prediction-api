"""Validated request and response contracts for current OHLCV prices."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

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


class AdjustedPriceFilters(_PriceModel):
    """Bounds over one explicitly pinned immutable adjustment-factor set."""

    factor_set_id: str = Field(
        min_length=71,
        max_length=71,
        pattern=r"^sha256:[0-9a-f]{64}$",
        description=(
            "Exact immutable factor-set content identity. There is deliberately no "
            "mutable latest-factor-set resolution."
        ),
    )
    start: AwareDatetime | None = Field(
        default=None,
        description="Inclusive UTC raw-bar observation bound within the factor window.",
    )
    end: AwareDatetime | None = Field(
        default=None,
        description=(
            "Exclusive UTC raw-bar observation bound within the factor window; also used "
            "for pagination."
        ),
    )
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("start", "end")
    @classmethod
    def normalize_bounds_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_time_range(self) -> AdjustedPriceFilters:
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
            "Conservative data-availability cutoff for this current row; not a historical "
            "point-in-time query parameter."
        )
    )
    recorded_at: AwareDatetime = Field(
        description=(
            "Database write-acceptance time for this exact current-row version; "
            "transaction commit and visibility may be later."
        )
    )

    @field_validator("timestamp", "fetched_at", "as_of", "recorded_at")
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
        if not self.timestamp <= self.fetched_at <= self.as_of <= self.recorded_at:
            raise ValueError("timestamp, fetched_at, as_of, and recorded_at must be nondecreasing")
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
        description="Newest data-availability cutoff among bars on this response page."
    )
    data_recorded_at: AwareDatetime | None = Field(
        description=(
            "Newest database write-acceptance time among bars on this response page; "
            "transaction commit and visibility may be later."
        )
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

    @field_validator("data_as_of", "data_recorded_at")
    @classmethod
    def normalize_data_as_of_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_response(self) -> PricesResponse:
        if self.count != len(self.bars):
            raise ValueError("count must equal the number of bars")
        return self


class AdjustedPriceBar(_PriceModel):
    """One derived OHLCV row tied to one exact raw version and factor ordinal."""

    raw_input_ordinal: int = Field(ge=0)
    timestamp: AwareDatetime
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(ge=0)
    vwap: float | None = Field(default=None, ge=0)
    trade_count: int | None = Field(default=None, ge=0)
    raw_version_recorded_at: AwareDatetime
    raw_available_at: AwareDatetime
    available_at: AwareDatetime = Field(
        description="DB-stamped factor-set publication time for this derived row."
    )
    price_factor_f64_be: str = Field(pattern=r"^[0-9a-f]{16}$")
    volume_factor_f64_be: str = Field(pattern=r"^[0-9a-f]{16}$")

    @field_validator(
        "timestamp",
        "raw_version_recorded_at",
        "raw_available_at",
        "available_at",
    )
    @classmethod
    def normalize_datetimes_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_row(self) -> AdjustedPriceBar:
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be greater than or equal to open and close")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be less than or equal to open and close")
        if not (
            self.timestamp
            <= self.raw_version_recorded_at
            <= self.raw_available_at
            <= self.available_at
        ):
            raise ValueError("raw and derived availability times must be nondecreasing")
        return self


class AdjustedPriceLineage(_PriceModel):
    """Complete immutable provenance for an adjusted response page."""

    factor_set_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    factor_set_recorded_at: AwareDatetime
    factor_set_available_at: AwareDatetime
    policy_version: str = Field(min_length=1, max_length=64)
    policy_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    cutoff: AwareDatetime
    anchor_date: date
    raw_coverage_start: AwareDatetime
    raw_coverage_end: AwareDatetime
    split_collection_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    split_collection_recorded_at: AwareDatetime
    split_collection_available_at: AwareDatetime
    dividend_collection_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    dividend_collection_recorded_at: AwareDatetime
    dividend_collection_available_at: AwareDatetime
    action_version_ids: tuple[str, ...]
    max_input_available_at: AwareDatetime
    data_available_at: AwareDatetime
    raw_input_count: int = Field(ge=1, le=5000)
    adjustment_basis: AdjustmentBasis

    @field_validator("action_version_ids")
    @classmethod
    def validate_action_version_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        pattern = r"^sha256:[0-9a-f]{64}$"
        if value != tuple(sorted(set(value))) or any(
            re.fullmatch(pattern, item) is None for item in value
        ):
            raise ValueError("action_version_ids must be unique sorted content identities")
        return value

    @field_validator(
        "factor_set_recorded_at",
        "factor_set_available_at",
        "cutoff",
        "raw_coverage_start",
        "raw_coverage_end",
        "split_collection_recorded_at",
        "split_collection_available_at",
        "dividend_collection_recorded_at",
        "dividend_collection_available_at",
        "max_input_available_at",
        "data_available_at",
    )
    @classmethod
    def normalize_datetimes_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_lineage(self) -> AdjustedPriceLineage:
        if self.adjustment_basis != "split_dividend_adjusted":
            raise ValueError("adjusted lineage must use split_dividend_adjusted")
        if self.raw_coverage_start > self.raw_coverage_end:
            raise ValueError("raw coverage bounds are reversed")
        if self.raw_coverage_end.date() != self.anchor_date:
            raise ValueError("raw coverage end must be the factor anchor")
        if not self.factor_set_recorded_at <= self.factor_set_available_at:
            raise ValueError("factor-set availability precedes its recording")
        if self.data_available_at != self.factor_set_available_at:
            raise ValueError("data availability must be the factor-set receipt time")
        return self


class AdjustedPricesResponse(_PriceModel):
    """Adjusted OHLCV from one fully validated immutable factor window."""

    symbol: str = Field(min_length=1, max_length=32)
    source: str = Field(min_length=1, max_length=64)
    timespan: Timespan
    multiplier: int = Field(ge=1)
    adjustment_basis: AdjustmentBasis
    factor_set_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    data_available_at: AwareDatetime
    count: int = Field(ge=0)
    page: PricePage
    lineage: AdjustedPriceLineage
    bars: list[AdjustedPriceBar]

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        return value.lower()

    @field_validator("data_available_at")
    @classmethod
    def normalize_data_available_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_response(self) -> AdjustedPricesResponse:
        if self.count != len(self.bars):
            raise ValueError("count must equal the number of bars")
        if self.factor_set_id != self.lineage.factor_set_id:
            raise ValueError("response and lineage factor-set identities differ")
        if self.data_available_at != self.lineage.data_available_at:
            raise ValueError("response and lineage availability times differ")
        if self.adjustment_basis != "split_dividend_adjusted":
            raise ValueError("adjusted response must use split_dividend_adjusted")
        if self.source != "polygon_open_close" or self.timespan != "day" or self.multiplier != 1:
            raise ValueError("adjusted response must use the exact polygon_open_close day/1 series")
        return self
