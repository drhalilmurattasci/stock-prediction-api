"""Strict public contracts for stored-series technical indicators."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class _IndicatorModel(BaseModel):
    """Strict, immutable, finite-only base for the public contract."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )


class IndicatorFilters(_IndicatorModel):
    """Select a fixed window within the one supported v1 stored series."""

    end: AwareDatetime | None = Field(
        default=None,
        description=(
            "Exclusive UTC stored-observation timestamp. This is not an availability as-of "
            "bound and does not reconstruct historical database state."
        ),
    )

    @field_validator("end")
    @classmethod
    def normalize_end_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None


class IndicatorParameters(_IndicatorModel):
    """Disclosed, server-owned formula parameters; clients cannot override them in v1."""

    return_period: int = Field(ge=1)
    sma_period: int = Field(ge=1)
    ema_period: int = Field(ge=1)
    volatility_period: int = Field(ge=2)
    rsi_period: int = Field(ge=1)
    macd_fast_period: int = Field(ge=1)
    macd_slow_period: int = Field(ge=1)
    macd_signal_period: int = Field(ge=1)
    bollinger_period: int = Field(ge=1)
    bollinger_standard_deviations: float = Field(gt=0)
    atr_period: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_macd_periods(self) -> IndicatorParameters:
        if self.macd_fast_period >= self.macd_slow_period:
            raise ValueError("macd_fast_period must be less than macd_slow_period")
        return self


class IndicatorWindow(_IndicatorModel):
    """Exact bounded-input selection and provenance for this calculation."""

    selection: Literal["newest_exact_series_before_exclusive_end"]
    calendar: Literal["XNYS"]
    calendar_ruleset: str = Field(
        min_length=1,
        description=(
            "Pinned exchange-calendars, pandas, tzdata, and schedule-range identity used "
            "to validate exact regular-session closes."
        ),
    )
    max_observations: int = Field(ge=1)
    required_observations: int = Field(ge=1)
    requested_end: AwareDatetime | None
    input_start: AwareDatetime | None
    input_end: AwareDatetime | None
    input_count: int = Field(ge=0)
    older_data_excluded: bool
    continuity: Literal["exact_consecutive_regular_session_closes"] | None
    latest_session_completeness: Literal["not_evaluated"]
    recursive_seed_semantics: Literal["window_relative"]
    warmup_semantics: Literal["structural_nulls"]
    input_digest_schema: Literal["ordered-current-bar-ieee754-hex-v1"] = Field(
        description=(
            "SHA-256 document schema: compact UTF-8 JSON with sorted keys; top-level "
            "schema/series/rows fields; series keys adjustment_basis, multiplier, source, "
            "symbol, and timespan; chronological row keys as_of, close, fetched_at, high, "
            "low, open, recorded_at, timestamp, trade_count, volume, and vwap; every float "
            "encoded with float.hex(); and UTC timestamps encoded with six fractional "
            "digits plus Z."
        )
    )
    input_sha256: str | None = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("requested_end", "input_start", "input_end")
    @classmethod
    def normalize_datetimes_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_window(self) -> IndicatorWindow:
        is_empty = self.input_count == 0
        if is_empty != (self.input_start is None and self.input_end is None):
            raise ValueError("input bounds must be absent if and only if the window is empty")
        if is_empty != (self.input_sha256 is None):
            raise ValueError("input_sha256 must be absent if and only if the window is empty")
        if self.input_count > self.max_observations:
            raise ValueError("input_count cannot exceed max_observations")
        if 0 < self.input_count < self.required_observations:
            raise ValueError("successful nonempty windows must satisfy required_observations")
        if is_empty != (self.continuity is None):
            raise ValueError("continuity must be absent if and only if the window is empty")
        if self.input_start is not None and self.input_end is not None:
            if self.input_start > self.input_end:
                raise ValueError("input_start must not be later than input_end")
            if self.requested_end is not None and self.input_end >= self.requested_end:
                raise ValueError("input_end must be earlier than requested_end")
        return self


class IndicatorObservation(_IndicatorModel):
    """One aligned source bar and its causal technical-indicator outputs.

    OHLCV is repeated deliberately so clients can audit input-digest and
    warm-up alignment without joining a representation that may have changed.
    """

    timestamp: AwareDatetime
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    vwap: float | None = Field(default=None, ge=0)
    trade_count: int | None = Field(default=None, ge=0)
    fetched_at: AwareDatetime
    as_of: AwareDatetime
    recorded_at: AwareDatetime
    simple_return: float | None
    log_return: float | None
    sma: float | None
    ema: float | None
    return_volatility: float | None
    rsi: float | None
    macd_line: float | None
    macd_signal: float | None
    macd_histogram: float | None
    bollinger_lower: float | None
    bollinger_middle: float | None
    bollinger_upper: float | None
    atr: float | None

    @field_validator("timestamp", "fetched_at", "as_of", "recorded_at")
    @classmethod
    def normalize_datetimes_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_source_bar(self) -> IndicatorObservation:
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("open and close must lie within the high-low range")
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        if not self.timestamp <= self.fetched_at <= self.as_of <= self.recorded_at:
            raise ValueError("source provenance timestamps must be nondecreasing")
        return self


class IndicatorsResponse(_IndicatorModel):
    """Causal technical indicators over one disclosed, fixed stored-data window."""

    symbol: str = Field(min_length=1, max_length=32)
    source: Literal["polygon_open_close"]
    timespan: Literal["day"]
    multiplier: Literal[1]
    adjustment_basis: Literal["raw"]
    data_semantics: Literal["current_snapshot_not_point_in_time"]
    endpoint_version: Literal["stored-indicators-v1"]
    calculation_version: str = Field(min_length=1)
    indicator_policy_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    window_policy_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    parameters: IndicatorParameters
    window: IndicatorWindow
    data_as_of: AwareDatetime | None
    data_recorded_at: AwareDatetime | None
    count: int = Field(ge=0)
    observations: list[IndicatorObservation]

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("data_as_of", "data_recorded_at")
    @classmethod
    def normalize_freshness_to_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def validate_alignment(self) -> IndicatorsResponse:
        if self.count != len(self.observations) or self.count != self.window.input_count:
            raise ValueError("count must equal the aligned observation and input counts")
        timestamps = [row.timestamp for row in self.observations]
        if any(
            current <= previous
            for previous, current in zip(timestamps, timestamps[1:], strict=False)
        ):
            raise ValueError("indicator observations must be strictly chronological")
        if timestamps:
            if timestamps[0] != self.window.input_start or timestamps[-1] != self.window.input_end:
                raise ValueError("window bounds must match the aligned observations")
            if self.data_as_of != max(row.as_of for row in self.observations):
                raise ValueError("data_as_of must cover every input observation")
            if self.data_recorded_at != max(row.recorded_at for row in self.observations):
                raise ValueError("data_recorded_at must cover every input observation")
        elif self.data_as_of is not None or self.data_recorded_at is not None:
            raise ValueError("empty responses cannot claim source freshness")
        return self
