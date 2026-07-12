"""Pydantic contract for probabilistic forecasts.

The forecast API is intentionally schema-first: model code can change, but the
public payload must keep point forecasts, intervals, calibration evidence, and
data-lineage provenance stable from the first implemented version.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.schemas.common import DISCLAIMER


def _canonical_coverage(value: float) -> float:
    rounded = round(value, 3)
    if abs(value - rounded) > 1e-12:
        raise ValueError("coverage supports at most three decimal places")
    return rounded


Coverage = Annotated[
    float,
    Field(
        ge=0.001,
        le=0.999,
        multiple_of=0.001,
        description="Nominal coverage, expressed to at most three decimal places.",
    ),
    AfterValidator(_canonical_coverage),
]
Probability = Annotated[float, Field(ge=0.0, le=1.0)]
QuantileLevel = Annotated[float, Field(gt=0.0, lt=1.0)]
FLOAT_TOLERANCE = 1e-9

ForecastTarget = Literal["close", "adjusted_close", "return", "log_return"]
ForecastSeriesBasis = Literal["raw", "split_adjusted", "split_dividend_adjusted"]
ForecastHorizonUnit = Literal["trading_day", "calendar_day", "minute", "hour", "week"]
ForecastModelSelector = Literal[
    "auto",
    "baseline_naive",
    "baseline_drift",
    "baseline_seasonal_naive",
    "arima",
    "chronos",
]
LookaheadStatus = Literal["passed", "failed", "not_run"]
CalibrationMethod = Literal[
    "conformal_quantile_regression",
    "adaptive_conformal",
    "empirical_residual",
    "none",
]


class ForecastBaseModel(BaseModel):
    """Strict base model for the public forecast contract."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )


class ForecastRequest(ForecastBaseModel):
    """Request shape shared by POST and documented GET query parameters."""

    symbol: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[A-Za-z0-9.\-_:]+$",
        description="Canonical symbol accepted by the API, e.g. AAPL.",
        examples=["AAPL"],
    )
    horizon: int = Field(
        default=5,
        ge=1,
        le=252,
        description="Number of forecast steps to return.",
    )
    horizon_unit: ForecastHorizonUnit = Field(
        default="trading_day",
        description="Unit for each forecast horizon step.",
    )
    target: ForecastTarget = Field(
        default="adjusted_close",
        description=(
            "Forecast target. Price targets use the response currency; return targets are unitless."
        ),
    )
    as_of: AwareDatetime | None = Field(
        default=None,
        description="Point-in-time cutoff. If omitted, the newest sealed data snapshot is used.",
    )
    snapshot_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Pinned immutable data snapshot. Overrides as_of when provided.",
    )
    model: ForecastModelSelector = Field(
        default="auto",
        description=(
            "Model selector. 'auto' routes to the promoted champion for the requested target."
        ),
    )
    interval_coverages: list[Coverage] = Field(
        default_factory=lambda: [0.5, 0.8, 0.95],
        min_length=1,
        max_length=9,
        description=(
            "Requested central prediction interval coverages, e.g. 0.8 for an 80% interval."
        ),
    )

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("interval_coverages")
    @classmethod
    def sort_unique_coverages(cls, value: list[float]) -> list[float]:
        ordered = sorted(value)
        if len(set(ordered)) != len(ordered):
            raise ValueError("interval_coverages must not contain duplicates")
        return ordered


class ForecastQuantile(ForecastBaseModel):
    """A single quantile forecast for a target timestamp."""

    level: QuantileLevel = Field(description="Quantile level, e.g. 0.1 or 0.9.")
    value: float = Field(description="Forecast value at this quantile.")


class ForecastInterval(ForecastBaseModel):
    """A central prediction interval for one target timestamp."""

    coverage: Coverage = Field(description="Nominal interval coverage, e.g. 0.8 for 80%.")
    lower_quantile: QuantileLevel = Field(description="Lower quantile level.")
    upper_quantile: QuantileLevel = Field(description="Upper quantile level.")
    lower: float = Field(description="Lower interval bound.")
    upper: float = Field(description="Upper interval bound.")

    @model_validator(mode="after")
    def validate_ordering(self) -> ForecastInterval:
        if self.upper_quantile <= self.lower_quantile:
            raise ValueError("upper_quantile must be greater than lower_quantile")
        if abs((self.upper_quantile - self.lower_quantile) - self.coverage) > FLOAT_TOLERANCE:
            raise ValueError("coverage must equal upper_quantile minus lower_quantile")
        if self.upper < self.lower:
            raise ValueError("upper must be greater than or equal to lower")
        return self


class ForecastStep(ForecastBaseModel):
    """Forecast values for one future timestamp."""

    step: int = Field(ge=1, description="One-indexed horizon step.")
    target_time: AwareDatetime = Field(description="Timestamp being forecast.")
    point: float = Field(description="Central point forecast, normally the predictive median.")
    quantiles: list[ForecastQuantile] = Field(
        min_length=1,
        description="Machine-readable quantile forecasts used to construct intervals.",
    )
    intervals: list[ForecastInterval] = Field(
        min_length=1,
        description="Central prediction intervals requested by the client.",
    )

    @field_validator("quantiles")
    @classmethod
    def quantiles_are_unique(cls, value: list[ForecastQuantile]) -> list[ForecastQuantile]:
        levels = [item.level for item in value]
        if len(set(levels)) != len(levels):
            raise ValueError("quantile levels must be unique per forecast step")
        return sorted(value, key=lambda item: item.level)

    @model_validator(mode="after")
    def validate_distribution(self) -> ForecastStep:
        for lower, upper in zip(self.quantiles, self.quantiles[1:], strict=False):
            if upper.value < lower.value:
                raise ValueError("quantile values must be nondecreasing by level")

        median = next(
            (item for item in self.quantiles if abs(item.level - 0.5) <= FLOAT_TOLERANCE),
            None,
        )
        if median is None or abs(median.value - self.point) > FLOAT_TOLERANCE:
            raise ValueError("point must equal the 0.5 quantile")

        coverages = [interval.coverage for interval in self.intervals]
        if len(set(coverages)) != len(coverages):
            raise ValueError("interval coverages must be unique per forecast step")
        for interval in self.intervals:
            lower_value = self._quantile_value(interval.lower_quantile)
            upper_value = self._quantile_value(interval.upper_quantile)
            if lower_value is None or upper_value is None:
                raise ValueError("interval bounds must reference included quantile levels")
            if (
                abs(interval.lower - lower_value) > FLOAT_TOLERANCE
                or abs(interval.upper - upper_value) > FLOAT_TOLERANCE
            ):
                raise ValueError("interval bounds must equal their referenced quantile values")
        return self

    def _quantile_value(self, level: float) -> float | None:
        match = next(
            (item for item in self.quantiles if abs(item.level - level) <= FLOAT_TOLERANCE),
            None,
        )
        return match.value if match is not None else None


class LookaheadCheck(ForecastBaseModel):
    """Mechanical proof that no feature used data newer than the forecast cutoff."""

    status: LookaheadStatus = Field(description="Whether the point-in-time leakage check passed.")
    checked_at: AwareDatetime = Field(description="When the leakage check was evaluated.")
    max_feature_available_at: AwareDatetime = Field(
        description="Newest feature availability timestamp included in the forecast input."
    )
    violations: list[str] = Field(
        default_factory=list,
        description="Feature names or rules that failed the check. Empty when status is passed.",
    )

    @model_validator(mode="after")
    def passed_has_no_violations(self) -> LookaheadCheck:
        if self.status == "passed" and self.violations:
            raise ValueError("passed lookahead_check cannot include violations")
        return self


class DataSourceLineage(ForecastBaseModel):
    """Data-source contribution used to produce a forecast."""

    name: str = Field(min_length=1, max_length=64, description="Source adapter name.")
    snapshot_id: str = Field(min_length=1, max_length=128, description="Source snapshot ID.")
    max_available_at: AwareDatetime = Field(
        description="Newest source record availability timestamp."
    )
    fields: list[str] = Field(
        default_factory=list,
        description="Derived feature groups used from this source, never raw vendor field dumps.",
    )


class ForecastProvenance(ForecastBaseModel):
    """Reproducibility and data-lineage fields for a forecast response."""

    forecast_id: UUID = Field(description="Stable identifier for this exact forecast payload.")
    snapshot_id: str = Field(
        min_length=1,
        max_length=128,
        description="Immutable data snapshot used by the forecast.",
    )
    model_version: str = Field(
        min_length=1,
        max_length=128,
        description="Versioned model identity, including router/champion version when model=auto.",
    )
    series_basis: ForecastSeriesBasis = Field(
        description="Exact raw or adjusted target-series convention used by the model."
    )
    feature_set_hash: str = Field(
        min_length=64,
        max_length=71,
        pattern=r"^(sha256:)?[A-Fa-f0-9]{64}$",
        description="SHA-256 hash of the resolved feature-set definition.",
    )
    max_available_at: AwareDatetime = Field(
        description="Newest data availability timestamp used across every feature."
    )
    generated_at: AwareDatetime = Field(
        description="Server timestamp when the forecast was generated."
    )
    code_version: str | None = Field(
        default=None,
        max_length=64,
        description="Git commit or build identifier, when available.",
    )
    data_sources: list[DataSourceLineage] = Field(
        default_factory=list,
        description="Derived-data lineage for reproducibility and vendor-audit boundaries.",
    )
    lookahead_check: LookaheadCheck = Field(
        description="Mechanical leakage check proving point-in-time correctness."
    )


class IntervalCalibration(ForecastBaseModel):
    """Empirical coverage for one nominal interval/horizon bucket."""

    horizon: int = Field(ge=1, description="Horizon bucket this calibration row describes.")
    nominal_coverage: Coverage = Field(description="Expected interval coverage.")
    empirical_coverage: Probability | None = Field(
        default=None,
        description="Observed realized coverage over the calibration window.",
    )
    sample_count: int = Field(ge=0, description="Number of realized forecasts in the window.")
    confidence_low: Probability | None = Field(
        default=None,
        description="Lower confidence bound for empirical coverage, when enough samples exist.",
    )
    confidence_high: Probability | None = Field(
        default=None,
        description="Upper confidence bound for empirical coverage, when enough samples exist.",
    )

    @model_validator(mode="after")
    def validate_confidence_band(self) -> IntervalCalibration:
        if self.empirical_coverage is None:
            raise ValueError("calibration evidence rows require empirical_coverage")
        if (self.confidence_low is None) != (self.confidence_high is None):
            raise ValueError("confidence_low and confidence_high must be supplied together")
        if (
            self.confidence_low is not None
            and self.confidence_high is not None
            and self.confidence_high < self.confidence_low
        ):
            raise ValueError("confidence_high must be greater than or equal to confidence_low")
        if (
            self.confidence_low is not None
            and self.confidence_high is not None
            and not self.confidence_low <= self.empirical_coverage <= self.confidence_high
        ):
            raise ValueError("confidence bounds must contain empirical_coverage")
        return self


class ForecastCalibration(ForecastBaseModel):
    """Calibration metadata attached to every forecast response."""

    calibration_set_version: str = Field(
        min_length=1,
        max_length=128,
        description="Versioned calibration residual/coverage set used for this payload.",
    )
    method: CalibrationMethod = Field(description="Interval calibration method.")
    window_start: date | None = Field(
        default=None,
        description="First realized forecast date included in the calibration window.",
    )
    window_end: date | None = Field(
        default=None,
        description="Last realized forecast date included in the calibration window.",
    )
    sample_count: int = Field(ge=0, description="Total realized forecasts used for calibration.")
    by_interval: list[IntervalCalibration] = Field(
        default_factory=list,
        description="Coverage evidence by nominal interval and horizon.",
    )

    @model_validator(mode="after")
    def validate_window(self) -> ForecastCalibration:
        if self.window_start and self.window_end and self.window_end < self.window_start:
            raise ValueError("window_end must be on or after window_start")
        if self.method == "none" and (
            self.window_start is not None
            or self.window_end is not None
            or self.sample_count != 0
            or self.by_interval
        ):
            raise ValueError("uncalibrated forecasts cannot claim calibration evidence")
        if self.method != "none" and (
            self.window_start is None
            or self.window_end is None
            or self.sample_count <= 0
            or not self.by_interval
            or any(
                row.sample_count <= 0 or row.sample_count > self.sample_count
                for row in self.by_interval
            )
        ):
            raise ValueError("calibrated forecasts require coherent nonzero evidence")
        buckets = [(row.horizon, row.nominal_coverage) for row in self.by_interval]
        if len(set(buckets)) != len(buckets):
            raise ValueError("calibration horizon/coverage buckets must be unique")
        return self


class ForecastResponse(ForecastBaseModel):
    """Stable `/v1/forecast` response contract."""

    symbol: str = Field(description="Canonical symbol forecasted by the API.")
    target: ForecastTarget = Field(description="Forecast target.")
    horizon: int = Field(ge=1, description="Number of forecast steps returned.")
    horizon_unit: ForecastHorizonUnit = Field(description="Unit for each forecast horizon step.")
    as_of: AwareDatetime = Field(
        description="Point-in-time cutoff used to resolve the data snapshot."
    )
    currency: str | None = Field(
        default="USD",
        pattern=r"^[A-Z]{3}$",
        description="ISO 4217 currency for price targets; null for unitless return targets.",
    )
    forecasts: list[ForecastStep] = Field(
        min_length=1,
        description=(
            "Ordered forecast path. Multi-step forecasts include one item per horizon step."
        ),
    )
    provenance: ForecastProvenance = Field(
        description=(
            "Forecast reproducibility, snapshot, model, feature, and leakage-check metadata."
        )
    )
    calibration: ForecastCalibration = Field(
        description="Calibration version and empirical interval coverage evidence."
    )
    disclaimer: str = Field(
        default=DISCLAIMER,
        description="Mandatory not-investment-advice disclaimer.",
    )

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_response_invariants(self) -> ForecastResponse:
        if self.horizon != len(self.forecasts):
            raise ValueError("horizon must match the number of forecast steps")
        if [item.step for item in self.forecasts] != list(range(1, self.horizon + 1)):
            raise ValueError("forecast steps must be contiguous and one-indexed")
        target_times = [item.target_time for item in self.forecasts]
        if any(target_time <= self.as_of for target_time in target_times):
            raise ValueError("forecast target times must be later than as_of")
        if any(
            current >= following
            for current, following in zip(target_times, target_times[1:], strict=False)
        ):
            raise ValueError("forecast target times must be strictly increasing")
        if self.provenance.max_available_at > self.as_of:
            raise ValueError("provenance max_available_at must not be later than as_of")
        if self.provenance.generated_at < self.as_of:
            raise ValueError("provenance generated_at must not be earlier than as_of")
        if any(
            source.max_available_at > self.provenance.max_available_at
            for source in self.provenance.data_sources
        ):
            raise ValueError("data-source availability must not exceed provenance cutoff")
        if (
            self.provenance.lookahead_check.max_feature_available_at
            != self.provenance.max_available_at
        ):
            raise ValueError("lookahead and provenance availability cutoffs must match")
        if self.target == "close" and self.provenance.series_basis != "raw":
            raise ValueError("close forecasts require raw series provenance")
        if self.target == "adjusted_close" and self.provenance.series_basis == "raw":
            raise ValueError("adjusted_close forecasts require adjusted series provenance")
        if self.calibration.method != "none":
            emitted_buckets = {
                (step.step, interval.coverage)
                for step in self.forecasts
                for interval in step.intervals
            }
            calibration_buckets = {
                (row.horizon, row.nominal_coverage) for row in self.calibration.by_interval
            }
            if calibration_buckets != emitted_buckets:
                raise ValueError("calibration evidence must match every emitted interval bucket")
        if self.target in {"return", "log_return"} and self.currency is not None:
            raise ValueError("currency must be null for return targets")
        if self.target in {"close", "adjusted_close"} and self.currency is None:
            raise ValueError("currency is required for price targets")
        return self
