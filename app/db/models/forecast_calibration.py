"""Immutable fitted-calibration and descriptive held-out evidence."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastFittedCalibrationSet(Base):
    """One content-addressed fitted conformal calibration artifact."""

    __tablename__ = "forecast_fitted_calibration_sets"
    __table_args__ = (
        CheckConstraint("schema_version = 2", name="schema_version_supported"),
        CheckConstraint(
            "calibration_set_version ~ '^sha256:[0-9a-f]{64}$'",
            name="calibration_set_version_format",
        ),
        CheckConstraint(
            "forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_evidence_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="hashes_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint("symbol ~ '^[A-Z0-9.\\-_:]+$'", name="symbol_format"),
        CheckConstraint(
            "target = 'close' AND series_basis = 'raw' "
            "AND horizon_unit = 'trading_day' AND currency = 'USD'",
            name="semantic_scope_supported",
        ),
        CheckConstraint(
            "method IN ('empirical_residual', 'conformal_quantile_regression')",
            name="method_supported",
        ),
        CheckConstraint(
            "source_calibration_method = 'none' "
            "AND source_calibration_set_version = 'uncalibrated:' || model_version",
            name="source_uncalibrated",
        ),
        CheckConstraint(
            "interval_policy_version = 'central-equal-tailed-v1' "
            "AND window_date_policy_version = 'utc-target-date-v1'",
            name="policies_supported",
        ),
        CheckConstraint("window_start <= window_end", name="window_order"),
        CheckConstraint("sample_count BETWEEN 1 AND 10000", name="sample_count_bounded"),
        CheckConstraint("bucket_count BETWEEN 1 AND 10000", name="bucket_count_bounded"),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        CheckConstraint(
            "octet_length(canonical_set) BETWEEN 1 AND 4194304",
            name="canonical_set_size_bounded",
        ),
        CheckConstraint(
            "calibration_set_version = 'sha256:' || encode(digest(canonical_set, 'sha256'), 'hex')",
            name="calibration_set_version_matches_payload",
        ),
        ForeignKeyConstraint(
            ("outcome_resolution_policy_hash", "outcome_availability_rule_set_hash"),
            (
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ),
            name="fk_fitted_calibration_sets_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "cohort_id",
            "method",
            name="uq_fitted_calibration_sets_cohort_method",
        ),
        Index("ix_forecast_fitted_calibration_sets_cohort_id", "cohort_id"),
    )

    calibration_set_version: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    series_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    horizon_unit: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(16), nullable=False)
    source_calibration_set_version: Mapped[str] = mapped_column(String(128), nullable=False)
    source_calibration_method: Mapped[str] = mapped_column(String(32), nullable=False)
    forecast_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    forecast_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    fit_evidence_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    window_start: Mapped[date] = mapped_column(Date, nullable=False)
    window_end: Mapped[date] = mapped_column(Date, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    cohort_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey("forecast_outcome_cohort_availability.cohort_id", ondelete="RESTRICT"),
        nullable=False,
    )
    selection_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    interval_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    window_date_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    bucket_count: Mapped[int] = mapped_column(Integer, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    canonical_set: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ForecastHeldoutCoverageRelease(Base):
    """One content-addressed descriptive held-out coverage measurement."""

    __tablename__ = "forecast_heldout_coverage_releases"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint("evidence_scope = 'descriptive-only'", name="scope_descriptive_only"),
        CheckConstraint(
            "release_id ~ '^sha256:[0-9a-f]{64}$'",
            name="release_id_format",
        ),
        CheckConstraint(
            "fitted_calibration_set_version ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_evidence_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_evidence_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="hashes_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint("symbol ~ '^[A-Z0-9.\\-_:]+$'", name="symbol_format"),
        CheckConstraint(
            "target = 'close' AND series_basis = 'raw' "
            "AND horizon_unit = 'trading_day' AND currency = 'USD'",
            name="semantic_scope_supported",
        ),
        CheckConstraint(
            "method IN ('empirical_residual', 'conformal_quantile_regression')",
            name="method_supported",
        ),
        CheckConstraint(
            "interval_policy_version = 'central-equal-tailed-v1' "
            "AND window_date_policy_version = 'utc-target-date-v1' "
            "AND estimator_policy_version = 'wilson-score-two-sided-v1'",
            name="policies_supported",
        ),
        CheckConstraint(
            "fit_cohort_id <> heldout_cohort_id",
            name="cohorts_distinct",
        ),
        CheckConstraint(
            "heldout_window_start <= heldout_window_end",
            name="heldout_window_order",
        ),
        CheckConstraint(
            "heldout_sample_count BETWEEN 1 AND 10000",
            name="heldout_sample_count_bounded",
        ),
        CheckConstraint("bucket_count BETWEEN 1 AND 10000", name="bucket_count_bounded"),
        CheckConstraint(
            "octet_length(confidence_level_f64_be) = 8",
            name="confidence_level_f64_size",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        CheckConstraint(
            "octet_length(canonical_release) BETWEEN 1 AND 4194304",
            name="canonical_release_size_bounded",
        ),
        CheckConstraint(
            "release_id = 'sha256:' || encode(digest(canonical_release, 'sha256'), 'hex')",
            name="release_id_matches_payload",
        ),
        ForeignKeyConstraint(
            ("outcome_resolution_policy_hash", "outcome_availability_rule_set_hash"),
            (
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ),
            name="fk_heldout_coverage_releases_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("fit_cohort_id",),
            ("forecast_outcome_cohort_availability.cohort_id",),
            name="fk_heldout_coverage_releases_fit_cohort",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("heldout_cohort_id",),
            ("forecast_outcome_cohort_availability.cohort_id",),
            name="fk_heldout_coverage_releases_heldout_cohort",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "heldout_cohort_id",
            "fitted_calibration_set_version",
            "confidence_level_f64_be",
            "estimator_policy_version",
            name="uq_heldout_coverage_releases_exact_analysis",
        ),
        Index(
            "ix_forecast_heldout_coverage_releases_fitted_set",
            "fitted_calibration_set_version",
        ),
        Index(
            "ix_forecast_heldout_coverage_releases_heldout_cohort",
            "heldout_cohort_id",
        ),
    )

    release_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    evidence_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    fitted_calibration_set_version: Mapped[str] = mapped_column(
        String(71),
        ForeignKey(
            "forecast_fitted_calibration_sets.calibration_set_version",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    series_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    horizon_unit: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(16), nullable=False)
    fit_cohort_id: Mapped[str] = mapped_column(
        String(71),
        nullable=False,
    )
    fit_selection_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    heldout_cohort_id: Mapped[str] = mapped_column(
        String(71),
        nullable=False,
    )
    heldout_selection_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    forecast_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    forecast_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    fit_evidence_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    heldout_evidence_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    heldout_window_start: Mapped[date] = mapped_column(Date, nullable=False)
    heldout_window_end: Mapped[date] = mapped_column(Date, nullable=False)
    heldout_sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence_level_f64_be: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    interval_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    window_date_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    estimator_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    bucket_count: Mapped[int] = mapped_column(Integer, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    canonical_release: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ForecastHeldoutCoverageReleaseBucket(Base):
    """Relational projection of one canonical held-out coverage bucket."""

    __tablename__ = "forecast_heldout_coverage_release_buckets"
    __table_args__ = (
        CheckConstraint("horizon BETWEEN 1 AND 252", name="horizon_bounded"),
        CheckConstraint(
            "coverage_millis BETWEEN 1 AND 999",
            name="coverage_millis_bounded",
        ),
        CheckConstraint(
            "covered_count BETWEEN 0 AND sample_count",
            name="covered_count_bounded",
        ),
        CheckConstraint("sample_count BETWEEN 1 AND 10000", name="sample_count_bounded"),
        CheckConstraint(
            "octet_length(empirical_coverage_f64_be) = 8 "
            "AND octet_length(confidence_low_f64_be) = 8 "
            "AND octet_length(confidence_high_f64_be) = 8",
            name="f64_values_size",
        ),
    )

    release_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey("forecast_heldout_coverage_releases.release_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    horizon: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    coverage_millis: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    covered_count: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    empirical_coverage_f64_be: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    confidence_low_f64_be: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    confidence_high_f64_be: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ForecastHeldoutCoverageReleaseAvailability(Base):
    """Second-transaction proof that a descriptive release committed."""

    __tablename__ = "forecast_heldout_coverage_release_availability"
    __table_args__ = (
        CheckConstraint(
            "available_at >= release_recorded_at",
            name="not_before_recording",
        ),
        CheckConstraint("sealer_xid > 0", name="sealer_xid_positive"),
    )

    release_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey("forecast_heldout_coverage_releases.release_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    release_recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    sealer_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


__all__ = [
    "ForecastFittedCalibrationSet",
    "ForecastHeldoutCoverageRelease",
    "ForecastHeldoutCoverageReleaseAvailability",
    "ForecastHeldoutCoverageReleaseBucket",
]
