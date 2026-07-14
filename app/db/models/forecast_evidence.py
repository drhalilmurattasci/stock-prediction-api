"""Immutable realized-outcome and precommitted cohort evidence."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastRealizedOutcome(Base):
    """One content-addressed raw-close truth observation under a named policy."""

    __tablename__ = "forecast_realized_outcomes"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "outcome_id ~ '^sha256:[0-9a-f]{64}$'",
            name="outcome_id_format",
        ),
        CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="resolution_policy_hash_format",
        ),
        CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="availability_rule_set_hash_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint("symbol ~ '^[A-Z0-9.\\-_:]+$'", name="symbol_format"),
        CheckConstraint("target = 'close'", name="target_supported"),
        CheckConstraint("series_basis = 'raw'", name="series_basis_supported"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
        CheckConstraint(
            "bar_timespan = 'day' AND bar_multiplier = 1 "
            "AND bar_source = 'polygon_open_close' "
            "AND bar_adjustment_basis = 'raw' AND bar_field = 'close'",
            name="source_supported",
        ),
        CheckConstraint(
            "bar_value > '-Infinity'::float8 AND bar_value < 'Infinity'::float8 "
            "AND realized_value > '-Infinity'::float8 "
            "AND realized_value < 'Infinity'::float8",
            name="values_finite",
        ),
        CheckConstraint(
            "bar_value >= 0 AND realized_value >= 0 AND bar_value = realized_value",
            name="raw_close_value_matches",
        ),
        CheckConstraint(
            "bar_observed_at = target_time "
            "AND bar_observed_at <= bar_fetched_at "
            "AND bar_fetched_at <= bar_source_as_of "
            "AND bar_source_as_of <= bar_version_recorded_at "
            "AND bar_version_recorded_at <= bar_available_at "
            "AND bar_available_at <= resolution_cutoff "
            "AND resolution_cutoff <= sealed_at",
            name="evidence_time_order",
        ),
        CheckConstraint(
            "octet_length(canonical_evidence) BETWEEN 1 AND 262144",
            name="evidence_size_bounded",
        ),
        CheckConstraint(
            "outcome_id = 'sha256:' || encode(digest(canonical_evidence, 'sha256'), 'hex')",
            name="outcome_id_matches_payload",
        ),
        UniqueConstraint(
            "outcome_resolution_policy_hash",
            "availability_rule_set_hash",
            "symbol",
            "target",
            "series_basis",
            "target_time",
            name="uq_forecast_realized_outcomes_semantic_key",
        ),
        ForeignKeyConstraint(
            (
                "symbol",
                "bar_timespan",
                "bar_multiplier",
                "bar_observed_at",
                "bar_source",
                "bar_adjustment_basis",
                "bar_version_recorded_at",
                "bar_available_at",
            ),
            (
                "bar_version_availability.symbol",
                "bar_version_availability.timespan",
                "bar_version_availability.multiplier",
                "bar_version_availability.ts",
                "bar_version_availability.source",
                "bar_version_availability.adjustment_basis",
                "bar_version_availability.version_recorded_at",
                "bar_version_availability.available_at",
            ),
            name=("fk_forecast_realized_outcomes_exact_bar_receipt_bar_version_availability"),
            ondelete="RESTRICT",
        ),
        Index(
            "ix_forecast_realized_outcomes_target",
            "symbol",
            "target_time",
            "outcome_resolution_policy_hash",
        ),
    )

    outcome_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    outcome_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    series_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolution_cutoff: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_timespan: Mapped[str] = mapped_column(String(16), nullable=False)
    bar_multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    bar_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_source: Mapped[str] = mapped_column(String(64), nullable=False)
    bar_adjustment_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    bar_version_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    bar_fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_source_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_field: Mapped[str] = mapped_column(String(32), nullable=False)
    bar_value: Mapped[float] = mapped_column(Float, nullable=False)
    realized_value: Mapped[float] = mapped_column(Float, nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    canonical_evidence: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ForecastOutcomeCohortManifest(Base):
    """Immutable future-target cohort membership, before availability proof."""

    __tablename__ = "forecast_outcome_cohort_manifests"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint("cohort_id ~ '^sha256:[0-9a-f]{64}$'", name="cohort_id_format"),
        CheckConstraint(
            "selection_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="selection_policy_hash_format",
        ),
        CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="outcome_resolution_policy_hash_format",
        ),
        CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "purpose IN ('calibration_fit', 'heldout_evaluation')",
            name="purpose_supported",
        ),
        CheckConstraint("member_count BETWEEN 1 AND 10000", name="member_count_bounded"),
        CheckConstraint(
            "creator_xid > 0 AND recorded_at < earliest_target_time "
            "AND earliest_target_time <= latest_target_time",
            name="time_order",
        ),
        CheckConstraint(
            "octet_length(canonical_manifest) BETWEEN 1 AND 4194304",
            name="manifest_size_bounded",
        ),
        CheckConstraint(
            "cohort_id = 'sha256:' || encode(digest(canonical_manifest, 'sha256'), 'hex')",
            name="cohort_id_matches_payload",
        ),
        Index(
            "ix_forecast_outcome_cohorts_target_window",
            "earliest_target_time",
            "latest_target_time",
        ),
    )

    cohort_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    selection_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    earliest_target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    latest_target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    canonical_manifest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ForecastOutcomeCohortAvailability(Base):
    """Second-transaction proof that a cohort was committed before its targets."""

    __tablename__ = "forecast_outcome_cohort_availability"
    __table_args__ = (
        CheckConstraint("sealed_at >= manifest_recorded_at", name="not_before_recording"),
        CheckConstraint("sealer_xid > 0", name="sealer_xid_positive"),
    )

    cohort_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey(
            "forecast_outcome_cohort_manifests.cohort_id",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    )
    manifest_recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    sealer_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class ForecastOutcomeCohortMember(Base):
    """Relational projection of one canonical, scheduled cohort member."""

    __tablename__ = "forecast_outcome_cohort_members"
    __table_args__ = (
        CheckConstraint("step BETWEEN 1 AND 252", name="step_bounded"),
        CheckConstraint(
            "opportunity_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="opportunity_hash_format",
        ),
        CheckConstraint(
            "output_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="output_hash_format",
        ),
        UniqueConstraint(
            "cohort_id",
            "opportunity_hash",
            "step",
            name="uq_forecast_outcome_cohort_members_opportunity_step",
        ),
        Index(
            "ix_forecast_outcome_cohort_members_target",
            "target_time",
            "cohort_id",
        ),
    )

    cohort_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey(
            "forecast_outcome_cohort_manifests.cohort_id",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    )
    forecast_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("forecast_runs.forecast_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    step: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    opportunity_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    output_hash: Mapped[str] = mapped_column(String(71), nullable=False)


__all__ = [
    "ForecastOutcomeCohortAvailability",
    "ForecastOutcomeCohortManifest",
    "ForecastOutcomeCohortMember",
    "ForecastRealizedOutcome",
]
