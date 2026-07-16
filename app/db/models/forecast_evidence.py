"""Immutable realized-outcome and precommitted cohort evidence."""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
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
from sqlalchemy.dialects.postgresql import ARRAY, ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastOutcomeResolutionPolicyRegistration(Base):
    """Immutable, content-addressed registration of one outcome policy."""

    __tablename__ = "forecast_outcome_resolution_policies"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="policy_hash_format",
        ),
        CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "resolution_lag_seconds BETWEEN 1 AND 31622400",
            name="resolution_lag_bounded",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        CheckConstraint(
            "octet_length(canonical_policy) BETWEEN 1 AND 262144",
            name="canonical_policy_size_bounded",
        ),
        CheckConstraint(
            "policy_hash = 'sha256:' || encode(digest(canonical_policy, 'sha256'), 'hex')",
            name="policy_hash_matches_payload",
        ),
        UniqueConstraint(
            "policy_hash",
            "availability_rule_set_hash",
            name="uq_forecast_outcome_resolution_policies_policy_rules",
        ),
    )

    policy_hash: Mapped[str] = mapped_column(String(71), primary_key=True)
    availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    resolution_lag_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_policy: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class ForecastSelectionPolicyRegistration(Base):
    """Immutable, content-addressed prospective selection-policy registration."""

    __tablename__ = "forecast_selection_policies"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="policy_hash_format",
        ),
        CheckConstraint(
            "forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="forecast_resolution_policy_hash_format",
        ),
        CheckConstraint(
            "forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="forecast_availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="outcome_resolution_policy_hash_format",
        ),
        CheckConstraint(
            "outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="outcome_availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "resolution_lag_seconds BETWEEN 1 AND 31622400",
            name="resolution_lag_bounded",
        ),
        CheckConstraint(
            "fit_window_start <= fit_window_end "
            "AND fit_window_end < heldout_window_start "
            "AND heldout_window_start <= heldout_window_end",
            name="window_order",
        ),
        CheckConstraint(
            "minimum_fit_member_count BETWEEN 1 AND 1000000 "
            "AND minimum_heldout_member_count BETWEEN 1 AND 1000000",
            name="minimum_member_counts_bounded",
        ),
        CheckConstraint(
            "minimum_seal_lead_seconds BETWEEN 14400 AND 31622400",
            name="minimum_seal_lead_bounded",
        ),
        CheckConstraint(
            "cardinality(selected_steps) BETWEEN 1 AND 252 "
            "AND array_position(selected_steps, NULL) IS NULL",
            name="selected_steps_bounded",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        CheckConstraint(
            "octet_length(canonical_policy) BETWEEN 1 AND 262144",
            name="canonical_policy_size_bounded",
        ),
        CheckConstraint(
            "policy_hash = 'sha256:' || encode(digest(canonical_policy, 'sha256'), 'hex')",
            name="policy_hash_matches_payload",
        ),
        ForeignKeyConstraint(
            (
                "outcome_resolution_policy_hash",
                "outcome_availability_rule_set_hash",
            ),
            (
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ),
            name="fk_forecast_selection_policies_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "policy_hash",
            "outcome_resolution_policy_hash",
            "outcome_availability_rule_set_hash",
            name="uq_forecast_selection_policies_outcome_epoch",
        ),
    )

    policy_hash: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    forecast_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    forecast_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    outcome_availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    resolution_lag_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    fit_window_start: Mapped[date] = mapped_column(Date, nullable=False)
    fit_window_end: Mapped[date] = mapped_column(Date, nullable=False)
    heldout_window_start: Mapped[date] = mapped_column(Date, nullable=False)
    heldout_window_end: Mapped[date] = mapped_column(Date, nullable=False)
    minimum_fit_member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    minimum_heldout_member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    minimum_seal_lead_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_steps: Mapped[list[int]] = mapped_column(ARRAY(SmallInteger), nullable=False)
    canonical_policy: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


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
        CheckConstraint("currency = 'USD'", name="currency_usd"),
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
        ForeignKeyConstraint(
            (
                "outcome_resolution_policy_hash",
                "availability_rule_set_hash",
            ),
            (
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ),
            name="fk_forecast_realized_outcomes_registered_policy",
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
        ForeignKeyConstraint(
            (
                "outcome_resolution_policy_hash",
                "availability_rule_set_hash",
            ),
            (
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ),
            name="fk_forecast_outcome_cohort_manifests_registered_policy",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            (
                "selection_policy_hash",
                "outcome_resolution_policy_hash",
                "availability_rule_set_hash",
            ),
            (
                "forecast_selection_policies.policy_hash",
                "forecast_selection_policies.outcome_resolution_policy_hash",
                "forecast_selection_policies.outcome_availability_rule_set_hash",
            ),
            name="fk_cohort_manifests_registered_selection_policy",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "cohort_id",
            "selection_policy_hash",
            "purpose",
            name="uq_forecast_outcome_cohort_manifests_selection_scope",
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
            "selection_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="selection_policy_hash_format",
        ),
        CheckConstraint(
            "purpose IN ('calibration_fit', 'heldout_evaluation')",
            name="purpose_supported",
        ),
        CheckConstraint(
            "output_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="output_hash_format",
        ),
        ForeignKeyConstraint(
            ("cohort_id", "selection_policy_hash", "purpose"),
            (
                "forecast_outcome_cohort_manifests.cohort_id",
                "forecast_outcome_cohort_manifests.selection_policy_hash",
                "forecast_outcome_cohort_manifests.purpose",
            ),
            name="fk_forecast_outcome_cohort_members_manifest_selection_scope",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "cohort_id",
            "opportunity_hash",
            "step",
            name="uq_forecast_outcome_cohort_members_opportunity_step",
        ),
        UniqueConstraint(
            "selection_policy_hash",
            "opportunity_hash",
            "step",
            name="uq_forecast_outcome_cohort_members_policy_opportunity_step",
        ),
        ExcludeConstraint(
            ("selection_policy_hash", "="),
            ("opportunity_hash", "="),
            ("purpose", "<>"),
            name="ex_forecast_outcome_cohort_members_cross_purpose",
            using="gist",
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
    selection_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    forecast_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("forecast_runs.forecast_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    step: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    opportunity_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    output_hash: Mapped[str] = mapped_column(String(71), nullable=False)


class ForecastRealizedOutcomePublication(Base):
    """One sealed scheduled-forecast member authorized to use an outcome row."""

    __tablename__ = "forecast_realized_outcome_publications"
    __table_args__ = (
        CheckConstraint("step BETWEEN 1 AND 252", name="step_bounded"),
        CheckConstraint("publisher_xid > 0", name="publisher_xid_positive"),
        ForeignKeyConstraint(
            ("cohort_id", "forecast_id", "step"),
            (
                "forecast_outcome_cohort_members.cohort_id",
                "forecast_outcome_cohort_members.forecast_id",
                "forecast_outcome_cohort_members.step",
            ),
            name=(
                "fk_forecast_realized_outcome_publications_cohort_member_"
                "forecast_outcome_cohort_members"
            ),
            ondelete="RESTRICT",
        ),
        Index(
            "ix_forecast_realized_outcome_publications_cohort_member",
            "cohort_id",
            "forecast_id",
            "step",
            "outcome_id",
        ),
    )

    outcome_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey("forecast_realized_outcomes.outcome_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    cohort_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    forecast_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    step: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    publisher_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


__all__ = [
    "ForecastOutcomeCohortAvailability",
    "ForecastOutcomeCohortManifest",
    "ForecastOutcomeCohortMember",
    "ForecastOutcomeResolutionPolicyRegistration",
    "ForecastSelectionPolicyRegistration",
    "ForecastRealizedOutcome",
    "ForecastRealizedOutcomePublication",
]
