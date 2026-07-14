"""Insert-only, byte-verifiable records of served forecast runs."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
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


class ForecastRun(Base):
    """One immutable request/response pair produced from a sealed snapshot.

    The canonical request and output are the audit source of truth. PostgreSQL
    verifies their SHA-256 identifiers before accepting the row, stamps the
    insertion time itself, and rejects every later mutation. A nullable HMAC
    digest provides retry-safe API idempotency without storing client tokens.
    """

    __tablename__ = "forecast_runs"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "origin_kind IN ('api', 'scheduled_evaluation')",
            name="origin_kind_supported",
        ),
        CheckConstraint(
            "idempotency_token_digest IS NULL OR "
            "idempotency_token_digest ~ '^hmac-sha256:[0-9a-f]{64}$'",
            name="idempotency_token_digest_format",
        ),
        CheckConstraint(
            "request_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="request_hash_format",
        ),
        CheckConstraint(
            "opportunity_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="opportunity_hash_format",
        ),
        CheckConstraint(
            "output_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="output_hash_format",
        ),
        CheckConstraint(
            "snapshot_id ~ '^sha256:[0-9a-f]{64}$'",
            name="snapshot_id_format",
        ),
        CheckConstraint(
            "resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="resolution_policy_hash_format",
        ),
        CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "feature_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="feature_set_hash_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name="symbol_format",
        ),
        CheckConstraint(
            "target IN ('close', 'adjusted_close', 'return', 'log_return')",
            name="target_supported",
        ),
        CheckConstraint(
            "horizon BETWEEN 1 AND 252",
            name="horizon_bounded",
        ),
        CheckConstraint(
            "horizon_unit IN ('trading_day', 'calendar_day', 'minute', 'hour', 'week')",
            name="horizon_unit_supported",
        ),
        CheckConstraint(
            "series_basis IN ('raw', 'split_adjusted', 'split_dividend_adjusted')",
            name="series_basis_supported",
        ),
        CheckConstraint(
            "(target = 'close' AND series_basis = 'raw') OR "
            "(target = 'adjusted_close' AND series_basis <> 'raw') OR "
            "target IN ('return', 'log_return')",
            name="target_series_basis",
        ),
        CheckConstraint(
            "max_available_at <= as_of AND as_of <= generated_at AND generated_at <= recorded_at",
            name="time_order",
        ),
        CheckConstraint(
            "calibration_method IN "
            "('conformal_quantile_regression', 'adaptive_conformal', "
            "'empirical_residual', 'none')",
            name="calibration_method_supported",
        ),
        CheckConstraint(
            "octet_length(canonical_request) BETWEEN 1 AND 1048576",
            name="request_size_bounded",
        ),
        CheckConstraint(
            "octet_length(canonical_output) BETWEEN 1 AND 4194304",
            name="output_size_bounded",
        ),
        CheckConstraint(
            "request_hash = 'sha256:' || encode(digest(canonical_request, 'sha256'), 'hex')",
            name="request_hash_matches_payload",
        ),
        CheckConstraint(
            "output_hash = 'sha256:' || encode(digest(canonical_output, 'sha256'), 'hex')",
            name="output_hash_matches_payload",
        ),
        UniqueConstraint(
            "idempotency_token_digest",
            name="uq_forecast_runs_idempotency_token_digest",
        ),
        Index("ix_forecast_runs_opportunity_hash", "opportunity_hash"),
        Index(
            "uq_forecast_runs_scheduled_opportunity",
            "opportunity_hash",
            unique=True,
            postgresql_where=text("origin_kind = 'scheduled_evaluation'"),
        ),
    )

    forecast_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    origin_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_token_digest: Mapped[str | None] = mapped_column(String(76), nullable=True)
    request_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    opportunity_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    output_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    snapshot_id: Mapped[str] = mapped_column(
        String(71),
        ForeignKey("forecast_input_snapshots.snapshot_id", ondelete="RESTRICT"),
        nullable=False,
    )
    resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    availability_rule_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    horizon: Mapped[int] = mapped_column(Integer, nullable=False)
    horizon_unit: Mapped[str] = mapped_column(String(32), nullable=False)
    series_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    max_available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    feature_set_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    code_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    calibration_set_version: Mapped[str] = mapped_column(String(128), nullable=False)
    calibration_method: Mapped[str] = mapped_column(String(32), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("clock_timestamp()"),
    )
    canonical_request: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    canonical_output: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


__all__ = ["ForecastRun"]
