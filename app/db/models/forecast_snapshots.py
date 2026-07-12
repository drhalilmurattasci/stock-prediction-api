"""Insert-only, content-addressed input snapshots for reproducible forecasts."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastInputSnapshot(Base):
    """One atomically inserted, immutable canonical forecast-input payload.

    ``snapshot_id`` is the SHA-256 of ``canonical_payload``. PostgreSQL verifies
    that relationship and mutation triggers make every committed row
    insert-only. Header columns duplicate routing and safety summaries so a
    resolver can select efficiently, then it byte-validates the payload before
    use.
    """

    __tablename__ = "forecast_input_snapshots"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
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
            "horizon_unit IN ('trading_day', 'calendar_day', 'minute', 'hour', 'week')",
            name="horizon_unit_supported",
        ),
        CheckConstraint(
            "series_basis IN ('raw', 'split_adjusted', 'split_dividend_adjusted')",
            name="series_basis_supported",
        ),
        CheckConstraint(
            "input_timespan IN ('minute', 'hour', 'day', 'week')",
            name="input_timespan_supported",
        ),
        CheckConstraint(
            "input_multiplier BETWEEN 1 AND 10000",
            name="input_multiplier_bounded",
        ),
        CheckConstraint(
            "(target = 'close' AND series_basis = 'raw') OR "
            "(target = 'adjusted_close' AND series_basis <> 'raw') OR "
            "target IN ('return', 'log_return')",
            name="target_series_basis",
        ),
        CheckConstraint(
            "(target IN ('close', 'adjusted_close') AND currency IS NOT NULL) OR "
            "(target IN ('return', 'log_return') AND currency IS NULL)",
            name="target_currency",
        ),
        CheckConstraint(
            "currency IS NULL OR currency ~ '^[A-Z]{3}$'",
            name="currency_format",
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
            "availability_rule_set_hash IS NULL OR "
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="availability_rule_set_hash_format",
        ),
        CheckConstraint(
            "observation_count BETWEEN 1 AND 10000",
            name="observation_count_bounded",
        ),
        CheckConstraint(
            "target_time_count BETWEEN 1 AND 252",
            name="target_time_count_bounded",
        ),
        CheckConstraint(
            "first_observed_at <= last_observed_at AND last_observed_at <= as_of",
            name="observation_window",
        ),
        CheckConstraint("max_available_at <= as_of", name="availability_cutoff"),
        CheckConstraint("sealed_at >= as_of", name="sealed_after_cutoff"),
        CheckConstraint(
            "octet_length(canonical_payload) BETWEEN 1 AND 4194304",
            name="payload_size_bounded",
        ),
        CheckConstraint(
            "snapshot_id = 'sha256:' || encode(digest(canonical_payload, 'sha256'), 'hex')",
            name="payload_hash_matches_id",
        ),
        CheckConstraint(
            "(availability_status = 'not_run' AND availability_rule_set_hash IS NULL "
            "AND availability_checked_at IS NULL) OR "
            "(availability_status = 'passed' AND availability_rule_set_hash IS NOT NULL "
            "AND availability_checked_at IS NOT NULL "
            "AND availability_checked_at >= max_available_at "
            "AND availability_checked_at >= as_of "
            "AND availability_checked_at <= sealed_at)",
            name="availability_evidence",
        ),
        UniqueConstraint(
            "schema_version",
            "resolution_policy_hash",
            "symbol",
            "target",
            "horizon_unit",
            "series_basis",
            "input_timespan",
            "input_multiplier",
            "as_of",
            name="uq_forecast_input_snapshots_semantic_key",
        ),
        Index(
            "ix_forecast_input_snapshots_resolve",
            "resolution_policy_hash",
            "symbol",
            "target",
            "horizon_unit",
            "series_basis",
            "input_timespan",
            "input_multiplier",
            "as_of",
            "snapshot_id",
        ),
    )

    snapshot_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    resolution_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    horizon_unit: Mapped[str] = mapped_column(String(32), nullable=False)
    series_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    input_timespan: Mapped[str] = mapped_column(String(16), nullable=False)
    input_multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    observation_count: Mapped[int] = mapped_column(Integer, nullable=False)
    target_time_count: Mapped[int] = mapped_column(Integer, nullable=False)
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    max_available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    availability_status: Mapped[str] = mapped_column(String(16), nullable=False)
    availability_rule_set_hash: Mapped[str | None] = mapped_column(String(71), nullable=True)
    availability_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    canonical_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


__all__ = ["ForecastInputSnapshot"]
