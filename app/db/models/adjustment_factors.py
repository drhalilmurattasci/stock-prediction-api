"""Immutable, content-addressed split/dividend adjustment-factor evidence."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKeyConstraint,
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


class AdjustmentFactorSetRecord(Base):
    """Header projected exclusively from one canonical factor-set payload."""

    __tablename__ = "adjustment_factor_sets"
    __table_args__ = (
        CheckConstraint(
            "factor_set_id ~ '^sha256:[0-9a-f]{64}$'",
            name="factor_set_id_format",
        ),
        CheckConstraint(
            "policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="policy_hash_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name="symbol_format",
        ),
        CheckConstraint(
            "split_collection_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND dividend_collection_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND split_collection_id <> dividend_collection_id",
            name="collection_ids_valid",
        ),
        CheckConstraint(
            "coverage_start <= anchor_date AND anchor_date = coverage_end",
            name="coverage_order",
        ),
        CheckConstraint("input_count BETWEEN 1 AND 5000", name="input_count_bounded"),
        CheckConstraint(
            "split_collection_recorded_at <= split_collection_available_at "
            "AND dividend_collection_recorded_at <= dividend_collection_available_at "
            "AND split_collection_available_at <= cutoff "
            "AND dividend_collection_available_at <= cutoff "
            "AND max_input_available_at <= cutoff",
            name="input_availability_cutoff",
        ),
        CheckConstraint(
            "octet_length(canonical_payload) BETWEEN 1 AND 4194304",
            name="canonical_payload_size_bounded",
        ),
        CheckConstraint(
            "factor_set_id = 'sha256:' || encode(digest(canonical_payload, 'sha256'), 'hex')",
            name="factor_set_id_matches_payload",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        UniqueConstraint(
            "factor_set_id",
            "recorded_at",
            name="uq_adjustment_factor_sets_exact_recording",
        ),
        ForeignKeyConstraint(
            (
                "split_collection_id",
                "split_collection_recorded_at",
                "split_collection_available_at",
            ),
            (
                "corporate_action_collection_availability.collection_id",
                "corporate_action_collection_availability.collection_recorded_at",
                "corporate_action_collection_availability.available_at",
            ),
            name="fk_adjustment_factor_sets_exact_split_collection_receipt",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            (
                "dividend_collection_id",
                "dividend_collection_recorded_at",
                "dividend_collection_available_at",
            ),
            (
                "corporate_action_collection_availability.collection_id",
                "corporate_action_collection_availability.collection_recorded_at",
                "corporate_action_collection_availability.available_at",
            ),
            name="fk_adjustment_factor_sets_exact_dividend_collection_receipt",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_adjustment_factor_sets_resolve",
            "symbol",
            "cutoff",
            "anchor_date",
            "factor_set_id",
        ),
    )

    factor_set_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    format: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    cutoff: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    anchor_date: Mapped[date] = mapped_column(Date, nullable=False)
    coverage_start: Mapped[date] = mapped_column(Date, nullable=False)
    coverage_end: Mapped[date] = mapped_column(Date, nullable=False)
    input_count: Mapped[int] = mapped_column(Integer, nullable=False)
    max_input_available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    split_collection_id: Mapped[str] = mapped_column(String(71), nullable=False)
    split_collection_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    split_collection_available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    dividend_collection_id: Mapped[str] = mapped_column(String(71), nullable=False)
    dividend_collection_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    dividend_collection_available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    canonical_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.clock_timestamp(),
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class AdjustmentFactorEntry(Base):
    """One byte-derived raw receipt and its corresponding adjustment factors."""

    __tablename__ = "adjustment_factor_entries"
    __table_args__ = (
        CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint(
            "timespan = 'day' AND multiplier = 1 "
            "AND source = 'polygon_open_close' AND adjustment_basis = 'raw'",
            name="raw_source_supported",
        ),
        CheckConstraint(
            "(observed_at AT TIME ZONE 'UTC')::date = observation_date "
            "AND observed_at <= version_recorded_at "
            "AND version_recorded_at <= raw_available_at",
            name="raw_receipt_time_order",
        ),
        CheckConstraint(
            "raw_close_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND raw_close_decimal::numeric > 0",
            name="raw_close_decimal_positive",
        ),
        CheckConstraint(
            "price_factor_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND price_factor_decimal::numeric > 0 "
            "AND volume_factor_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND volume_factor_decimal::numeric > 0",
            name="factor_decimals_positive",
        ),
        CheckConstraint(
            "octet_length(raw_close_f64_be) = 8 "
            "AND octet_length(price_factor_f64_be) = 8 "
            "AND octet_length(volume_factor_f64_be) = 8",
            name="binary64_width",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        ForeignKeyConstraint(
            ("factor_set_id",),
            ("adjustment_factor_sets.factor_set_id",),
            name="fk_adjustment_factor_entries_factor_set",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            (
                "symbol",
                "timespan",
                "multiplier",
                "observed_at",
                "source",
                "adjustment_basis",
                "version_recorded_at",
                "raw_available_at",
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
            name="fk_adjustment_factor_entries_exact_bar_receipt",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "factor_set_id",
            "observation_date",
            name="uq_adjustment_factor_entries_observation",
        ),
    )

    factor_set_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    observation_date: Mapped[date] = mapped_column(Date, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    timespan: Mapped[str] = mapped_column(String(16), nullable=False)
    multiplier: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    adjustment_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    version_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    raw_available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    raw_close_decimal: Mapped[str] = mapped_column(String(400), nullable=False)
    raw_close_f64_be: Mapped[bytes] = mapped_column(LargeBinary(8), nullable=False)
    price_factor_decimal: Mapped[str] = mapped_column(String(400), nullable=False)
    price_factor_f64_be: Mapped[bytes] = mapped_column(LargeBinary(8), nullable=False)
    volume_factor_decimal: Mapped[str] = mapped_column(String(400), nullable=False)
    volume_factor_f64_be: Mapped[bytes] = mapped_column(LargeBinary(8), nullable=False)
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class AdjustmentFactorSetAvailability(Base):
    """DB-stamped later-transaction receipt for one complete factor set."""

    __tablename__ = "adjustment_factor_set_availability"
    __table_args__ = (
        CheckConstraint(
            "available_at >= factor_set_recorded_at",
            name="available_after_factor_set",
        ),
        ForeignKeyConstraint(
            ("factor_set_id", "factor_set_recorded_at"),
            (
                "adjustment_factor_sets.factor_set_id",
                "adjustment_factor_sets.recorded_at",
            ),
            name="fk_adjustment_factor_set_availability_exact_factor_set",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "factor_set_id",
            "factor_set_recorded_at",
            "available_at",
            name="uq_adjustment_factor_set_availability_exact_receipt",
        ),
    )

    factor_set_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    factor_set_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.clock_timestamp(),
    )


__all__ = [
    "AdjustmentFactorEntry",
    "AdjustmentFactorSetAvailability",
    "AdjustmentFactorSetRecord",
]
