"""Append-only corporate-action collections and exact source versions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CorporateActionVersion(Base):
    """One immutable content version of a provider-identified action."""

    __tablename__ = "corporate_action_versions"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "action_version_id ~ '^sha256:[0-9a-f]{64}$'",
            name="action_version_id_format",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name="symbol_format",
        ),
        CheckConstraint(
            "action_type IN ('split', 'dividend')",
            name="action_type_supported",
        ),
        CheckConstraint("source = 'polygon'", name="source_supported"),
        CheckConstraint(
            "provider_event_id ~ '^[A-Za-z0-9._:\\-]+$'",
            name="provider_event_id_format",
        ),
        CheckConstraint("status = 'active'", name="status_supported"),
        CheckConstraint(
            "(action_type = 'split' AND split_from IS NOT NULL "
            "AND split_to IS NOT NULL AND split_from > 0 AND split_to > 0 "
            "AND adjustment_type IS NOT NULL AND adjustment_type IN "
            "('forward_split', 'reverse_split', 'stock_dividend') "
            "AND historical_adjustment_factor IS NOT NULL "
            "AND historical_adjustment_factor > 0 "
            "AND cash_amount IS NULL AND currency IS NULL "
            "AND declaration_date IS NULL AND record_date IS NULL "
            "AND pay_date IS NULL AND frequency IS NULL "
            "AND distribution_type IS NULL "
            "AND split_adjusted_cash_amount IS NULL) OR "
            "(action_type = 'dividend' AND split_from IS NULL "
            "AND split_to IS NULL AND cash_amount IS NOT NULL "
            "AND cash_amount > 0 AND split_adjusted_cash_amount IS NOT NULL "
            "AND split_adjusted_cash_amount > 0 "
            "AND currency IS NOT NULL AND currency ~ '^[A-Z]{3}$' "
            "AND distribution_type IS NOT NULL AND distribution_type IN "
            "('recurring', 'special', 'supplemental', 'irregular', 'unknown') "
            "AND historical_adjustment_factor IS NOT NULL "
            "AND historical_adjustment_factor > 0 AND adjustment_type IS NULL)",
            name="action_shape",
        ),
        CheckConstraint(
            "historical_adjustment_factor IS NULL OR "
            "(historical_adjustment_factor > 0 "
            "AND historical_adjustment_factor < 'Infinity'::numeric)",
            name="historical_factor_positive",
        ),
        CheckConstraint(
            "split_adjusted_cash_amount IS NULL OR "
            "(split_adjusted_cash_amount > 0 "
            "AND split_adjusted_cash_amount < 'Infinity'::numeric)",
            name="split_adjusted_cash_positive",
        ),
        CheckConstraint(
            "split_from IS NULL OR (split_from > 0 AND split_from < 'Infinity'::numeric)",
            name="split_from_finite_positive",
        ),
        CheckConstraint(
            "split_to IS NULL OR (split_to > 0 AND split_to < 'Infinity'::numeric)",
            name="split_to_finite_positive",
        ),
        CheckConstraint(
            "cash_amount IS NULL OR (cash_amount > 0 AND cash_amount < 'Infinity'::numeric)",
            name="cash_amount_finite_positive",
        ),
        CheckConstraint(
            "frequency IS NULL OR frequency >= 0",
            name="frequency_nonnegative",
        ),
        CheckConstraint(
            "octet_length(canonical_event) BETWEEN 1 AND 65536",
            name="canonical_event_size_bounded",
        ),
        CheckConstraint(
            "action_version_id = 'sha256:' || encode(digest(canonical_event, 'sha256'), 'hex')",
            name="action_version_id_matches_payload",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        UniqueConstraint(
            "source",
            "action_type",
            "provider_event_id",
            "action_version_id",
            name="uq_corporate_action_versions_source_event_version",
        ),
        Index(
            "ix_corporate_action_versions_series_date",
            "source",
            "symbol",
            "action_type",
            "effective_date",
        ),
    )

    action_version_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    action_type: Mapped[str] = mapped_column(String(16), nullable=False)
    provider_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)

    split_from: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    split_to: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    adjustment_type: Mapped[str | None] = mapped_column(String(32))
    cash_amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    split_adjusted_cash_amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    currency: Mapped[str | None] = mapped_column(String(3))
    declaration_date: Mapped[date | None] = mapped_column(Date)
    record_date: Mapped[date | None] = mapped_column(Date)
    pay_date: Mapped[date | None] = mapped_column(Date)
    frequency: Mapped[int | None] = mapped_column(Integer)
    distribution_type: Mapped[str | None] = mapped_column(String(32))
    historical_adjustment_factor: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))

    canonical_event: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.clock_timestamp(),
        nullable=False,
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CorporateActionCollection(Base):
    """One complete, content-addressed response for an exact bounded query."""

    __tablename__ = "corporate_action_collections"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "collection_id ~ '^sha256:[0-9a-f]{64}$'",
            name="collection_id_format",
        ),
        CheckConstraint(
            "query_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name="query_policy_hash_format",
        ),
        CheckConstraint(
            "action_type IN ('split', 'dividend')",
            name="action_type_supported",
        ),
        CheckConstraint("symbol = upper(symbol)", name="symbol_uppercase"),
        CheckConstraint("coverage_start <= coverage_end", name="coverage_order"),
        CheckConstraint("page_limit = 5000", name="page_limit_supported"),
        CheckConstraint("page_count = 1", name="page_count_supported"),
        CheckConstraint("event_count BETWEEN 0 AND 5000", name="event_count_bounded"),
        CheckConstraint("pagination_exhausted", name="pagination_must_be_exhausted"),
        CheckConstraint(
            "octet_length(canonical_manifest) BETWEEN 1 AND 1048576",
            name="canonical_manifest_size_bounded",
        ),
        CheckConstraint(
            "collection_id = 'sha256:' || encode(digest(canonical_manifest, 'sha256'), 'hex')",
            name="collection_id_matches_payload",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        UniqueConstraint(
            "collection_id",
            "recorded_at",
            name="uq_corporate_action_collections_exact_recording",
        ),
        Index(
            "ix_corporate_action_collections_scope",
            "source",
            "symbol",
            "action_type",
            "coverage_start",
            "coverage_end",
            "recorded_at",
            "collection_id",
        ),
    )

    collection_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    query_policy_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False)
    action_type: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    coverage_start: Mapped[date] = mapped_column(Date, nullable=False)
    coverage_end: Mapped[date] = mapped_column(Date, nullable=False)
    page_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    page_count: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    pagination_exhausted: Mapped[bool] = mapped_column(nullable=False)
    provider_request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    canonical_manifest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.clock_timestamp(),
        nullable=False,
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CorporateActionCollectionMember(Base):
    """Relational projection of one manifest member."""

    __tablename__ = "corporate_action_collection_members"
    __table_args__ = (
        CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        ForeignKeyConstraint(
            ("collection_id",),
            ("corporate_action_collections.collection_id",),
            name="fk_corporate_action_collection_members_collection",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("action_version_id",),
            ("corporate_action_versions.action_version_id",),
            name="fk_corporate_action_collection_members_action_version",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "collection_id",
            "action_version_id",
            name="uq_corporate_action_collection_members_version",
        ),
    )

    collection_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    action_version_id: Mapped[str] = mapped_column(String(71), nullable=False)
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CorporateActionCollectionAvailability(Base):
    """DB-stamped post-commit receipt for one complete collection."""

    __tablename__ = "corporate_action_collection_availability"
    __table_args__ = (
        CheckConstraint(
            "available_at >= collection_recorded_at",
            name="available_after_collection",
        ),
        UniqueConstraint(
            "collection_id",
            "collection_recorded_at",
            "available_at",
            name="uq_corporate_action_collection_availability_exact_receipt",
        ),
        ForeignKeyConstraint(
            ("collection_id", "collection_recorded_at"),
            (
                "corporate_action_collections.collection_id",
                "corporate_action_collections.recorded_at",
            ),
            name="fk_corporate_action_collection_availability_exact_collection",
            ondelete="RESTRICT",
        ),
    )

    collection_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    collection_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.clock_timestamp(),
        nullable=False,
    )


__all__ = [
    "CorporateActionCollection",
    "CorporateActionCollectionAvailability",
    "CorporateActionCollectionMember",
    "CorporateActionVersion",
]
