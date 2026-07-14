"""SQLAlchemy models for OHLCV bars and append-only restatement history."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    Identity,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Bar(Base):
    """Current OHLCV value for one provider/source-adjustment bar key."""

    __tablename__ = "bars"
    __table_args__ = (
        CheckConstraint("multiplier >= 1", name="multiplier_positive"),
        CheckConstraint(
            "open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0",
            name="ohlcv_nonnegative",
        ),
        # Finiteness: Postgres orders NaN GREATER than every value (NaN >= 0 is
        # TRUE, NaN = NaN is TRUE), so the nonnegativity checks above cannot
        # exclude NaN/+Infinity — but the finite-only read contract would 500 on
        # such a row. ``col < 'Infinity'`` is FALSE for both NaN and +Infinity
        # (and -Infinity already fails ``>= 0``), giving finite-nonnegative.
        CheckConstraint(
            "open < 'Infinity'::float8 AND high < 'Infinity'::float8 "
            "AND low < 'Infinity'::float8 AND close < 'Infinity'::float8 "
            "AND volume < 'Infinity'::float8",
            name="ohlcv_finite",
        ),
        CheckConstraint("vwap IS NULL OR vwap >= 0", name="vwap_nonnegative"),
        CheckConstraint("vwap IS NULL OR vwap < 'Infinity'::float8", name="vwap_finite"),
        CheckConstraint("trade_count IS NULL OR trade_count >= 0", name="trade_count_nonnegative"),
        CheckConstraint("high >= low", name="high_gte_low"),
        CheckConstraint("high >= open AND high >= close", name="high_gte_open_close"),
        CheckConstraint("low <= open AND low <= close", name="low_lte_open_close"),
        CheckConstraint("fetched_at >= ts", name="fetched_not_before_bar"),
        CheckConstraint("as_of >= fetched_at", name="as_of_not_before_fetch"),
        CheckConstraint("recorded_at >= as_of", name="recorded_not_before_as_of"),
        CheckConstraint("version_creator_xid >= 0", name="version_creator_xid_nonnegative"),
        Index("ix_bars_symbol_ts", "symbol", "ts"),
        Index("ix_bars_source_as_of", "source", "as_of"),
        # Covering series index: every equality column of the /v1/prices read
        # (and the upsert conflict key) ahead of ``ts``, so a bounded
        # ``ORDER BY ts DESC LIMIT n`` is a pure index range (Postgres walks a
        # btree backwards for DESC). Without it the PK only covers the
        # (symbol, timespan, multiplier) prefix — source/adjustment_basis sit
        # AFTER ts — so a sparse/absent series degrades to scanning the whole
        # prefix across every hypertable chunk and LIMIT stops bounding work.
        Index(
            "ix_bars_series_ts",
            "symbol",
            "timespan",
            "multiplier",
            "source",
            "adjustment_basis",
            "ts",
        ),
    )

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    timespan: Mapped[str] = mapped_column(String(16), primary_key=True)
    multiplier: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    adjustment_basis: Mapped[str] = mapped_column(String(32), primary_key=True)

    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    vwap: Mapped[float | None] = mapped_column(Float)
    trade_count: Mapped[int | None] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.clock_timestamp(),
    )
    version_creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)


class BarRevision(Base):
    """Append-only record of a vendor restatement/correction to a bar."""

    __tablename__ = "bars_revisions"
    __table_args__ = (
        CheckConstraint("multiplier >= 1", name="revision_multiplier_positive"),
        CheckConstraint(
            "previous_open >= 0 AND previous_high >= 0 AND previous_low >= 0 "
            "AND previous_close >= 0 AND previous_volume >= 0 "
            "AND incoming_open >= 0 AND incoming_high >= 0 AND incoming_low >= 0 "
            "AND incoming_close >= 0 AND incoming_volume >= 0",
            name="revision_ohlcv_nonnegative",
        ),
        CheckConstraint(
            "previous_open < 'Infinity'::float8 "
            "AND previous_high < 'Infinity'::float8 "
            "AND previous_low < 'Infinity'::float8 "
            "AND previous_close < 'Infinity'::float8 "
            "AND previous_volume < 'Infinity'::float8 "
            "AND incoming_open < 'Infinity'::float8 "
            "AND incoming_high < 'Infinity'::float8 "
            "AND incoming_low < 'Infinity'::float8 "
            "AND incoming_close < 'Infinity'::float8 "
            "AND incoming_volume < 'Infinity'::float8",
            name="revision_ohlcv_finite",
        ),
        CheckConstraint(
            "(previous_vwap IS NULL OR (previous_vwap >= 0 "
            "AND previous_vwap < 'Infinity'::float8)) "
            "AND (incoming_vwap IS NULL OR (incoming_vwap >= 0 "
            "AND incoming_vwap < 'Infinity'::float8))",
            name="revision_vwap_finite_nonnegative",
        ),
        CheckConstraint(
            "(previous_trade_count IS NULL OR previous_trade_count >= 0) "
            "AND (incoming_trade_count IS NULL OR incoming_trade_count >= 0)",
            name="revision_trade_count_nonnegative",
        ),
        CheckConstraint(
            "previous_high >= previous_low "
            "AND previous_high >= previous_open AND previous_high >= previous_close "
            "AND previous_low <= previous_open AND previous_low <= previous_close "
            "AND incoming_high >= incoming_low "
            "AND incoming_high >= incoming_open AND incoming_high >= incoming_close "
            "AND incoming_low <= incoming_open AND incoming_low <= incoming_close",
            name="revision_ohlc_shape",
        ),
        CheckConstraint(
            "previous_fetched_at >= ts AND incoming_fetched_at >= ts "
            "AND previous_as_of >= previous_fetched_at "
            "AND incoming_as_of >= incoming_fetched_at",
            name="revision_availability_order",
        ),
        CheckConstraint(
            "(previous_recorded_at IS NULL AND incoming_recorded_at IS NULL) OR "
            "(previous_recorded_at IS NOT NULL AND incoming_recorded_at IS NOT NULL "
            "AND previous_recorded_at < incoming_recorded_at "
            "AND previous_recorded_at >= previous_as_of "
            "AND incoming_recorded_at >= incoming_as_of "
            "AND incoming_recorded_at = revised_at "
            "AND previous_as_of < incoming_as_of "
            "AND previous_fetched_at < incoming_fetched_at)",
            name="revision_version_evidence",
        ),
        CheckConstraint(
            "previous_creator_xid >= 0 AND incoming_creator_xid >= 0",
            name="creator_xids_nonnegative",
        ),
        Index("ix_bars_revisions_conflict_key", "symbol", "timespan", "multiplier", "ts"),
        Index("ix_bars_revisions_revised_at", "revised_at"),
        Index(
            "ix_bars_revisions_series_version",
            "symbol",
            "timespan",
            "multiplier",
            "source",
            "adjustment_basis",
            "ts",
            "incoming_recorded_at",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)

    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    timespan: Mapped[str] = mapped_column(String(16), nullable=False)
    multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    adjustment_basis: Mapped[str] = mapped_column(String(32), nullable=False)

    previous_open: Mapped[float] = mapped_column(Float, nullable=False)
    previous_high: Mapped[float] = mapped_column(Float, nullable=False)
    previous_low: Mapped[float] = mapped_column(Float, nullable=False)
    previous_close: Mapped[float] = mapped_column(Float, nullable=False)
    previous_volume: Mapped[float] = mapped_column(Float, nullable=False)
    previous_vwap: Mapped[float | None] = mapped_column(Float)
    previous_trade_count: Mapped[int | None] = mapped_column(Integer)
    previous_fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    previous_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    incoming_open: Mapped[float] = mapped_column(Float, nullable=False)
    incoming_high: Mapped[float] = mapped_column(Float, nullable=False)
    incoming_low: Mapped[float] = mapped_column(Float, nullable=False)
    incoming_close: Mapped[float] = mapped_column(Float, nullable=False)
    incoming_volume: Mapped[float] = mapped_column(Float, nullable=False)
    incoming_vwap: Mapped[float | None] = mapped_column(Float)
    incoming_trade_count: Mapped[int | None] = mapped_column(Integer)
    incoming_fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    incoming_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    previous_recorded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    incoming_recorded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    previous_creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    incoming_creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)

    revised_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BarVersionAvailability(Base):
    """DB-stamped post-commit publication receipt for one exact bar version."""

    __tablename__ = "bar_version_availability"
    __table_args__ = (
        CheckConstraint("multiplier >= 1", name="availability_multiplier_positive"),
        CheckConstraint(
            "available_at >= version_recorded_at",
            name="availability_not_before_recording",
        ),
        Index(
            "ix_bar_version_availability_series_time",
            "symbol",
            "timespan",
            "multiplier",
            "source",
            "adjustment_basis",
            "ts",
            "available_at",
        ),
        # The primary key identifies the exact bar version; including the
        # DB-stamped receipt time in a second candidate key lets immutable
        # evidence rows bind the receipt timestamp itself through a composite
        # foreign key rather than copying an unchecked scalar.
        UniqueConstraint(
            "symbol",
            "timespan",
            "multiplier",
            "ts",
            "source",
            "adjustment_basis",
            "version_recorded_at",
            "available_at",
            name="uq_bar_version_availability_exact_receipt",
        ),
    )

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    timespan: Mapped[str] = mapped_column(String(16), primary_key=True)
    multiplier: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    adjustment_basis: Mapped[str] = mapped_column(String(32), primary_key=True)
    version_recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.clock_timestamp(),
    )
