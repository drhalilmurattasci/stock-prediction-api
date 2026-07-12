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


class BarRevision(Base):
    """Append-only record of a vendor restatement/correction to a bar."""

    __tablename__ = "bars_revisions"
    __table_args__ = (
        CheckConstraint("multiplier >= 1", name="revision_multiplier_positive"),
        Index("ix_bars_revisions_conflict_key", "symbol", "timespan", "multiplier", "ts"),
        Index("ix_bars_revisions_revised_at", "revised_at"),
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

    revised_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
