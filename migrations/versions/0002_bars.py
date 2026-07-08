"""create bars hypertable and revision ledger

Revision ID: 0002_bars
Revises: 0001_initial
Create Date: 2026-07-08

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0002_bars"
down_revision: str | None = "0001_initial"
branch_labels = None
depends_on = None

BAR_CONFLICT_KEY = ("symbol", "timespan", "multiplier", "ts", "source", "adjustment_basis")


def upgrade() -> None:
    op.create_table(
        "bars",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("timespan", sa.String(length=16), nullable=False),
        sa.Column("multiplier", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("adjustment_basis", sa.String(length=32), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vwap", sa.Float(), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("multiplier >= 1", name=op.f("ck_bars_multiplier_positive")),
        sa.CheckConstraint(
            "open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0",
            name=op.f("ck_bars_ohlcv_nonnegative"),
        ),
        sa.CheckConstraint("vwap IS NULL OR vwap >= 0", name=op.f("ck_bars_vwap_nonnegative")),
        sa.CheckConstraint(
            "trade_count IS NULL OR trade_count >= 0",
            name=op.f("ck_bars_trade_count_nonnegative"),
        ),
        sa.CheckConstraint("high >= low", name=op.f("ck_bars_high_gte_low")),
        sa.CheckConstraint(
            "high >= open AND high >= close",
            name=op.f("ck_bars_high_gte_open_close"),
        ),
        sa.CheckConstraint(
            "low <= open AND low <= close",
            name=op.f("ck_bars_low_lte_open_close"),
        ),
        sa.PrimaryKeyConstraint(*BAR_CONFLICT_KEY, name=op.f("pk_bars")),
    )
    op.create_index("ix_bars_symbol_ts", "bars", ["symbol", "ts"])
    op.create_index("ix_bars_source_as_of", "bars", ["source", "as_of"])
    op.execute("SELECT create_hypertable('bars', 'ts', if_not_exists => TRUE)")

    op.create_table(
        "bars_revisions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("timespan", sa.String(length=16), nullable=False),
        sa.Column("multiplier", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("adjustment_basis", sa.String(length=32), nullable=False),
        sa.Column("previous_open", sa.Float(), nullable=False),
        sa.Column("previous_high", sa.Float(), nullable=False),
        sa.Column("previous_low", sa.Float(), nullable=False),
        sa.Column("previous_close", sa.Float(), nullable=False),
        sa.Column("previous_volume", sa.Float(), nullable=False),
        sa.Column("previous_vwap", sa.Float(), nullable=True),
        sa.Column("previous_trade_count", sa.Integer(), nullable=True),
        sa.Column("previous_fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("previous_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("incoming_open", sa.Float(), nullable=False),
        sa.Column("incoming_high", sa.Float(), nullable=False),
        sa.Column("incoming_low", sa.Float(), nullable=False),
        sa.Column("incoming_close", sa.Float(), nullable=False),
        sa.Column("incoming_volume", sa.Float(), nullable=False),
        sa.Column("incoming_vwap", sa.Float(), nullable=True),
        sa.Column("incoming_trade_count", sa.Integer(), nullable=True),
        sa.Column("incoming_fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("incoming_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revised_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "multiplier >= 1",
            name=op.f("ck_bars_revisions_revision_multiplier_positive"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_bars_revisions")),
    )
    op.create_index(
        "ix_bars_revisions_conflict_key",
        "bars_revisions",
        ["symbol", "timespan", "multiplier", "ts"],
    )
    op.create_index("ix_bars_revisions_revised_at", "bars_revisions", ["revised_at"])


def downgrade() -> None:
    op.drop_index("ix_bars_revisions_revised_at", table_name="bars_revisions")
    op.drop_index("ix_bars_revisions_conflict_key", table_name="bars_revisions")
    op.drop_table("bars_revisions")
    op.drop_index("ix_bars_source_as_of", table_name="bars")
    op.drop_index("ix_bars_symbol_ts", table_name="bars")
    op.drop_table("bars")
