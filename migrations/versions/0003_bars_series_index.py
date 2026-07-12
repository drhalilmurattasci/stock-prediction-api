"""add covering series index for bounded price reads

Revision ID: 0003_bars_series_index
Revises: 0002_bars
Create Date: 2026-07-12

The /v1/prices read filters (symbol, timespan, multiplier, source,
adjustment_basis) by equality and orders by ts DESC with a LIMIT. The primary
key orders source/adjustment_basis AFTER ts, so it only covers the
(symbol, timespan, multiplier) prefix: a sparse or absent series forces a walk
of the entire prefix across every hypertable chunk and LIMIT stops bounding
work (scan-amplification DoS). This index puts every equality column ahead of
``ts`` so the query is a pure, LIMIT-bounded index range; Postgres scans the
btree backwards to satisfy ``ORDER BY ts DESC`` without a DESC modifier.
"""

from __future__ import annotations

from alembic import op

revision: str = "0003_bars_series_index"
down_revision: str | None = "0002_bars"
branch_labels = None
depends_on = None

BAR_SERIES_INDEX = ("symbol", "timespan", "multiplier", "source", "adjustment_basis", "ts")


def upgrade() -> None:
    op.create_index("ix_bars_series_ts", "bars", list(BAR_SERIES_INDEX))


def downgrade() -> None:
    op.drop_index("ix_bars_series_ts", table_name="bars")
