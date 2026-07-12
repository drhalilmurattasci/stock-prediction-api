"""reject non-finite OHLCV values at the database layer

Revision ID: 0004_bars_finiteness
Revises: 0003_bars_series_index
Create Date: 2026-07-12

Postgres orders NaN GREATER than every other value (``NaN >= 0`` and
``NaN = NaN`` are both TRUE), so the existing nonnegativity CHECKs cannot
exclude NaN or +Infinity — yet the read contract is finite-only, so a stored
non-finite value would 500 every read of its page until manually corrected.
``col < 'Infinity'::float8`` is FALSE for both NaN and +Infinity (and
-Infinity already fails ``>= 0``), yielding finite-nonnegative storage.
The ingestion DTOs also reject non-finite floats (allow_inf_nan=False);
this is the belt-and-suspenders storage guarantee behind them.
"""

from __future__ import annotations

from alembic import op

revision: str = "0004_bars_finiteness"
down_revision: str | None = "0003_bars_series_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        op.f("ck_bars_ohlcv_finite"),
        "bars",
        "open < 'Infinity'::float8 AND high < 'Infinity'::float8 "
        "AND low < 'Infinity'::float8 AND close < 'Infinity'::float8 "
        "AND volume < 'Infinity'::float8",
    )
    op.create_check_constraint(
        op.f("ck_bars_vwap_finite"),
        "bars",
        "vwap IS NULL OR vwap < 'Infinity'::float8",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_bars_vwap_finite"), "bars", type_="check")
    op.drop_constraint(op.f("ck_bars_ohlcv_finite"), "bars", type_="check")
