"""initial baseline: enable timescaledb extension

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-06

"""

from __future__ import annotations

from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent: the db-init script also enables this on a fresh container, but
    # tracking it here makes a from-scratch `alembic upgrade head` self-sufficient.
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")


def downgrade() -> None:
    # Extension intentionally left in place on downgrade (other objects may depend on it).
    pass
