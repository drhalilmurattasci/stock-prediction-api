"""add immutable content-addressed forecast input snapshots

Revision ID: 0005_forecast_input_snapshots
Revises: 0004_bars_finiteness
Create Date: 2026-07-12

Snapshots are born sealed as one canonical byte payload. There is no draft
state for readers to observe: one INSERT is the commit boundary, PostgreSQL
verifies the SHA-256, and triggers reject UPDATE, DELETE, and TRUNCATE.
The resolution-policy hash versions calendar/source/availability rules; retries
reuse their persisted proof timestamp so the semantic unique key is idempotent.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0005_forecast_input_snapshots"
down_revision: str | None = "0004_bars_finiteness"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The extension may be shared by later integrity features. Downgrade leaves
    # it installed because this migration cannot know whether it pre-existed.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.create_table(
        "forecast_input_snapshots",
        sa.Column("snapshot_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=32), nullable=False),
        sa.Column("horizon_unit", sa.String(length=32), nullable=False),
        sa.Column("series_basis", sa.String(length=32), nullable=False),
        sa.Column("input_timespan", sa.String(length=16), nullable=False),
        sa.Column("input_multiplier", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "sealed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column("observation_count", sa.Integer(), nullable=False),
        sa.Column("target_time_count", sa.Integer(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("max_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("availability_status", sa.String(length=16), nullable=False),
        sa.Column("availability_rule_set_hash", sa.String(length=71), nullable=True),
        sa.Column("availability_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("canonical_payload", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_input_snapshots_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_forecast_input_snapshots_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_forecast_input_snapshots_symbol_format"),
        ),
        sa.CheckConstraint(
            "target IN ('close', 'adjusted_close', 'return', 'log_return')",
            name=op.f("ck_forecast_input_snapshots_target_supported"),
        ),
        sa.CheckConstraint(
            "horizon_unit IN ('trading_day', 'calendar_day', 'minute', 'hour', 'week')",
            name=op.f("ck_forecast_input_snapshots_horizon_unit_supported"),
        ),
        sa.CheckConstraint(
            "series_basis IN ('raw', 'split_adjusted', 'split_dividend_adjusted')",
            name=op.f("ck_forecast_input_snapshots_series_basis_supported"),
        ),
        sa.CheckConstraint(
            "input_timespan IN ('minute', 'hour', 'day', 'week')",
            name=op.f("ck_forecast_input_snapshots_input_timespan_supported"),
        ),
        sa.CheckConstraint(
            "input_multiplier BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_input_snapshots_input_multiplier_bounded"),
        ),
        sa.CheckConstraint(
            "(target = 'close' AND series_basis = 'raw') OR "
            "(target = 'adjusted_close' AND series_basis <> 'raw') OR "
            "target IN ('return', 'log_return')",
            name=op.f("ck_forecast_input_snapshots_target_series_basis"),
        ),
        sa.CheckConstraint(
            "(target IN ('close', 'adjusted_close') AND currency IS NOT NULL) OR "
            "(target IN ('return', 'log_return') AND currency IS NULL)",
            name=op.f("ck_forecast_input_snapshots_target_currency"),
        ),
        sa.CheckConstraint(
            "currency IS NULL OR currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_forecast_input_snapshots_currency_format"),
        ),
        sa.CheckConstraint(
            "snapshot_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_input_snapshots_snapshot_id_format"),
        ),
        sa.CheckConstraint(
            "resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_input_snapshots_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "availability_rule_set_hash IS NULL OR "
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_input_snapshots_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "observation_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_input_snapshots_observation_count_bounded"),
        ),
        sa.CheckConstraint(
            "target_time_count BETWEEN 1 AND 252",
            name=op.f("ck_forecast_input_snapshots_target_time_count_bounded"),
        ),
        sa.CheckConstraint(
            "first_observed_at <= last_observed_at AND last_observed_at <= as_of",
            name=op.f("ck_forecast_input_snapshots_observation_window"),
        ),
        sa.CheckConstraint(
            "max_available_at <= as_of",
            name=op.f("ck_forecast_input_snapshots_availability_cutoff"),
        ),
        sa.CheckConstraint(
            "sealed_at >= as_of",
            name=op.f("ck_forecast_input_snapshots_sealed_after_cutoff"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_payload) BETWEEN 1 AND 4194304",
            name=op.f("ck_forecast_input_snapshots_payload_size_bounded"),
        ),
        sa.CheckConstraint(
            "snapshot_id = 'sha256:' || encode(digest(canonical_payload, 'sha256'), 'hex')",
            name=op.f("ck_forecast_input_snapshots_payload_hash_matches_id"),
        ),
        sa.CheckConstraint(
            "(availability_status = 'not_run' AND availability_rule_set_hash IS NULL "
            "AND availability_checked_at IS NULL) OR "
            "(availability_status = 'passed' AND availability_rule_set_hash IS NOT NULL "
            "AND availability_checked_at IS NOT NULL "
            "AND availability_checked_at >= max_available_at "
            "AND availability_checked_at >= as_of "
            "AND availability_checked_at <= sealed_at)",
            name=op.f("ck_forecast_input_snapshots_availability_evidence"),
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            name=op.f("pk_forecast_input_snapshots"),
        ),
        sa.UniqueConstraint(
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
    )
    op.create_index(
        "ix_forecast_input_snapshots_resolve",
        "forecast_input_snapshots",
        [
            "resolution_policy_hash",
            "symbol",
            "target",
            "horizon_unit",
            "series_basis",
            "input_timespan",
            "input_multiplier",
            "as_of",
            "snapshot_id",
        ],
    )
    # Ignore any caller-supplied timestamp. ``clock_timestamp`` is the
    # database-observed insertion time; row visibility remains atomic at commit.
    op.execute(
        """
        CREATE FUNCTION stamp_forecast_input_snapshot_sealed_at()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            NEW.sealed_at := clock_timestamp();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER forecast_input_snapshots_stamp_sealed_at
        BEFORE INSERT ON forecast_input_snapshots
        FOR EACH ROW EXECUTE FUNCTION stamp_forecast_input_snapshot_sealed_at()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_forecast_input_snapshot_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'forecast input snapshots are insert-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER forecast_input_snapshots_no_row_mutation
        BEFORE UPDATE OR DELETE ON forecast_input_snapshots
        FOR EACH ROW EXECUTE FUNCTION reject_forecast_input_snapshot_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER forecast_input_snapshots_no_truncate
        BEFORE TRUNCATE ON forecast_input_snapshots
        FOR EACH STATEMENT EXECUTE FUNCTION reject_forecast_input_snapshot_mutation()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_input_snapshots_no_truncate ON forecast_input_snapshots"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_input_snapshots_no_row_mutation "
        "ON forecast_input_snapshots"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_input_snapshots_stamp_sealed_at "
        "ON forecast_input_snapshots"
    )
    op.execute("DROP FUNCTION IF EXISTS reject_forecast_input_snapshot_mutation()")
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_input_snapshot_sealed_at()")
    op.drop_index(
        "ix_forecast_input_snapshots_resolve",
        table_name="forecast_input_snapshots",
    )
    op.drop_table("forecast_input_snapshots")
