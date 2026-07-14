"""add immutable, byte-verifiable forecast-run records

Revision ID: 0009_forecast_runs
Revises: 0008_bar_version_availability
Create Date: 2026-07-14

Each row atomically archives canonical request and schema-validated response
representations for one sealed input snapshot. PostgreSQL verifies both SHA-256
identifiers, stamps the acceptance time, and rejects UPDATE, DELETE, and
TRUNCATE. Runtime code may only insert and read; neither the snapshot builder
nor PUBLIC receives access. The optional idempotency identity is a full
HMAC-SHA-256 digest, never the caller's raw token.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0009_forecast_runs"
down_revision: str | None = "0008_bar_version_availability"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 0005 installed pgcrypto for snapshot hashes. Reassert the dependency so
    # this migration also remains self-describing on a repaired database.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.create_table(
        "forecast_runs",
        sa.Column("forecast_id", sa.Uuid(), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("origin_kind", sa.String(length=32), nullable=False),
        sa.Column("idempotency_token_digest", sa.String(length=76), nullable=True),
        sa.Column("request_hash", sa.String(length=71), nullable=False),
        sa.Column("opportunity_hash", sa.String(length=71), nullable=False),
        sa.Column("output_hash", sa.String(length=71), nullable=False),
        sa.Column("snapshot_id", sa.String(length=71), nullable=False),
        sa.Column("resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=32), nullable=False),
        sa.Column("horizon", sa.Integer(), nullable=False),
        sa.Column("horizon_unit", sa.String(length=32), nullable=False),
        sa.Column("series_basis", sa.String(length=32), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("max_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("feature_set_hash", sa.String(length=71), nullable=False),
        sa.Column("code_version", sa.String(length=64), nullable=True),
        sa.Column("calibration_set_version", sa.String(length=128), nullable=False),
        sa.Column("calibration_method", sa.String(length=32), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("canonical_request", sa.LargeBinary(), nullable=False),
        sa.Column("canonical_output", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_runs_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "origin_kind IN ('api', 'scheduled_evaluation')",
            name=op.f("ck_forecast_runs_origin_kind_supported"),
        ),
        sa.CheckConstraint(
            "idempotency_token_digest IS NULL OR "
            "idempotency_token_digest ~ '^hmac-sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_idempotency_token_digest_format"),
        ),
        sa.CheckConstraint(
            "request_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_request_hash_format"),
        ),
        sa.CheckConstraint(
            "opportunity_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_opportunity_hash_format"),
        ),
        sa.CheckConstraint(
            "output_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_output_hash_format"),
        ),
        sa.CheckConstraint(
            "snapshot_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_snapshot_id_format"),
        ),
        sa.CheckConstraint(
            "resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "feature_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_runs_feature_set_hash_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_forecast_runs_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_forecast_runs_symbol_format"),
        ),
        sa.CheckConstraint(
            "target IN ('close', 'adjusted_close', 'return', 'log_return')",
            name=op.f("ck_forecast_runs_target_supported"),
        ),
        sa.CheckConstraint(
            "horizon BETWEEN 1 AND 252",
            name=op.f("ck_forecast_runs_horizon_bounded"),
        ),
        sa.CheckConstraint(
            "horizon_unit IN ('trading_day', 'calendar_day', 'minute', 'hour', 'week')",
            name=op.f("ck_forecast_runs_horizon_unit_supported"),
        ),
        sa.CheckConstraint(
            "series_basis IN ('raw', 'split_adjusted', 'split_dividend_adjusted')",
            name=op.f("ck_forecast_runs_series_basis_supported"),
        ),
        sa.CheckConstraint(
            "(target = 'close' AND series_basis = 'raw') OR "
            "(target = 'adjusted_close' AND series_basis <> 'raw') OR "
            "target IN ('return', 'log_return')",
            name=op.f("ck_forecast_runs_target_series_basis"),
        ),
        sa.CheckConstraint(
            "max_available_at <= as_of AND as_of <= generated_at AND generated_at <= recorded_at",
            name=op.f("ck_forecast_runs_time_order"),
        ),
        sa.CheckConstraint(
            "calibration_method IN "
            "('conformal_quantile_regression', 'adaptive_conformal', "
            "'empirical_residual', 'none')",
            name=op.f("ck_forecast_runs_calibration_method_supported"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_request) BETWEEN 1 AND 1048576",
            name=op.f("ck_forecast_runs_request_size_bounded"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_output) BETWEEN 1 AND 4194304",
            name=op.f("ck_forecast_runs_output_size_bounded"),
        ),
        sa.CheckConstraint(
            "request_hash = 'sha256:' || encode(digest(canonical_request, 'sha256'), 'hex')",
            name=op.f("ck_forecast_runs_request_hash_matches_payload"),
        ),
        sa.CheckConstraint(
            "output_hash = 'sha256:' || encode(digest(canonical_output, 'sha256'), 'hex')",
            name=op.f("ck_forecast_runs_output_hash_matches_payload"),
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["forecast_input_snapshots.snapshot_id"],
            name=op.f("fk_forecast_runs_snapshot_id_forecast_input_snapshots"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("forecast_id", name=op.f("pk_forecast_runs")),
        sa.UniqueConstraint(
            "idempotency_token_digest",
            name="uq_forecast_runs_idempotency_token_digest",
        ),
    )
    op.create_index(
        "ix_forecast_runs_opportunity_hash",
        "forecast_runs",
        ["opportunity_hash"],
    )
    op.create_index(
        "uq_forecast_runs_scheduled_opportunity",
        "forecast_runs",
        ["opportunity_hash"],
        unique=True,
        postgresql_where=sa.text("origin_kind = 'scheduled_evaluation'"),
    )

    # The caller cannot choose an audit timestamp. The trigger runs before the
    # time-order CHECK, so a future generated_at is rejected by PostgreSQL.
    op.execute(
        """
        CREATE FUNCTION stamp_forecast_run_recorded_at()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            NEW.recorded_at := clock_timestamp();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_forecast_run_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'forecast runs are insert-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    op.execute("REVOKE ALL ON FUNCTION public.stamp_forecast_run_recorded_at() FROM PUBLIC")
    op.execute("REVOKE ALL ON FUNCTION public.reject_forecast_run_mutation() FROM PUBLIC")
    op.execute(
        """
        CREATE TRIGGER forecast_runs_stamp_recorded_at
        BEFORE INSERT ON forecast_runs
        FOR EACH ROW EXECUTE FUNCTION stamp_forecast_run_recorded_at()
        """
    )
    op.execute(
        """
        CREATE TRIGGER forecast_runs_no_row_mutation
        BEFORE UPDATE OR DELETE ON forecast_runs
        FOR EACH ROW EXECUTE FUNCTION reject_forecast_run_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER forecast_runs_no_truncate
        BEFORE TRUNCATE ON forecast_runs
        FOR EACH STATEMENT EXECUTE FUNCTION reject_forecast_run_mutation()
        """
    )

    op.execute("REVOKE ALL ON TABLE public.forecast_runs FROM PUBLIC")
    op.execute("REVOKE ALL PRIVILEGES ON TABLE public.forecast_runs FROM stockapi_app")
    op.execute("REVOKE ALL PRIVILEGES ON TABLE public.forecast_runs FROM stockapi_snapshot_builder")
    op.execute("GRANT SELECT, INSERT ON TABLE public.forecast_runs TO stockapi_app")
    op.execute(
        "REVOKE ALL ON FUNCTION public.stamp_forecast_run_recorded_at(), "
        "public.reject_forecast_run_mutation() FROM stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        """
        DO $$
        DECLARE
            app_role oid;
            builder_role oid;
        BEGIN
            SELECT oid INTO STRICT app_role
            FROM pg_roles WHERE rolname = 'stockapi_app';
            SELECT oid INTO STRICT builder_role
            FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder';

            IF NOT has_table_privilege(app_role, 'public.forecast_runs', 'SELECT')
               OR NOT has_table_privilege(app_role, 'public.forecast_runs', 'INSERT')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'UPDATE')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'DELETE')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'TRUNCATE')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'REFERENCES')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'TRIGGER')
               OR has_table_privilege(app_role, 'public.forecast_runs', 'MAINTAIN')
               OR has_any_column_privilege(app_role, 'public.forecast_runs', 'UPDATE')
               OR has_any_column_privilege(app_role, 'public.forecast_runs', 'REFERENCES')
            THEN
                RAISE EXCEPTION 'runtime forecast-run privileges are not exact';
            END IF;
            IF has_table_privilege(builder_role, 'public.forecast_runs', 'SELECT')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'INSERT')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'UPDATE')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'DELETE')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'TRUNCATE')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'REFERENCES')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'TRIGGER')
               OR has_table_privilege(builder_role, 'public.forecast_runs', 'MAINTAIN')
               OR has_any_column_privilege(builder_role, 'public.forecast_runs', 'SELECT')
               OR has_any_column_privilege(builder_role, 'public.forecast_runs', 'INSERT')
               OR has_any_column_privilege(builder_role, 'public.forecast_runs', 'UPDATE')
               OR has_any_column_privilege(builder_role, 'public.forecast_runs', 'REFERENCES')
            THEN
                RAISE EXCEPTION 'builder forecast-run privileges are not empty';
            END IF;
            IF has_function_privilege(
                app_role, 'public.stamp_forecast_run_recorded_at()', 'EXECUTE'
            ) OR has_function_privilege(
                app_role, 'public.reject_forecast_run_mutation()', 'EXECUTE'
            ) OR has_function_privilege(
                builder_role, 'public.stamp_forecast_run_recorded_at()', 'EXECUTE'
            ) OR has_function_privilege(
                builder_role, 'public.reject_forecast_run_mutation()', 'EXECUTE'
            ) THEN
                RAISE EXCEPTION 'forecast-run trigger functions are directly executable';
            END IF;
        END;
        $$
        """
    )


def downgrade() -> None:
    # Remove role ACL dependencies before dropping the protected objects. Role
    # checks keep downgrade usable in an offline repair database.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_app') THEN
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_runs '
                        'FROM stockapi_app';
                EXECUTE 'REVOKE ALL ON FUNCTION '
                        'public.stamp_forecast_run_recorded_at(), '
                        'public.reject_forecast_run_mutation() FROM stockapi_app';
            END IF;
            IF EXISTS (
                SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder'
            ) THEN
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_runs '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL ON FUNCTION '
                        'public.stamp_forecast_run_recorded_at(), '
                        'public.reject_forecast_run_mutation() '
                        'FROM stockapi_snapshot_builder';
            END IF;
        END;
        $$
        """
    )
    op.execute("REVOKE ALL ON TABLE public.forecast_runs FROM PUBLIC")
    op.execute("DROP TRIGGER IF EXISTS forecast_runs_no_truncate ON forecast_runs")
    op.execute("DROP TRIGGER IF EXISTS forecast_runs_no_row_mutation ON forecast_runs")
    op.execute("DROP TRIGGER IF EXISTS forecast_runs_stamp_recorded_at ON forecast_runs")
    op.execute("DROP FUNCTION IF EXISTS reject_forecast_run_mutation()")
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_run_recorded_at()")
    op.drop_index(
        "uq_forecast_runs_scheduled_opportunity",
        table_name="forecast_runs",
        postgresql_where=sa.text("origin_kind = 'scheduled_evaluation'"),
    )
    op.drop_index("ix_forecast_runs_opportunity_hash", table_name="forecast_runs")
    op.drop_table("forecast_runs")
