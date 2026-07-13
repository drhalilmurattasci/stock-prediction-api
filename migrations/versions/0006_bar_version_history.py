"""make bar version history DB-recorded and append-only

Revision ID: 0006_bar_version_history
Revises: 0005_forecast_input_snapshots
Create Date: 2026-07-12

``as_of`` remains the conservative data-availability cutoff supplied by the
ingest boundary. ``recorded_at`` is distinct: PostgreSQL stamps each version's
write-acceptance time (transaction commit and visibility may be later). A row
trigger captures every value-changing UPDATE into ``bars_revisions`` and
rejects non-monotonic or metadata-only updates. DELETE/TRUNCATE of current bars
and mutation of revision history fail.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0006_bar_version_history"
down_revision: str | None = "0005_forecast_input_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing rows become conservatively visible at migration time. We do not
    # invent an earlier historical commit timestamp for legacy data.
    op.add_column(
        "bars",
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
    )
    op.add_column(
        "bars_revisions",
        sa.Column("previous_recorded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "bars_revisions",
        sa.Column("incoming_recorded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_bars_fetched_not_before_bar"),
        "bars",
        "fetched_at >= ts",
    )
    op.create_check_constraint(
        op.f("ck_bars_as_of_not_before_fetch"),
        "bars",
        "as_of >= fetched_at",
    )
    op.create_check_constraint(
        op.f("ck_bars_recorded_not_before_as_of"),
        "bars",
        "recorded_at >= as_of",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_ohlcv_nonnegative"),
        "bars_revisions",
        "previous_open >= 0 AND previous_high >= 0 AND previous_low >= 0 "
        "AND previous_close >= 0 AND previous_volume >= 0 "
        "AND incoming_open >= 0 AND incoming_high >= 0 AND incoming_low >= 0 "
        "AND incoming_close >= 0 AND incoming_volume >= 0",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_ohlcv_finite"),
        "bars_revisions",
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
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_vwap_finite_nonnegative"),
        "bars_revisions",
        "(previous_vwap IS NULL OR (previous_vwap >= 0 "
        "AND previous_vwap < 'Infinity'::float8)) "
        "AND (incoming_vwap IS NULL OR (incoming_vwap >= 0 "
        "AND incoming_vwap < 'Infinity'::float8))",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_trade_count_nonnegative"),
        "bars_revisions",
        "(previous_trade_count IS NULL OR previous_trade_count >= 0) "
        "AND (incoming_trade_count IS NULL OR incoming_trade_count >= 0)",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_ohlc_shape"),
        "bars_revisions",
        "previous_high >= previous_low "
        "AND previous_high >= previous_open AND previous_high >= previous_close "
        "AND previous_low <= previous_open AND previous_low <= previous_close "
        "AND incoming_high >= incoming_low "
        "AND incoming_high >= incoming_open AND incoming_high >= incoming_close "
        "AND incoming_low <= incoming_open AND incoming_low <= incoming_close",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_availability_order"),
        "bars_revisions",
        "previous_fetched_at >= ts AND incoming_fetched_at >= ts "
        "AND previous_as_of >= previous_fetched_at "
        "AND incoming_as_of >= incoming_fetched_at",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_revision_version_evidence"),
        "bars_revisions",
        "(previous_recorded_at IS NULL AND incoming_recorded_at IS NULL) OR "
        "(previous_recorded_at IS NOT NULL AND incoming_recorded_at IS NOT NULL "
        "AND previous_recorded_at < incoming_recorded_at "
        "AND previous_recorded_at >= previous_as_of "
        "AND incoming_recorded_at >= incoming_as_of "
        "AND incoming_recorded_at = revised_at "
        "AND previous_as_of < incoming_as_of "
        "AND previous_fetched_at < incoming_fetched_at)",
    )
    op.create_index(
        "ix_bars_revisions_series_version",
        "bars_revisions",
        [
            "symbol",
            "timespan",
            "multiplier",
            "source",
            "adjustment_basis",
            "ts",
            "incoming_recorded_at",
        ],
    )

    op.execute(
        """
        CREATE FUNCTION version_bar_write()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            recorded timestamptz;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.recorded_at := clock_timestamp();
                RETURN NEW;
            END IF;
            recorded := GREATEST(
                clock_timestamp(),
                OLD.recorded_at + interval '1 microsecond'
            );

            IF ROW(OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                   OLD.source, OLD.adjustment_basis)
               IS DISTINCT FROM
               ROW(NEW.symbol, NEW.timespan, NEW.multiplier, NEW.ts,
                   NEW.source, NEW.adjustment_basis) THEN
                RAISE EXCEPTION 'bar conflict keys are immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF ROW(OLD.open, OLD.high, OLD.low, OLD.close, OLD.volume,
                   OLD.vwap, OLD.trade_count)
               IS NOT DISTINCT FROM
               ROW(NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume,
                   NEW.vwap, NEW.trade_count) THEN
                RETURN NULL;
            END IF;
            IF NEW.as_of <= OLD.as_of OR NEW.fetched_at <= OLD.fetched_at THEN
                RAISE EXCEPTION 'changed bars require newer fetched_at and as_of'
                    USING ERRCODE = '55000';
            END IF;

            NEW.recorded_at := recorded;
            INSERT INTO public.bars_revisions (
                symbol, timespan, multiplier, ts, source, adjustment_basis,
                previous_open, previous_high, previous_low, previous_close,
                previous_volume, previous_vwap, previous_trade_count,
                previous_fetched_at, previous_as_of,
                incoming_open, incoming_high, incoming_low, incoming_close,
                incoming_volume, incoming_vwap, incoming_trade_count,
                incoming_fetched_at, incoming_as_of,
                previous_recorded_at, incoming_recorded_at, revised_at
            ) VALUES (
                OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                OLD.source, OLD.adjustment_basis,
                OLD.open, OLD.high, OLD.low, OLD.close,
                OLD.volume, OLD.vwap, OLD.trade_count,
                OLD.fetched_at, OLD.as_of,
                NEW.open, NEW.high, NEW.low, NEW.close,
                NEW.volume, NEW.vwap, NEW.trade_count,
                NEW.fetched_at, NEW.as_of,
                OLD.recorded_at, recorded, recorded
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute("REVOKE ALL ON FUNCTION version_bar_write() FROM PUBLIC")
    op.execute(
        """
        CREATE FUNCTION reject_bar_history_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'bar version history is append-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION require_bar_revision_version_evidence()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.previous_recorded_at IS NULL OR NEW.incoming_recorded_at IS NULL THEN
                RAISE EXCEPTION 'new bar revisions require DB-recorded version evidence'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_version_write
        BEFORE INSERT OR UPDATE ON bars
        FOR EACH ROW EXECUTE FUNCTION version_bar_write()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_no_delete
        BEFORE DELETE ON bars
        FOR EACH ROW EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_no_truncate
        BEFORE TRUNCATE ON bars
        FOR EACH STATEMENT EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_revisions_require_evidence
        BEFORE INSERT ON bars_revisions
        FOR EACH ROW EXECUTE FUNCTION require_bar_revision_version_evidence()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_revisions_no_row_mutation
        BEFORE UPDATE OR DELETE ON bars_revisions
        FOR EACH ROW EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bars_revisions_no_truncate
        BEFORE TRUNCATE ON bars_revisions
        FOR EACH STATEMENT EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )
    op.execute("REVOKE ALL ON TABLE bars, bars_revisions, forecast_input_snapshots FROM PUBLIC")
    op.execute("REVOKE ALL ON SEQUENCE bars_revisions_id_seq FROM PUBLIC")
    op.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    op.execute(
        "REVOKE ALL ON FUNCTION public.version_bar_write(), "
        "public.reject_bar_history_mutation(), "
        "public.require_bar_revision_version_evidence(), "
        "public.stamp_forecast_input_snapshot_sealed_at(), "
        "public.reject_forecast_input_snapshot_mutation() FROM PUBLIC"
    )
    op.execute(
        """
        DO $$
        DECLARE
            runtime_role oid;
        BEGIN
            SELECT oid INTO runtime_role
            FROM pg_roles
            WHERE rolname = 'stockapi_app';
            IF runtime_role IS NULL THEN
                RAISE EXCEPTION 'required runtime role stockapi_app is missing'
                    USING HINT = 'run scripts/db-init/02-runtime-role.sh as the '
                                 'database owner before Alembic';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE oid = runtime_role
                  AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication
                       OR rolbypassrls OR rolinherit OR NOT rolcanlogin)
            ) THEN
                RAISE EXCEPTION 'stockapi_app has unsafe runtime attributes'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF EXISTS (
                SELECT 1 FROM pg_auth_members
                WHERE member = runtime_role OR roleid = runtime_role
            ) THEN
                RAISE EXCEPTION 'stockapi_app retains a cluster-role membership'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_db_role_setting
                WHERE setrole = runtime_role
                  AND setdatabase = 0
                  AND setconfig = ARRAY['search_path=pg_catalog, public']::text[]
            ) OR EXISTS (
                SELECT 1 FROM pg_db_role_setting
                WHERE setrole = runtime_role
                  AND (
                    setdatabase <> 0
                    OR setconfig <> ARRAY['search_path=pg_catalog, public']::text[]
                  )
            ) THEN
                RAISE EXCEPTION 'stockapi_app does not have the pinned runtime search_path'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_database WHERE datdba = runtime_role)
               OR EXISTS (
                   SELECT 1 FROM pg_namespace
                   WHERE nspname = 'public' AND nspowner = runtime_role
               )
               OR EXISTS (
                   SELECT 1
                   FROM pg_class AS object
                   JOIN pg_namespace AS namespace ON namespace.oid = object.relnamespace
                   WHERE namespace.nspname = 'public'
                     AND object.relname IN (
                         'bars', 'bars_revisions', 'bars_revisions_id_seq',
                         'forecast_input_snapshots'
                     )
                     AND object.relowner = runtime_role
               )
               OR EXISTS (
                   SELECT 1
                   FROM pg_proc AS object
                   WHERE object.proowner = runtime_role
               ) THEN
                RAISE EXCEPTION 'stockapi_app owns protected database objects'
                    USING HINT = 'transfer ownership to the migration owner before Alembic';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM pg_shdepend
                WHERE refclassid = 'pg_authid'::regclass
                  AND refobjid = runtime_role
                  AND deptype = 'o'
            ) THEN
                RAISE EXCEPTION 'stockapi_app owns an object in the cluster'
                    USING HINT = 'transfer ownership before Alembic';
            END IF;

            -- REVOKE by one grantor cannot remove a grant issued by another.
            -- Ownership is empty, so this safely scrubs every current-DB ACL
            -- dependency before the exact runtime boundary is rebuilt.
            EXECUTE 'DROP OWNED BY stockapi_app';
            EXECUTE format(
                'REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC',
                current_database()
            );
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON DATABASE %I FROM stockapi_app',
                current_database()
            );
            EXECUTE format(
                'GRANT CONNECT ON DATABASE %I TO stockapi_app',
                current_database()
            );
            EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_app';
            EXECUTE 'GRANT USAGE ON SCHEMA public TO stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public '
                    'FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public '
                    'FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public '
                    'FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars_revisions '
                    'FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_input_snapshots '
                    'FROM stockapi_app';
            EXECUTE 'REVOKE ALL PRIVILEGES ON SEQUENCE public.bars_revisions_id_seq '
                    'FROM stockapi_app';
            EXECUTE 'GRANT SELECT, INSERT, UPDATE ON TABLE public.bars TO stockapi_app';
            EXECUTE 'GRANT SELECT ON TABLE public.bars_revisions TO stockapi_app';
            EXECUTE 'GRANT SELECT ON TABLE public.forecast_input_snapshots TO stockapi_app';

            IF NOT has_database_privilege(
                runtime_role, current_database(), 'CONNECT'
            ) OR has_database_privilege(
                runtime_role, current_database(), 'TEMPORARY'
            ) THEN
                RAISE EXCEPTION 'stockapi_app database privileges are not exact';
            END IF;
            IF NOT has_schema_privilege(runtime_role, 'public', 'USAGE')
               OR has_schema_privilege(runtime_role, 'public', 'CREATE') THEN
                RAISE EXCEPTION 'stockapi_app schema privileges are not exact';
            END IF;
            IF NOT has_table_privilege(runtime_role, 'public.bars', 'SELECT')
               OR NOT has_table_privilege(runtime_role, 'public.bars', 'INSERT')
               OR NOT has_table_privilege(runtime_role, 'public.bars', 'UPDATE')
               OR has_table_privilege(runtime_role, 'public.bars', 'DELETE')
               OR has_table_privilege(runtime_role, 'public.bars', 'TRUNCATE')
               OR has_table_privilege(runtime_role, 'public.bars', 'REFERENCES')
               OR has_table_privilege(runtime_role, 'public.bars', 'TRIGGER') THEN
                RAISE EXCEPTION 'stockapi_app bars privileges are not exact';
            END IF;
            IF NOT has_table_privilege(
                runtime_role, 'public.bars_revisions', 'SELECT'
            ) OR has_table_privilege(
                runtime_role, 'public.bars_revisions', 'INSERT'
            ) OR has_table_privilege(
                runtime_role, 'public.bars_revisions', 'UPDATE'
            ) OR has_table_privilege(
                runtime_role, 'public.bars_revisions', 'DELETE'
            ) OR has_table_privilege(
                runtime_role, 'public.bars_revisions', 'TRUNCATE'
            ) OR has_any_column_privilege(
                runtime_role, 'public.bars_revisions', 'INSERT'
            ) OR has_any_column_privilege(
                runtime_role, 'public.bars_revisions', 'UPDATE'
            ) THEN
                RAISE EXCEPTION 'stockapi_app revision privileges are not exact';
            END IF;
            IF NOT has_table_privilege(
                runtime_role, 'public.forecast_input_snapshots', 'SELECT'
            ) OR has_table_privilege(
                runtime_role, 'public.forecast_input_snapshots', 'INSERT'
            ) OR has_table_privilege(
                runtime_role, 'public.forecast_input_snapshots', 'UPDATE'
            ) OR has_table_privilege(
                runtime_role, 'public.forecast_input_snapshots', 'DELETE'
            ) OR has_table_privilege(
                runtime_role, 'public.forecast_input_snapshots', 'TRUNCATE'
            ) THEN
                RAISE EXCEPTION 'stockapi_app snapshot privileges are not exact';
            END IF;
            IF has_sequence_privilege(
                runtime_role, 'public.bars_revisions_id_seq', 'USAGE'
            ) OR has_sequence_privilege(
                runtime_role, 'public.bars_revisions_id_seq', 'SELECT'
            ) OR has_sequence_privilege(
                runtime_role, 'public.bars_revisions_id_seq', 'UPDATE'
            ) THEN
                RAISE EXCEPTION 'stockapi_app retains revision-sequence privileges';
            END IF;
            IF has_function_privilege(
                runtime_role, 'public.version_bar_write()', 'EXECUTE'
            ) OR has_function_privilege(
                runtime_role, 'public.reject_bar_history_mutation()', 'EXECUTE'
            ) OR has_function_privilege(
                runtime_role,
                'public.require_bar_revision_version_evidence()',
                'EXECUTE'
            ) OR has_function_privilege(
                runtime_role,
                'public.stamp_forecast_input_snapshot_sealed_at()',
                'EXECUTE'
            ) OR has_function_privilege(
                runtime_role,
                'public.reject_forecast_input_snapshot_mutation()',
                'EXECUTE'
            ) THEN
                RAISE EXCEPTION 'stockapi_app can directly execute a trigger function';
            END IF;
        END;
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_app') THEN
                EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_app';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars FROM stockapi_app';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars_revisions '
                        'FROM stockapi_app';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_input_snapshots '
                        'FROM stockapi_app';
                EXECUTE 'REVOKE ALL PRIVILEGES ON SEQUENCE public.bars_revisions_id_seq '
                        'FROM stockapi_app';
                EXECUTE 'GRANT USAGE ON SCHEMA public TO stockapi_app';
                EXECUTE 'GRANT SELECT, INSERT, UPDATE ON TABLE public.bars TO stockapi_app';
                EXECUTE 'GRANT SELECT, INSERT ON TABLE public.bars_revisions TO stockapi_app';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE public.bars_revisions_id_seq '
                        'TO stockapi_app';
                EXECUTE 'GRANT SELECT ON TABLE public.forecast_input_snapshots TO stockapi_app';
            END IF;
        END;
        $$
        """
    )
    for trigger, table in (
        ("bars_revisions_no_truncate", "bars_revisions"),
        ("bars_revisions_no_row_mutation", "bars_revisions"),
        ("bars_revisions_require_evidence", "bars_revisions"),
        ("bars_no_truncate", "bars"),
        ("bars_no_delete", "bars"),
        ("bars_version_write", "bars"),
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {trigger} ON {table}")
    op.execute("DROP FUNCTION IF EXISTS reject_bar_history_mutation()")
    op.execute("DROP FUNCTION IF EXISTS require_bar_revision_version_evidence()")
    op.execute("DROP FUNCTION IF EXISTS version_bar_write()")
    op.drop_index("ix_bars_revisions_series_version", table_name="bars_revisions")
    for name in (
        "revision_version_evidence",
        "revision_availability_order",
        "revision_ohlc_shape",
        "revision_trade_count_nonnegative",
        "revision_vwap_finite_nonnegative",
        "revision_ohlcv_finite",
        "revision_ohlcv_nonnegative",
    ):
        op.drop_constraint(op.f(f"ck_bars_revisions_{name}"), "bars_revisions", type_="check")
    op.drop_constraint(op.f("ck_bars_as_of_not_before_fetch"), "bars", type_="check")
    op.drop_constraint(op.f("ck_bars_recorded_not_before_as_of"), "bars", type_="check")
    op.drop_constraint(op.f("ck_bars_fetched_not_before_bar"), "bars", type_="check")
    op.drop_column("bars_revisions", "incoming_recorded_at")
    op.drop_column("bars_revisions", "previous_recorded_at")
    op.drop_column("bars", "recorded_at")
