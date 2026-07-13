"""grant the snapshot builder its narrow write boundary

Revision ID: 0007_snapshot_builder_privileges
Revises: 0006_bar_version_history
Create Date: 2026-07-13

The snapshot builder can read point-in-time bar history and atomically insert a
sealed forecast-input snapshot. It cannot mutate bars, revision history, or an
existing snapshot. Role bootstrap remains separate because Alembic migrations
must not create or password cluster-global login roles.
"""

from __future__ import annotations

from alembic import op

revision: str = "0007_snapshot_builder_privileges"
down_revision: str | None = "0006_bar_version_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # These were established by 0006. Reasserting them prevents privileges
    # inherited through PUBLIC from widening either runtime role's table access.
    op.execute(
        "REVOKE ALL ON TABLE public.bars, public.bars_revisions, "
        "public.forecast_input_snapshots FROM PUBLIC"
    )
    op.execute("REVOKE ALL ON SEQUENCE public.bars_revisions_id_seq FROM PUBLIC")
    op.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    op.execute(
        "REVOKE ALL ON FUNCTION public.version_bar_write(), "
        "public.reject_bar_history_mutation(), "
        "public.require_bar_revision_version_evidence(), "
        "public.stamp_forecast_input_snapshot_sealed_at(), "
        "public.reject_forecast_input_snapshot_mutation() FROM PUBLIC"
    )
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC")
    op.execute(
        """
        DO $$
        DECLARE
            builder_role oid;
        BEGIN
            SELECT oid INTO builder_role
            FROM pg_roles
            WHERE rolname = 'stockapi_snapshot_builder';
            IF builder_role IS NULL THEN
                RAISE EXCEPTION 'required runtime role stockapi_snapshot_builder is missing'
                    USING HINT = 'run scripts/db-init/02-runtime-role.sh as the '
                                 'database owner before Alembic';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE oid = builder_role
                  AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication
                       OR rolbypassrls OR rolinherit OR NOT rolcanlogin)
            ) THEN
                RAISE EXCEPTION 'stockapi_snapshot_builder has unsafe runtime attributes'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF EXISTS (
                SELECT 1
                FROM pg_auth_members
                WHERE member = builder_role OR roleid = builder_role
            ) THEN
                RAISE EXCEPTION 'stockapi_snapshot_builder retains a cluster-role membership'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_db_role_setting
                WHERE setrole = builder_role
                  AND setdatabase = 0
                  AND setconfig = ARRAY['search_path=pg_catalog, public']::text[]
            ) OR EXISTS (
                SELECT 1 FROM pg_db_role_setting
                WHERE setrole = builder_role
                  AND (
                    setdatabase <> 0
                    OR setconfig <> ARRAY['search_path=pg_catalog, public']::text[]
                  )
            ) THEN
                RAISE EXCEPTION 'stockapi_snapshot_builder does not have the pinned search_path'
                    USING HINT = 'rerun scripts/db-init/02-runtime-role.sh before Alembic';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_database WHERE datdba = builder_role)
               OR EXISTS (SELECT 1 FROM pg_namespace WHERE nspowner = builder_role)
               OR EXISTS (
                   SELECT 1
                   FROM pg_class AS object
                   WHERE object.relowner = builder_role
               )
               OR EXISTS (
                   SELECT 1
                   FROM pg_proc AS object
                   WHERE object.proowner = builder_role
               ) THEN
                RAISE EXCEPTION 'stockapi_snapshot_builder owns database objects'
                    USING HINT = 'transfer ownership to the migration owner before Alembic';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM pg_shdepend
                WHERE refclassid = 'pg_authid'::regclass
                  AND refobjid = builder_role
                  AND deptype = 'o'
            ) THEN
                RAISE EXCEPTION 'stockapi_snapshot_builder owns an object in the cluster'
                    USING HINT = 'transfer ownership before Alembic';
            END IF;

            -- REVOKE by one grantor cannot remove grants issued by another.
            -- Ownership was proved empty above, so DROP OWNED safely scrubs
            -- every ACL dependency for this role in the current database.
            EXECUTE 'DROP OWNED BY stockapi_snapshot_builder';
            EXECUTE format(
                'REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC',
                current_database()
            );
            EXECUTE format(
                'GRANT CONNECT ON DATABASE %I TO stockapi_snapshot_builder',
                current_database()
            );
            EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA public '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'GRANT USAGE ON SCHEMA public TO stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars_revisions '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_input_snapshots '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'REVOKE ALL PRIVILEGES ON SEQUENCE public.bars_revisions_id_seq '
                    'FROM stockapi_snapshot_builder';
            EXECUTE 'GRANT SELECT ON TABLE public.bars TO stockapi_snapshot_builder';
            EXECUTE 'GRANT SELECT ON TABLE public.bars_revisions TO stockapi_snapshot_builder';
            EXECUTE 'GRANT SELECT, INSERT ON TABLE public.forecast_input_snapshots '
                    'TO stockapi_snapshot_builder';

            IF NOT has_database_privilege(
                builder_role, current_database(), 'CONNECT'
            ) OR has_database_privilege(
                builder_role, current_database(), 'TEMPORARY'
            ) THEN
                RAISE EXCEPTION 'snapshot builder database privileges are not exact';
            END IF;
            IF NOT has_schema_privilege(builder_role, 'public', 'USAGE')
               OR has_schema_privilege(builder_role, 'public', 'CREATE') THEN
                RAISE EXCEPTION 'snapshot builder schema privileges are not exact';
            END IF;
            IF NOT has_table_privilege(builder_role, 'public.bars', 'SELECT')
               OR has_table_privilege(builder_role, 'public.bars', 'INSERT')
               OR has_table_privilege(builder_role, 'public.bars', 'UPDATE')
               OR has_table_privilege(builder_role, 'public.bars', 'DELETE')
               OR has_table_privilege(builder_role, 'public.bars', 'TRUNCATE')
               OR has_any_column_privilege(builder_role, 'public.bars', 'INSERT')
               OR has_any_column_privilege(builder_role, 'public.bars', 'UPDATE') THEN
                RAISE EXCEPTION 'snapshot builder bars privileges are not exact';
            END IF;
            IF NOT has_table_privilege(builder_role, 'public.bars_revisions', 'SELECT')
               OR has_table_privilege(builder_role, 'public.bars_revisions', 'INSERT')
               OR has_table_privilege(builder_role, 'public.bars_revisions', 'UPDATE')
               OR has_table_privilege(builder_role, 'public.bars_revisions', 'DELETE')
               OR has_table_privilege(builder_role, 'public.bars_revisions', 'TRUNCATE')
               OR has_any_column_privilege(
                    builder_role, 'public.bars_revisions', 'INSERT'
               )
               OR has_any_column_privilege(
                    builder_role, 'public.bars_revisions', 'UPDATE'
               ) THEN
                RAISE EXCEPTION 'snapshot builder revision privileges are not exact';
            END IF;
            IF NOT has_table_privilege(
                builder_role, 'public.forecast_input_snapshots', 'SELECT'
            ) OR NOT has_table_privilege(
                builder_role, 'public.forecast_input_snapshots', 'INSERT'
            ) OR has_table_privilege(
                builder_role, 'public.forecast_input_snapshots', 'UPDATE'
            ) OR has_table_privilege(
                builder_role, 'public.forecast_input_snapshots', 'DELETE'
            ) OR has_table_privilege(
                builder_role, 'public.forecast_input_snapshots', 'TRUNCATE'
            ) OR has_any_column_privilege(
                builder_role, 'public.forecast_input_snapshots', 'UPDATE'
            ) THEN
                RAISE EXCEPTION 'snapshot builder snapshot privileges are not exact';
            END IF;
            IF has_sequence_privilege(
                builder_role, 'public.bars_revisions_id_seq', 'USAGE'
            ) OR has_sequence_privilege(
                builder_role, 'public.bars_revisions_id_seq', 'SELECT'
            ) OR has_sequence_privilege(
                builder_role, 'public.bars_revisions_id_seq', 'UPDATE'
            ) THEN
                RAISE EXCEPTION 'snapshot builder retains sequence privileges';
            END IF;
            IF has_function_privilege(
                builder_role, 'public.version_bar_write()', 'EXECUTE'
            ) OR has_function_privilege(
                builder_role, 'public.reject_bar_history_mutation()', 'EXECUTE'
            ) OR has_function_privilege(
                builder_role,
                'public.require_bar_revision_version_evidence()',
                'EXECUTE'
            ) OR has_function_privilege(
                builder_role,
                'public.stamp_forecast_input_snapshot_sealed_at()',
                'EXECUTE'
            ) OR has_function_privilege(
                builder_role,
                'public.reject_forecast_input_snapshot_mutation()',
                'EXECUTE'
            ) THEN
                RAISE EXCEPTION 'snapshot builder retains executable project functions';
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
            IF EXISTS (
                SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder'
            ) THEN
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES ON DATABASE %I '
                    'FROM stockapi_snapshot_builder',
                    current_database()
                );
                EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA public '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.bars_revisions '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE public.forecast_input_snapshots '
                        'FROM stockapi_snapshot_builder';
                EXECUTE 'REVOKE ALL PRIVILEGES ON SEQUENCE public.bars_revisions_id_seq '
                        'FROM stockapi_snapshot_builder';
            END IF;
        END;
        $$
        """
    )
