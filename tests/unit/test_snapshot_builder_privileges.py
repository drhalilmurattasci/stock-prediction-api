from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ROLE_INIT = ROOT / "scripts" / "db-init" / "02-runtime-role.sh"
MIGRATION = ROOT / "migrations" / "versions" / "0007_snapshot_builder_privileges.py"
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"


def test_snapshot_builder_role_bootstrap_is_idempotent_and_hardened() -> None:
    role_init = ROLE_INIT.read_text(encoding="utf-8")

    assert (
        "${POSTGRES_SNAPSHOT_BUILDER_PASSWORD:?POSTGRES_SNAPSHOT_BUILDER_PASSWORD must be set}"
        in role_init
    )
    assert "\\getenv snapshot_builder_password POSTGRES_SNAPSHOT_BUILDER_PASSWORD" in role_init
    assert "BEGIN;" in role_init and "COMMIT;" in role_init
    assert (
        "WHERE NOT EXISTS (SELECT 1 FROM pg_roles "
        "WHERE rolname = 'stockapi_snapshot_builder')" in role_init
    )
    no_login = role_init.index("ALTER ROLE stockapi_snapshot_builder WITH NOLOGIN")
    login = role_init.index("ALTER ROLE stockapi_snapshot_builder WITH LOGIN PASSWORD %L")
    assert no_login < role_init.index("pg_shdepend") < login
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT" in role_init
    assert "ALTER ROLE stockapi_snapshot_builder RESET ALL" in role_init
    assert "ALTER ROLE stockapi_snapshot_builder IN DATABASE %I RESET ALL" in role_init
    assert "REVOKE %I FROM stockapi_snapshot_builder" in role_init
    assert "REVOKE stockapi_snapshot_builder FROM %I" in role_init
    assert "refobjid = builder_oid" in role_init
    assert "deptype = 'o'" in role_init
    assert "classid = 'pg_database'::regclass" in role_init
    assert "objid = current_db_oid" in role_init
    assert "REVOKE ALL PRIVILEGES ON DATABASE" in role_init
    assert "REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_snapshot_builder" in role_init
    assert "GRANT CONNECT ON DATABASE %I TO stockapi_snapshot_builder" in role_init
    assert "GRANT USAGE ON SCHEMA public TO stockapi_snapshot_builder" in role_init
    assert "ALTER ROLE stockapi_snapshot_builder SET search_path TO pg_catalog, public" in role_init
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC" in role_init


def test_snapshot_builder_migration_validates_role_before_granting() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "0007_snapshot_builder_privileges"' in migration
    assert 'down_revision: str | None = "0006_bar_version_history"' in migration
    assert "required runtime role stockapi_snapshot_builder is missing" in migration
    assert "stockapi_snapshot_builder has unsafe runtime attributes" in migration
    assert "rolbypassrls OR rolinherit OR NOT rolcanlogin" in migration
    assert "member = builder_role OR roleid = builder_role" in migration
    assert "setconfig = ARRAY['search_path=pg_catalog, public']::text[]" in migration
    assert "pg_database WHERE datdba = builder_role" in migration
    assert "object.relowner = builder_role" in migration
    assert "object.proowner = builder_role" in migration
    assert "DROP OWNED BY stockapi_snapshot_builder" in migration
    assert "pg_shdepend" in migration


def test_snapshot_builder_migration_grants_only_the_insert_boundary() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    upgrade = migration.split("def downgrade() -> None:", maxsplit=1)[0]

    assert "GRANT CONNECT ON DATABASE %I TO stockapi_snapshot_builder" in upgrade
    assert "GRANT USAGE ON SCHEMA public TO stockapi_snapshot_builder" in upgrade
    assert "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public" in upgrade
    assert "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public" in upgrade
    assert "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public" in upgrade
    assert "GRANT SELECT ON TABLE public.bars TO stockapi_snapshot_builder" in upgrade
    assert "GRANT SELECT ON TABLE public.bars_revisions TO stockapi_snapshot_builder" in upgrade
    assert "GRANT SELECT, INSERT ON TABLE public.forecast_input_snapshots " in upgrade
    assert "'TO stockapi_snapshot_builder';" in upgrade
    assert "GRANT UPDATE" not in upgrade
    assert "GRANT DELETE" not in upgrade
    assert "GRANT TRUNCATE" not in upgrade
    assert "GRANT ALL" not in upgrade
    assert "GRANT CREATE" not in upgrade
    assert "GRANT INSERT ON TABLE public.bars" not in upgrade
    assert "GRANT INSERT ON TABLE public.bars_revisions" not in upgrade
    assert "stockapi_app" not in upgrade
    assert "has_any_column_privilege" in upgrade
    assert "snapshot builder snapshot privileges are not exact" in upgrade
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC" in upgrade


def test_snapshot_builder_downgrade_revokes_every_builder_grant() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    downgrade = migration.split("def downgrade() -> None:", maxsplit=1)[1]

    assert "GRANT " not in downgrade
    assert "REVOKE ALL PRIVILEGES ON DATABASE %I" in downgrade
    assert "REVOKE ALL PRIVILEGES ON SCHEMA public" in downgrade
    assert "REVOKE ALL PRIVILEGES ON TABLE public.bars" in downgrade
    assert "REVOKE ALL PRIVILEGES ON TABLE public.bars_revisions" in downgrade
    assert "REVOKE ALL PRIVILEGES ON TABLE public.forecast_input_snapshots" in downgrade
    assert "REVOKE ALL PRIVILEGES ON SEQUENCE public.bars_revisions_id_seq" in downgrade


def test_compose_isolates_builder_credentials_and_queue_from_api_and_worker() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    api = compose.split("\n  api:", maxsplit=1)[1].split("\n  worker:", maxsplit=1)[0]
    worker = compose.split("\n  worker:", maxsplit=1)[1].split("\n  snapshot-builder:", maxsplit=1)[
        0
    ]
    builder = compose.split("\n  snapshot-builder:", maxsplit=1)[1].split("\n  beat:", maxsplit=1)[
        0
    ]

    assert "POSTGRES_SNAPSHOT_BUILDER_PASSWORD" in compose
    assert "stockapi_snapshot_builder:${POSTGRES_SNAPSHOT_BUILDER_URL_PASSWORD}" in builder
    assert '"--queues=snapshot-builder"' in builder
    assert "ingestion.snapshot_celery_app.snapshot_celery_app" in builder
    assert "POLYGON_API_KEY" not in builder
    assert "POSTGRES_SNAPSHOT_BUILDER" not in api
    assert "POSTGRES_SNAPSHOT_BUILDER" not in worker

    env_example = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert "POSTGRES_SNAPSHOT_BUILDER_PASSWORD=" in env_example
    assert "POSTGRES_SNAPSHOT_BUILDER_URL_PASSWORD=" in env_example
    assert "FORECAST_RESOLUTION_POLICY_HASH=" in env_example
    assert "FORECAST_TRUSTED_AVAILABILITY_RULE_SET_HASH=" in env_example
