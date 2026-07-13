#!/bin/sh
set -eu

# The bootstrap POSTGRES_USER owns the database and runs Alembic only. Runtime
# API/worker and snapshot-builder processes connect as fixed, non-owner roles.
# This script is one transaction: hostile pre-existing state cannot be partly
# hardened and then accidentally left LOGIN-enabled when a later audit fails.
: "${POSTGRES_APP_PASSWORD:?POSTGRES_APP_PASSWORD must be set}"
: "${POSTGRES_SNAPSHOT_BUILDER_PASSWORD:?POSTGRES_SNAPSHOT_BUILDER_PASSWORD must be set}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 <<'SQL'
\getenv app_password POSTGRES_APP_PASSWORD
\getenv snapshot_builder_password POSTGRES_SNAPSHOT_BUILDER_PASSWORD
\getenv db_name POSTGRES_DB

BEGIN;

SELECT 'CREATE ROLE stockapi_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_app')
\gexec

SELECT 'CREATE ROLE stockapi_snapshot_builder NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder')
\gexec

-- Disable both principals before touching any other state. This is
-- transactional, so a failed audit restores the exact pre-run state.
ALTER ROLE stockapi_app WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS NOINHERIT;
ALTER ROLE stockapi_snapshot_builder WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS NOINHERIT;

ALTER ROLE stockapi_app RESET ALL;
ALTER ROLE stockapi_app SET search_path TO pg_catalog, public;
ALTER ROLE stockapi_snapshot_builder RESET ALL;
ALTER ROLE stockapi_snapshot_builder SET search_path TO pg_catalog, public;

-- Remove database-specific settings in every database. The sole allowed
-- cluster-level setting is the pinned search path above.
SELECT format('ALTER ROLE stockapi_app IN DATABASE %I RESET ALL', database.datname)
FROM pg_db_role_setting AS setting
JOIN pg_roles AS role ON role.oid = setting.setrole
JOIN pg_database AS database ON database.oid = setting.setdatabase
WHERE role.rolname = 'stockapi_app' AND setting.setdatabase <> 0
\gexec

SELECT format(
  'ALTER ROLE stockapi_snapshot_builder IN DATABASE %I RESET ALL',
  database.datname
)
FROM pg_db_role_setting AS setting
JOIN pg_roles AS role ON role.oid = setting.setrole
JOIN pg_database AS database ON database.oid = setting.setdatabase
WHERE role.rolname = 'stockapi_snapshot_builder' AND setting.setdatabase <> 0
\gexec

-- NOINHERIT does not prevent SET ROLE. Remove every membership edge in both
-- directions before either principal can log in.
SELECT format('REVOKE %I FROM stockapi_app', granted.rolname)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted ON granted.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE member_role.rolname = 'stockapi_app'
\gexec

SELECT format('REVOKE stockapi_app FROM %I', member_role.rolname)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted ON granted.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE granted.rolname = 'stockapi_app'
\gexec

SELECT format('REVOKE %I FROM stockapi_snapshot_builder', granted.rolname)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted ON granted.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE member_role.rolname = 'stockapi_snapshot_builder'
\gexec

SELECT format('REVOKE stockapi_snapshot_builder FROM %I', member_role.rolname)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted ON granted.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE granted.rolname = 'stockapi_snapshot_builder'
\gexec

DO $$
DECLARE
  app_oid oid;
  builder_oid oid;
  current_db_oid oid;
BEGIN
  SELECT oid INTO STRICT app_oid FROM pg_roles WHERE rolname = 'stockapi_app';
  SELECT oid INTO STRICT builder_oid
  FROM pg_roles
  WHERE rolname = 'stockapi_snapshot_builder';
  SELECT oid INTO STRICT current_db_oid
  FROM pg_database
  WHERE datname = current_database();

  -- pg_shdepend is cluster-wide, unlike pg_class/pg_proc in one database.
  IF EXISTS (
    SELECT 1 FROM pg_shdepend
    WHERE refclassid = 'pg_authid'::regclass
      AND refobjid = app_oid
      AND deptype = 'o'
  ) THEN
    RAISE EXCEPTION 'stockapi_app owns database objects; transfer ownership before runtime bootstrap';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_shdepend
    WHERE refclassid = 'pg_authid'::regclass
      AND refobjid = builder_oid
      AND deptype = 'o'
  ) THEN
    RAISE EXCEPTION 'stockapi_snapshot_builder owns database objects; transfer ownership before runtime bootstrap';
  END IF;

  -- Explicit ACLs outside this dedicated application database cannot be
  -- safely scrubbed while connected here. Fail instead of leaving a hidden
  -- cross-database capability. Current-DB ACLs are normalized by migrations.
  IF EXISTS (
    SELECT 1 FROM pg_shdepend
    WHERE refclassid = 'pg_authid'::regclass
      AND refobjid IN (app_oid, builder_oid)
      AND deptype = 'a'
      AND NOT (
        dbid = current_db_oid
        OR (
          dbid = 0
          AND classid = 'pg_database'::regclass
          AND objid = current_db_oid
        )
      )
  ) THEN
    RAISE EXCEPTION 'runtime roles retain explicit ACLs in another database';
  END IF;
END;
$$;

-- This database is dedicated to the application. PUBLIC must not restore
-- CONNECT/TEMP or schema creation after direct role revocation.
SELECT format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', :'db_name')
\gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

SELECT format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM stockapi_app', :'db_name')
\gexec
REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_app;
SELECT format('GRANT CONNECT ON DATABASE %I TO stockapi_app', :'db_name')
\gexec
GRANT USAGE ON SCHEMA public TO stockapi_app;

SELECT format(
  'REVOKE ALL PRIVILEGES ON DATABASE %I FROM stockapi_snapshot_builder',
  :'db_name'
)
\gexec
REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_snapshot_builder;
SELECT format('GRANT CONNECT ON DATABASE %I TO stockapi_snapshot_builder', :'db_name')
\gexec
GRANT USAGE ON SCHEMA public TO stockapi_snapshot_builder;

-- Credentials are the last mutations. Every audit and privilege prerequisite
-- above must succeed before either principal becomes usable.
SELECT format(
  'ALTER ROLE stockapi_app WITH LOGIN PASSWORD %L VALID UNTIL ''infinity'' CONNECTION LIMIT -1',
  :'app_password'
)
\gexec
SELECT format(
  'ALTER ROLE stockapi_snapshot_builder WITH LOGIN PASSWORD %L VALID UNTIL ''infinity'' CONNECTION LIMIT -1',
  :'snapshot_builder_password'
)
\gexec

COMMIT;
SQL
