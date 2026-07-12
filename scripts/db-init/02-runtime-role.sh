#!/bin/sh
set -eu

# The bootstrap POSTGRES_USER owns the database and runs Alembic only. Runtime
# API/worker processes connect as this fixed, non-owner role. The password stays
# environment-provided; psql's %L quoting prevents SQL injection through it.
: "${POSTGRES_APP_PASSWORD:?POSTGRES_APP_PASSWORD must be set}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=db_name="$POSTGRES_DB" \
  --set=app_password="$POSTGRES_APP_PASSWORD" <<'SQL'
SELECT format(
  'CREATE ROLE stockapi_app LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT',
  :'app_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_app')
\gexec

SELECT format(
  'ALTER ROLE stockapi_app WITH LOGIN PASSWORD %L VALID UNTIL ''infinity'' CONNECTION LIMIT -1 NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT',
  :'app_password'
)
\gexec

ALTER ROLE stockapi_app RESET ALL;
SELECT format('ALTER ROLE stockapi_app IN DATABASE %I RESET ALL', database.datname)
FROM pg_db_role_setting AS setting
JOIN pg_roles AS role ON role.oid = setting.setrole
JOIN pg_database AS database ON database.oid = setting.setdatabase
WHERE role.rolname = 'stockapi_app' AND setting.setdatabase <> 0
\gexec

-- NOINHERIT does not prevent SET ROLE. Remove every cluster-role membership so
-- a pre-existing runtime role cannot retain an escalation path.
SELECT format('REVOKE %I FROM stockapi_app', granted.rolname)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted ON granted.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE member_role.rolname = 'stockapi_app'
\gexec

DO $$
DECLARE
  app_oid oid;
BEGIN
  SELECT oid INTO STRICT app_oid FROM pg_roles WHERE rolname = 'stockapi_app';
  IF EXISTS (SELECT 1 FROM pg_database WHERE datdba = app_oid)
     OR EXISTS (SELECT 1 FROM pg_namespace WHERE nspowner = app_oid)
     OR EXISTS (
       SELECT 1
       FROM pg_class AS object
       JOIN pg_namespace AS namespace ON namespace.oid = object.relnamespace
       WHERE object.relowner = app_oid AND namespace.nspname = 'public'
     )
     OR EXISTS (
       SELECT 1
       FROM pg_proc AS object
       JOIN pg_namespace AS namespace ON namespace.oid = object.pronamespace
       WHERE object.proowner = app_oid AND namespace.nspname = 'public'
     ) THEN
    RAISE EXCEPTION 'stockapi_app owns database objects; transfer ownership before runtime bootstrap';
  END IF;
END;
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE :"db_name" FROM stockapi_app;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM stockapi_app;
GRANT CONNECT ON DATABASE :"db_name" TO stockapi_app;
GRANT USAGE ON SCHEMA public TO stockapi_app;
SQL
