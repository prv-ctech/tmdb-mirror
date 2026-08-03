#!/usr/bin/env bash
set -Eeuo pipefail

umask 077
shared_password="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

ensure_internal_login_role() {
    local role_name="$1"
    local password_name="$2"
    local role_password="${!password_name:-$shared_password}"

    # A deployment may deliberately select one of the historical internal role
    # names as its PostgreSQL owner. That owner is the role that bootstraps the
    # cluster, so it must remain untouched and retain the privileges required
    # to install extensions and apply migrations.
    if [[ "$POSTGRES_USER" == "$role_name" ]]; then
        return
    fi

    psql -X -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        --set=shared_password="$role_password" \
        --set=role_name="$role_name" <<'SQL'
SET password_encryption = 'scram-sha-256';

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name') AS role_exists \gset
\if :role_exists
ALTER ROLE :"role_name" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE :"role_name" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif
SQL
}

ensure_internal_login_role migrator TMDB_MIGRATOR_PASSWORD
ensure_internal_login_role api_reader TMDB_API_READER_PASSWORD
ensure_internal_login_role api_job_submitter TMDB_API_JOB_SUBMITTER_PASSWORD
ensure_internal_login_role ingest_writer TMDB_INGEST_WRITER_PASSWORD
ensure_internal_login_role image_writer TMDB_IMAGE_WRITER_PASSWORD
ensure_internal_login_role monitor TMDB_MONITOR_PASSWORD

if [[ "$POSTGRES_USER" != "api_reader" ]]; then
    psql -X -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" <<'SQL'
ALTER ROLE api_reader SET default_transaction_read_only = on;
SQL
fi

psql -X -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=database_name="$POSTGRES_DB" <<'SQL'

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CONNECT ON DATABASE :"database_name" FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE :"database_name" FROM migrator, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT CONNECT ON DATABASE :"database_name" TO migrator, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT CREATE ON DATABASE :"database_name" TO migrator;
SQL

unset shared_password
