#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

read_secret() {
    local secret_path="$1"
    local secret_size
    local secret_value

    if [[ ! -f "$secret_path" ]]; then
        printf 'Required PostgreSQL secret file is missing: %s\n' "$secret_path" >&2
        return 1
    fi
    secret_size="$(wc -c < "$secret_path")"
    if [[ "$secret_size" -ne 43 ]]; then
        printf 'PostgreSQL secret file has an invalid byte length: %s\n' "$secret_path" >&2
        return 1
    fi
    secret_value="$(<"$secret_path")"
    if [[ ! "$secret_value" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
        printf 'PostgreSQL secret file is not a 32-byte base64url value: %s\n' "$secret_path" >&2
        return 1
    fi
    printf '%s' "$secret_value"
}

migrator_password="$(read_secret /run/secrets/migrator_password)"
api_reader_password="$(read_secret /run/secrets/api_reader_password)"
api_job_submitter_password="$(read_secret /run/secrets/api_job_submitter_password)"
ingest_writer_password="$(read_secret /run/secrets/ingest_writer_password)"
image_writer_password="$(read_secret /run/secrets/image_writer_password)"
monitor_password="$(read_secret /run/secrets/monitor_password)"

psql -X -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=migrator_password="$migrator_password" \
    --set=api_reader_password="$api_reader_password" \
    --set=api_job_submitter_password="$api_job_submitter_password" \
    --set=ingest_writer_password="$ingest_writer_password" \
    --set=image_writer_password="$image_writer_password" \
    --set=monitor_password="$monitor_password" <<'SQL'
SET password_encryption = 'scram-sha-256';

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'migrator') AS migrator_exists \gset
\if :migrator_exists
ALTER ROLE migrator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'migrator_password';
\else
CREATE ROLE migrator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'migrator_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') AS api_reader_exists \gset
\if :api_reader_exists
ALTER ROLE api_reader LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_reader_password';
\else
CREATE ROLE api_reader LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_reader_password';
\endif
-- PgBouncer may not preserve client startup options. Enforce the read-only
-- session policy at the role so pooled API connections remain read-only too.
ALTER ROLE api_reader SET default_transaction_read_only = on;

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_job_submitter') AS api_job_submitter_exists \gset
\if :api_job_submitter_exists
ALTER ROLE api_job_submitter LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_job_submitter_password';
\else
CREATE ROLE api_job_submitter LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_job_submitter_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ingest_writer') AS ingest_writer_exists \gset
\if :ingest_writer_exists
ALTER ROLE ingest_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'ingest_writer_password';
\else
CREATE ROLE ingest_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'ingest_writer_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'image_writer') AS image_writer_exists \gset
\if :image_writer_exists
ALTER ROLE image_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'image_writer_password';
\else
CREATE ROLE image_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'image_writer_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitor') AS monitor_exists \gset
\if :monitor_exists
ALTER ROLE monitor LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'monitor_password';
\else
CREATE ROLE monitor LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'monitor_password';
\endif

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CONNECT ON DATABASE tmdb FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE tmdb FROM migrator, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT CONNECT ON DATABASE tmdb TO migrator, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT CREATE ON DATABASE tmdb TO migrator;
SQL

unset migrator_password api_reader_password api_job_submitter_password
unset ingest_writer_password image_writer_password monitor_password
