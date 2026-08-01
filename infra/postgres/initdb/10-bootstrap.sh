#!/usr/bin/env bash
set -Eeuo pipefail

umask 077
shared_password="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

psql -X -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=shared_password="$shared_password" <<'SQL'
SET password_encryption = 'scram-sha-256';

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'migrator') AS migrator_exists \gset
\if :migrator_exists
ALTER ROLE migrator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE migrator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') AS api_reader_exists \gset
\if :api_reader_exists
ALTER ROLE api_reader LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE api_reader LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif
ALTER ROLE api_reader SET default_transaction_read_only = on;

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_job_submitter') AS api_job_submitter_exists \gset
\if :api_job_submitter_exists
ALTER ROLE api_job_submitter LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE api_job_submitter LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ingest_writer') AS ingest_writer_exists \gset
\if :ingest_writer_exists
ALTER ROLE ingest_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE ingest_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'image_writer') AS image_writer_exists \gset
\if :image_writer_exists
ALTER ROLE image_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE image_writer LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\endif

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitor') AS monitor_exists \gset
\if :monitor_exists
ALTER ROLE monitor LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
\else
CREATE ROLE monitor LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'shared_password';
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

unset shared_password
