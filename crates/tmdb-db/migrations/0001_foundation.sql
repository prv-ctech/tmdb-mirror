CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS search;
CREATE SCHEMA IF NOT EXISTS assets;
CREATE SCHEMA IF NOT EXISTS auth;

ALTER SCHEMA ops OWNER TO migrator;
ALTER SCHEMA catalog OWNER TO migrator;
ALTER SCHEMA source OWNER TO migrator;
ALTER SCHEMA search OWNER TO migrator;
ALTER SCHEMA assets OWNER TO migrator;
ALTER SCHEMA auth OWNER TO migrator;

REVOKE ALL ON SCHEMA ops, catalog, source, search, assets, auth FROM PUBLIC;
GRANT USAGE ON SCHEMA catalog, search, assets, ops TO api_reader;
GRANT USAGE ON SCHEMA ops TO api_job_submitter;
GRANT USAGE ON SCHEMA catalog, source, search TO ingest_writer;
GRANT USAGE ON SCHEMA assets TO image_writer;
GRANT USAGE ON SCHEMA ops TO monitor;

CREATE TABLE ops.service_metadata (
    key text PRIMARY KEY CHECK (btrim(key) <> ''),
    value jsonb NOT NULL CHECK (jsonb_typeof(value) = 'object'),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE ops.job_type_registry (
    job_type text PRIMARY KEY CHECK (btrim(job_type) <> ''),
    payload_version integer NOT NULL CHECK (payload_version > 0),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE source.ingest_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type text NOT NULL CHECK (run_type IN ('bulk', 'incremental')),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    watermark jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(watermark) = 'object'),
    counts jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(counts) = 'object'),
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ingest_runs_status_timestamps_check CHECK (
        (status = 'pending' AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('succeeded', 'failed', 'cancelled')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL)
    ),
    CONSTRAINT ingest_runs_timestamp_order_check CHECK (
        (finished_at IS NULL OR finished_at >= started_at)
        AND updated_at >= created_at
    )
);

CREATE TABLE auth.api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identifier text NOT NULL UNIQUE CHECK (btrim(identifier) <> ''),
    hmac_digest bytea NOT NULL UNIQUE CHECK (octet_length(hmac_digest) = 32),
    owner text NOT NULL CHECK (btrim(owner) <> ''),
    scopes text[] NOT NULL,
    expires_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT api_keys_scopes_check CHECK (
        array_ndims(scopes) = 1
        AND cardinality(scopes) > 0
        AND array_position(scopes, NULL) IS NULL
        AND scopes <@ ARRAY['catalog:read', 'jobs:submit', 'jobs:cancel', 'jobs:read']::text[]
        AND cardinality(array_positions(scopes, 'catalog:read')) <= 1
        AND cardinality(array_positions(scopes, 'jobs:submit')) <= 1
        AND cardinality(array_positions(scopes, 'jobs:cancel')) <= 1
        AND cardinality(array_positions(scopes, 'jobs:read')) <= 1
    ),
    CONSTRAINT api_keys_expiry_check CHECK (expires_at IS NULL OR expires_at > created_at),
    CONSTRAINT api_keys_revocation_check CHECK (revoked_at IS NULL OR revoked_at >= created_at),
    CONSTRAINT api_keys_timestamp_order_check CHECK (updated_at >= created_at)
);

ALTER TABLE ops._sqlx_migrations OWNER TO migrator;
ALTER TABLE ops.service_metadata OWNER TO migrator;
ALTER TABLE ops.job_type_registry OWNER TO migrator;
ALTER TABLE source.ingest_runs OWNER TO migrator;
ALTER TABLE auth.api_keys OWNER TO migrator;

REVOKE ALL ON ALL TABLES IN SCHEMA ops, source, auth FROM PUBLIC;
REVOKE ALL ON TABLE ops._sqlx_migrations, ops.service_metadata, ops.job_type_registry,
    source.ingest_runs, auth.api_keys
    FROM api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE source.ingest_runs TO ingest_writer;

INSERT INTO ops.service_metadata(key, value)
VALUES (
    'schema',
    jsonb_build_object(
        'revision', '0001',
        'migrated_at', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    )
);

INSERT INTO ops.job_type_registry(job_type, payload_version)
VALUES ('system.noop', 1);

CREATE VIEW ops.readiness AS
SELECT value ->> 'revision' AS schema_revision,
       (value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata
WHERE key = 'schema'
  AND value ->> 'revision' = '0001'
  AND (SELECT count(*) FROM ops._sqlx_migrations) = 1
  AND (SELECT count(*) FROM ops._sqlx_migrations WHERE version = 1 AND success) = 1;

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness FROM PUBLIC, api_job_submitter, ingest_writer, image_writer;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;

ALTER DEFAULT PRIVILEGES FOR ROLE migrator
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA catalog
    GRANT SELECT ON TABLES TO api_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA catalog
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA source
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA search
    GRANT SELECT ON TABLES TO api_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA search
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
    GRANT SELECT ON TABLES TO api_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO image_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA catalog, source, search
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO ingest_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO image_writer;
