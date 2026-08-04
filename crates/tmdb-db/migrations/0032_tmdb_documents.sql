-- Preserve the exact JSON returned by TMDB for local, shape-compatible reads.

CREATE TABLE source.tmdb_documents (
    endpoint_path text NOT NULL,
    query_string text NOT NULL DEFAULT '',
    response jsonb NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT tmdb_documents_endpoint_check CHECK (
        pg_catalog.char_length(endpoint_path) BETWEEN 1 AND 512
        AND endpoint_path !~ '[[:cntrl:]]'
        AND endpoint_path !~ '(^|/)\.\.?(/|$)'
        AND endpoint_path !~ '(^|/)\.'
        AND endpoint_path !~ '(^|/)/'
        AND endpoint_path !~ '[?#]'
        AND endpoint_path !~ '\\'
    ),
    CONSTRAINT tmdb_documents_query_check CHECK (
        pg_catalog.char_length(query_string) <= 2_048
        AND query_string !~ '[[:cntrl:]]'
        AND query_string !~ '[#]'
    ),
    PRIMARY KEY (endpoint_path, query_string)
);

ALTER TABLE source.tmdb_documents OWNER TO migrator;
GRANT SELECT ON TABLE source.tmdb_documents TO api_reader, monitor;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE source.tmdb_documents TO ingest_writer;

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('ingest.configuration', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE
SET enabled = EXCLUDED.enabled;

ALTER TABLE ops.service_metadata
    DROP CONSTRAINT IF EXISTS service_metadata_schema_revision_check;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0032',
        'migrated_at', pg_catalog.to_char(
            pg_catalog.clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    ),
    updated_at = pg_catalog.clock_timestamp()
WHERE key = 'schema';

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0032'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 32
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
  ]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
