-- Register the ingest job contracts used by the Rust worker and admin CLI.
-- Keeping these rows in the database registry makes submission fail closed
-- for unknown job types while allowing a fresh deployment to enqueue refresh
-- and daily-export work immediately after migration.
INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('ingest.refresh_movie', 1, true),
       ('ingest.refresh_tv', 1, true),
       ('ingest.changes_sync', 1, true),
       ('ingest.daily_export', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE
SET enabled = EXCLUDED.enabled;

GRANT EXECUTE ON FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text)
    TO api_job_submitter, ingest_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0012',
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
  AND metadata.value ->> 'revision' = '0012'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 12
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
