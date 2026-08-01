-- Keep the public readiness projection truthful after the season-ingest
-- migration: the schema revision alone is not enough when a migration row is
-- missing, failed, or unexpectedly added.

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 16
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
