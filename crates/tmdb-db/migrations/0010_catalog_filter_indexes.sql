-- Filter-specific partial indexes for the bounded discovery API.
-- Relationship predicates already use the reverse indexes from 0005/0008; these
-- indexes keep runtime/status filters index-backed after anime/media isolation.

CREATE INDEX titles_non_anime_runtime_idx
    ON catalog.titles (media_type, runtime_minutes, id DESC)
    WHERE active AND NOT is_anime AND runtime_minutes IS NOT NULL;

CREATE INDEX titles_anime_runtime_idx
    ON catalog.titles (media_type, runtime_minutes, id DESC)
    WHERE active AND is_anime AND runtime_minutes IS NOT NULL;

CREATE INDEX titles_non_anime_status_idx
    ON catalog.titles (media_type, status, id DESC)
    WHERE active AND NOT is_anime AND status IS NOT NULL;

CREATE INDEX titles_anime_status_idx
    ON catalog.titles (media_type, status, id DESC)
    WHERE active AND is_anime AND status IS NOT NULL;

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('image.download', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE SET enabled = true;

GRANT EXECUTE ON FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text)
    TO ingest_writer;

-- The image worker resolves canonical catalog owners before writing assets.
-- Keep its role read-only in catalog while allowing writes only in assets.
GRANT USAGE ON SCHEMA catalog TO image_writer;
GRANT SELECT ON catalog.titles, catalog.people, catalog.companies,
    catalog.networks, catalog.collections, catalog.languages, catalog.seasons,
    catalog.episodes TO image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0010',
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
  AND metadata.value ->> 'revision' = '0010'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 10
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
