-- TMDB exposes movie and TV namespaces; anime is a classification, not a
-- separate public media partition.

DROP INDEX IF EXISTS catalog.titles_non_anime_popularity_idx;
DROP INDEX IF EXISTS catalog.titles_anime_popularity_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_release_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_popularity_global_idx;
DROP INDEX IF EXISTS catalog.titles_anime_popularity_global_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_first_air_idx;
DROP INDEX IF EXISTS catalog.titles_anime_release_idx;
DROP INDEX IF EXISTS catalog.titles_anime_first_air_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_top_rating_global_idx;
DROP INDEX IF EXISTS catalog.titles_anime_top_rating_global_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_runtime_idx;
DROP INDEX IF EXISTS catalog.titles_anime_runtime_idx;
DROP INDEX IF EXISTS catalog.titles_non_anime_status_idx;
DROP INDEX IF EXISTS catalog.titles_anime_status_idx;

ALTER TABLE catalog.titles DROP COLUMN is_anime;

UPDATE ops.service_metadata
SET value = jsonb_build_object(
        'revision', '0033',
        'migrated_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    ),
    updated_at = clock_timestamp()
WHERE key = 'schema';

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0033'
  AND (SELECT count(*) FROM ops._sqlx_migrations) = 33
  AND (
      SELECT array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
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
