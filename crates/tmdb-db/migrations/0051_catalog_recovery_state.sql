-- Track successful optional enrichment independently from the title and season census.

ALTER TABLE catalog.titles
    ADD COLUMN enriched_at timestamptz;

ALTER TABLE catalog.seasons
    ADD COLUMN enriched_at timestamptz;

-- Preserve known completion state when upgrading an existing production database.
UPDATE catalog.titles AS title
SET enriched_at = completed.finished_at
FROM (
    SELECT job.job_type,
           (job.payload ->> 'tmdb_id')::bigint AS tmdb_id,
           pg_catalog.max(job.finished_at) AS finished_at
    FROM ops.jobs AS job
    WHERE job.status = 'succeeded'
      AND job.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
      AND job.payload ->> 'tmdb_id' ~ '^[1-9][0-9]*$'
      AND NOT COALESCE(job.result_summary ? 'skipped', false)
    GROUP BY job.job_type, (job.payload ->> 'tmdb_id')::bigint
) AS completed
WHERE title.tmdb_id = completed.tmdb_id
  AND title.media_type = CASE completed.job_type
      WHEN 'ingest.enrich_movie' THEN 'movie'
      WHEN 'ingest.enrich_tv' THEN 'tv'
  END;

UPDATE catalog.seasons AS season
SET enriched_at = completed.finished_at
FROM catalog.titles AS title,
     (
         SELECT (job.payload ->> 'tv_id')::bigint AS tv_id,
                (job.payload ->> 'season_number')::integer AS season_number,
                pg_catalog.max(job.finished_at) AS finished_at
         FROM ops.jobs AS job
         WHERE job.status = 'succeeded'
           AND job.job_type = 'ingest.refresh_season'
           AND job.payload ->> 'tv_id' ~ '^[1-9][0-9]*$'
           AND job.payload ->> 'season_number' ~ '^[0-9]+$'
           AND NOT COALESCE(job.result_summary ? 'skipped', false)
         GROUP BY (job.payload ->> 'tv_id')::bigint,
                  (job.payload ->> 'season_number')::integer
     ) AS completed
WHERE title.id = season.title_id
  AND title.media_type = 'tv'
  AND title.tmdb_id = completed.tv_id
  AND season.season_number = completed.season_number;

CREATE INDEX titles_missing_enrichment_idx
    ON catalog.titles (media_type, id)
    WHERE active AND enriched_at IS NULL;

CREATE INDEX seasons_missing_enrichment_idx
    ON catalog.seasons (id)
    WHERE enriched_at IS NULL;

CREATE INDEX jobs_dead_catalog_refresh_idx
    ON ops.jobs (job_type, ((payload ->> 'tmdb_id')), finished_at DESC)
    WHERE status = 'dead_letter'
      AND job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv');

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0051',
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
  AND metadata.value ->> 'revision' = '0051'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 51
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
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
