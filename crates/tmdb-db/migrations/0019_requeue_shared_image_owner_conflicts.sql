-- Migration 0018 removes the global source-path ownership restriction. Retry
-- only terminal image jobs that were provably blocked by that old restriction:
-- their source path already belongs to a different, materialized owner. Other
-- invalid payloads remain terminal.

WITH candidates AS (
    SELECT job.id
    FROM ops.jobs AS job
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN job.payload ->> 'entityId' ~ '^[1-9][0-9]{0,18}$'
             AND (
                    char_length(job.payload ->> 'entityId') < 19
                    OR job.payload ->> 'entityId' <= '9223372036854775807'
                 )
                THEN (job.payload ->> 'entityId')::bigint
        END AS entity_id
    ) AS payload
    CROSS JOIN LATERAL (
        SELECT CASE job.payload ->> 'entityType'
                   WHEN 'movie' THEN 1
                   WHEN 'tv' THEN 1
                   WHEN 'person' THEN 2
                   WHEN 'company' THEN 3
                   WHEN 'network' THEN 4
                   WHEN 'collection' THEN 5
                   WHEN 'season' THEN 6
                   WHEN 'episode' THEN 7
               END AS owner_type,
               CASE job.payload ->> 'entityType'
                   WHEN 'movie' THEN (
                       SELECT title.id
                       FROM catalog.titles AS title
                       WHERE title.media_type = 'movie'
                         AND title.tmdb_id = payload.entity_id
                         AND title.active
                   )
                   WHEN 'tv' THEN (
                       SELECT title.id
                       FROM catalog.titles AS title
                       WHERE title.media_type = 'tv'
                         AND title.tmdb_id = payload.entity_id
                         AND title.active
                   )
                   WHEN 'person' THEN (
                       SELECT person.id FROM catalog.people AS person
                       WHERE person.id = payload.entity_id
                   )
                   WHEN 'company' THEN (
                       SELECT company.id FROM catalog.companies AS company
                       WHERE company.id = payload.entity_id
                   )
                   WHEN 'network' THEN (
                       SELECT network.id FROM catalog.networks AS network
                       WHERE network.id = payload.entity_id
                   )
                   WHEN 'collection' THEN (
                       SELECT collection.id FROM catalog.collections AS collection
                       WHERE collection.id = payload.entity_id
                   )
                   WHEN 'season' THEN (
                       SELECT season.id
                       FROM catalog.seasons AS season
                       JOIN catalog.titles AS title ON title.id = season.title_id
                       WHERE season.id = payload.entity_id
                         AND title.active
                   )
                   WHEN 'episode' THEN (
                       SELECT episode.id
                       FROM catalog.episodes AS episode
                       JOIN catalog.titles AS title ON title.id = episode.title_id
                       WHERE episode.id = payload.entity_id
                         AND title.active
                   )
               END AS owner_id
    ) AS expected
    WHERE job.job_type = 'image.download'
      AND job.status = 'dead_letter'
      AND job.error_code = 'invalid_payload'
      AND payload.entity_id IS NOT NULL
      AND expected.owner_type IS NOT NULL
      AND expected.owner_id IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM assets.image_assets AS asset
          WHERE asset.source = 'tmdb'
            AND asset.source_key = job.payload ->> 'tmdbPath'
            AND (asset.owner_type, asset.owner_id)
                IS DISTINCT FROM (expected.owner_type, expected.owner_id)
      )
      AND NOT EXISTS (
          SELECT 1
          FROM ops.jobs AS active
          WHERE active.id <> job.id
            AND active.job_type = job.job_type
            AND active.dedup_key = job.dedup_key
            AND active.status IN ('queued', 'retry_wait', 'running')
      )
), repaired AS (
    UPDATE ops.jobs AS job
       SET status = 'queued',
           attempts = 0,
           available_at = clock_timestamp(),
           lease_owner = NULL,
           lease_expires_at = NULL,
           cancellation_requested = false,
           result_summary = NULL,
           error_message = NULL,
           error_code = NULL,
           updated_at = clock_timestamp(),
           finished_at = NULL
      FROM candidates
     WHERE job.id = candidates.id
    RETURNING job.id
)
INSERT INTO ops.job_events (
    id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
)
SELECT pg_catalog.gen_random_uuid(),
       repaired.id,
       'retry_scheduled',
       'dead_letter',
       'queued',
       NULL,
       pg_catalog.jsonb_build_object('reason', 'shared_source_owner_identity_repair'),
       pg_catalog.clock_timestamp()
FROM repaired;

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 19
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
