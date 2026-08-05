-- Keep title metadata refreshes small and move optional enrichment to a
-- separately prioritized durable job.

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('ingest.enrich_movie', 1, true),
       ('ingest.enrich_tv', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE
SET enabled = EXCLUDED.enabled;

-- Enrichment is title work for the bounded queue. Replace the trigger
-- function so the new job types cannot bypass the existing cap.
CREATE OR REPLACE FUNCTION ops.enforce_image_queue_limit()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    queue_name text;
    active_jobs bigint;
    max_active_jobs bigint;
BEGIN
    queue_name := CASE
        WHEN NEW.job_type = 'image.download' THEN 'image.download'
        WHEN NEW.job_type IN (
            'ingest.refresh_movie', 'ingest.refresh_tv',
            'ingest.enrich_movie', 'ingest.enrich_tv'
        ) THEN 'title.refresh'
        WHEN NEW.job_type = 'ingest.refresh_season' THEN 'season.refresh'
        WHEN NEW.job_type = 'ingest.refresh_reusable_gallery' THEN 'reusable.gallery'
        ELSE NULL
    END;
    max_active_jobs := CASE queue_name
        WHEN 'image.download' THEN 10000
        WHEN 'title.refresh' THEN 1000
        WHEN 'season.refresh' THEN 1000
        WHEN 'reusable.gallery' THEN 1000
        ELSE NULL
    END;

    IF queue_name IS NOT NULL
       AND NEW.status IN ('queued', 'running', 'retry_wait')
    THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('queue:' || queue_name, 0)
        );

        IF EXISTS (
            SELECT 1
              FROM ops.jobs AS existing
             WHERE existing.job_type = NEW.job_type
               AND existing.dedup_key = NEW.dedup_key
               AND existing.status IN ('queued', 'running', 'retry_wait')
        ) THEN
            RETURN NEW;
        END IF;

        SELECT pg_catalog.count(*)
          INTO active_jobs
          FROM ops.jobs AS existing
         WHERE (
                (queue_name = 'image.download' AND existing.job_type = 'image.download')
                OR (queue_name = 'title.refresh'
                    AND existing.job_type IN (
                        'ingest.refresh_movie', 'ingest.refresh_tv',
                        'ingest.enrich_movie', 'ingest.enrich_tv'
                    ))
                OR (queue_name = 'season.refresh'
                    AND existing.job_type = 'ingest.refresh_season')
                OR (queue_name = 'reusable.gallery'
                    AND existing.job_type = 'ingest.refresh_reusable_gallery')
               )
           AND existing.status IN ('queued', 'running', 'retry_wait');
        IF active_jobs >= max_active_jobs THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0004',
                MESSAGE = 'job queue capacity reached';
        END IF;
    END IF;
    RETURN NEW;
END
$function$;

ALTER FUNCTION ops.enforce_image_queue_limit() OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.enforce_image_queue_limit() FROM PUBLIC;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0042',
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
  AND metadata.value ->> 'revision' = '0042'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 42
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42
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
