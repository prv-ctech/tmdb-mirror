-- Keep every scan fan-out bounded at the durable submission boundary.
-- Parent-job limits alone do not protect the season, reusable-entity, or
-- image queues created by a title refresh.

CREATE FUNCTION ops.enforce_image_queue_limit()
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
        WHEN NEW.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
            THEN 'title.refresh'
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

        -- A retry of an already active deduplicated job must remain a no-op
        -- even while the queue is full.
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
                    AND existing.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv'))
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

CREATE TRIGGER jobs_image_queue_limit
BEFORE INSERT ON ops.jobs
FOR EACH ROW
EXECUTE FUNCTION ops.enforce_image_queue_limit();

-- The original maintenance function predates admin and media-scan history.
-- Keep referenced jobs intact so an explicit prune cannot break audit lookups.
CREATE OR REPLACE FUNCTION ops.prune_finished_jobs(p_before timestamptz, p_limit integer)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    IF p_before IS NULL
       OR p_limit NOT BETWEEN 1 AND 10000
       OR p_before >= pg_catalog.clock_timestamp()
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid job prune arguments';
    END IF;

    WITH candidates AS MATERIALIZED (
        SELECT job.id
          FROM ops.jobs AS job
         WHERE job.status IN ('succeeded', 'dead_letter', 'cancelled')
           AND job.finished_at IS NOT NULL
           AND job.finished_at < p_before
           AND NOT EXISTS (
               SELECT 1 FROM ops.admin_requests AS request
                WHERE request.job_id = job.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM ops.backup_requests AS backup
                WHERE backup.job_id = job.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM ops.media_scan_runs AS scan
                WHERE scan.job_id = job.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM ops.media_scan_job_links AS link
                WHERE link.job_id = job.id
           )
         ORDER BY job.finished_at, job.id
         LIMIT p_limit
         FOR UPDATE OF job SKIP LOCKED
    ), deleted_events AS (
        DELETE FROM ops.job_events AS event
         USING candidates
         WHERE event.job_id = candidates.id
    ), deleted_jobs AS (
        DELETE FROM ops.jobs AS job
         USING candidates
         WHERE job.id = candidates.id
        RETURNING 1
    )
    SELECT pg_catalog.count(*)::integer INTO deleted_count FROM deleted_jobs;
    RETURN deleted_count;
END
$function$;

ALTER FUNCTION ops.prune_finished_jobs(timestamptz, integer) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.prune_finished_jobs(timestamptz, integer)
    FROM PUBLIC, api_reader, api_job_submitter, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.prune_finished_jobs(timestamptz, integer)
    TO ingest_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0034',
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
  AND metadata.value ->> 'revision' = '0034'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 34
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
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
