-- Keep exact active-job ceilings without serializing every producer behind one
-- transaction-scoped advisory lock. Each active bounded job owns one durable
-- queue slot; concurrent producers claim different free rows with SKIP LOCKED.

LOCK TABLE ops.jobs IN SHARE ROW EXCLUSIVE MODE;

DROP TRIGGER IF EXISTS jobs_image_queue_limit ON ops.jobs;
DROP FUNCTION IF EXISTS ops.enforce_image_queue_limit();

CREATE TABLE ops.job_queue_slots (
    queue_name text NOT NULL,
    slot_number integer NOT NULL,
    job_id uuid UNIQUE REFERENCES ops.jobs(id) ON DELETE SET NULL,
    PRIMARY KEY (queue_name, slot_number),
    CONSTRAINT job_queue_slots_name_check CHECK (
        queue_name IN (
            'image.download', 'title.refresh', 'title.enrichment', 'season.refresh'
        )
    ),
    CONSTRAINT job_queue_slots_number_check CHECK (
        slot_number > 0
        AND (
            (queue_name = 'image.download' AND slot_number <= 10000)
            OR (queue_name <> 'image.download' AND slot_number <= 1000)
        )
    )
);

CREATE INDEX job_queue_slots_available_idx
    ON ops.job_queue_slots (queue_name, slot_number)
    WHERE job_id IS NULL;

INSERT INTO ops.job_queue_slots (queue_name, slot_number)
SELECT capacity.queue_name, slot.slot_number
  FROM (VALUES
      ('image.download'::text, 10000),
      ('title.refresh'::text, 1000),
      ('title.enrichment'::text, 1000),
      ('season.refresh'::text, 1000)
  ) AS capacity(queue_name, maximum)
  CROSS JOIN LATERAL pg_catalog.generate_series(1, capacity.maximum) AS slot(slot_number);

DO $block$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM (
              SELECT CASE
                         WHEN job.job_type = 'image.download' THEN 'image.download'
                         WHEN job.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
                             THEN 'title.refresh'
                         WHEN job.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
                             THEN 'title.enrichment'
                         WHEN job.job_type = 'ingest.refresh_season' THEN 'season.refresh'
                     END AS queue_name,
                     pg_catalog.count(*) AS active_jobs
                FROM ops.jobs AS job
               WHERE job.status IN ('queued', 'running', 'retry_wait')
                 AND job.job_type IN (
                     'image.download',
                     'ingest.refresh_movie', 'ingest.refresh_tv',
                     'ingest.enrich_movie', 'ingest.enrich_tv',
                     'ingest.refresh_season'
                 )
               GROUP BY 1
          ) AS active
         WHERE active.active_jobs > CASE active.queue_name
             WHEN 'image.download' THEN 10000 ELSE 1000 END
    ) THEN
        RAISE EXCEPTION 'active job queue exceeds its configured capacity';
    END IF;
END
$block$;

WITH active AS (
    SELECT job.id,
           CASE
               WHEN job.job_type = 'image.download' THEN 'image.download'
               WHEN job.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
                   THEN 'title.refresh'
               WHEN job.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
                   THEN 'title.enrichment'
               WHEN job.job_type = 'ingest.refresh_season' THEN 'season.refresh'
           END AS queue_name,
           pg_catalog.row_number() OVER (
               PARTITION BY CASE
                   WHEN job.job_type = 'image.download' THEN 'image.download'
                   WHEN job.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
                       THEN 'title.refresh'
                   WHEN job.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
                       THEN 'title.enrichment'
                   WHEN job.job_type = 'ingest.refresh_season' THEN 'season.refresh'
               END
               ORDER BY job.created_at, job.id
           )::integer AS slot_number
      FROM ops.jobs AS job
     WHERE job.status IN ('queued', 'running', 'retry_wait')
       AND job.job_type IN (
           'image.download',
           'ingest.refresh_movie', 'ingest.refresh_tv',
           'ingest.enrich_movie', 'ingest.enrich_tv',
           'ingest.refresh_season'
       )
)
UPDATE ops.job_queue_slots AS slot
   SET job_id = active.id
  FROM active
 WHERE slot.queue_name = active.queue_name
   AND slot.slot_number = active.slot_number;

CREATE FUNCTION ops.sync_job_queue_slot()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_old_queue text;
    v_new_queue text;
    v_old_active boolean := false;
    v_new_active boolean;
    v_slot_number integer;
BEGIN
    v_new_queue := CASE
        WHEN NEW.job_type = 'image.download' THEN 'image.download'
        WHEN NEW.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
            THEN 'title.refresh'
        WHEN NEW.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
            THEN 'title.enrichment'
        WHEN NEW.job_type = 'ingest.refresh_season' THEN 'season.refresh'
    END;
    v_new_active := v_new_queue IS NOT NULL
        AND NEW.status IN ('queued', 'running', 'retry_wait');

    IF TG_OP = 'UPDATE' THEN
        v_old_queue := CASE
            WHEN OLD.job_type = 'image.download' THEN 'image.download'
            WHEN OLD.job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
                THEN 'title.refresh'
            WHEN OLD.job_type IN ('ingest.enrich_movie', 'ingest.enrich_tv')
                THEN 'title.enrichment'
            WHEN OLD.job_type = 'ingest.refresh_season' THEN 'season.refresh'
        END;
        v_old_active := v_old_queue IS NOT NULL
            AND OLD.status IN ('queued', 'running', 'retry_wait');

        IF v_old_active AND (NOT v_new_active OR v_old_queue IS DISTINCT FROM v_new_queue) THEN
            UPDATE ops.job_queue_slots AS slot
               SET job_id = NULL
             WHERE slot.job_id = OLD.id;
        END IF;
    END IF;

    IF v_new_active
       AND (TG_OP = 'INSERT' OR NOT v_old_active OR v_old_queue IS DISTINCT FROM v_new_queue)
    THEN
        SELECT slot.slot_number
          INTO v_slot_number
          FROM ops.job_queue_slots AS slot
         WHERE slot.queue_name = v_new_queue
           AND slot.job_id IS NULL
         ORDER BY slot.slot_number
         FOR UPDATE SKIP LOCKED
         LIMIT 1;
        IF v_slot_number IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0004',
                MESSAGE = 'job queue capacity reached';
        END IF;
        UPDATE ops.job_queue_slots AS slot
           SET job_id = NEW.id
         WHERE slot.queue_name = v_new_queue
           AND slot.slot_number = v_slot_number;
    END IF;

    RETURN NEW;
END
$function$;

CREATE TRIGGER jobs_queue_slot_insert
AFTER INSERT ON ops.jobs
FOR EACH ROW
EXECUTE FUNCTION ops.sync_job_queue_slot();

CREATE TRIGGER jobs_queue_slot_update
AFTER UPDATE OF job_type, status ON ops.jobs
FOR EACH ROW
EXECUTE FUNCTION ops.sync_job_queue_slot();

ALTER TABLE ops.job_queue_slots OWNER TO migrator;
ALTER FUNCTION ops.sync_job_queue_slot() OWNER TO migrator;

REVOKE ALL ON TABLE ops.job_queue_slots
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.job_queue_slots TO monitor;
REVOKE ALL ON FUNCTION ops.sync_job_queue_slot()
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0053',
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
  AND metadata.value ->> 'revision' = '0053'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 53
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53
  ]::bigint[]
  AND NOT EXISTS (
      SELECT 1 FROM ops._sqlx_migrations AS migration WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
