-- Bounded, security-definer cleanup for terminal jobs.  The worker never
-- receives direct write access to ops.jobs; it invokes this allowlisted
-- maintenance function instead.
CREATE FUNCTION ops.prune_finished_jobs(p_before timestamptz, p_limit integer)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    IF p_before IS NULL
       OR p_limit NOT BETWEEN 1 AND 10000
       OR p_before >= pg_catalog.clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid job prune arguments';
    END IF;

    WITH candidates AS MATERIALIZED (
        SELECT id
          FROM ops.jobs
         WHERE status IN ('succeeded', 'dead_letter', 'cancelled')
           AND finished_at IS NOT NULL
           AND finished_at < p_before
         ORDER BY finished_at, id
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
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
        'revision', '0014',
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
  AND metadata.value ->> 'revision' = '0014'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 14
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
