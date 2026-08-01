-- Terminal worker failures are distinct from exhausted transient retries.
-- The function preserves the existing lease and permission boundary while
-- making permanent upstream validation/auth/not-found errors immediately
-- visible as dead letters.

CREATE FUNCTION ops.dead_letter_job(
    p_job_id uuid,
    p_worker_id text,
    p_message text
)
RETURNS TABLE (disposition text, next_available_at timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_job ops.jobs%ROWTYPE;
    v_status text;
BEGIN
    IF p_job_id IS NULL
       OR NOT ops.job_valid_ascii(p_worker_id, 128)
       OR p_message IS NULL
       OR p_message <> pg_catalog.btrim(p_message)
       OR pg_catalog.char_length(p_message) NOT BETWEEN 1 AND 2048
       OR p_message ~ '[[:cntrl:]]'
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job terminal failure rejected';
    END IF;

    SELECT job.*
    INTO v_job
    FROM ops.jobs AS job
    WHERE job.id = p_job_id
    FOR UPDATE OF job;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
    END IF;
    IF v_job.status <> 'running'
       OR v_job.lease_owner <> p_worker_id
       OR v_job.lease_expires_at <= v_now
    THEN
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'job lease lost';
    END IF;

    v_status := CASE
        WHEN v_job.cancellation_requested THEN 'cancelled'
        ELSE 'dead_letter'
    END;
    UPDATE ops.jobs AS job
    SET status = v_status,
        lease_owner = NULL,
        lease_expires_at = NULL,
        cancellation_requested = false,
        error_message = NULL,
        error_code = CASE
            WHEN v_status = 'cancelled' THEN NULL
            ELSE ops.job_failure_code(p_message)
        END,
        updated_at = v_now,
        finished_at = v_now
    WHERE job.id = p_job_id
    RETURNING job.* INTO v_job;

    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(), p_job_id,
        CASE v_status
            WHEN 'dead_letter' THEN 'dead_lettered'
            ELSE 'cancelled'
        END,
        'running', v_status, p_worker_id,
        pg_catalog.jsonb_build_object('terminal', true),
        v_now
    );
    RETURN QUERY
    SELECT CASE v_status
               WHEN 'dead_letter' THEN 'dead_lettered'
               ELSE 'cancelled'
           END,
           v_job.available_at;
END
$function$;

ALTER FUNCTION ops.dead_letter_job(uuid, text, text) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.dead_letter_job(uuid, text, text)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.dead_letter_job(uuid, text, text)
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0009',
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
  AND metadata.value ->> 'revision' = '0009'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 9
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
