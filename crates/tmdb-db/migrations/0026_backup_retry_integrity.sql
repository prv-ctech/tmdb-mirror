-- Keep a durable backup-request record in step with the job retry state.
-- pgBackRest can fail transiently (for example when a different backup holds
-- the repository lock). In that case ops.fail_job moves the job to
-- retry_wait; the paired backup request must become claimable again instead
-- of being left permanently failed.

CREATE FUNCTION ops.fail_backup_request_and_job(
    p_job_id uuid,
    p_worker_id text,
    p_failure_step text,
    p_retry_microseconds bigint
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_disposition text;
    v_error_code text;
    v_error_message text;
BEGIN
    IF p_failure_step IS NULL OR p_failure_step NOT IN (
        'backup', 'archive_check', 'verify', 'invalid_payload',
        'already_running', 'unknown'
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'backup failure rejected';
    END IF;

    SELECT failure.disposition
      INTO v_disposition
      FROM ops.fail_job(
          p_job_id,
          p_worker_id,
          'execution_failed',
          p_retry_microseconds
      ) AS failure;

    v_error_code := CASE p_failure_step
        WHEN 'archive_check' THEN 'archive_check_failed'
        WHEN 'verify' THEN 'backup_verify_failed'
        ELSE 'backup_failed'
    END;
    v_error_message := CASE p_failure_step
        WHEN 'archive_check' THEN 'archive_check_failed'
        WHEN 'verify' THEN 'backup_verify_failed'
        WHEN 'invalid_payload' THEN 'invalid_payload'
        WHEN 'already_running' THEN 'backup_already_running'
        ELSE 'backup_failed'
    END;

    UPDATE ops.backup_requests AS request
       SET status = CASE
                        WHEN v_disposition = 'retry_scheduled' THEN 'queued'
                        ELSE 'failed'
                    END,
           started_at = CASE
                            WHEN v_disposition = 'retry_scheduled' THEN NULL
                            ELSE request.started_at
                        END,
           finished_at = CASE
                             WHEN v_disposition = 'retry_scheduled' THEN NULL
                             ELSE pg_catalog.clock_timestamp()
                         END,
           worker_id = CASE
                           WHEN v_disposition = 'retry_scheduled' THEN NULL
                           ELSE request.worker_id
                       END,
           error_code = CASE
                            WHEN v_disposition = 'retry_scheduled' THEN NULL
                            ELSE v_error_code
                        END,
           error_message = CASE
                               WHEN v_disposition = 'retry_scheduled' THEN NULL
                               ELSE v_error_message
                           END,
           result = NULL
     WHERE request.job_id = p_job_id
       AND request.status = 'running'
       AND request.worker_id = p_worker_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'backup request lease lost';
    END IF;

    RETURN v_disposition;
END
$function$;

ALTER FUNCTION ops.fail_backup_request_and_job(uuid, text, text, bigint)
    OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.fail_backup_request_and_job(uuid, text, text, bigint)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.fail_backup_request_and_job(uuid, text, text, bigint)
    TO ingest_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0026',
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
  AND metadata.value ->> 'revision' = '0026'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 26
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26
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
