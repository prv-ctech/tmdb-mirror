-- Prevent specialized workers from claiming one another's jobs. The legacy
-- two-argument claim function remains available for generic consumers; the
-- worker runtime uses the filtered function so ingest and image queues are
-- isolated at the durable claim boundary rather than relying on executor
-- error handling.
CREATE FUNCTION ops.claim_job_for_types(
    p_worker_id text,
    p_lease_microseconds bigint,
    p_job_types text[]
)
RETURNS TABLE (
    job_id uuid,
    job_type text,
    payload_version integer,
    payload jsonb,
    attempts integer,
    max_attempts integer,
    lease_expires_at timestamptz,
    cancellation_requested boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_job ops.jobs%ROWTYPE;
    v_job_id uuid;
    v_from_status text;
    v_now timestamptz;
BEGIN
    IF NOT ops.job_valid_ascii(p_worker_id, 128)
       OR p_lease_microseconds IS NULL
       OR p_lease_microseconds NOT BETWEEN 1 AND 3600000000
       OR (
           p_job_types IS NOT NULL
           AND (
               pg_catalog.cardinality(p_job_types) NOT BETWEEN 1 AND 64
               OR EXISTS (
                   SELECT 1
                   FROM pg_catalog.unnest(p_job_types) AS allowed(job_type)
                   WHERE NOT ops.job_valid_ascii(allowed.job_type, 128)
               )
           )
       )
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job claim rejected';
    END IF;

    v_now := pg_catalog.clock_timestamp();
    SELECT job.*
    INTO v_job
    FROM ops.jobs AS job
    WHERE job.status = 'running'
      AND job.cancellation_requested
      AND job.lease_expires_at <= v_now
      AND (p_job_types IS NULL OR job.job_type = ANY(p_job_types))
    ORDER BY job.lease_expires_at, job.priority DESC, job.available_at, job.created_at, job.id
    FOR UPDATE OF job SKIP LOCKED
    LIMIT 1;

    IF FOUND THEN
        UPDATE ops.jobs AS job
        SET status = 'cancelled',
            lease_owner = NULL,
            lease_expires_at = NULL,
            cancellation_requested = false,
            error_message = NULL,
            error_code = NULL,
            updated_at = v_now,
            finished_at = v_now
        WHERE job.id = v_job.id
        RETURNING job.* INTO v_job;
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        ) VALUES (
            pg_catalog.gen_random_uuid(), v_job.id, 'cancelled', 'running', 'cancelled', NULL,
            pg_catalog.jsonb_build_object('reason', 'cancellation_requested'), v_now
        );
    END IF;

    v_now := pg_catalog.clock_timestamp();
    SELECT job.*
    INTO v_job
    FROM ops.jobs AS job
    WHERE job.status = 'running'
      AND job.attempts >= job.max_attempts
      AND job.lease_expires_at <= v_now
      AND (p_job_types IS NULL OR job.job_type = ANY(p_job_types))
    ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
    FOR UPDATE OF job SKIP LOCKED
    LIMIT 1;

    IF FOUND THEN
        UPDATE ops.jobs AS job
        SET status = CASE
                WHEN job.cancellation_requested THEN 'cancelled'
                ELSE 'dead_letter'
            END,
            lease_owner = NULL,
            lease_expires_at = NULL,
            cancellation_requested = false,
            error_message = NULL,
            error_code = CASE
                WHEN job.cancellation_requested THEN NULL
                ELSE 'lease_expired'
            END,
            updated_at = v_now,
            finished_at = v_now
        WHERE job.id = v_job.id
        RETURNING job.* INTO v_job;
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        ) VALUES (
            pg_catalog.gen_random_uuid(), v_job.id,
            CASE WHEN v_job.status = 'cancelled' THEN 'cancelled' ELSE 'dead_lettered' END,
            'running', v_job.status, NULL,
            pg_catalog.jsonb_build_object('reason', 'lease_expired'), v_now
        );
    END IF;

    v_now := pg_catalog.clock_timestamp();
    WITH ready_candidate AS MATERIALIZED (
        SELECT job.id, job.status AS from_status, job.priority,
               job.available_at, job.created_at
        FROM ops.jobs AS job
        JOIN ops.job_type_registry AS registry
          ON registry.job_type = job.job_type
         AND registry.payload_version = job.payload_version
         AND registry.enabled
        WHERE job.claimable
          AND job.status IN ('queued', 'retry_wait')
          AND job.available_at <= v_now
          AND (p_job_types IS NULL OR job.job_type = ANY(p_job_types))
        ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
        FOR UPDATE OF job SKIP LOCKED
        LIMIT 1
    ), reclaim_candidate AS MATERIALIZED (
        SELECT job.id, job.status AS from_status, job.priority,
               job.available_at, job.created_at
        FROM ops.jobs AS job
        JOIN ops.job_type_registry AS registry
          ON registry.job_type = job.job_type
         AND registry.payload_version = job.payload_version
         AND registry.enabled
        WHERE job.claimable
          AND job.status = 'running'
          AND job.attempts < job.max_attempts
          AND NOT job.cancellation_requested
          AND job.lease_expires_at <= v_now
          AND (p_job_types IS NULL OR job.job_type = ANY(p_job_types))
        ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
        FOR UPDATE OF job SKIP LOCKED
        LIMIT 1
    ), candidate AS (
        SELECT ranked.id, ranked.from_status
        FROM (
            SELECT * FROM ready_candidate
            UNION ALL
            SELECT * FROM reclaim_candidate
        ) AS ranked
        ORDER BY ranked.priority DESC, ranked.available_at, ranked.created_at, ranked.id
        LIMIT 1
    )
    UPDATE ops.jobs AS job
    SET status = 'running',
        lease_owner = p_worker_id,
        lease_expires_at = v_now + pg_catalog.make_interval(
            secs => p_lease_microseconds::double precision / 1000000.0
        ),
        attempts = job.attempts + 1,
        updated_at = v_now
    FROM candidate
    WHERE job.id = candidate.id
    RETURNING job.id, candidate.from_status INTO v_job_id, v_from_status;

    IF NOT FOUND THEN
        RETURN;
    END IF;
    SELECT job.* INTO STRICT v_job FROM ops.jobs AS job WHERE job.id = v_job_id;
    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(), v_job.id,
        CASE WHEN v_from_status = 'running' THEN 'reclaimed' ELSE 'claimed' END,
        v_from_status, 'running', p_worker_id,
        pg_catalog.jsonb_build_object(
            'attempts', v_job.attempts,
            'lease_expires_at', v_job.lease_expires_at,
            'job_type_filter', p_job_types
        ),
        v_now
    );
    RETURN QUERY
    SELECT v_job.id,
           v_job.job_type,
           v_job.payload_version,
           v_job.payload,
           v_job.attempts,
           v_job.max_attempts,
           v_job.lease_expires_at,
           v_job.cancellation_requested;
END
$function$;

ALTER FUNCTION ops.claim_job_for_types(text, bigint, text[]) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.claim_job_for_types(text, bigint, text[])
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.claim_job_for_types(text, bigint, text[])
    TO ingest_writer, image_writer;

-- Keep the public generic repository API and existing integrations intact.
CREATE OR REPLACE FUNCTION ops.claim_job(p_worker_id text, p_lease_microseconds bigint)
RETURNS TABLE (
    job_id uuid,
    job_type text,
    payload_version integer,
    payload jsonb,
    attempts integer,
    max_attempts integer,
    lease_expires_at timestamptz,
    cancellation_requested boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT * FROM ops.claim_job_for_types($1, $2, NULL::text[])
$function$;

ALTER FUNCTION ops.claim_job(text, bigint) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.claim_job(text, bigint)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.claim_job(text, bigint)
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0011',
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
  AND metadata.value ->> 'revision' = '0011'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 11
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
