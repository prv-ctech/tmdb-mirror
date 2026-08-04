-- Make both job workers idle until an administrator starts them.

CREATE TABLE ops.worker_control (
    worker_kind text PRIMARY KEY,
    state text NOT NULL DEFAULT 'stopped',
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT worker_control_kind_check CHECK (worker_kind IN ('ingest', 'media')),
    CONSTRAINT worker_control_state_check CHECK (state IN ('running', 'paused', 'stopped'))
);

INSERT INTO ops.worker_control (worker_kind, state)
VALUES ('ingest', 'stopped'), ('media', 'stopped');

CREATE TABLE ops.worker_requests (
    worker_kind text NOT NULL,
    idempotency_key text NOT NULL,
    action text NOT NULL,
    state text NOT NULL,
    request_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    PRIMARY KEY (worker_kind, idempotency_key),
    CONSTRAINT worker_requests_kind_check CHECK (worker_kind IN ('ingest', 'media')),
    CONSTRAINT worker_requests_key_check CHECK (
        idempotency_key = pg_catalog.btrim(idempotency_key)
        AND pg_catalog.char_length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key !~ '[[:cntrl:]]'
    ),
    CONSTRAINT worker_requests_action_check CHECK (
        action IN ('start', 'pause', 'resume', 'cancel')
    ),
    CONSTRAINT worker_requests_state_check CHECK (
        state IN ('running', 'paused', 'stopped')
    )
);

INSERT INTO ops.worker_requests (
    worker_kind, idempotency_key, action, state, request_id, created_at
)
SELECT 'media', request.idempotency_key, request.action, request.state,
       request.request_id, request.created_at
  FROM ops.media_worker_requests AS request
ON CONFLICT (worker_kind, idempotency_key) DO NOTHING;

ALTER TABLE ops.worker_control OWNER TO migrator;
ALTER TABLE ops.worker_requests OWNER TO migrator;

CREATE FUNCTION ops.worker_claim_enabled(p_worker_kind text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT COALESCE(
        (SELECT control.state = 'running'
           FROM ops.worker_control AS control
          WHERE control.worker_kind = p_worker_kind),
        false
    )
$function$;

ALTER FUNCTION ops.worker_claim_enabled(text) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.worker_claim_enabled(text)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.worker_claim_enabled(text)
    TO ingest_writer, image_writer;

CREATE OR REPLACE FUNCTION ops.media_worker_claim_enabled()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT ops.worker_claim_enabled('media')
$function$;

ALTER FUNCTION ops.media_worker_claim_enabled() OWNER TO migrator;

CREATE OR REPLACE FUNCTION ops.set_worker_state(
    p_worker_kind text,
    p_action text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (state text, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_existing ops.worker_requests%ROWTYPE;
    v_state text;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_worker_kind NOT IN ('ingest', 'media')
       OR p_action NOT IN ('start', 'pause', 'resume', 'cancel')
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'worker request rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'worker.' || p_worker_kind || E'\\x1f' || p_idempotency_key, 0
        )
    );
    SELECT request.*
      INTO v_existing
      FROM ops.worker_requests AS request
     WHERE request.worker_kind = p_worker_kind
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.action <> p_action THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'worker idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.state, true;
        RETURN;
    END IF;

    v_state := CASE p_action
        WHEN 'pause' THEN 'paused'
        WHEN 'cancel' THEN 'stopped'
        ELSE 'running'
    END;
    UPDATE ops.worker_control
       SET state = v_state, updated_at = v_now
     WHERE worker_kind = p_worker_kind;

    IF p_action = 'cancel' THEN
        WITH candidates AS MATERIALIZED (
            SELECT job.id, job.status AS from_status
              FROM ops.jobs AS job
             WHERE (
                    (p_worker_kind = 'media'
                     AND job.job_type IN ('image.download', 'admin.media_audit', 'system.noop'))
                    OR
                    (p_worker_kind = 'ingest'
                     AND (job.job_type LIKE 'ingest.%'
                          OR job.job_type IN ('admin.scan', 'admin.media_scan', 'admin.analyze')))
                   )
               AND job.status IN ('queued', 'retry_wait')
             FOR UPDATE
        ), cancelled AS (
            UPDATE ops.jobs AS job
               SET status = 'cancelled',
                   updated_at = v_now,
                   finished_at = v_now
              FROM candidates
             WHERE job.id = candidates.id
            RETURNING job.id
        )
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        )
        SELECT pg_catalog.gen_random_uuid(), candidates.id, 'cancelled', candidates.from_status,
               'cancelled', NULL,
               pg_catalog.jsonb_build_object('reason', p_worker_kind || '_worker_cancelled'), v_now
          FROM candidates
          JOIN cancelled ON cancelled.id = candidates.id;

        WITH requested AS (
            UPDATE ops.jobs AS job
               SET cancellation_requested = true,
                   updated_at = v_now
             WHERE (
                    (p_worker_kind = 'media'
                     AND job.job_type IN ('image.download', 'admin.media_audit', 'system.noop'))
                    OR
                    (p_worker_kind = 'ingest'
                     AND (job.job_type LIKE 'ingest.%'
                          OR job.job_type IN ('admin.scan', 'admin.media_scan', 'admin.analyze')))
                   )
               AND job.status = 'running'
               AND NOT job.cancellation_requested
            RETURNING job.id
        )
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        )
        SELECT pg_catalog.gen_random_uuid(), requested.id, 'cancellation_requested',
               'running', 'running', NULL,
               pg_catalog.jsonb_build_object('reason', p_worker_kind || '_worker_cancelled'), v_now
          FROM requested;
    END IF;

    INSERT INTO ops.worker_requests (
        worker_kind, idempotency_key, action, state, request_id
    ) VALUES (
        p_worker_kind, p_idempotency_key, p_action, v_state, p_request_id
    );
    RETURN QUERY SELECT v_state, false;
END
$function$;

ALTER FUNCTION ops.set_worker_state(text, text, text, uuid) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.set_worker_state(text, text, text, uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.set_worker_state(text, text, text, uuid)
    TO api_job_submitter;

-- Keep the old internal media claim gate as a harmless adapter for the claim
-- implementation installed by migration 0029.
CREATE OR REPLACE FUNCTION ops.set_media_worker_state(
    p_action text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (state text, was_duplicate boolean)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT * FROM ops.set_worker_state('media', $1, $2, $3)
$function$;

ALTER FUNCTION ops.set_media_worker_state(text, text, uuid) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.set_media_worker_state(text, text, uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.set_media_worker_state(text, text, uuid)
    TO api_job_submitter;

-- The old media singleton and request table are replaced by the single
-- worker-control store. Existing media requests were copied above.
DROP TABLE ops.media_worker_requests;
DROP TABLE ops.media_worker_control;

-- Rename the existing claim implementation and add one state gate around it.
-- The inner implementation still performs all lease, retry, and event logic.
ALTER FUNCTION ops.claim_job_for_types(text, bigint, text[])
    RENAME TO claim_job_for_types_uncontrolled;

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
BEGIN
    IF p_job_types IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_catalog.unnest(p_job_types) AS allowed(job_type)
           WHERE allowed.job_type IN ('image.download', 'admin.media_audit', 'system.noop')
       )
    THEN
        IF NOT ops.worker_claim_enabled('media') THEN
            RETURN;
        END IF;
    ELSIF p_job_types IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_catalog.unnest(p_job_types) AS allowed(job_type)
           WHERE allowed.job_type LIKE 'ingest.%'
              OR allowed.job_type IN ('admin.scan', 'admin.media_scan', 'admin.analyze')
       )
    THEN
        IF NOT ops.worker_claim_enabled('ingest') THEN
            RETURN;
        END IF;
    END IF;
    RETURN QUERY
    SELECT *
      FROM ops.claim_job_for_types_uncontrolled(
          p_worker_id, p_lease_microseconds, p_job_types
      );
END
$function$;

ALTER FUNCTION ops.claim_job_for_types(text, bigint, text[]) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.claim_job_for_types(text, bigint, text[])
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.claim_job_for_types(text, bigint, text[])
    TO ingest_writer, image_writer;

GRANT SELECT ON TABLE ops.worker_control TO monitor;
GRANT SELECT ON TABLE ops.worker_requests TO monitor;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0031',
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
  AND metadata.value ->> 'revision' = '0031'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 31
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
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
