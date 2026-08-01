-- Repair the round-one 0003 schema in place.  Fresh databases already have the
-- round-two objects from 0003, so every object operation below is safe when it
-- is repeated against that shape as well.

DO $constraints$
DECLARE
    v_definition text;
BEGIN
    -- Round-one 0003 replaced these 0002 checks with byte/finite restrictions.
    -- Restore the published 0002 checks when that older shape is encountered;
    -- fresh round-two 0003 databases retain their original checks unchanged.
    SELECT pg_catalog.pg_get_constraintdef(constraint_oid)
      INTO v_definition
      FROM (
          SELECT c.oid AS constraint_oid
          FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
          WHERE n.nspname = 'ops'
            AND r.relname = 'job_type_registry'
            AND c.conname = 'job_type_registry_job_type_check'
      ) AS constraint_row;
    IF v_definition LIKE '%octet_length%' THEN
        ALTER TABLE ops.job_type_registry
            DROP CONSTRAINT job_type_registry_job_type_check;
        ALTER TABLE ops.job_type_registry
            ADD CONSTRAINT job_type_registry_job_type_check CHECK (
                pg_catalog.btrim(job_type) <> ''
            );
    END IF;

    SELECT pg_catalog.pg_get_constraintdef(constraint_oid)
      INTO v_definition
      FROM (
          SELECT c.oid AS constraint_oid
          FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
          WHERE n.nspname = 'ops'
            AND r.relname = 'jobs'
            AND c.conname = 'jobs_job_type_check'
      ) AS constraint_row;
    IF v_definition LIKE '%octet_length%' THEN
        ALTER TABLE ops.jobs DROP CONSTRAINT jobs_job_type_check;
        ALTER TABLE ops.jobs ADD CONSTRAINT jobs_job_type_check CHECK (
            job_type = pg_catalog.btrim(job_type)
            AND pg_catalog.char_length(job_type) BETWEEN 1 AND 128
            AND job_type !~ '[[:cntrl:]]'
        );
    END IF;

    SELECT pg_catalog.pg_get_constraintdef(constraint_oid)
      INTO v_definition
      FROM (
          SELECT c.oid AS constraint_oid
          FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
          WHERE n.nspname = 'ops'
            AND r.relname = 'jobs'
            AND c.conname = 'jobs_dedup_key_check'
      ) AS constraint_row;
    IF v_definition LIKE '%octet_length%' THEN
        ALTER TABLE ops.jobs DROP CONSTRAINT jobs_dedup_key_check;
        ALTER TABLE ops.jobs ADD CONSTRAINT jobs_dedup_key_check CHECK (
            dedup_key = pg_catalog.btrim(dedup_key)
            AND pg_catalog.char_length(dedup_key) BETWEEN 1 AND 256
            AND dedup_key !~ '[[:cntrl:]]'
        );
    END IF;

    SELECT pg_catalog.pg_get_constraintdef(constraint_oid)
      INTO v_definition
      FROM (
          SELECT c.oid AS constraint_oid
          FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
          WHERE n.nspname = 'ops'
            AND r.relname = 'jobs'
            AND c.conname = 'jobs_lease_owner_check'
      ) AS constraint_row;
    IF v_definition LIKE '%octet_length%' THEN
        ALTER TABLE ops.jobs DROP CONSTRAINT jobs_lease_owner_check;
        ALTER TABLE ops.jobs ADD CONSTRAINT jobs_lease_owner_check CHECK (
            lease_owner IS NULL
            OR (
                lease_owner = pg_catalog.btrim(lease_owner)
                AND pg_catalog.char_length(lease_owner) BETWEEN 1 AND 128
                AND lease_owner !~ '[[:cntrl:]]'
            )
        );
    END IF;

    SELECT pg_catalog.pg_get_constraintdef(constraint_oid)
      INTO v_definition
      FROM (
          SELECT c.oid AS constraint_oid
          FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
          WHERE n.nspname = 'ops'
            AND r.relname = 'job_events'
            AND c.conname = 'job_events_worker_check'
      ) AS constraint_row;
    IF v_definition LIKE '%octet_length%' THEN
        ALTER TABLE ops.job_events DROP CONSTRAINT job_events_worker_check;
        ALTER TABLE ops.job_events ADD CONSTRAINT job_events_worker_check CHECK (
            worker_id IS NULL
            OR (
                worker_id = pg_catalog.btrim(worker_id)
                AND pg_catalog.char_length(worker_id) BETWEEN 1 AND 128
                AND worker_id !~ '[[:cntrl:]]'
            )
        );
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint AS c
        JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
        WHERE n.nspname = 'ops'
          AND r.relname = 'jobs'
          AND c.conname = 'jobs_available_at_check'
    ) THEN
        ALTER TABLE ops.jobs DROP CONSTRAINT jobs_available_at_check;
    END IF;
END
$constraints$;

ALTER TABLE ops.job_events
    DROP CONSTRAINT IF EXISTS job_events_kind_check;
ALTER TABLE ops.job_events
    ADD CONSTRAINT job_events_kind_check CHECK (
        event_kind IN (
            'submitted', 'claimed', 'reclaimed', 'heartbeat', 'retry_scheduled',
            'succeeded', 'dead_lettered', 'cancellation_requested', 'cancelled',
            'claimability_changed'
        )
    );

ALTER TABLE ops.jobs
    ADD COLUMN IF NOT EXISTS claimable boolean NOT NULL DEFAULT false;

UPDATE ops.jobs AS job
SET claimable = registry.enabled,
    cancellation_requested = CASE
        WHEN NOT registry.enabled AND job.status = 'running' THEN true
        ELSE job.cancellation_requested
    END,
    updated_at = CASE
        WHEN NOT registry.enabled
             AND job.status = 'running'
             AND NOT job.cancellation_requested
        THEN pg_catalog.clock_timestamp()
        ELSE job.updated_at
    END
FROM ops.job_type_registry AS registry
WHERE registry.job_type = job.job_type
  AND registry.payload_version = job.payload_version
  AND (
      job.claimable IS DISTINCT FROM registry.enabled
      OR (NOT registry.enabled AND job.status = 'running' AND NOT job.cancellation_requested)
  );

INSERT INTO ops.job_events (
    id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
)
SELECT
    pg_catalog.gen_random_uuid(),
    job.id,
    CASE WHEN job.status = 'running'
              AND NOT EXISTS (
                  SELECT 1
                  FROM ops.job_events AS cancellation
                  WHERE cancellation.job_id = job.id
                    AND cancellation.event_kind = 'cancellation_requested'
              )
         THEN 'cancellation_requested'
         ELSE 'claimability_changed'
    END,
    job.status,
    job.status,
    NULL,
    pg_catalog.jsonb_build_object(
        'reason', 'migration_reconcile',
        'type', job.job_type,
        'job_type', job.job_type,
        'payload_version', job.payload_version,
        'enabled', registry.enabled
    ),
    pg_catalog.clock_timestamp()
FROM ops.jobs AS job
JOIN ops.job_type_registry AS registry
  ON registry.job_type = job.job_type
 AND registry.payload_version = job.payload_version
WHERE NOT registry.enabled
  AND job.status IN ('queued', 'retry_wait', 'running')
  AND NOT EXISTS (
      SELECT 1
      FROM ops.job_events AS event
      WHERE event.job_id = job.id
        AND event.details ->> 'reason' = 'migration_reconcile'
  );

CREATE OR REPLACE FUNCTION ops.sync_job_claimable()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    SELECT registry.enabled
      INTO NEW.claimable
      FROM ops.job_type_registry AS registry
     WHERE registry.job_type = NEW.job_type
       AND registry.payload_version = NEW.payload_version;
    NEW.claimable := COALESCE(NEW.claimable, false);
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION ops.job_failure_code(p_message text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT CASE
        WHEN p_message IN (
            'execution_failed', 'upstream_unavailable', 'rate_limited',
            'invalid_payload', 'lease_expired', 'attempts_exhausted'
        ) THEN p_message
        ELSE 'execution_failed'
    END
$function$;

ALTER FUNCTION ops.job_failure_code(text) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.job_failure_code(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION ops.fail_job(
    p_job_id uuid,
    p_worker_id text,
    p_message text,
    p_retry_microseconds bigint
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
       OR p_retry_microseconds IS NULL
       OR p_retry_microseconds NOT BETWEEN 1 AND 604800000000
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job failure rejected';
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
        WHEN v_job.attempts < v_job.max_attempts THEN 'retry_wait'
        ELSE 'dead_letter'
    END;
    UPDATE ops.jobs AS job
    SET status = v_status,
        available_at = CASE
            WHEN v_status = 'retry_wait' THEN
                v_now + pg_catalog.make_interval(
                    secs => p_retry_microseconds::double precision / 1000000.0
                )
            ELSE job.available_at
        END,
        lease_owner = NULL,
        lease_expires_at = NULL,
        cancellation_requested = false,
        error_message = NULL,
        error_code = CASE
            WHEN v_status = 'cancelled' THEN NULL
            ELSE ops.job_failure_code(p_message)
        END,
        updated_at = v_now,
        finished_at = CASE WHEN v_status IN ('cancelled', 'dead_letter') THEN v_now ELSE NULL END
    WHERE job.id = p_job_id
    RETURNING job.* INTO v_job;
    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(), p_job_id,
        CASE v_status
            WHEN 'retry_wait' THEN 'retry_scheduled'
            WHEN 'dead_letter' THEN 'dead_lettered'
            ELSE 'cancelled'
        END,
        'running', v_status, p_worker_id,
        CASE WHEN v_status = 'retry_wait'
            THEN pg_catalog.jsonb_build_object('available_at', v_job.available_at)
            ELSE '{}'::jsonb
        END,
        v_now
    );
    RETURN QUERY
    SELECT CASE v_status
               WHEN 'retry_wait' THEN 'retry_scheduled'
               WHEN 'dead_letter' THEN 'dead_lettered'
               ELSE 'cancelled'
           END,
           v_job.available_at;
END
$function$;

CREATE OR REPLACE FUNCTION ops.request_job_cancel(p_job_id uuid, p_message text)
RETURNS TABLE (job_status text, cancellation_requested boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_job ops.jobs%ROWTYPE;
    v_from_status text;
    v_enabled boolean;
BEGIN
    IF p_job_id IS NULL
       OR p_message IS NULL
       OR p_message <> pg_catalog.btrim(p_message)
       OR pg_catalog.char_length(p_message) NOT BETWEEN 1 AND 1024
       OR p_message ~ '[[:cntrl:]]'
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job cancellation rejected';
    END IF;

    SELECT job.*
    INTO v_job
    FROM ops.jobs AS job
    WHERE job.id = p_job_id
    FOR UPDATE OF job;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
    END IF;
    SELECT registry.enabled
    INTO STRICT v_enabled
    FROM ops.job_type_registry AS registry
    WHERE registry.job_type = v_job.job_type
      AND registry.payload_version = v_job.payload_version;

    v_from_status := v_job.status;
    IF v_job.status IN ('queued', 'retry_wait')
       OR (
           v_job.status = 'running'
           AND v_job.lease_expires_at <= v_now
           AND NOT v_enabled
       )
    THEN
        UPDATE ops.jobs AS job
        SET status = 'cancelled',
            lease_owner = NULL,
            lease_expires_at = NULL,
            cancellation_requested = false,
            error_message = NULL,
            error_code = NULL,
            updated_at = v_now,
            finished_at = v_now
        WHERE job.id = p_job_id
        RETURNING job.* INTO v_job;
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        ) VALUES (
            pg_catalog.gen_random_uuid(), p_job_id, 'cancelled', v_from_status, 'cancelled',
            NULL, pg_catalog.jsonb_build_object('message', p_message), v_now
        );
    ELSIF v_job.status = 'running' AND NOT v_job.cancellation_requested THEN
        UPDATE ops.jobs AS job
        SET cancellation_requested = true,
            updated_at = v_now
        WHERE job.id = p_job_id
        RETURNING job.* INTO v_job;
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        ) VALUES (
            pg_catalog.gen_random_uuid(), p_job_id, 'cancellation_requested', 'running',
            'running', NULL, pg_catalog.jsonb_build_object('message', p_message), v_now
        );
    END IF;
    RETURN QUERY SELECT v_job.status, v_job.cancellation_requested;
END
$function$;

CREATE OR REPLACE FUNCTION ops.sync_registry_job_claimability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_job ops.jobs%ROWTYPE;
    v_now timestamptz;
    v_enabled_changed boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_enabled_changed := true;
    ELSE
        v_enabled_changed := NEW.enabled IS DISTINCT FROM OLD.enabled;
    END IF;

    IF v_enabled_changed THEN
        v_now := pg_catalog.clock_timestamp();
        FOR v_job IN
            SELECT job.*
            FROM ops.jobs AS job
            WHERE job.job_type = NEW.job_type
              AND job.payload_version = NEW.payload_version
              AND job.status IN ('queued', 'retry_wait', 'running')
            FOR UPDATE OF job
        LOOP
            IF v_job.status = 'running'
               AND NOT NEW.enabled
               AND NOT v_job.cancellation_requested
            THEN
                UPDATE ops.jobs AS job
                   SET claimable = NEW.enabled,
                       cancellation_requested = true,
                       updated_at = v_now
                 WHERE job.id = v_job.id;
                INSERT INTO ops.job_events (
                    id, job_id, event_kind, from_status, to_status,
                    worker_id, details, created_at
                ) VALUES (
                    pg_catalog.gen_random_uuid(), v_job.id,
                    'cancellation_requested', 'running', 'running', NULL,
                    pg_catalog.jsonb_build_object(
                        'reason', 'job_type_disabled',
                        'type', NEW.job_type,
                        'job_type', NEW.job_type,
                        'payload_version', NEW.payload_version,
                        'enabled', NEW.enabled
                    ),
                    v_now
                );
            ELSIF v_job.claimable IS DISTINCT FROM NEW.enabled THEN
                UPDATE ops.jobs AS job
                   SET claimable = NEW.enabled
                 WHERE job.id = v_job.id;
                INSERT INTO ops.job_events (
                    id, job_id, event_kind, from_status, to_status,
                    worker_id, details, created_at
                ) VALUES (
                    pg_catalog.gen_random_uuid(), v_job.id,
                    'claimability_changed', v_job.status, v_job.status, NULL,
                    pg_catalog.jsonb_build_object(
                        'reason', 'registry_claimability_changed',
                        'type', NEW.job_type,
                        'job_type', NEW.job_type,
                        'payload_version', NEW.payload_version,
                        'enabled', NEW.enabled
                    ),
                    v_now
                );
            END IF;
        END LOOP;
    END IF;
    RETURN NEW;
END
$function$;

ALTER FUNCTION ops.sync_job_claimable() OWNER TO migrator;
ALTER FUNCTION ops.sync_registry_job_claimability() OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.sync_job_claimable() FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.sync_registry_job_claimability() FROM PUBLIC;

DROP TRIGGER IF EXISTS jobs_claimable_sync ON ops.jobs;
CREATE TRIGGER jobs_claimable_sync
BEFORE INSERT OR UPDATE OF job_type, payload_version
ON ops.jobs
FOR EACH ROW EXECUTE FUNCTION ops.sync_job_claimable();

DROP TRIGGER IF EXISTS registry_job_claimability_sync ON ops.job_type_registry;
CREATE TRIGGER registry_job_claimability_sync
AFTER INSERT OR UPDATE OF enabled
ON ops.job_type_registry
FOR EACH ROW EXECUTE FUNCTION ops.sync_registry_job_claimability();

DROP INDEX IF EXISTS ops.jobs_claim_ready_idx;
DROP INDEX IF EXISTS ops.jobs_reclaim_ready_idx;
DROP INDEX IF EXISTS ops.jobs_lease_expiry_idx;
DROP INDEX IF EXISTS ops.jobs_cancel_requested_expired_idx;
CREATE INDEX jobs_claim_ready_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    INCLUDE (job_type, payload_version)
    WHERE claimable AND status IN ('queued', 'retry_wait');
CREATE INDEX jobs_reclaim_ready_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    INCLUDE (lease_expires_at, job_type, payload_version)
    WHERE claimable AND status = 'running' AND attempts < max_attempts;
CREATE INDEX jobs_cancel_requested_expired_idx
    ON ops.jobs (lease_expires_at, priority DESC, available_at, created_at, id)
    WHERE status = 'running' AND cancellation_requested;

DROP VIEW IF EXISTS ops.job_status;
CREATE VIEW ops.job_status AS
SELECT job.id,
       job.status,
       job.attempts,
       job.max_attempts,
       job.available_at,
       job.cancellation_requested,
       job.error_code AS error_message,
       job.created_at,
       job.updated_at,
       job.finished_at
FROM ops.jobs AS job;

ALTER VIEW ops.job_status OWNER TO migrator;
REVOKE ALL ON TABLE ops.job_status
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.job_status
    TO api_job_submitter, ingest_writer, image_writer;

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
            'lease_expires_at', v_job.lease_expires_at
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

ALTER FUNCTION ops.claim_job(text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.fail_job(uuid, text, text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.request_job_cancel(uuid, text) OWNER TO migrator;

REVOKE ALL ON TABLE ops.jobs, ops.job_events
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
REVOKE ALL ON FUNCTION
    ops.claim_job(text, bigint),
    ops.fail_job(uuid, text, text, bigint),
    ops.request_job_cancel(uuid, text)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION
    ops.request_job_cancel(uuid, text)
    TO api_job_submitter;
GRANT EXECUTE ON FUNCTION
    ops.claim_job(text, bigint),
    ops.fail_job(uuid, text, text, bigint)
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0004',
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
  AND metadata.value ->> 'revision' = '0004'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 4
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
