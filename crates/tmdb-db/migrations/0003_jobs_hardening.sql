GRANT USAGE ON SCHEMA ops TO ingest_writer, image_writer;

CREATE FUNCTION ops.job_valid_ascii(p_value text, p_max_chars integer)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT p_value IS NOT NULL
       AND p_max_chars > 0
       AND pg_catalog.char_length(p_value) BETWEEN 1 AND p_max_chars
       AND pg_catalog.octet_length(p_value) = pg_catalog.char_length(p_value)
       AND p_value = pg_catalog.btrim(p_value)
       AND p_value !~ '[[:cntrl:]]'
$function$;

ALTER FUNCTION ops.job_valid_ascii(text, integer) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.job_valid_ascii(text, integer) FROM PUBLIC;

ALTER TABLE ops.jobs
    ADD COLUMN error_code text,
    ADD COLUMN claimable boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT jobs_error_code_check CHECK (
        error_code IS NULL
        OR error_code IN (
            'execution_failed', 'upstream_unavailable', 'rate_limited',
            'invalid_payload', 'lease_expired', 'attempts_exhausted'
        )
    );

UPDATE ops.jobs AS job
SET claimable = registry.enabled,
    cancellation_requested = CASE
        WHEN NOT registry.enabled AND job.status = 'running' THEN true
        ELSE job.cancellation_requested
    END,
    updated_at = CASE
        WHEN NOT registry.enabled AND job.status = 'running'
        THEN pg_catalog.clock_timestamp()
        ELSE job.updated_at
    END
FROM ops.job_type_registry AS registry
WHERE registry.job_type = job.job_type
  AND registry.payload_version = job.payload_version;

CREATE FUNCTION ops.sync_job_claimable()
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

CREATE FUNCTION ops.sync_registry_job_claimability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE ops.jobs AS job
           SET claimable = NEW.enabled
         WHERE job.job_type = NEW.job_type
           AND job.payload_version = NEW.payload_version
           AND job.status IN ('queued', 'retry_wait', 'running');
    ELSIF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
        UPDATE ops.jobs AS job
           SET claimable = NEW.enabled,
               cancellation_requested = CASE
                   WHEN NOT NEW.enabled AND job.status = 'running' THEN true
                   ELSE job.cancellation_requested
               END,
               updated_at = CASE
                   WHEN NOT NEW.enabled AND job.status = 'running'
                   THEN pg_catalog.clock_timestamp()
                   ELSE job.updated_at
               END
         WHERE job.job_type = NEW.job_type
           AND job.payload_version = NEW.payload_version
           AND job.status IN ('queued', 'retry_wait', 'running');
    END IF;
    RETURN NEW;
END
$function$;

ALTER FUNCTION ops.sync_job_claimable() OWNER TO migrator;
ALTER FUNCTION ops.sync_registry_job_claimability() OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.sync_job_claimable() FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.sync_registry_job_claimability() FROM PUBLIC;

CREATE TRIGGER jobs_claimable_sync
BEFORE INSERT OR UPDATE OF job_type, payload_version
ON ops.jobs
FOR EACH ROW EXECUTE FUNCTION ops.sync_job_claimable();

CREATE TRIGGER registry_job_claimability_sync
AFTER INSERT OR UPDATE OF enabled
ON ops.job_type_registry
FOR EACH ROW EXECUTE FUNCTION ops.sync_registry_job_claimability();

DROP INDEX ops.jobs_claim_ready_idx;
DROP INDEX ops.jobs_lease_expiry_idx;
CREATE INDEX jobs_claim_ready_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    INCLUDE (job_type, payload_version)
    WHERE claimable AND status IN ('queued', 'retry_wait');
CREATE INDEX jobs_reclaim_ready_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    INCLUDE (lease_expires_at, job_type, payload_version)
    WHERE claimable AND status = 'running' AND attempts < max_attempts;
CREATE INDEX jobs_exhausted_expired_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    INCLUDE (lease_expires_at)
    WHERE status = 'running' AND attempts >= max_attempts;
CREATE INDEX jobs_cancel_requested_expired_idx
    ON ops.jobs (lease_expires_at, priority DESC, available_at, created_at, id)
    WHERE status = 'running' AND cancellation_requested;

DROP VIEW ops.job_status;
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

CREATE OR REPLACE FUNCTION ops.submit_job(
    p_id uuid,
    p_job_type text,
    p_payload_version integer,
    p_payload text,
    p_priority smallint,
    p_max_attempts integer,
    p_available_at timestamptz,
    p_dedup_key text
)
RETURNS TABLE (job_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_job_id uuid;
    v_was_duplicate boolean := false;
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_payload jsonb;
BEGIN
    IF p_id IS NULL
       OR NOT ops.job_valid_ascii(p_job_type, 128)
       OR p_payload_version IS NULL
       OR p_payload_version <= 0
       OR p_payload IS NULL
       OR pg_catalog.octet_length(p_payload) > 65536
       OR p_priority IS NULL
       OR p_priority NOT BETWEEN -1000 AND 1000
       OR p_max_attempts IS NULL
       OR p_max_attempts NOT BETWEEN 1 AND 100
       OR (
           p_available_at IS NOT NULL
           AND (
               NOT pg_catalog.isfinite(p_available_at)
               OR EXTRACT(year FROM p_available_at) NOT BETWEEN 1 AND 9999
           )
       )
       OR NOT ops.job_valid_ascii(p_dedup_key, 256)
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job request rejected';
    END IF;

    BEGIN
        v_payload := p_payload::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job request rejected';
    END;
    IF pg_catalog.jsonb_typeof(v_payload) <> 'object' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job request rejected';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM ops.job_type_registry AS registry
        WHERE registry.job_type = p_job_type
          AND registry.payload_version = p_payload_version
          AND registry.enabled
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job request rejected';
    END IF;

    INSERT INTO ops.jobs (
        id, job_type, payload_version, payload, priority, status, attempts, max_attempts,
        available_at, dedup_key, created_at, updated_at
    ) VALUES (
        p_id, p_job_type, p_payload_version, v_payload, p_priority, 'queued', 0,
        p_max_attempts, COALESCE(p_available_at, v_now), p_dedup_key, v_now, v_now
    )
    ON CONFLICT (job_type, dedup_key)
        WHERE status IN ('queued', 'retry_wait', 'running')
    DO NOTHING
    RETURNING ops.jobs.id INTO v_job_id;

    IF v_job_id IS NULL THEN
        v_was_duplicate := true;
        SELECT job.id
        INTO v_job_id
        FROM ops.jobs AS job
        WHERE job.job_type = p_job_type
          AND job.dedup_key = p_dedup_key
          AND job.status IN ('queued', 'retry_wait', 'running')
        ORDER BY job.created_at, job.id
        LIMIT 1;
        IF v_job_id IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'job submission retry required';
        END IF;
    ELSE
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        ) VALUES (
            pg_catalog.gen_random_uuid(), v_job_id, 'submitted', NULL, 'queued', NULL,
            pg_catalog.jsonb_build_object('payload_version', p_payload_version), v_now
        );
    END IF;

    RETURN QUERY SELECT v_job_id, v_was_duplicate;
END
$function$;

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

CREATE OR REPLACE FUNCTION ops.heartbeat_job(
    p_job_id uuid,
    p_worker_id text,
    p_lease_microseconds bigint
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_expires_at timestamptz;
BEGIN
    IF p_job_id IS NULL
       OR NOT ops.job_valid_ascii(p_worker_id, 128)
       OR p_lease_microseconds IS NULL
       OR p_lease_microseconds NOT BETWEEN 1 AND 3600000000
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job heartbeat rejected';
    END IF;

    UPDATE ops.jobs AS job
    SET lease_expires_at = v_now + pg_catalog.make_interval(
            secs => p_lease_microseconds::double precision / 1000000.0
        ),
        updated_at = v_now
    WHERE job.id = p_job_id
      AND job.status = 'running'
      AND job.lease_owner = p_worker_id
      AND job.lease_expires_at > v_now
    RETURNING job.lease_expires_at INTO v_expires_at;
    IF NOT FOUND THEN
        IF NOT EXISTS (SELECT 1 FROM ops.jobs AS job WHERE job.id = p_job_id) THEN
            RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
        END IF;
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'job lease lost';
    END IF;
    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(), p_job_id, 'heartbeat', 'running', 'running',
        p_worker_id, pg_catalog.jsonb_build_object('lease_expires_at', v_expires_at), v_now
    );
    RETURN v_expires_at;
END
$function$;

CREATE OR REPLACE FUNCTION ops.complete_job(p_job_id uuid, p_worker_id text, p_result text)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_status text;
    v_result jsonb;
BEGIN
    IF p_job_id IS NULL
       OR NOT ops.job_valid_ascii(p_worker_id, 128)
       OR p_result IS NULL
       OR pg_catalog.octet_length(p_result) > 65536
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job completion rejected';
    END IF;

    BEGIN
        v_result := p_result::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job completion rejected';
    END;
    IF pg_catalog.jsonb_typeof(v_result) <> 'object' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job completion rejected';
    END IF;

    UPDATE ops.jobs AS job
    SET status = CASE WHEN job.cancellation_requested THEN 'cancelled' ELSE 'succeeded' END,
        result_summary = CASE WHEN job.cancellation_requested THEN NULL ELSE v_result END,
        error_message = NULL,
        error_code = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        cancellation_requested = false,
        updated_at = v_now,
        finished_at = v_now
    WHERE job.id = p_job_id
      AND job.status = 'running'
      AND job.lease_owner = p_worker_id
      AND job.lease_expires_at > v_now
    RETURNING job.status INTO v_status;
    IF NOT FOUND THEN
        IF NOT EXISTS (SELECT 1 FROM ops.jobs AS job WHERE job.id = p_job_id) THEN
            RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
        END IF;
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'job lease lost';
    END IF;
    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(), p_job_id,
        CASE WHEN v_status = 'cancelled' THEN 'cancelled' ELSE 'succeeded' END,
        'running', v_status, p_worker_id, '{}'::jsonb, v_now
    );
    RETURN v_status;
END
$function$;

CREATE FUNCTION ops.job_failure_code(p_message text)
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

ALTER FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text)
    OWNER TO migrator;
ALTER FUNCTION ops.claim_job(text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.heartbeat_job(uuid, text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.complete_job(uuid, text, text) OWNER TO migrator;
ALTER FUNCTION ops.fail_job(uuid, text, text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.request_job_cancel(uuid, text) OWNER TO migrator;

REVOKE ALL ON TABLE ops.jobs, ops.job_events
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
REVOKE ALL ON FUNCTION
    ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text),
    ops.claim_job(text, bigint),
    ops.heartbeat_job(uuid, text, bigint),
    ops.complete_job(uuid, text, text),
    ops.fail_job(uuid, text, text, bigint),
    ops.request_job_cancel(uuid, text)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION
    ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text),
    ops.request_job_cancel(uuid, text)
    TO api_job_submitter;
GRANT EXECUTE ON FUNCTION
    ops.claim_job(text, bigint),
    ops.heartbeat_job(uuid, text, bigint),
    ops.complete_job(uuid, text, text),
    ops.fail_job(uuid, text, text, bigint)
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0003',
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
  AND metadata.value ->> 'revision' = '0003'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 3
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
