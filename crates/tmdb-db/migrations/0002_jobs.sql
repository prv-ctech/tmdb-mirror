ALTER TABLE ops.job_type_registry
    DROP CONSTRAINT job_type_registry_pkey;
ALTER TABLE ops.job_type_registry
    ADD CONSTRAINT job_type_registry_pkey PRIMARY KEY (job_type, payload_version);

CREATE TABLE ops.jobs (
    id uuid PRIMARY KEY,
    job_type text NOT NULL,
    payload_version integer NOT NULL,
    payload jsonb NOT NULL,
    priority smallint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'queued',
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_expires_at timestamptz,
    dedup_key text NOT NULL,
    cancellation_requested boolean NOT NULL DEFAULT false,
    result_summary jsonb,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    CONSTRAINT jobs_registered_type_fk
        FOREIGN KEY (job_type, payload_version)
        REFERENCES ops.job_type_registry(job_type, payload_version),
    CONSTRAINT jobs_job_type_check CHECK (
        job_type = btrim(job_type)
        AND char_length(job_type) BETWEEN 1 AND 128
        AND job_type !~ '[[:cntrl:]]'
    ),
    CONSTRAINT jobs_payload_check CHECK (
        jsonb_typeof(payload) = 'object'
        AND octet_length(payload::text) <= 131072
    ),
    CONSTRAINT jobs_priority_check CHECK (priority BETWEEN -1000 AND 1000),
    CONSTRAINT jobs_status_check CHECK (
        status IN ('queued', 'running', 'retry_wait', 'succeeded', 'dead_letter', 'cancelled')
    ),
    CONSTRAINT jobs_attempts_check CHECK (
        attempts BETWEEN 0 AND 100
        AND max_attempts BETWEEN 1 AND 100
        AND attempts <= max_attempts
    ),
    CONSTRAINT jobs_dedup_key_check CHECK (
        dedup_key = btrim(dedup_key)
        AND char_length(dedup_key) BETWEEN 1 AND 256
        AND dedup_key !~ '[[:cntrl:]]'
    ),
    CONSTRAINT jobs_lease_owner_check CHECK (
        lease_owner IS NULL
        OR (
            lease_owner = btrim(lease_owner)
            AND char_length(lease_owner) BETWEEN 1 AND 128
            AND lease_owner !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT jobs_result_check CHECK (
        result_summary IS NULL
        OR (
            jsonb_typeof(result_summary) = 'object'
            AND octet_length(result_summary::text) <= 131072
        )
    ),
    CONSTRAINT jobs_error_check CHECK (
        error_message IS NULL
        OR (
            error_message = btrim(error_message)
            AND char_length(error_message) BETWEEN 1 AND 2048
            AND error_message !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT jobs_state_shape_check CHECK (
        (
            status IN ('queued', 'retry_wait')
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND NOT cancellation_requested
            AND finished_at IS NULL
        )
        OR (
            status = 'running'
            AND lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            status IN ('succeeded', 'dead_letter', 'cancelled')
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND NOT cancellation_requested
            AND finished_at IS NOT NULL
        )
    ),
    CONSTRAINT jobs_timestamp_order_check CHECK (
        updated_at >= created_at
        AND (finished_at IS NULL OR finished_at >= created_at)
    )
);

CREATE TABLE ops.job_events (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES ops.jobs(id),
    event_kind text NOT NULL,
    from_status text,
    to_status text NOT NULL,
    worker_id text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT job_events_kind_check CHECK (
        event_kind IN (
            'submitted', 'claimed', 'reclaimed', 'heartbeat', 'retry_scheduled',
            'succeeded', 'dead_lettered', 'cancellation_requested', 'cancelled'
        )
    ),
    CONSTRAINT job_events_from_status_check CHECK (
        from_status IS NULL
        OR from_status IN (
            'queued', 'running', 'retry_wait', 'succeeded', 'dead_letter', 'cancelled'
        )
    ),
    CONSTRAINT job_events_to_status_check CHECK (
        to_status IN (
            'queued', 'running', 'retry_wait', 'succeeded', 'dead_letter', 'cancelled'
        )
    ),
    CONSTRAINT job_events_worker_check CHECK (
        worker_id IS NULL
        OR (
            worker_id = btrim(worker_id)
            AND char_length(worker_id) BETWEEN 1 AND 128
            AND worker_id !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT job_events_details_check CHECK (
        jsonb_typeof(details) = 'object'
        AND octet_length(details::text) <= 8192
    ),
    CONSTRAINT job_events_submission_shape_check CHECK (
        (event_kind = 'submitted' AND from_status IS NULL AND to_status = 'queued')
        OR (event_kind <> 'submitted' AND from_status IS NOT NULL)
    )
);

CREATE UNIQUE INDEX jobs_active_dedup_uidx
    ON ops.jobs (job_type, dedup_key)
    WHERE status IN ('queued', 'retry_wait', 'running');
CREATE INDEX jobs_claim_ready_idx
    ON ops.jobs (priority DESC, available_at, created_at, id)
    WHERE status IN ('queued', 'retry_wait');
CREATE INDEX jobs_lease_expiry_idx
    ON ops.jobs (lease_expires_at, priority DESC, available_at, created_at, id)
    WHERE status = 'running';
CREATE INDEX job_events_job_history_idx
    ON ops.job_events (job_id, created_at, id);

CREATE VIEW ops.job_status AS
SELECT job.id,
       job.job_type,
       job.payload_version,
       job.priority,
       job.status,
       job.attempts,
       job.max_attempts,
       job.available_at,
       job.lease_owner,
       job.lease_expires_at,
       job.dedup_key,
       job.cancellation_requested,
       job.result_summary,
       job.error_message,
       job.created_at,
       job.updated_at,
       job.finished_at
FROM ops.jobs AS job;

CREATE FUNCTION ops.submit_job(
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
       OR p_job_type IS NULL
       OR p_job_type <> pg_catalog.btrim(p_job_type)
       OR pg_catalog.char_length(p_job_type) NOT BETWEEN 1 AND 128
       OR p_job_type ~ '[[:cntrl:]]'
       OR p_payload_version IS NULL
       OR p_payload_version <= 0
       OR p_payload IS NULL
       OR pg_catalog.octet_length(p_payload) > 65536
       OR p_priority IS NULL
       OR p_priority NOT BETWEEN -1000 AND 1000
       OR p_max_attempts IS NULL
       OR p_max_attempts NOT BETWEEN 1 AND 100
       OR p_dedup_key IS NULL
       OR p_dedup_key <> pg_catalog.btrim(p_dedup_key)
       OR pg_catalog.char_length(p_dedup_key) NOT BETWEEN 1 AND 256
       OR p_dedup_key ~ '[[:cntrl:]]'
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

CREATE FUNCTION ops.claim_job(p_worker_id text, p_lease_microseconds bigint)
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
    IF p_worker_id IS NULL
       OR p_worker_id <> pg_catalog.btrim(p_worker_id)
       OR pg_catalog.char_length(p_worker_id) NOT BETWEEN 1 AND 128
       OR p_worker_id ~ '[[:cntrl:]]'
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
      AND job.lease_expires_at <= v_now
      AND job.attempts >= job.max_attempts
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
            error_message = CASE
                WHEN job.cancellation_requested THEN NULL
                ELSE 'lease expired after maximum attempts'
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
    WITH candidate AS (
        SELECT job.id, job.status AS from_status
        FROM ops.jobs AS job
        WHERE (job.status IN ('queued', 'retry_wait') AND job.available_at <= v_now)
           OR (
               job.status = 'running'
               AND job.lease_expires_at <= v_now
               AND job.attempts < job.max_attempts
           )
        ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
        FOR UPDATE OF job SKIP LOCKED
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

CREATE FUNCTION ops.heartbeat_job(
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
       OR p_worker_id IS NULL
       OR p_worker_id <> pg_catalog.btrim(p_worker_id)
       OR pg_catalog.char_length(p_worker_id) NOT BETWEEN 1 AND 128
       OR p_worker_id ~ '[[:cntrl:]]'
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

CREATE FUNCTION ops.complete_job(p_job_id uuid, p_worker_id text, p_result text)
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
       OR p_worker_id IS NULL
       OR p_worker_id <> pg_catalog.btrim(p_worker_id)
       OR pg_catalog.char_length(p_worker_id) NOT BETWEEN 1 AND 128
       OR p_worker_id ~ '[[:cntrl:]]'
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

CREATE FUNCTION ops.fail_job(
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
       OR p_worker_id IS NULL
       OR p_worker_id <> pg_catalog.btrim(p_worker_id)
       OR pg_catalog.char_length(p_worker_id) NOT BETWEEN 1 AND 128
       OR p_worker_id ~ '[[:cntrl:]]'
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
        error_message = CASE WHEN v_status = 'cancelled' THEN NULL ELSE p_message END,
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

CREATE FUNCTION ops.request_job_cancel(p_job_id uuid, p_message text)
RETURNS TABLE (job_status text, cancellation_requested boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_job ops.jobs%ROWTYPE;
    v_from_status text;
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
    IF NOT EXISTS (
        SELECT 1
        FROM ops.job_type_registry AS registry
        WHERE registry.job_type = v_job.job_type
          AND registry.payload_version = v_job.payload_version
          AND registry.enabled
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job cancellation rejected';
    END IF;

    v_from_status := v_job.status;
    IF v_job.status IN ('queued', 'retry_wait') THEN
        UPDATE ops.jobs AS job
        SET status = 'cancelled',
            cancellation_requested = false,
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

ALTER TABLE ops.jobs OWNER TO migrator;
ALTER TABLE ops.job_events OWNER TO migrator;
ALTER VIEW ops.job_status OWNER TO migrator;
ALTER FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text)
    OWNER TO migrator;
ALTER FUNCTION ops.claim_job(text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.heartbeat_job(uuid, text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.complete_job(uuid, text, text) OWNER TO migrator;
ALTER FUNCTION ops.fail_job(uuid, text, text, bigint) OWNER TO migrator;
ALTER FUNCTION ops.request_job_cancel(uuid, text) OWNER TO migrator;

REVOKE ALL ON TABLE ops.jobs, ops.job_events, ops.job_status
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.job_status
    TO api_reader, api_job_submitter, ingest_writer, image_writer, monitor;

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
        'revision', '0002',
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
  AND metadata.value ->> 'revision' = '0002'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 2
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
