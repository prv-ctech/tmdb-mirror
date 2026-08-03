-- Reusable-gallery backfill, durable media scans, and persistent media-worker control.

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('ingest.refresh_reusable_gallery', 1, true),
       ('admin.media_scan', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE
SET enabled = EXCLUDED.enabled;

CREATE TABLE ops.media_scan_runs (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL UNIQUE REFERENCES ops.jobs(id),
    mode text NOT NULL,
    repair boolean NOT NULL DEFAULT false,
    phase text NOT NULL DEFAULT 'queued',
    status text NOT NULL DEFAULT 'queued',
    requested_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    queued_count bigint NOT NULL DEFAULT 0,
    completed_count bigint NOT NULL DEFAULT 0,
    failed_count bigint NOT NULL DEFAULT 0,
    audited_count bigint NOT NULL DEFAULT 0,
    invalid_count bigint NOT NULL DEFAULT 0,
    repair_queued_count bigint NOT NULL DEFAULT 0,
    error_code text,
    CONSTRAINT media_scan_runs_mode_check CHECK (mode IN ('full', 'missing', 'audit')),
    CONSTRAINT media_scan_runs_phase_check CHECK (
        phase IN ('queued', 'catalog', 'media', 'audit', 'completed')
    ),
    CONSTRAINT media_scan_runs_status_check CHECK (
        status IN ('queued', 'running', 'paused', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT media_scan_runs_counts_check CHECK (
        queued_count >= 0
        AND completed_count >= 0
        AND failed_count >= 0
        AND audited_count >= 0
        AND invalid_count >= 0
        AND repair_queued_count >= 0
    ),
    CONSTRAINT media_scan_runs_timestamps_check CHECK (
        (status = 'queued' AND started_at IS NULL AND finished_at IS NULL)
        OR (status IN ('running', 'paused') AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('succeeded', 'failed', 'cancelled')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL)
    ),
    CONSTRAINT media_scan_runs_error_check CHECK (
        error_code IS NULL
        OR (
            pg_catalog.char_length(error_code) BETWEEN 1 AND 128
            AND error_code !~ '[[:cntrl:]]'
        )
    )
);

CREATE INDEX media_scan_runs_requested_idx
    ON ops.media_scan_runs (requested_at DESC, id DESC);

CREATE TABLE ops.media_scan_job_links (
    run_id uuid NOT NULL REFERENCES ops.media_scan_runs(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES ops.jobs(id),
    phase text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    PRIMARY KEY (run_id, job_id),
    CONSTRAINT media_scan_job_links_phase_check CHECK (phase IN ('catalog', 'media', 'audit'))
);

CREATE INDEX media_scan_job_links_job_idx
    ON ops.media_scan_job_links (job_id, run_id);

CREATE TABLE ops.media_worker_control (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    state text NOT NULL DEFAULT 'running',
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT media_worker_control_state_check CHECK (state IN ('running', 'paused', 'stopped'))
);

INSERT INTO ops.media_worker_control (singleton, state)
VALUES (true, 'running')
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE ops.media_worker_requests (
    idempotency_key text PRIMARY KEY,
    action text NOT NULL,
    state text NOT NULL,
    request_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT media_worker_requests_key_check CHECK (
        idempotency_key = pg_catalog.btrim(idempotency_key)
        AND pg_catalog.char_length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key !~ '[[:cntrl:]]'
    ),
    CONSTRAINT media_worker_requests_action_check CHECK (
        action IN ('start', 'pause', 'resume', 'cancel')
    ),
    CONSTRAINT media_worker_requests_state_check CHECK (
        state IN ('running', 'paused', 'stopped')
    )
);

ALTER TABLE ops.admin_requests
    DROP CONSTRAINT admin_requests_operation_check,
    ADD CONSTRAINT admin_requests_operation_check CHECK (
        operation IN (
            'admin.scan', 'admin.media_scan', 'admin.media_audit', 'admin.analyze',
            'database.backup_full', 'database.backup_diff', 'job.cancel', 'job.retry'
        )
    );

CREATE FUNCTION ops.submit_media_scan(
    p_id uuid,
    p_payload text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (job_id uuid, run_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_payload jsonb;
    v_existing ops.admin_requests%ROWTYPE;
    v_job_id uuid;
    v_run_id uuid;
    v_job_payload jsonb;
    v_duplicate boolean;
    v_mode text;
    v_repair boolean;
BEGIN
    IF p_id IS NULL
       OR p_payload IS NULL
       OR pg_catalog.octet_length(p_payload) > 8192
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media scan request rejected';
    END IF;

    BEGIN
        v_payload := p_payload::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media scan request rejected';
    END;
    IF pg_catalog.jsonb_typeof(v_payload) <> 'object'
       OR (v_payload ->> 'mode') NOT IN ('full', 'missing', 'audit')
       OR (
           v_payload ? 'repair'
           AND pg_catalog.jsonb_typeof(v_payload -> 'repair') <> 'boolean'
       )
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media scan request rejected';
    END IF;
    v_mode := v_payload ->> 'mode';
    v_repair := COALESCE((v_payload ->> 'repair')::boolean, false);

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('admin.media_scan' || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.*
      INTO v_existing
      FROM ops.admin_requests AS request
     WHERE request.operation = 'admin.media_scan'
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.request_payload IS DISTINCT FROM v_payload THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'admin idempotency conflict';
        END IF;
        SELECT scan.id
          INTO v_run_id
          FROM ops.media_scan_runs AS scan
         WHERE scan.job_id = v_existing.job_id;
        IF v_run_id IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'media scan state is inconsistent';
        END IF;
        RETURN QUERY SELECT v_existing.job_id, v_run_id, true;
        RETURN;
    END IF;

    v_run_id := pg_catalog.gen_random_uuid();
    v_job_payload := v_payload || pg_catalog.jsonb_build_object('runId', v_run_id);
    SELECT submitted.job_id, submitted.was_duplicate
      INTO v_job_id, v_duplicate
      FROM ops.submit_job(
          p_id,
          'admin.media_scan',
          1,
          v_job_payload::text,
          100::smallint,
          3,
          NULL::timestamptz,
          'admin:' || p_idempotency_key
      ) AS submitted;
    INSERT INTO ops.media_scan_runs (id, job_id, mode, repair)
    VALUES (v_run_id, v_job_id, v_mode, v_repair);
    INSERT INTO ops.admin_requests (
        operation, idempotency_key, request_payload, request_id, job_id
    ) VALUES (
        'admin.media_scan', p_idempotency_key, v_payload, p_request_id, v_job_id
    );
    UPDATE ops.job_events AS event
       SET details = event.details || pg_catalog.jsonb_build_object('request_id', p_request_id)
     WHERE event.job_id = v_job_id
       AND event.event_kind = 'submitted';
    RETURN QUERY SELECT v_job_id, v_run_id, v_duplicate;
END
$function$;

CREATE FUNCTION ops.media_worker_claim_enabled()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT COALESCE(
        (SELECT control.state = 'running'
           FROM ops.media_worker_control AS control
          WHERE control.singleton),
        false
    )
$function$;

CREATE FUNCTION ops.set_media_worker_state(
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
    v_existing ops.media_worker_requests%ROWTYPE;
    v_state text;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_action NOT IN ('start', 'pause', 'resume', 'cancel')
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media worker request rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('media.worker' || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.*
      INTO v_existing
      FROM ops.media_worker_requests AS request
     WHERE request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.action <> p_action THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'media worker idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.state, true;
        RETURN;
    END IF;

    v_state := CASE p_action
        WHEN 'pause' THEN 'paused'
        WHEN 'cancel' THEN 'stopped'
        ELSE 'running'
    END;
    UPDATE ops.media_worker_control
       SET state = v_state, updated_at = v_now
     WHERE singleton;

    IF p_action = 'cancel' THEN
        WITH candidates AS MATERIALIZED (
            SELECT job.id, job.status AS from_status
              FROM ops.jobs AS job
             WHERE job.job_type IN ('image.download', 'admin.media_audit')
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
               pg_catalog.jsonb_build_object('reason', 'media_worker_cancelled'), v_now
          FROM candidates
          JOIN cancelled ON cancelled.id = candidates.id;

        WITH requested AS (
            UPDATE ops.jobs AS job
               SET cancellation_requested = true,
                   updated_at = v_now
             WHERE job.job_type IN ('image.download', 'admin.media_audit')
               AND job.status = 'running'
               AND NOT job.cancellation_requested
            RETURNING job.id
        )
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        )
        SELECT pg_catalog.gen_random_uuid(), requested.id, 'cancellation_requested',
               'running', 'running', NULL,
               pg_catalog.jsonb_build_object('reason', 'media_worker_cancelled'), v_now
          FROM requested;
    END IF;

    INSERT INTO ops.media_worker_requests (
        idempotency_key, action, state, request_id
    ) VALUES (p_idempotency_key, p_action, v_state, p_request_id);
    RETURN QUERY SELECT v_state, false;
END
$function$;

ALTER TABLE ops.media_scan_runs OWNER TO migrator;
ALTER TABLE ops.media_scan_job_links OWNER TO migrator;
ALTER TABLE ops.media_worker_control OWNER TO migrator;
ALTER TABLE ops.media_worker_requests OWNER TO migrator;
ALTER FUNCTION ops.submit_media_scan(uuid, text, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.media_worker_claim_enabled() OWNER TO migrator;
ALTER FUNCTION ops.set_media_worker_state(text, text, uuid) OWNER TO migrator;

REVOKE ALL ON TABLE
    ops.media_scan_runs, ops.media_scan_job_links,
    ops.media_worker_control, ops.media_worker_requests
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.media_scan_runs, ops.media_scan_job_links TO monitor;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE ops.media_scan_runs, ops.media_scan_job_links TO ingest_writer;
GRANT SELECT ON TABLE assets.image_assets TO ingest_writer;
GRANT SELECT ON TABLE ops.media_worker_control TO monitor;

-- The public job projection intentionally hides job type and result payloads.
-- The media-scan coordinator needs only these fields to wait for its linked
-- work and aggregate audit counters.
CREATE VIEW ops.media_scan_job_status AS
SELECT job.id, job.job_type, job.status, job.result_summary, job.created_at
FROM ops.jobs AS job;

ALTER VIEW ops.media_scan_job_status OWNER TO migrator;
REVOKE ALL ON TABLE ops.media_scan_job_status
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.media_scan_job_status TO ingest_writer;

REVOKE ALL ON FUNCTION
    ops.submit_media_scan(uuid, text, text, uuid),
    ops.media_worker_claim_enabled(),
    ops.set_media_worker_state(text, text, uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.submit_media_scan(uuid, text, text, uuid),
    ops.set_media_worker_state(text, text, uuid)
    TO api_job_submitter;
GRANT EXECUTE ON FUNCTION ops.media_worker_claim_enabled()
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0029',
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
  AND metadata.value ->> 'revision' = '0029'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 29
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29
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

-- Keep specialized workers at the durable claim boundary while allowing the
-- current media download to finish when the queue is paused.
CREATE OR REPLACE FUNCTION ops.claim_job_for_types(
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
          AND (
              job.job_type NOT IN ('image.download', 'admin.media_audit')
              OR ops.media_worker_claim_enabled()
          )
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
          AND (
              job.job_type NOT IN ('image.download', 'admin.media_audit')
              OR ops.media_worker_claim_enabled()
          )
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
        id, job_id, event_kind, from_status, to_status, worker_id, details
    ) VALUES (
        pg_catalog.gen_random_uuid(), v_job.id,
        CASE WHEN v_from_status = 'running' THEN 'reclaimed' ELSE 'claimed' END,
        v_from_status, 'running', p_worker_id,
        pg_catalog.jsonb_build_object(
            'attempts', v_job.attempts,
            'lease_expires_at', v_job.lease_expires_at,
            'job_type_filter', p_job_types
        )
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
