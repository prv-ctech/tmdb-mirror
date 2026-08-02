-- Durable, authenticated administrative operations.  The HTTP layer only calls
-- these narrow functions; it never accepts raw SQL, shell commands, restore
-- requests, or filesystem paths.

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('admin.scan', 1, true),
       ('admin.media_audit', 1, true),
       ('admin.analyze', 1, true),
       ('database.backup_full', 1, true),
       ('database.backup_diff', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE
SET enabled = EXCLUDED.enabled;

CREATE TABLE ops.admin_requests (
    operation text NOT NULL,
    idempotency_key text NOT NULL,
    request_payload jsonb NOT NULL,
    request_id uuid NOT NULL,
    job_id uuid NOT NULL REFERENCES ops.jobs(id),
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    PRIMARY KEY (operation, idempotency_key),
    CONSTRAINT admin_requests_operation_check CHECK (
        operation IN (
            'admin.scan', 'admin.media_audit', 'admin.analyze',
            'database.backup_full', 'database.backup_diff', 'job.cancel', 'job.retry'
        )
    ),
    CONSTRAINT admin_requests_idempotency_check CHECK (
        idempotency_key = pg_catalog.btrim(idempotency_key)
        AND pg_catalog.char_length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key !~ '[[:cntrl:]]'
    ),
    CONSTRAINT admin_requests_payload_check CHECK (
        pg_catalog.jsonb_typeof(request_payload) = 'object'
        AND pg_catalog.octet_length(request_payload::text) <= 8192
    )
);

CREATE INDEX admin_requests_job_idx ON ops.admin_requests (job_id, created_at DESC);

CREATE TABLE ops.backup_requests (
    id uuid PRIMARY KEY DEFAULT pg_catalog.gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES ops.jobs(id),
    backup_type text NOT NULL,
    request_source text NOT NULL DEFAULT 'manual',
    scheduled_for date,
    status text NOT NULL DEFAULT 'queued',
    requested_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    worker_id text,
    error_code text,
    error_message text,
    result jsonb,
    CONSTRAINT backup_requests_job_id_unique UNIQUE (job_id),
    CONSTRAINT backup_requests_type_check CHECK (backup_type IN ('full', 'diff')),
    CONSTRAINT backup_requests_source_check CHECK (request_source IN ('manual', 'schedule')),
    CONSTRAINT backup_requests_schedule_check CHECK (
        (request_source = 'manual' AND scheduled_for IS NULL)
        OR (request_source = 'schedule' AND scheduled_for IS NOT NULL)
    ),
    CONSTRAINT backup_requests_status_check CHECK (
        status IN ('queued', 'running', 'succeeded', 'failed')
    ),
    CONSTRAINT backup_requests_worker_check CHECK (
        worker_id IS NULL
        OR (
            worker_id = pg_catalog.btrim(worker_id)
            AND pg_catalog.char_length(worker_id) BETWEEN 1 AND 128
            AND worker_id !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT backup_requests_error_code_check CHECK (
        error_code IS NULL
        OR error_code IN ('backup_failed', 'backup_verify_failed', 'archive_check_failed')
    ),
    CONSTRAINT backup_requests_error_message_check CHECK (
        error_message IS NULL
        OR (
            error_message = pg_catalog.btrim(error_message)
            AND pg_catalog.char_length(error_message) BETWEEN 1 AND 512
            AND error_message !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT backup_requests_result_check CHECK (
        result IS NULL
        OR (
            pg_catalog.jsonb_typeof(result) = 'object'
            AND pg_catalog.octet_length(result::text) <= 8192
        )
    ),
    CONSTRAINT backup_requests_timestamps_check CHECK (
        (started_at IS NULL OR started_at >= requested_at)
        AND (finished_at IS NULL OR started_at IS NOT NULL)
        AND (finished_at IS NULL OR finished_at >= started_at)
    )
);

CREATE INDEX backup_requests_status_idx
    ON ops.backup_requests (status, requested_at, id);
CREATE INDEX backup_requests_finished_idx
    ON ops.backup_requests (finished_at DESC NULLS LAST, id DESC);
CREATE UNIQUE INDEX backup_requests_schedule_once_uidx
    ON ops.backup_requests (request_source, scheduled_for)
    WHERE request_source = 'schedule';

ALTER TABLE ops.job_events
    DROP CONSTRAINT job_events_kind_check,
    ADD CONSTRAINT job_events_kind_check CHECK (
        event_kind IN (
            'submitted', 'claimed', 'reclaimed', 'heartbeat', 'retry_scheduled',
            'succeeded', 'dead_lettered', 'cancellation_requested', 'cancelled',
            'claimability_changed', 'retried'
        )
    );

CREATE FUNCTION ops.admin_request_payload(p_job_id uuid)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pg_catalog.jsonb_build_object('job_id', p_job_id)
$function$;

CREATE FUNCTION ops.submit_admin_job(
    p_id uuid,
    p_operation text,
    p_payload text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (job_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_payload jsonb;
    v_existing ops.admin_requests%ROWTYPE;
    v_job_id uuid;
    v_duplicate boolean;
    v_backup_type text;
BEGIN
    IF p_id IS NULL
       OR p_operation NOT IN (
           'admin.scan', 'admin.media_audit', 'admin.analyze',
           'database.backup_full', 'database.backup_diff'
       )
       OR p_payload IS NULL
       OR pg_catalog.octet_length(p_payload) > 8192
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin request rejected';
    END IF;

    BEGIN
        v_payload := p_payload::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin request rejected';
    END;
    IF pg_catalog.jsonb_typeof(v_payload) <> 'object' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin request rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_operation || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.*
      INTO v_existing
      FROM ops.admin_requests AS request
     WHERE request.operation = p_operation
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.request_payload IS DISTINCT FROM v_payload THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'admin idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.job_id, true;
        RETURN;
    END IF;

    SELECT submitted.job_id, submitted.was_duplicate
      INTO v_job_id, v_duplicate
      FROM ops.submit_job(
          p_id,
          p_operation,
          1,
          p_payload,
          100::smallint,
          3,
          NULL::timestamptz,
          'admin:' || p_idempotency_key
      ) AS submitted;

    INSERT INTO ops.admin_requests (
        operation, idempotency_key, request_payload, request_id, job_id
    ) VALUES (
        p_operation, p_idempotency_key, v_payload, p_request_id, v_job_id
    );

    UPDATE ops.job_events AS event
       SET details = event.details || pg_catalog.jsonb_build_object('request_id', p_request_id)
     WHERE event.job_id = v_job_id
       AND event.event_kind = 'submitted';

    v_backup_type := CASE p_operation
        WHEN 'database.backup_full' THEN 'full'
        WHEN 'database.backup_diff' THEN 'diff'
        ELSE NULL
    END;
    IF v_backup_type IS NOT NULL THEN
        INSERT INTO ops.backup_requests (job_id, backup_type, request_source)
        VALUES (v_job_id, v_backup_type, 'manual')
        ON CONFLICT ON CONSTRAINT backup_requests_job_id_unique DO NOTHING;
    END IF;
    RETURN QUERY SELECT v_job_id, v_duplicate;
END
$function$;

CREATE FUNCTION ops.request_admin_job_cancel(
    p_job_id uuid,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (job_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_payload jsonb := ops.admin_request_payload(p_job_id);
    v_existing ops.admin_requests%ROWTYPE;
    v_status text;
BEGIN
    IF p_job_id IS NULL
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin cancellation rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('job.cancel' || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.*
      INTO v_existing
      FROM ops.admin_requests AS request
     WHERE request.operation = 'job.cancel'
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.request_payload IS DISTINCT FROM v_payload THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'admin idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.job_id, true;
        RETURN;
    END IF;

    SELECT job.status
      INTO v_status
      FROM ops.jobs AS job
     WHERE job.id = p_job_id
     FOR UPDATE OF job;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
    END IF;
    IF v_status IN ('succeeded', 'dead_letter', 'cancelled') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'job cancellation rejected';
    END IF;

    PERFORM request_result.job_status
      FROM ops.request_job_cancel(p_job_id, 'admin cancellation request') AS request_result;
    INSERT INTO ops.admin_requests (
        operation, idempotency_key, request_payload, request_id, job_id
    ) VALUES (
        'job.cancel', p_idempotency_key, v_payload, p_request_id, p_job_id
    );
    RETURN QUERY SELECT p_job_id, false;
END
$function$;

CREATE FUNCTION ops.retry_admin_job(
    p_id uuid,
    p_job_id uuid,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (job_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_payload jsonb := ops.admin_request_payload(p_job_id);
    v_existing ops.admin_requests%ROWTYPE;
    v_source ops.jobs%ROWTYPE;
    v_job_id uuid;
    v_duplicate boolean;
BEGIN
    IF p_id IS NULL
       OR p_job_id IS NULL
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin retry rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('job.retry' || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.*
      INTO v_existing
      FROM ops.admin_requests AS request
     WHERE request.operation = 'job.retry'
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.request_payload IS DISTINCT FROM v_payload THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'admin idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.job_id, true;
        RETURN;
    END IF;

    SELECT job.*
      INTO v_source
      FROM ops.jobs AS job
     WHERE job.id = p_job_id
     FOR UPDATE OF job;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'job not found';
    END IF;
    IF v_source.status NOT IN ('dead_letter', 'cancelled') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'admin retry rejected';
    END IF;

    SELECT submitted.job_id, submitted.was_duplicate
      INTO v_job_id, v_duplicate
      FROM ops.submit_job(
          p_id,
          v_source.job_type,
          v_source.payload_version,
          v_source.payload::text,
          v_source.priority,
          v_source.max_attempts,
          NULL::timestamptz,
          'retry:' || p_job_id::text || ':' || p_idempotency_key
      ) AS submitted;
    INSERT INTO ops.admin_requests (
        operation, idempotency_key, request_payload, request_id, job_id
    ) VALUES (
        'job.retry', p_idempotency_key, v_payload, p_request_id, v_job_id
    );
    INSERT INTO ops.job_events (
        id, job_id, event_kind, from_status, to_status, worker_id, details
    ) VALUES (
        pg_catalog.gen_random_uuid(), p_job_id, 'retried', v_source.status, v_source.status,
        NULL, pg_catalog.jsonb_build_object('retry_job_id', v_job_id, 'request_id', p_request_id)
    );
    RETURN QUERY SELECT v_job_id, v_duplicate;
END
$function$;

-- A date-keyed scheduler entry point. The caller supplies the local calendar
-- date; the unique index and advisory lock make the fall DST hour harmless.
CREATE FUNCTION ops.submit_scheduled_backup(p_backup_type text, p_scheduled_for date)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_existing uuid;
    v_job_id uuid;
    v_duplicate boolean;
    v_job_type text;
    v_payload text;
BEGIN
    IF p_backup_type NOT IN ('full', 'diff') OR p_scheduled_for IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'scheduled backup rejected';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('backup.schedule' || E'\\x1f' || p_scheduled_for::text, 0)
    );
    SELECT request.job_id
      INTO v_existing
      FROM ops.backup_requests AS request
     WHERE request.request_source = 'schedule'
       AND request.scheduled_for = p_scheduled_for;
    IF FOUND THEN
        RETURN v_existing;
    END IF;

    v_job_type := CASE p_backup_type
        WHEN 'full' THEN 'database.backup_full'
        ELSE 'database.backup_diff'
    END;
    v_payload := pg_catalog.jsonb_build_object(
        'source', 'schedule',
        'scheduledFor', p_scheduled_for
    )::text;
    SELECT submitted.job_id, submitted.was_duplicate
      INTO v_job_id, v_duplicate
      FROM ops.submit_job(
          pg_catalog.gen_random_uuid(),
          v_job_type,
          1,
          v_payload,
          100::smallint,
          3,
          NULL::timestamptz,
          'backup-schedule:' || p_scheduled_for::text
      ) AS submitted;
    INSERT INTO ops.backup_requests (
        job_id, backup_type, request_source, scheduled_for
    ) VALUES (
        v_job_id, p_backup_type, 'schedule', p_scheduled_for
    );
    RETURN v_job_id;
END
$function$;

ALTER TABLE ops.admin_requests OWNER TO migrator;
ALTER TABLE ops.backup_requests OWNER TO migrator;
ALTER FUNCTION ops.admin_request_payload(uuid) OWNER TO migrator;
ALTER FUNCTION ops.submit_admin_job(uuid, text, text, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.request_admin_job_cancel(uuid, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.retry_admin_job(uuid, uuid, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.submit_scheduled_backup(text, date) OWNER TO migrator;

REVOKE ALL ON TABLE ops.admin_requests, ops.backup_requests
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
REVOKE ALL ON FUNCTION
    ops.admin_request_payload(uuid),
    ops.submit_admin_job(uuid, text, text, text, uuid),
    ops.request_admin_job_cancel(uuid, text, uuid),
    ops.retry_admin_job(uuid, uuid, text, uuid),
    ops.submit_scheduled_backup(text, date)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION
    ops.submit_admin_job(uuid, text, text, text, uuid),
    ops.request_admin_job_cancel(uuid, text, uuid),
    ops.retry_admin_job(uuid, uuid, text, uuid)
    TO api_job_submitter;
GRANT EXECUTE ON FUNCTION ops.submit_scheduled_backup(text, date)
    TO ingest_writer;
GRANT SELECT ON TABLE ops.backup_requests TO monitor;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0022',
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
  AND metadata.value ->> 'revision' = '0022'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 22
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
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
