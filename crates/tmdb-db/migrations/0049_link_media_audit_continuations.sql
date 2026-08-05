CREATE FUNCTION ops.link_media_scan_audit_job(
    p_run_id uuid,
    p_job_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_rows integer;
BEGIN
    IF p_run_id IS NULL OR p_job_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media audit link rejected';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM ops.media_scan_runs AS scan
         WHERE scan.id = p_run_id
           AND scan.mode = 'audit'
           AND scan.status IN ('queued', 'running', 'paused')
    ) OR NOT EXISTS (
        SELECT 1
          FROM ops.jobs AS job
         WHERE job.id = p_job_id
           AND job.job_type = 'admin.media_audit'
           AND job.payload ->> 'runId' = p_run_id::text
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media audit link rejected';
    END IF;

    INSERT INTO ops.media_scan_job_links (run_id, job_id, phase)
    VALUES (p_run_id, p_job_id, 'audit')
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END
$function$;

ALTER FUNCTION ops.link_media_scan_audit_job(uuid, uuid) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.link_media_scan_audit_job(uuid, uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.link_media_scan_audit_job(uuid, uuid)
    TO image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0049',
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
  AND metadata.value ->> 'revision' = '0049'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 49
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49
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
