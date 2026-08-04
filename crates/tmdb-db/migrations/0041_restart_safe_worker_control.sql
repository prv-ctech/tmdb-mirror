-- A process restart must not resume work without a new administrator action.

CREATE FUNCTION ops.stop_worker_on_startup(p_worker_kind text)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_state text;
BEGIN
    IF p_worker_kind NOT IN ('ingest', 'media') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'worker kind rejected';
    END IF;

    UPDATE ops.worker_control
       SET state = 'stopped', updated_at = pg_catalog.clock_timestamp()
     WHERE worker_kind = p_worker_kind
       AND state = 'running'
    RETURNING state INTO v_state;

    IF v_state IS NOT NULL THEN
        RETURN v_state;
    END IF;

    SELECT state INTO v_state
      FROM ops.worker_control
     WHERE worker_kind = p_worker_kind;
    RETURN v_state;
END
$function$;

ALTER FUNCTION ops.stop_worker_on_startup(text) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.stop_worker_on_startup(text)
    FROM PUBLIC, api_reader, api_job_submitter, monitor;
GRANT EXECUTE ON FUNCTION ops.stop_worker_on_startup(text)
    TO ingest_writer, image_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0041',
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
  AND metadata.value ->> 'revision' = '0041'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 41
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41
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
