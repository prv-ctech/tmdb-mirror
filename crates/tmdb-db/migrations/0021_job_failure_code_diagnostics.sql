-- Preserve the bounded failure codes emitted by the consolidated workers.
-- Previously these safe, actionable causes were accepted by Rust as legacy
-- text but collapsed to execution_failed by PostgreSQL, which hid the cause
-- from terminal logs, job history, and retry diagnosis.

ALTER TABLE ops.jobs
    DROP CONSTRAINT jobs_error_code_check,
    ADD CONSTRAINT jobs_error_code_check CHECK (
        error_code IS NULL
        OR error_code IN (
            'execution_failed', 'upstream_unavailable', 'upstream_unauthorized',
            'rate_limited', 'invalid_payload', 'lease_expired', 'attempts_exhausted',
            'entity_not_ready', 'export_storage', 'database_unavailable',
            'export_queue_incomplete'
        )
    );

CREATE OR REPLACE FUNCTION ops.job_failure_code(p_message text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT CASE
        WHEN p_message IN (
            'execution_failed', 'upstream_unavailable', 'upstream_unauthorized',
            'rate_limited', 'invalid_payload', 'lease_expired', 'attempts_exhausted',
            'entity_not_ready', 'export_storage', 'database_unavailable',
            'export_queue_incomplete'
        ) THEN p_message
        ELSE 'execution_failed'
    END
$function$;

ALTER FUNCTION ops.job_failure_code(text) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.job_failure_code(text) FROM PUBLIC;

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 21
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
