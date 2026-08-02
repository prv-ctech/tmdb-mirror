-- Repair the administrative-operation migration without rewriting its
-- immutable history. The registry trigger has emitted claimability_changed
-- events since migration 0004, so that durable audit kind must remain valid
-- alongside the later retried event. The helper below is only invoked by
-- SECURITY DEFINER administrative functions and must not be public.

ALTER TABLE ops.job_events
    DROP CONSTRAINT job_events_kind_check,
    ADD CONSTRAINT job_events_kind_check CHECK (
        event_kind IN (
            'submitted', 'claimed', 'reclaimed', 'heartbeat', 'retry_scheduled',
            'succeeded', 'dead_lettered', 'cancellation_requested', 'cancelled',
            'claimability_changed', 'retried'
        )
    );

REVOKE ALL ON FUNCTION ops.admin_request_payload(uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0025',
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
  AND metadata.value ->> 'revision' = '0025'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 25
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25
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
