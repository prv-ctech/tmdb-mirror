-- Add bounded TV-season refresh jobs so episode metadata and stills are
-- ingested separately from the potentially large TV detail response.

INSERT INTO ops.job_type_registry (job_type, payload_version, enabled)
VALUES ('ingest.refresh_season', 1, true)
ON CONFLICT (job_type, payload_version) DO UPDATE SET enabled = true;

GRANT EXECUTE ON FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text)
    TO ingest_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0015',
        'migrated_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    ),
    updated_at = clock_timestamp()
WHERE key = 'schema';

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015';

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
