-- Catalog detail payloads contain unordered shared entities (genres, people,
-- companies, networks, seasons, and episodes). The ingest worker collects its
-- complete mutation set before writing, then calls this function as the first
-- statement of the catalog transaction. Acquiring every transaction-scoped
-- advisory lock in lexical resource order removes cross-path lock cycles while
-- preserving the upstream write and durable-job submission order.

CREATE OR REPLACE FUNCTION ops.lock_catalog_write_resources(resource_keys text[])
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    resource_key text;
BEGIN
    FOR resource_key IN
        SELECT DISTINCT requested.resource_key
          FROM pg_catalog.unnest(resource_keys) AS requested(resource_key)
         WHERE requested.resource_key IS NOT NULL
           AND requested.resource_key <> ''
         ORDER BY requested.resource_key
    LOOP
        -- A hash collision only serializes unrelated writes; it cannot weaken
        -- the ordering guarantee because every logical key is still visited.
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(resource_key, 0)
        );
    END LOOP;
END;
$function$;

ALTER FUNCTION ops.lock_catalog_write_resources(text[]) OWNER TO migrator;
REVOKE ALL ON FUNCTION ops.lock_catalog_write_resources(text[])
    FROM PUBLIC, api_reader, api_job_submitter, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.lock_catalog_write_resources(text[]) TO ingest_writer;

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 20
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
