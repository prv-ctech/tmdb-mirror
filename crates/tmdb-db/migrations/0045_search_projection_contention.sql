-- Avoid rewriting an unchanged search projection during repeated title refreshes.
-- The title row already serializes concurrent writes for the same title, while
-- unrelated titles must not contend on shared search-document index entries.

CREATE OR REPLACE FUNCTION catalog.sync_search_document()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, catalog, search
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM search.search_documents WHERE title_id = OLD.id;
        RETURN OLD;
    END IF;

    UPDATE search.search_documents
       SET title = NEW.display_title,
           original_title = NEW.original_title,
           overview = NEW.overview
     WHERE title_id = NEW.id
       AND locale = ''
       AND (title, original_title, overview) IS DISTINCT FROM
           (NEW.display_title, NEW.original_title, NEW.overview);

    IF NOT FOUND THEN
        INSERT INTO search.search_documents (
            title_id, locale, title, original_title, overview
        )
        SELECT NEW.id, '', NEW.display_title, NEW.original_title, NEW.overview
         WHERE NOT EXISTS (
             SELECT 1
               FROM search.search_documents
              WHERE title_id = NEW.id AND locale = ''
         )
        ON CONFLICT (title_id, locale) DO NOTHING;
    END IF;
    RETURN NEW;
END
$function$;

ALTER FUNCTION catalog.sync_search_document() OWNER TO migrator;
REVOKE ALL ON FUNCTION catalog.sync_search_document() FROM PUBLIC;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0045',
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
  AND metadata.value ->> 'revision' = '0045'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 45
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45
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
