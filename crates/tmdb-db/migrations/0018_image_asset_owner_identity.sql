-- A TMDB image source path is not a globally unique catalog asset. The same
-- still can legitimately be reused by several episodes, each of which needs
-- its own semantic local file and metadata row. Keep source deduplication at
-- the private media-master layer while making the relational identity owner
-- specific.

ALTER TABLE assets.image_assets
    DROP CONSTRAINT image_assets_source_key_unique;

ALTER TABLE assets.image_assets
    ADD COLUMN owner_type smallint GENERATED ALWAYS AS (
        CASE
            WHEN title_id IS NOT NULL THEN 1
            WHEN person_id IS NOT NULL THEN 2
            WHEN company_id IS NOT NULL THEN 3
            WHEN network_id IS NOT NULL THEN 4
            WHEN collection_id IS NOT NULL THEN 5
            WHEN season_id IS NOT NULL THEN 6
            WHEN episode_id IS NOT NULL THEN 7
            ELSE 0
        END
    ) STORED,
    ADD COLUMN owner_id bigint GENERATED ALWAYS AS (
        coalesce(
            title_id,
            person_id,
            company_id,
            network_id,
            collection_id,
            season_id,
            episode_id,
            0
        )
    ) STORED;

ALTER TABLE assets.image_assets
    ALTER COLUMN owner_type SET NOT NULL,
    ALTER COLUMN owner_id SET NOT NULL,
    ADD CONSTRAINT image_assets_source_owner_unique
        UNIQUE (source, source_key, owner_type, owner_id);

CREATE OR REPLACE VIEW ops.readiness AS
SELECT metadata.value ->> 'revision' AS schema_revision,
       (metadata.value ->> 'migrated_at')::timestamptz AS migrated_at
FROM ops.service_metadata AS metadata
WHERE metadata.key = 'schema'
  AND metadata.value ->> 'revision' = '0015'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 18
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
