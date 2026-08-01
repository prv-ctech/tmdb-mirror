-- Top-rated catalog ordering and readiness contract.
-- Keep this migration immutable once published; the nullable rating columns are excluded so
-- every returned row has a stable (vote_average, vote_count, id) keyset cursor.

CREATE INDEX titles_non_anime_top_rating_global_idx
    ON catalog.titles (
        vote_average DESC NULLS LAST,
        vote_count DESC NULLS LAST,
        id DESC
    )
    WHERE active
      AND NOT is_anime
      AND vote_average IS NOT NULL
      AND vote_count IS NOT NULL;

CREATE INDEX titles_anime_top_rating_global_idx
    ON catalog.titles (
        vote_average DESC NULLS LAST,
        vote_count DESC NULLS LAST,
        id DESC
    )
    WHERE active
      AND is_anime
      AND vote_average IS NOT NULL
      AND vote_count IS NOT NULL;

UPDATE ops.service_metadata
SET value = jsonb_build_object(
        'revision', '0007',
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
  AND metadata.value ->> 'revision' = '0007'
  AND (SELECT count(*) FROM ops._sqlx_migrations) = 7
  AND (
      SELECT array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
