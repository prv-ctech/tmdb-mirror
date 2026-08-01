-- Query-plan and numeric-boundary hardening for the shared catalog.
-- Keep this separate from 0005: SQLx migrations are immutable once published.

ALTER TABLE catalog.titles
    ADD CONSTRAINT titles_popularity_finite_check CHECK (
        popularity IS NULL
        OR (
            popularity > '-Infinity'::double precision
            AND popularity < 'Infinity'::double precision
        )
    ),
    ADD CONSTRAINT titles_vote_average_finite_check CHECK (
        vote_average IS NULL
        OR (
            vote_average > '-Infinity'::double precision
            AND vote_average < 'Infinity'::double precision
        )
    );

ALTER TABLE catalog.movie_details
    ADD CONSTRAINT movie_details_collection_fkey
        FOREIGN KEY (collection_id)
        REFERENCES catalog.collections (id)
        ON DELETE SET NULL;

-- The media-type-leading indexes serve filtered lists. These global partial indexes
-- serve the common all-media routes without forcing a generic plan to scan the table.
CREATE INDEX titles_non_anime_popularity_global_idx
    ON catalog.titles ((coalesce(popularity, 0::double precision)) DESC, id DESC)
    WHERE active AND NOT is_anime;
CREATE INDEX titles_anime_popularity_global_idx
    ON catalog.titles ((coalesce(popularity, 0::double precision)) DESC, id DESC)
    WHERE active AND is_anime;
CREATE INDEX titles_non_anime_first_air_idx
    ON catalog.titles (media_type, first_air_date DESC NULLS LAST, id DESC)
    WHERE active AND NOT is_anime;
CREATE INDEX titles_anime_release_idx
    ON catalog.titles (media_type, release_date DESC NULLS LAST, id DESC)
    WHERE active AND is_anime;
CREATE INDEX titles_anime_first_air_idx
    ON catalog.titles (media_type, first_air_date DESC NULLS LAST, id DESC)
    WHERE active AND is_anime;

-- Each fuzzy candidate column has its own operator-class index so the OR candidate
-- path can use bitmap scans instead of falling back to a sequential scan.
CREATE INDEX search_documents_normalized_original_title_trgm_idx
    ON search.search_documents USING gist (normalized_original_title gist_trgm_ops);
CREATE INDEX search_documents_normalized_aliases_trgm_idx
    ON search.search_documents USING gist (normalized_aliases gist_trgm_ops);

-- The projection is maintained by SECURITY DEFINER triggers. Ingest writers may
-- update catalog rows, but cannot bypass normalization by writing the projection.
REVOKE INSERT, UPDATE, DELETE ON search.search_documents FROM ingest_writer;

-- Future search projections must remain trigger-maintained. Keep the reader
-- grant, but prevent the broad 0001 default from granting ingest DML again.
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA search
    REVOKE INSERT, UPDATE, DELETE ON TABLES FROM ingest_writer;

UPDATE ops.service_metadata
SET value = jsonb_build_object(
        'revision', '0006',
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
  AND metadata.value ->> 'revision' = '0006'
  AND (SELECT count(*) FROM ops._sqlx_migrations) = 6
  AND (
      SELECT array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
