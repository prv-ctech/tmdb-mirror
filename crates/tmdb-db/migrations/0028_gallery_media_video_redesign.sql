-- Gallery identity, source-byte metadata, and optimized-only derivatives.
-- Development databases may be recreated; no old media paths are supported.

ALTER TABLE assets.image_assets
    ADD COLUMN gallery_index smallint NOT NULL DEFAULT 1,
    ADD COLUMN source_mime_type text,
    ADD COLUMN source_width integer,
    ADD COLUMN source_height integer,
    ADD COLUMN source_file_size_bytes bigint,
    ADD COLUMN source_sha256 text,
    ADD COLUMN source_storage_path text;

ALTER TABLE assets.image_assets
    DROP CONSTRAINT image_assets_kind_check,
    ADD CONSTRAINT image_assets_kind_check CHECK (
        image_kind IN ('poster', 'backdrop', 'logo', 'profile', 'still', 'avatar', 'other')
    ),
    ADD CONSTRAINT image_assets_gallery_index_check CHECK (gallery_index BETWEEN 1 AND 99),
    ADD CONSTRAINT image_assets_gallery_identity_unique
        UNIQUE (owner_type, owner_id, image_kind, gallery_index),
    ADD CONSTRAINT image_assets_source_metadata_check CHECK (
        (source_mime_type IS NULL OR source_mime_type IN (
            'image/jpeg', 'image/png', 'image/webp', 'image/gif'
        ))
        AND (source_width IS NULL OR source_width > 0)
        AND (source_height IS NULL OR source_height > 0)
        AND (source_file_size_bytes IS NULL OR source_file_size_bytes > 0)
        AND (source_sha256 IS NULL OR source_sha256 ~ '^[0-9a-fA-F]{64}$')
        AND (source_storage_path IS NULL OR (
            pg_catalog.char_length(source_storage_path) BETWEEN 1 AND 512
            AND source_storage_path !~ '[[:cntrl:]]'
            AND source_storage_path !~ '(^|/)\.\.?(/|$)'
            AND source_storage_path !~ '(^|/)\.'
            AND source_storage_path !~ '(^|/)optimized/'
            AND source_storage_path !~ '\\'
        ))
    ),
    ADD CONSTRAINT image_assets_storage_path_check CHECK (
        storage_path IS NULL OR (
            pg_catalog.char_length(storage_path) BETWEEN 1 AND 512
            AND storage_path !~ '[[:cntrl:]]'
            AND storage_path !~ '(^|/)\.\.?(/|$)'
            AND storage_path !~ '(^|/)\.'
            AND storage_path !~ '\\'
        )
    );

ALTER TABLE assets.image_variants
    DROP CONSTRAINT image_variants_mime_check,
    ADD CONSTRAINT image_variants_mime_check CHECK (mime_type IN ('image/jpeg', 'image/png')),
    DROP CONSTRAINT image_variants_path_check,
    ADD CONSTRAINT image_variants_path_check CHECK (
        storage_path ~ '(^|/)optimized/'
        AND pg_catalog.char_length(storage_path) BETWEEN 1 AND 512
        AND storage_path !~ '[[:cntrl:]]'
        AND storage_path !~ '(^|/)\.\.?(/|$)'
        AND storage_path !~ '(^|/)\.'
        AND storage_path !~ '\\'
    ),
    ADD CONSTRAINT image_variants_thumbnail_width_check CHECK (
        storage_path !~ '(^|/)optimized/thumbnails/' OR width <= 640
    );

ALTER TABLE catalog.title_videos
    DROP CONSTRAINT title_videos_pkey,
    ADD PRIMARY KEY (title_id, site, video_key);

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0028',
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
  AND metadata.value ->> 'revision' = '0028'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 28
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28
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
