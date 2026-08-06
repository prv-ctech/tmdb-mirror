-- Replace whole-catalog media scans with bounded, local-catalog media requests.

-- Legacy media work is no longer valid under the new path and rendition policy.
WITH legacy AS MATERIALIZED (
    SELECT job.id, job.status AS from_status
      FROM ops.jobs AS job
     WHERE job.job_type IN ('image.download', 'admin.media_scan', 'admin.media_audit')
       AND job.status IN ('queued', 'retry_wait', 'running')
     FOR UPDATE
), cancelled AS (
    UPDATE ops.jobs AS job
       SET status = 'cancelled',
           lease_owner = NULL,
           lease_expires_at = NULL,
           cancellation_requested = false,
           updated_at = pg_catalog.clock_timestamp(),
           finished_at = pg_catalog.clock_timestamp()
      FROM legacy
     WHERE job.id = legacy.id
    RETURNING job.id
)
INSERT INTO ops.job_events (
    id, job_id, event_kind, from_status, to_status, details, created_at
)
SELECT pg_catalog.gen_random_uuid(), legacy.id, 'cancelled', legacy.from_status,
       'cancelled', pg_catalog.jsonb_build_object('reason', 'media_policy_replaced'),
       pg_catalog.clock_timestamp()
  FROM legacy
  JOIN cancelled ON cancelled.id = legacy.id;

UPDATE ops.job_type_registry
   SET enabled = false
 WHERE job_type IN ('admin.media_scan', 'admin.media_audit', 'ingest.refresh_reusable_gallery');

-- Local media is explicitly disposable during this forward migration.  Use a
-- metadata-only truncate instead of a row-by-row delete so a populated
-- production table does not generate avoidable WAL and bloat.
TRUNCATE TABLE assets.image_variants, assets.image_assets;
DROP TABLE assets.image_variants;

ALTER TABLE assets.image_assets
    DROP CONSTRAINT image_assets_source_metadata_check,
    DROP CONSTRAINT image_assets_storage_path_check,
    DROP CONSTRAINT image_assets_gallery_index_check;

ALTER TABLE assets.image_assets
    DROP COLUMN source_mime_type,
    DROP COLUMN source_width,
    DROP COLUMN source_height,
    DROP COLUMN source_file_size_bytes,
    DROP COLUMN source_sha256,
    DROP COLUMN source_storage_path,
    ALTER COLUMN gallery_index TYPE integer,
    ADD COLUMN verified_at timestamptz,
    ADD CONSTRAINT image_assets_gallery_index_check CHECK (gallery_index > 0),
    ADD CONSTRAINT image_assets_storage_path_check CHECK (
        storage_path IS NULL OR (
            pg_catalog.char_length(storage_path) BETWEEN 1 AND 512
            AND storage_path !~ '[[:cntrl:]]'
            AND storage_path !~ '(^|/)\.\.?(/|$)'
            AND storage_path !~ '(^|/)\.'
            AND storage_path !~ '(^|/)(optimized|\.masters)(/|$)'
            AND storage_path !~ '\\'
        )
    );

DROP VIEW IF EXISTS ops.media_scan_job_status;
DROP FUNCTION IF EXISTS ops.link_media_scan_audit_job(uuid, uuid);
DROP FUNCTION IF EXISTS ops.submit_media_scan(uuid, text, text, uuid);
DROP TABLE IF EXISTS ops.media_scan_job_links;
DROP TABLE IF EXISTS ops.media_scan_runs;
DROP TABLE IF EXISTS ops.media_worker_requests;
DROP TABLE IF EXISTS ops.media_worker_control;
DROP FUNCTION IF EXISTS ops.set_media_worker_state(text, text, uuid);

-- The typed claim boundary from the legacy media controller depended on its
-- compatibility helper. Rebind that boundary to the generic worker control
-- before removing the obsolete helper itself.
DO $block$
DECLARE
    v_definition text;
BEGIN
    SELECT pg_catalog.pg_get_functiondef(
               'ops.claim_job_for_types_uncontrolled(text,bigint,text[])'::pg_catalog.regprocedure
           )
      INTO v_definition;
    IF pg_catalog.strpos(v_definition, 'ops.media_worker_claim_enabled()') = 0 THEN
        RAISE EXCEPTION 'typed job claim definition is not the expected revision';
    END IF;
    v_definition := pg_catalog.replace(
        v_definition,
        'ops.media_worker_claim_enabled()',
        'ops.worker_claim_enabled(''media'')'
    );
    v_definition := pg_catalog.replace(
        v_definition,
        'job.job_type NOT IN (''image.download'', ''admin.media_audit'')',
        'job.job_type <> ''image.download'''
    );
    EXECUTE v_definition;
END
$block$;
DROP FUNCTION ops.media_worker_claim_enabled();

DELETE FROM ops.admin_requests
 WHERE operation IN ('admin.media_scan', 'admin.media_audit');

ALTER TABLE ops.admin_requests
    DROP CONSTRAINT admin_requests_operation_check,
    ADD CONSTRAINT admin_requests_operation_check CHECK (
        operation IN (
            'admin.scan', 'admin.analyze', 'database.backup_full',
            'database.backup_diff', 'job.cancel', 'job.retry'
        )
    );

CREATE TABLE ops.media_requests (
    id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    source_cursor bigint NOT NULL DEFAULT 0,
    expansion_complete boolean NOT NULL DEFAULT false,
    lease_owner text,
    lease_expires_at timestamptz,
    available_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    requested_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT media_requests_key_check CHECK (
        idempotency_key = pg_catalog.btrim(idempotency_key)
        AND pg_catalog.char_length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key !~ '[[:cntrl:]]'
    ),
    CONSTRAINT media_requests_payload_check CHECK (
        pg_catalog.jsonb_typeof(request_payload) = 'object'
        AND pg_catalog.jsonb_typeof(request_payload -> 'items') = 'array'
        AND pg_catalog.jsonb_array_length(request_payload -> 'items') BETWEEN 1 AND 100
    ),
    CONSTRAINT media_requests_status_check CHECK (
        status IN ('queued', 'running', 'succeeded', 'partial', 'failed', 'cancelled')
    ),
    CONSTRAINT media_requests_cursor_check CHECK (source_cursor >= 0),
    CONSTRAINT media_requests_lease_check CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT media_requests_time_check CHECK (
        (status = 'queued' AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('succeeded', 'partial', 'failed', 'cancelled')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL)
    )
);

CREATE TABLE ops.media_request_items (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES ops.media_requests(id) ON DELETE CASCADE,
    title_id bigint NOT NULL REFERENCES catalog.titles(id) ON DELETE RESTRICT,
    media_type text NOT NULL,
    tmdb_id bigint NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    catalog_incomplete boolean NOT NULL DEFAULT false,
    source_assets_found bigint NOT NULL DEFAULT 0,
    queued_count bigint NOT NULL DEFAULT 0,
    ready_count bigint NOT NULL DEFAULT 0,
    reused_count bigint NOT NULL DEFAULT 0,
    deleted_count bigint NOT NULL DEFAULT 0,
    failed_count bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT media_request_items_media_type_check CHECK (media_type IN ('movie', 'tv')),
    CONSTRAINT media_request_items_tmdb_id_check CHECK (tmdb_id > 0),
    CONSTRAINT media_request_items_status_check CHECK (
        status IN ('queued', 'running', 'succeeded', 'partial', 'failed', 'cancelled')
    ),
    CONSTRAINT media_request_items_counts_check CHECK (
        source_assets_found >= 0 AND queued_count >= 0 AND ready_count >= 0
        AND reused_count >= 0 AND deleted_count >= 0 AND failed_count >= 0
    ),
    UNIQUE (request_id, media_type, tmdb_id)
);

CREATE TABLE ops.media_request_assets (
    request_item_id bigint NOT NULL REFERENCES ops.media_request_items(id) ON DELETE CASCADE,
    source_cursor bigint NOT NULL,
    owner_type smallint NOT NULL,
    owner_id bigint NOT NULL,
    image_kind text NOT NULL,
    gallery_index integer NOT NULL,
    source_key text NOT NULL,
    job_id uuid REFERENCES ops.jobs(id),
    reused boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    PRIMARY KEY (request_item_id, source_cursor),
    CONSTRAINT media_request_assets_cursor_check CHECK (source_cursor > 0),
    CONSTRAINT media_request_assets_owner_check CHECK (owner_type BETWEEN 1 AND 7 AND owner_id > 0),
    CONSTRAINT media_request_assets_kind_check CHECK (
        image_kind IN ('poster', 'backdrop', 'logo', 'profile', 'still', 'other')
    ),
    CONSTRAINT media_request_assets_index_check CHECK (gallery_index > 0),
    CONSTRAINT media_request_assets_source_check CHECK (
        source_key ~ '^/[A-Za-z0-9._/-]+$'
        AND pg_catalog.char_length(source_key) <= 500
        AND source_key !~ '(^|/)\.\.?(/|$)'
    ),
    UNIQUE (request_item_id, owner_type, owner_id, image_kind, gallery_index),
    UNIQUE (request_item_id, owner_type, owner_id, image_kind, source_key)
);

CREATE INDEX media_requests_claim_idx
    ON ops.media_requests (available_at, requested_at, id)
    WHERE status IN ('queued', 'running');
CREATE INDEX media_request_items_active_title_idx
    ON ops.media_request_items (media_type, tmdb_id, request_id)
    WHERE status IN ('queued', 'running');
CREATE INDEX media_request_assets_job_idx
    ON ops.media_request_assets (job_id) WHERE job_id IS NOT NULL;

CREATE TABLE assets.pending_file_deletions (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    request_item_id bigint REFERENCES ops.media_request_items(id) ON DELETE SET NULL,
    owner_type smallint NOT NULL,
    owner_id bigint NOT NULL,
    storage_path text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT pending_file_deletions_owner_check CHECK (
        owner_type BETWEEN 1 AND 7 AND owner_id > 0
    ),
    CONSTRAINT pending_file_deletions_path_check CHECK (
        storage_path ~ '^[A-Za-z0-9._/-]+$'
        AND pg_catalog.char_length(storage_path) <= 512
        AND storage_path !~ '(^|/)\.\.?(/|$)'
    )
);

ALTER TABLE ops.media_requests OWNER TO migrator;
ALTER TABLE ops.media_request_items OWNER TO migrator;
ALTER TABLE ops.media_request_assets OWNER TO migrator;
ALTER TABLE assets.pending_file_deletions OWNER TO migrator;

CREATE FUNCTION ops.submit_media_request(
    p_id uuid,
    p_payload text,
    p_idempotency_key text
)
RETURNS TABLE (
    request_id uuid,
    was_duplicate boolean,
    outcome text,
    invalid_items jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, catalog, pg_temp
AS $function$
DECLARE
    v_payload jsonb;
    v_existing ops.media_requests%ROWTYPE;
    v_invalid jsonb;
    v_active bigint;
    v_new bigint;
    v_inserted bigint;
BEGIN
    IF p_id IS NULL OR p_payload IS NULL OR pg_catalog.octet_length(p_payload) > 32768
       OR p_idempotency_key IS NULL OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]'
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media request rejected';
    END IF;
    BEGIN
        v_payload := p_payload::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media request rejected';
    END;
    IF pg_catalog.jsonb_typeof(v_payload) <> 'object'
       OR (SELECT pg_catalog.array_agg(key ORDER BY key)
             FROM pg_catalog.jsonb_object_keys(v_payload) AS key) <> ARRAY['items']::text[]
       OR pg_catalog.jsonb_typeof(v_payload -> 'items') <> 'array'
       OR pg_catalog.jsonb_array_length(v_payload -> 'items') NOT BETWEEN 1 AND 100
       OR EXISTS (
           SELECT 1
             FROM pg_catalog.jsonb_array_elements(v_payload -> 'items') AS item(value)
            WHERE pg_catalog.jsonb_typeof(item.value) <> 'object'
               OR (SELECT pg_catalog.array_agg(key ORDER BY key)
                     FROM pg_catalog.jsonb_object_keys(item.value) AS key)
                    <> ARRAY['mediaType', 'tmdbId']::text[]
               OR item.value ->> 'mediaType' NOT IN ('movie', 'tv')
               OR pg_catalog.jsonb_typeof(item.value -> 'tmdbId') <> 'number'
               OR item.value ->> 'tmdbId' !~ '^[1-9][0-9]*$'
       )
       OR (SELECT pg_catalog.count(*)
             FROM pg_catalog.jsonb_array_elements(v_payload -> 'items'))
          <> (SELECT pg_catalog.count(DISTINCT (item.value ->> 'mediaType', item.value ->> 'tmdbId'))
                FROM pg_catalog.jsonb_array_elements(v_payload -> 'items') AS item(value))
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media request rejected';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('media.request' || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.* INTO v_existing
      FROM ops.media_requests AS request
     WHERE request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.request_payload IS DISTINCT FROM v_payload THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'media request idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.id, true, 'accepted'::text, NULL::jsonb;
        RETURN;
    END IF;

    SELECT pg_catalog.jsonb_agg(item.value ORDER BY item.ordinality)
      INTO v_invalid
      FROM pg_catalog.jsonb_array_elements(v_payload -> 'items') WITH ORDINALITY AS item(value, ordinality)
      LEFT JOIN catalog.titles AS title
        ON title.media_type = item.value ->> 'mediaType'
       AND title.tmdb_id = (item.value ->> 'tmdbId')::bigint
       AND title.active
     WHERE title.id IS NULL;
    IF v_invalid IS NOT NULL THEN
        RETURN QUERY SELECT NULL::uuid, false, 'invalid'::text, v_invalid;
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('media.request.capacity', 0)
    );
    SELECT pg_catalog.count(DISTINCT (item.media_type, item.tmdb_id))
      INTO v_active
      FROM ops.media_request_items AS item
     WHERE item.status IN ('queued', 'running');
    SELECT pg_catalog.count(*) INTO v_new
      FROM pg_catalog.jsonb_array_elements(v_payload -> 'items') AS input(value)
     WHERE NOT EXISTS (
         SELECT 1 FROM ops.media_request_items AS active
          WHERE active.media_type = input.value ->> 'mediaType'
            AND active.tmdb_id = (input.value ->> 'tmdbId')::bigint
            AND active.status IN ('queued', 'running')
     );
    IF v_active + v_new > 1000 THEN
        RETURN QUERY SELECT NULL::uuid, false, 'capacity'::text, NULL::jsonb;
        RETURN;
    END IF;

    INSERT INTO ops.media_requests (id, idempotency_key, request_payload)
    VALUES (p_id, p_idempotency_key, v_payload);
    INSERT INTO ops.media_request_items (request_id, title_id, media_type, tmdb_id)
    SELECT p_id, title.id, title.media_type, title.tmdb_id
      FROM pg_catalog.jsonb_array_elements(v_payload -> 'items') WITH ORDINALITY AS item(value, ordinality)
      JOIN catalog.titles AS title
        ON title.media_type = item.value ->> 'mediaType'
       AND title.tmdb_id = (item.value ->> 'tmdbId')::bigint
       AND title.active
     ORDER BY item.ordinality;
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> pg_catalog.jsonb_array_length(v_payload -> 'items') THEN
        -- A concurrent deactivation between validation and insertion must
        -- reject the whole request, never silently accept a subset.
        DELETE FROM ops.media_requests WHERE id = p_id;
        SELECT pg_catalog.jsonb_agg(item.value ORDER BY item.ordinality)
          INTO v_invalid
          FROM pg_catalog.jsonb_array_elements(v_payload -> 'items')
               WITH ORDINALITY AS item(value, ordinality)
          LEFT JOIN catalog.titles AS title
            ON title.media_type = item.value ->> 'mediaType'
           AND title.tmdb_id = (item.value ->> 'tmdbId')::bigint
           AND title.active
         WHERE title.id IS NULL;
        RETURN QUERY SELECT NULL::uuid, false, 'invalid'::text,
            COALESCE(v_invalid, v_payload -> 'items');
        RETURN;
    END IF;
    RETURN QUERY SELECT p_id, false, 'accepted'::text, NULL::jsonb;
END
$function$;

-- Locks a claimed request for the caller's transaction.  Coordinators use
-- this immediately before submitting/linking one image job so worker
-- cancellation cannot race a new unlinked job into the queue.
CREATE FUNCTION ops.lock_media_request_claim(p_request_id uuid, p_worker_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_valid boolean;
BEGIN
    SELECT request.status = 'running'
           AND request.lease_owner = p_worker_id
           AND request.lease_expires_at > pg_catalog.clock_timestamp()
           AND ops.worker_claim_enabled('media')
      INTO v_valid
      FROM ops.media_requests AS request
     WHERE request.id = p_request_id
     FOR UPDATE;
    RETURN COALESCE(v_valid, false);
END
$function$;

CREATE FUNCTION ops.media_image_job_source(p_job_id uuid)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT job.payload ->> 'tmdbPath'
      FROM ops.jobs AS job
     WHERE job.id = p_job_id AND job.job_type = 'image.download'
$function$;

CREATE FUNCTION ops.claim_media_request(p_worker_id text, p_lease_microseconds bigint)
RETURNS TABLE (request_id uuid, source_cursor bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_id uuid;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_worker_id IS NULL OR pg_catalog.char_length(p_worker_id) NOT BETWEEN 1 AND 128
       OR p_worker_id ~ '[[:cntrl:]]'
       OR p_lease_microseconds NOT BETWEEN 1000000 AND 3600000000
       OR NOT ops.worker_claim_enabled('media')
    THEN
        RETURN;
    END IF;
    SELECT request.id INTO v_id
      FROM ops.media_requests AS request
     WHERE request.status IN ('queued', 'running')
       AND request.available_at <= v_now
       AND (request.lease_expires_at IS NULL OR request.lease_expires_at <= v_now)
     ORDER BY request.requested_at, request.id
     FOR UPDATE SKIP LOCKED
     LIMIT 1;
    IF NOT FOUND THEN RETURN; END IF;
    UPDATE ops.media_requests AS request
       SET status = 'running',
           started_at = COALESCE(request.started_at, v_now),
           lease_owner = p_worker_id,
           lease_expires_at = v_now + pg_catalog.make_interval(
               secs => p_lease_microseconds::double precision / 1000000.0
           ),
           updated_at = v_now
     WHERE request.id = v_id;
    UPDATE ops.media_request_items AS item
       SET status = 'running',
           catalog_incomplete = item.catalog_incomplete
               OR title.enriched_at IS NULL
               OR (item.media_type = 'tv' AND EXISTS (
                   SELECT 1 FROM catalog.seasons AS season
                    WHERE season.title_id = title.id AND season.enriched_at IS NULL
               ))
               OR title.updated_at > request.requested_at,
           updated_at = v_now
      FROM catalog.titles AS title, ops.media_requests AS request
     WHERE item.request_id = v_id
       AND item.status = 'queued'
       AND title.id = item.title_id
       AND request.id = item.request_id;
    RETURN QUERY SELECT request.id, request.source_cursor
      FROM ops.media_requests AS request WHERE request.id = v_id;
END
$function$;

-- Select no more than 250 sources from existing relational catalog rows and
-- exact locally stored TMDB documents. No upstream metadata call is possible.
CREATE FUNCTION assets.select_media_request_sources(
    p_request_id uuid,
    p_after_cursor bigint,
    p_limit integer
)
RETURNS TABLE (
    source_cursor bigint,
    request_item_id bigint,
    entity_type text,
    entity_id bigint,
    owner_id bigint,
    title_tmdb_id bigint,
    season_number integer,
    episode_number integer,
    image_kind text,
    source_path text,
    language_code text,
    gallery_index integer,
    catalog_incomplete boolean
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, catalog, source, assets, pg_temp
AS $function$
WITH requested AS (
    SELECT item.id AS request_item_id, item.title_id, item.media_type, item.tmdb_id,
           title.poster_path, title.backdrop_path,
           (item.catalog_incomplete OR title.enriched_at IS NULL OR (
               item.media_type = 'tv' AND EXISTS (
                   SELECT 1 FROM catalog.seasons AS season
                    WHERE season.title_id = title.id AND season.enriched_at IS NULL
               )
           ) OR title.updated_at > request.requested_at) AS catalog_incomplete
      FROM ops.media_request_items AS item
      JOIN ops.media_requests AS request ON request.id = item.request_id
      JOIN catalog.titles AS title ON title.id = item.title_id AND title.active
     WHERE item.request_id = p_request_id
), document_images AS (
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete,
           document.endpoint_path, image_kind.kind AS image_kind,
           image.value ->> 'file_path' AS source_path,
           image.value ->> 'iso_639_1' AS language_code
      FROM requested
      JOIN source.tmdb_documents AS document
        ON document.endpoint_path IN (
            requested.media_type || '/' || requested.tmdb_id::text,
            requested.media_type || '/' || requested.tmdb_id::text || '/images'
        )
      CROSS JOIN LATERAL (VALUES ('poster'), ('backdrop'), ('logo')) AS image_kind(kind)
      CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
          COALESCE(
              document.response #> ARRAY['images', image_kind.kind || 's'],
              document.response -> (image_kind.kind || 's'),
              '[]'::jsonb
          )
      ) AS image(value)
     WHERE image.value ->> 'file_path' ~ '^/[A-Za-z0-9._/-]+$'
       AND pg_catalog.char_length(image.value ->> 'file_path') <= 500
       AND (image.value ->> 'iso_639_1' IS NULL OR image.value ->> 'iso_639_1' = 'en')
), season_document_images AS (
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, season.id AS season_id,
           season.season_number, image.value ->> 'file_path' AS source_path,
           image.value ->> 'iso_639_1' AS language_code
      FROM requested
      JOIN catalog.seasons AS season ON season.title_id = requested.title_id
      JOIN source.tmdb_documents AS document
        ON document.endpoint_path IN (
            'tv/' || requested.tmdb_id::text || '/season/' || season.season_number::text,
            'tv/' || requested.tmdb_id::text || '/season/' || season.season_number::text || '/images'
        )
      CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
          COALESCE(document.response #> ARRAY['images', 'posters'], document.response -> 'posters', '[]'::jsonb)
      ) AS image(value)
     WHERE requested.media_type = 'tv'
       AND image.value ->> 'file_path' ~ '^/[A-Za-z0-9._/-]+$'
       AND pg_catalog.char_length(image.value ->> 'file_path') <= 500
       AND (image.value ->> 'iso_639_1' IS NULL OR image.value ->> 'iso_639_1' = 'en')
), episode_document_images AS (
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, episode.id AS episode_id,
           season.season_number, episode.episode_number,
           image.value ->> 'file_path' AS source_path,
           image.value ->> 'iso_639_1' AS language_code
      FROM requested
      JOIN catalog.seasons AS season ON season.title_id = requested.title_id
      JOIN catalog.episodes AS episode ON episode.season_id = season.id
      JOIN source.tmdb_documents AS document
        ON document.endpoint_path IN (
            'tv/' || requested.tmdb_id::text || '/season/' || season.season_number::text
                || '/episode/' || episode.episode_number::text,
            'tv/' || requested.tmdb_id::text || '/season/' || season.season_number::text
                || '/episode/' || episode.episode_number::text || '/images'
        )
      CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
          COALESCE(document.response #> ARRAY['images', 'stills'], document.response -> 'stills', '[]'::jsonb)
      ) AS image(value)
     WHERE requested.media_type = 'tv'
       AND image.value ->> 'file_path' ~ '^/[A-Za-z0-9._/-]+$'
       AND pg_catalog.char_length(image.value ->> 'file_path') <= 500
       AND (image.value ->> 'iso_639_1' IS NULL OR image.value ->> 'iso_639_1' = 'en')
), sources AS (
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete,
           requested.media_type AS entity_type, requested.tmdb_id AS entity_id,
           NULL::integer AS season_number, NULL::integer AS episode_number,
           primary_image.image_kind, primary_image.source_path, NULL::text AS language_code,
           0 AS primary_order
      FROM requested
      CROSS JOIN LATERAL (VALUES
          ('poster', requested.poster_path), ('backdrop', requested.backdrop_path)
      ) AS primary_image(image_kind, source_path)
     WHERE primary_image.source_path IS NOT NULL
    UNION ALL
    SELECT image.request_item_id, image.title_id, image.media_type, image.tmdb_id,
           image.catalog_incomplete, image.media_type, image.tmdb_id,
           NULL, NULL, image.image_kind, image.source_path, image.language_code, 1
      FROM document_images AS image
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'season', season.id,
           season.season_number, NULL, 'poster', season.poster_path, NULL, 0
      FROM requested JOIN catalog.seasons AS season ON season.title_id = requested.title_id
     WHERE season.poster_path IS NOT NULL
    UNION ALL
    SELECT image.request_item_id, image.title_id, image.media_type, image.tmdb_id,
           image.catalog_incomplete, 'season', image.season_id,
           image.season_number, NULL, 'poster', image.source_path, image.language_code, 1
      FROM season_document_images AS image
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'episode', episode.id,
           season.season_number, episode.episode_number, 'still', episode.still_path, NULL, 0
      FROM requested
      JOIN catalog.seasons AS season ON season.title_id = requested.title_id
     JOIN catalog.episodes AS episode ON episode.season_id = season.id
     WHERE episode.still_path IS NOT NULL
    UNION ALL
    SELECT image.request_item_id, image.title_id, image.media_type, image.tmdb_id,
           image.catalog_incomplete, 'episode', image.episode_id,
           image.season_number, image.episode_number, 'still', image.source_path,
           image.language_code, 1
      FROM episode_document_images AS image
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'person', person.id,
           NULL, NULL, 'profile', person.profile_path, NULL, 0
      FROM requested
      JOIN catalog.title_credits AS credit ON credit.title_id = requested.title_id
      JOIN catalog.people AS person ON person.id = credit.person_id
     WHERE person.profile_path IS NOT NULL
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'company', company.id,
           NULL, NULL, 'logo', company.logo_path, NULL, 0
      FROM requested
      JOIN catalog.title_companies AS relation ON relation.title_id = requested.title_id
      JOIN catalog.companies AS company ON company.id = relation.company_id
     WHERE company.logo_path IS NOT NULL
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'network', network.id,
           NULL, NULL, 'logo', network.logo_path, NULL, 0
      FROM requested
      JOIN catalog.title_networks AS relation ON relation.title_id = requested.title_id
      JOIN catalog.networks AS network ON network.id = relation.network_id
     WHERE network.logo_path IS NOT NULL
    UNION ALL
    SELECT requested.request_item_id, requested.title_id, requested.media_type,
           requested.tmdb_id, requested.catalog_incomplete, 'collection', collection.id,
           NULL, NULL, image.image_kind, image.source_path, NULL, 0
      FROM requested
      JOIN catalog.title_collections AS relation ON relation.title_id = requested.title_id
      JOIN catalog.collections AS collection ON collection.id = relation.collection_id
      CROSS JOIN LATERAL (VALUES
          ('poster', collection.poster_path), ('backdrop', collection.backdrop_path)
      ) AS image(image_kind, source_path)
     WHERE image.source_path IS NOT NULL
), unique_sources AS (
    SELECT DISTINCT ON (
        request_item_id, entity_type, entity_id, image_kind, source_path
    ) *
      FROM sources
     WHERE source_path ~ '^/[A-Za-z0-9._/-]+$'
       AND pg_catalog.char_length(source_path) <= 500
     ORDER BY request_item_id, entity_type, entity_id, image_kind, source_path, primary_order
), owned AS (
    SELECT source.*,
           CASE source.entity_type
               WHEN 'movie' THEN source.title_id
               WHEN 'tv' THEN source.title_id
               ELSE source.entity_id
           END AS owner_id,
           CASE source.entity_type
               WHEN 'movie' THEN 1 WHEN 'tv' THEN 1 WHEN 'person' THEN 2
               WHEN 'company' THEN 3 WHEN 'network' THEN 4 WHEN 'collection' THEN 5
               WHEN 'season' THEN 6 WHEN 'episode' THEN 7
           END::smallint AS owner_type
      FROM unique_sources AS source
), remaining AS (
    SELECT owned.*
      FROM owned
     WHERE NOT EXISTS (
         SELECT 1
           FROM ops.media_request_assets AS linked
          WHERE linked.request_item_id = owned.request_item_id
            AND linked.owner_type = owned.owner_type
            AND linked.owner_id = owned.owner_id
            AND linked.image_kind = owned.image_kind
            AND linked.source_key = owned.source_path
     )
), indexed AS (
    SELECT remaining.*,
           COALESCE((
               SELECT pg_catalog.max(linked.gallery_index)
                 FROM ops.media_request_assets AS linked
                WHERE linked.request_item_id = remaining.request_item_id
                  AND linked.owner_type = remaining.owner_type
                  AND linked.owner_id = remaining.owner_id
                  AND linked.image_kind = remaining.image_kind
           ), 0) + pg_catalog.row_number() OVER (
               PARTITION BY request_item_id, owner_type, owner_id, image_kind
               ORDER BY primary_order, source_path
           )::integer AS gallery_index
      FROM remaining
), numbered AS (
    SELECT p_after_cursor + pg_catalog.row_number() OVER (
               ORDER BY request_item_id, owner_type, owner_id, image_kind,
                        gallery_index, source_path
           )::bigint AS source_cursor,
           indexed.*
      FROM indexed
)
SELECT numbered.source_cursor, numbered.request_item_id, numbered.entity_type,
       numbered.entity_id, numbered.owner_id,
       numbered.tmdb_id, numbered.season_number,
       numbered.episode_number, numbered.image_kind, numbered.source_path,
       numbered.language_code, numbered.gallery_index, numbered.catalog_incomplete
  FROM numbered
 WHERE p_after_cursor >= 0 AND p_limit BETWEEN 1 AND 250
 ORDER BY numbered.source_cursor
 LIMIT p_limit
$function$;

CREATE FUNCTION assets.media_image_configuration()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, source, pg_temp
AS $function$
    SELECT document.response -> 'images'
      FROM source.tmdb_documents AS document
     WHERE document.endpoint_path = 'configuration'
     ORDER BY document.updated_at DESC
     LIMIT 1
$function$;

CREATE FUNCTION ops.advance_media_request(
    p_request_id uuid,
    p_worker_id text,
    p_source_cursor bigint,
    p_expansion_complete boolean,
    p_delay_seconds integer
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    IF p_source_cursor < 0 OR p_delay_seconds NOT BETWEEN 0 AND 300 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'media request advance rejected';
    END IF;
    UPDATE ops.media_requests
       SET source_cursor = p_source_cursor,
           expansion_complete = p_expansion_complete,
           lease_owner = NULL,
           lease_expires_at = NULL,
           available_at = pg_catalog.clock_timestamp()
               + pg_catalog.make_interval(secs => p_delay_seconds),
           updated_at = pg_catalog.clock_timestamp()
     WHERE id = p_request_id AND lease_owner = p_worker_id AND status = 'running';
    RETURN FOUND;
END
$function$;

CREATE FUNCTION ops.link_media_request_asset(
    p_request_id uuid,
    p_worker_id text,
    p_request_item_id bigint,
    p_source_cursor bigint,
    p_owner_type smallint,
    p_owner_id bigint,
    p_image_kind text,
    p_gallery_index integer,
    p_source_key text,
    p_job_id uuid,
    p_reused boolean,
    p_catalog_incomplete boolean
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM ops.media_request_items AS item
          JOIN ops.media_requests AS request ON request.id = item.request_id
         WHERE item.id = p_request_item_id
           AND request.id = p_request_id
           AND request.status = 'running'
           AND request.lease_owner = p_worker_id
           AND request.lease_expires_at > pg_catalog.clock_timestamp()
           AND ops.worker_claim_enabled('media')
    ) THEN
        RETURN false;
    END IF;
    INSERT INTO ops.media_request_assets (
        request_item_id, source_cursor, owner_type, owner_id, image_kind,
        gallery_index, source_key, job_id, reused
    ) VALUES (
        p_request_item_id, p_source_cursor, p_owner_type, p_owner_id, p_image_kind,
        p_gallery_index, p_source_key, p_job_id, p_reused
    ) ON CONFLICT (request_item_id, source_cursor) DO NOTHING;
    UPDATE ops.media_request_items AS item
       SET catalog_incomplete = item.catalog_incomplete OR p_catalog_incomplete,
           source_assets_found = (
               SELECT pg_catalog.count(*) FROM ops.media_request_assets AS asset
                WHERE asset.request_item_id = item.id
           ),
           queued_count = (
               SELECT pg_catalog.count(*) FROM ops.media_request_assets AS asset
                WHERE asset.request_item_id = item.id AND asset.job_id IS NOT NULL
           ),
           reused_count = (
               SELECT pg_catalog.count(*) FROM ops.media_request_assets AS asset
                WHERE asset.request_item_id = item.id AND asset.reused
           ),
           updated_at = pg_catalog.clock_timestamp()
     WHERE item.id = p_request_item_id;
    RETURN true;
END
$function$;

CREATE FUNCTION ops.refresh_media_request(p_request_id uuid)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_status text;
    v_expanded boolean;
    v_active bigint;
    v_ready bigint;
    v_failed bigint;
    v_incomplete boolean;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    SELECT request.expansion_complete INTO v_expanded
      FROM ops.media_requests AS request
     WHERE request.id = p_request_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'media request not found';
    END IF;

    UPDATE ops.media_request_items AS item
       SET ready_count = summary.ready_count,
           failed_count = summary.failed_count,
           status = CASE
               WHEN summary.active_count > 0 THEN 'running'
               WHEN summary.failed_count > 0 AND summary.ready_count = 0 AND summary.reused_count = 0
                   THEN 'failed'
               WHEN item.catalog_incomplete OR summary.failed_count > 0 THEN 'partial'
               ELSE 'succeeded'
           END,
           updated_at = v_now
      FROM (
          SELECT asset.request_item_id,
                 pg_catalog.count(*) FILTER (WHERE job.status IN ('queued', 'running', 'retry_wait')) AS active_count,
                 pg_catalog.count(*) FILTER (
                     WHERE ready.id IS NOT NULL AND NOT asset.reused
                 ) AS ready_count,
                 pg_catalog.count(*) FILTER (
                     WHERE ready.id IS NULL
                       AND job.status IN ('succeeded', 'dead_letter', 'cancelled')
                 ) AS failed_count,
                 pg_catalog.count(*) FILTER (
                     WHERE ready.id IS NOT NULL AND asset.reused
                 ) AS reused_count
            FROM ops.media_request_assets AS asset
            LEFT JOIN ops.jobs AS job ON job.id = asset.job_id
            LEFT JOIN assets.image_assets AS ready
              ON ready.owner_type = asset.owner_type
             AND ready.owner_id = asset.owner_id
             AND ready.source = 'tmdb'
             AND ready.source_key = asset.source_key
             AND ready.status = 'ready'
           GROUP BY asset.request_item_id
     ) AS summary
     WHERE item.id = summary.request_item_id AND item.request_id = p_request_id;

    IF v_expanded THEN
        UPDATE ops.media_request_items AS item
           SET status = CASE WHEN item.catalog_incomplete THEN 'partial' ELSE 'succeeded' END,
               updated_at = v_now
         WHERE item.request_id = p_request_id
           AND NOT EXISTS (
               SELECT 1 FROM ops.media_request_assets AS asset
                WHERE asset.request_item_id = item.id
           );
    END IF;

    SELECT pg_catalog.count(*) FILTER (WHERE item.status IN ('queued', 'running')),
           pg_catalog.sum(item.ready_count + item.reused_count),
           pg_catalog.sum(item.failed_count),
           pg_catalog.bool_or(item.catalog_incomplete)
      INTO v_active, v_ready, v_failed, v_incomplete
      FROM ops.media_requests AS request
      JOIN ops.media_request_items AS item ON item.request_id = request.id
     WHERE request.id = p_request_id
     GROUP BY request.id;
    IF NOT v_expanded OR v_active > 0 THEN
        v_status := 'running';
    ELSIF v_failed > 0 AND v_ready = 0 THEN
        v_status := 'failed';
    ELSIF v_incomplete OR v_failed > 0 THEN
        v_status := 'partial';
    ELSE
        v_status := 'succeeded';
    END IF;
    UPDATE ops.media_requests
       SET status = v_status,
           finished_at = CASE WHEN v_status IN ('succeeded', 'partial', 'failed') THEN v_now ELSE NULL END,
           lease_owner = NULL,
           lease_expires_at = NULL,
           available_at = CASE WHEN v_status = 'running' THEN v_now + interval '2 seconds' ELSE available_at END,
           updated_at = v_now
     WHERE id = p_request_id AND status <> 'cancelled';
    RETURN v_status;
END
$function$;

CREATE FUNCTION assets.queue_obsolete_media_request_files(p_request_id uuid)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, assets, pg_temp
AS $function$
DECLARE
    v_count bigint;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM ops.media_requests AS request
         WHERE request.id = p_request_id
           AND request.expansion_complete
           AND request.status = 'running'
    ) OR EXISTS (
        SELECT 1
          FROM ops.media_request_assets AS requested
          JOIN ops.media_request_items AS item ON item.id = requested.request_item_id
          JOIN ops.jobs AS job ON job.id = requested.job_id
         WHERE item.request_id = p_request_id
           AND job.status IN ('queued', 'running', 'retry_wait')
    ) OR EXISTS (
        SELECT 1
          FROM ops.media_request_items AS item
          JOIN ops.media_requests AS request ON request.id = item.request_id
          JOIN catalog.titles AS title ON title.id = item.title_id
         WHERE item.request_id = p_request_id
           AND (
               item.catalog_incomplete
               OR title.enriched_at IS NULL
               OR title.updated_at > request.requested_at
               OR (title.media_type = 'tv' AND EXISTS (
                   SELECT 1 FROM catalog.seasons AS season
                    WHERE season.title_id = title.id AND season.enriched_at IS NULL
               ))
           )
    ) THEN
        RETURN 0;
    END IF;
    WITH current_owners AS (
        SELECT item.id AS request_item_id, 1::smallint AS owner_type, item.title_id AS owner_id
          FROM ops.media_request_items AS item WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 2::smallint, credit.person_id
          FROM ops.media_request_items AS item
          JOIN catalog.title_credits AS credit ON credit.title_id = item.title_id
         WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 3::smallint, relation.company_id
          FROM ops.media_request_items AS item
          JOIN catalog.title_companies AS relation ON relation.title_id = item.title_id
         WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 4::smallint, relation.network_id
          FROM ops.media_request_items AS item
          JOIN catalog.title_networks AS relation ON relation.title_id = item.title_id
         WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 5::smallint, relation.collection_id
          FROM ops.media_request_items AS item
          JOIN catalog.title_collections AS relation ON relation.title_id = item.title_id
         WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 6::smallint, season.id
          FROM ops.media_request_items AS item
          JOIN catalog.seasons AS season ON season.title_id = item.title_id
         WHERE item.request_id = p_request_id
        UNION
        SELECT item.id, 7::smallint, episode.id
          FROM ops.media_request_items AS item
          JOIN catalog.seasons AS season ON season.title_id = item.title_id
          JOIN catalog.episodes AS episode ON episode.season_id = season.id
         WHERE item.request_id = p_request_id
    ), all_desired_owners AS (
        SELECT current.owner_type, current.owner_id, current.request_item_id
          FROM current_owners AS current
        UNION ALL
        SELECT requested.owner_type, requested.owner_id, requested.request_item_id
          FROM ops.media_request_assets AS requested
          JOIN ops.media_request_items AS item ON item.id = requested.request_item_id
         WHERE item.request_id = p_request_id
    ), desired_owners AS (
        SELECT owner.owner_type, owner.owner_id,
               pg_catalog.min(owner.request_item_id) AS request_item_id
          FROM all_desired_owners AS owner
         GROUP BY owner.owner_type, owner.owner_id
    ), obsolete AS MATERIALIZED (
        SELECT asset.id, owner.request_item_id, asset.owner_type, asset.owner_id,
               asset.storage_path
          FROM assets.image_assets AS asset
          JOIN desired_owners AS owner
            ON owner.owner_type = asset.owner_type AND owner.owner_id = asset.owner_id
         WHERE asset.storage_path IS NOT NULL
           AND NOT EXISTS (
               SELECT 1
                 FROM ops.media_request_assets AS requested
                 JOIN ops.media_request_items AS item ON item.id = requested.request_item_id
                WHERE item.request_id = p_request_id
                  AND requested.owner_type = asset.owner_type
                  AND requested.owner_id = asset.owner_id
                  AND requested.source_key = asset.source_key
           )
    ), queued AS (
        INSERT INTO assets.pending_file_deletions (
            request_item_id, owner_type, owner_id, storage_path
        )
        SELECT obsolete.request_item_id, obsolete.owner_type, obsolete.owner_id,
               obsolete.storage_path
          FROM obsolete
        ON CONFLICT (storage_path) DO NOTHING
        RETURNING storage_path
    ), removed AS (
        DELETE FROM assets.image_assets AS asset
         USING obsolete
         WHERE asset.id = obsolete.id
        RETURNING asset.id
    )
    SELECT pg_catalog.count(*) INTO v_count FROM queued;
    RETURN v_count;
END
$function$;

CREATE FUNCTION assets.queue_image_asset_replacements(
    p_owner_type smallint,
    p_owner_id bigint,
    p_image_kind text,
    p_gallery_index integer,
    p_source_key text,
    p_new_storage_path text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, assets, pg_temp
AS $function$
BEGIN
    IF p_owner_type NOT BETWEEN 1 AND 7 OR p_owner_id <= 0
       OR p_image_kind NOT IN ('poster', 'backdrop', 'logo', 'profile', 'still', 'other')
       OR p_gallery_index <= 0
       OR p_source_key !~ '^/[A-Za-z0-9._/-]+$'
       OR pg_catalog.char_length(p_source_key) > 500
       OR p_new_storage_path !~ '^[A-Za-z0-9._/-]+$'
       OR pg_catalog.char_length(p_new_storage_path) > 512
       OR p_new_storage_path ~ '(^|/)\.\.?(/|$)'
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'image replacement rejected';
    END IF;
    INSERT INTO assets.pending_file_deletions (owner_type, owner_id, storage_path)
    SELECT asset.owner_type, asset.owner_id, asset.storage_path
      FROM assets.image_assets AS asset
     WHERE asset.owner_type = p_owner_type AND asset.owner_id = p_owner_id
       AND asset.storage_path IS NOT NULL
       AND asset.storage_path <> p_new_storage_path
       AND (
           (asset.source = 'tmdb' AND asset.source_key = p_source_key)
           OR (asset.image_kind = p_image_kind AND asset.gallery_index = p_gallery_index)
       )
    ON CONFLICT (storage_path) DO NOTHING;
    DELETE FROM assets.image_assets AS asset
     WHERE asset.owner_type = p_owner_type AND asset.owner_id = p_owner_id
       AND asset.image_kind = p_image_kind AND asset.gallery_index = p_gallery_index
       AND NOT (asset.source = 'tmdb' AND asset.source_key = p_source_key);
    RETURN true;
END
$function$;

CREATE FUNCTION assets.pending_media_file_deletions(p_limit integer)
RETURNS TABLE (
    deletion_id bigint,
    request_item_id bigint,
    storage_path text,
    expected_directory text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, assets, catalog, pg_temp
AS $function$
    SELECT deletion.id, deletion.request_item_id, deletion.storage_path,
           CASE deletion.owner_type
               WHEN 1 THEN (
                   SELECT CASE title.media_type
                       WHEN 'movie' THEN 'movies/' || title.tmdb_id::text || '/'
                       ELSE 'tv/' || title.tmdb_id::text || '/'
                   END
                     FROM catalog.titles AS title WHERE title.id = deletion.owner_id
               )
               WHEN 2 THEN 'people/' || deletion.owner_id::text || '/'
               WHEN 3 THEN 'companies/' || deletion.owner_id::text || '/'
               WHEN 4 THEN 'networks/' || deletion.owner_id::text || '/'
               WHEN 5 THEN 'collections/' || deletion.owner_id::text || '/'
               WHEN 6 THEN (
                   SELECT 'tv/' || title.tmdb_id::text || '/'
                     FROM catalog.seasons AS season
                     JOIN catalog.titles AS title ON title.id = season.title_id
                    WHERE season.id = deletion.owner_id
               )
               WHEN 7 THEN (
                   SELECT 'tv/' || title.tmdb_id::text || '/'
                     FROM catalog.episodes AS episode
                     JOIN catalog.titles AS title ON title.id = episode.title_id
                    WHERE episode.id = deletion.owner_id
               )
           END AS expected_directory
      FROM assets.pending_file_deletions AS deletion
     WHERE p_limit BETWEEN 1 AND 250
     ORDER BY deletion.id
     LIMIT p_limit
$function$;

CREATE FUNCTION assets.complete_media_file_deletion(p_deletion_id bigint)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, assets, pg_temp
AS $function$
DECLARE
    v_request_item_id bigint;
BEGIN
    DELETE FROM assets.pending_file_deletions
     WHERE id = p_deletion_id
    RETURNING request_item_id INTO v_request_item_id;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    IF v_request_item_id IS NOT NULL THEN
        UPDATE ops.media_request_items
           SET deleted_count = deleted_count + 1,
               updated_at = pg_catalog.clock_timestamp()
         WHERE id = v_request_item_id;
    END IF;
    RETURN true;
END
$function$;

-- Durable catalog schedule slots and change-list watermarks. The worker owns
-- cron evaluation; PostgreSQL owns duplicate prevention and synchronization state.
CREATE TABLE ops.catalog_schedule_slots (
    mode text NOT NULL,
    scheduled_for timestamptz NOT NULL,
    job_id uuid REFERENCES ops.jobs(id),
    outcome text NOT NULL,
    window_start date,
    window_end date,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    PRIMARY KEY (mode, scheduled_for),
    CONSTRAINT catalog_schedule_slots_mode_check CHECK (
        mode IN ('daily_sync', 'missing_only', 'reconcile')
    ),
    CONSTRAINT catalog_schedule_slots_outcome_check CHECK (
        outcome IN ('submitted', 'pending', 'full_sweep_required')
    ),
    CONSTRAINT catalog_schedule_slots_window_check CHECK (
        (mode = 'daily_sync' AND window_start IS NOT NULL AND window_end IS NOT NULL
            AND window_start <= window_end AND window_end - window_start <= 13)
        OR (mode <> 'daily_sync' AND window_start IS NULL AND window_end IS NULL)
    )
);

CREATE TABLE ops.catalog_sync_state (
    mode text PRIMARY KEY,
    last_successful_window_end date,
    full_sweep_required boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT catalog_sync_state_mode_check CHECK (
        mode IN ('daily_sync', 'missing_only', 'reconcile', 'full_sweep')
    )
);

INSERT INTO ops.catalog_sync_state (mode, last_successful_window_end, full_sweep_required)
VALUES (
    'daily_sync',
    (
        SELECT pg_catalog.min(document.updated_at::date)
          FROM source.tmdb_documents AS document
         WHERE document.endpoint_path IN ('movie/changes', 'tv/changes')
    ),
    (SELECT pg_catalog.count(DISTINCT document.endpoint_path) < 2
       FROM source.tmdb_documents AS document
      WHERE document.endpoint_path IN ('movie/changes', 'tv/changes'))
), ('missing_only', NULL, false), ('reconcile', NULL, false), ('full_sweep', NULL, false);

CREATE FUNCTION ops.submit_scheduled_catalog_scan(
    p_mode text,
    p_scheduled_for timestamptz,
    p_window_start date,
    p_window_end date
)
RETURNS TABLE (job_id uuid, outcome text, full_sweep_required boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_existing ops.catalog_schedule_slots%ROWTYPE;
    v_job_id uuid;
    v_duplicate boolean;
    v_payload jsonb;
    v_full_sweep_required boolean := false;
BEGIN
    IF p_mode NOT IN ('daily_sync', 'missing_only', 'reconcile')
       OR p_scheduled_for IS NULL
       OR p_scheduled_for <> pg_catalog.date_trunc('minute', p_scheduled_for)
       OR (p_mode = 'daily_sync' AND (
            p_window_start IS NULL OR p_window_end IS NULL
            OR p_window_start > p_window_end OR p_window_end - p_window_start > 13
       ))
       OR (p_mode <> 'daily_sync' AND (p_window_start IS NOT NULL OR p_window_end IS NOT NULL))
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'catalog schedule rejected';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('catalog.maintenance', 0)
    );
    SELECT slot.* INTO v_existing
      FROM ops.catalog_schedule_slots AS slot
     WHERE slot.mode = p_mode AND slot.scheduled_for = p_scheduled_for;
    IF FOUND AND (
        v_existing.window_start IS DISTINCT FROM p_window_start
        OR v_existing.window_end IS DISTINCT FROM p_window_end
    ) THEN
        RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'catalog schedule conflict';
    END IF;
    IF FOUND AND v_existing.outcome <> 'pending' THEN
        RETURN QUERY SELECT v_existing.job_id, v_existing.outcome,
            v_existing.outcome = 'full_sweep_required';
        RETURN;
    END IF;
    IF p_mode = 'daily_sync' THEN
        SELECT state.full_sweep_required
          INTO v_full_sweep_required
          FROM ops.catalog_sync_state AS state
         WHERE state.mode = 'daily_sync'
         FOR UPDATE;
        IF v_full_sweep_required THEN
            INSERT INTO ops.catalog_schedule_slots (
                mode, scheduled_for, outcome, window_start, window_end
            ) VALUES (
                p_mode, p_scheduled_for, 'full_sweep_required', p_window_start, p_window_end
            ) ON CONFLICT (mode, scheduled_for) DO UPDATE
                  SET outcome = EXCLUDED.outcome,
                      job_id = NULL;
            RETURN QUERY SELECT NULL::uuid, 'full_sweep_required'::text, true;
            RETURN;
        END IF;
    END IF;
    IF EXISTS (
        SELECT 1 FROM ops.jobs AS job
         WHERE job.status IN ('queued', 'running', 'retry_wait')
           AND (job.job_type = 'admin.scan' OR job.job_type LIKE 'ingest.%')
    ) THEN
        INSERT INTO ops.catalog_schedule_slots (
            mode, scheduled_for, outcome, window_start, window_end
        ) VALUES (p_mode, p_scheduled_for, 'pending', p_window_start, p_window_end)
        ON CONFLICT (mode, scheduled_for) DO UPDATE
              SET outcome = 'pending', job_id = NULL;
        RETURN QUERY SELECT NULL::uuid, 'pending'::text, false;
        RETURN;
    END IF;
    v_payload := pg_catalog.jsonb_build_object(
        'mode', p_mode,
        'mediaTypes', pg_catalog.jsonb_build_array('movie', 'tv')
    );
    IF p_mode = 'daily_sync' THEN
        v_payload := v_payload || pg_catalog.jsonb_build_object(
            'windowStart', p_window_start,
            'windowEnd', p_window_end
        );
    END IF;
    SELECT submitted.job_id, submitted.was_duplicate
      INTO v_job_id, v_duplicate
      FROM ops.submit_job(
          pg_catalog.gen_random_uuid(), 'admin.scan', 1, v_payload::text,
          100::smallint, 100, NULL::timestamptz,
          'catalog-schedule:' || p_mode || ':' || p_scheduled_for::text
      ) AS submitted;
    INSERT INTO ops.catalog_schedule_slots (
        mode, scheduled_for, job_id, outcome, window_start, window_end
    ) VALUES (p_mode, p_scheduled_for, v_job_id, 'submitted', p_window_start, p_window_end)
    ON CONFLICT (mode, scheduled_for) DO UPDATE
          SET job_id = EXCLUDED.job_id,
              outcome = EXCLUDED.outcome;
    RETURN QUERY SELECT v_job_id, 'submitted'::text, false;
END
$function$;

CREATE FUNCTION ops.complete_catalog_sync(p_mode text, p_window_end date)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    IF p_mode NOT IN ('daily_sync', 'missing_only', 'reconcile', 'full_sweep')
       OR p_window_end IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'catalog synchronization rejected';
    END IF;
    UPDATE ops.catalog_sync_state
       SET last_successful_window_end = GREATEST(last_successful_window_end, p_window_end),
           full_sweep_required = CASE
               WHEN p_mode = 'daily_sync' THEN false
               ELSE full_sweep_required
           END,
           updated_at = pg_catalog.clock_timestamp()
     WHERE mode = p_mode;
    IF p_mode = 'full_sweep' THEN
        UPDATE ops.catalog_sync_state
           SET last_successful_window_end = p_window_end,
               full_sweep_required = false,
               updated_at = pg_catalog.clock_timestamp()
         WHERE mode = 'daily_sync';
    END IF;
    RETURN FOUND;
END
$function$;

CREATE FUNCTION ops.mark_catalog_full_sweep_required()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    UPDATE ops.catalog_sync_state
       SET full_sweep_required = true,
           updated_at = pg_catalog.clock_timestamp()
     WHERE mode = 'daily_sync';
    RETURN FOUND;
END
$function$;

-- Admin request status is exposed through a narrow execute-only interface so
-- the public API role never receives direct access to operational tables.
CREATE FUNCTION ops.media_request_status(p_request_id uuid)
RETURNS TABLE (
    request_id uuid,
    status text,
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    title_count bigint,
    source_assets_found bigint,
    queued_count bigint,
    downloading_count bigint,
    ready_count bigint,
    reused_count bigint,
    deleted_count bigint,
    failed_count bigint,
    catalog_incomplete_count bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
    SELECT request.id, request.status, request.requested_at, request.started_at,
           request.finished_at, item_summary.title_count,
           item_summary.source_assets_found, job_summary.queued_count,
           job_summary.downloading_count, item_summary.ready_count,
           item_summary.reused_count, item_summary.deleted_count,
           item_summary.failed_count, item_summary.catalog_incomplete_count
      FROM ops.media_requests AS request
      CROSS JOIN LATERAL (
          SELECT pg_catalog.count(*)::bigint AS title_count,
                 COALESCE(pg_catalog.sum(item.source_assets_found), 0)::bigint AS source_assets_found,
                 COALESCE(pg_catalog.sum(item.ready_count), 0)::bigint AS ready_count,
                 COALESCE(pg_catalog.sum(item.reused_count), 0)::bigint AS reused_count,
                 COALESCE(pg_catalog.sum(item.deleted_count), 0)::bigint AS deleted_count,
                 COALESCE(pg_catalog.sum(item.failed_count), 0)::bigint AS failed_count,
                 pg_catalog.count(*) FILTER (WHERE item.catalog_incomplete)::bigint
                     AS catalog_incomplete_count
            FROM ops.media_request_items AS item
           WHERE item.request_id = request.id
      ) AS item_summary
      CROSS JOIN LATERAL (
          SELECT pg_catalog.count(*) FILTER (
                     WHERE job.status IN ('queued', 'retry_wait')
                 )::bigint AS queued_count,
                 pg_catalog.count(*) FILTER (WHERE job.status = 'running')::bigint
                     AS downloading_count
            FROM ops.media_request_assets AS asset
            JOIN ops.media_request_items AS item ON item.id = asset.request_item_id
            JOIN ops.jobs AS job ON job.id = asset.job_id
           WHERE item.request_id = request.id
      ) AS job_summary
     WHERE request.id = p_request_id
$function$;

CREATE FUNCTION ops.submit_manual_catalog_scan(
    p_id uuid,
    p_payload text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (job_id uuid, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('catalog.maintenance', 0)
    );
    IF EXISTS (
        SELECT 1 FROM ops.admin_requests AS request
         WHERE request.operation = 'admin.scan'
           AND request.idempotency_key = p_idempotency_key
    ) THEN
        RETURN QUERY SELECT * FROM ops.submit_admin_job(
            p_id, 'admin.scan', p_payload, p_idempotency_key, p_request_id
        );
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM ops.jobs AS job
         WHERE job.status IN ('queued', 'running', 'retry_wait')
           AND (job.job_type = 'admin.scan' OR job.job_type LIKE 'ingest.%')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'catalog maintenance already active';
    END IF;
    RETURN QUERY SELECT * FROM ops.submit_admin_job(
        p_id, 'admin.scan', p_payload, p_idempotency_key, p_request_id
    );
END
$function$;

CREATE OR REPLACE FUNCTION ops.set_worker_state(
    p_worker_kind text,
    p_action text,
    p_idempotency_key text,
    p_request_id uuid
)
RETURNS TABLE (state text, was_duplicate boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    v_existing ops.worker_requests%ROWTYPE;
    v_state text;
    v_now timestamptz;
BEGIN
    IF p_worker_kind NOT IN ('ingest', 'media')
       OR p_action NOT IN ('start', 'pause', 'resume', 'cancel')
       OR p_idempotency_key IS NULL
       OR p_idempotency_key <> pg_catalog.btrim(p_idempotency_key)
       OR pg_catalog.char_length(p_idempotency_key) NOT BETWEEN 1 AND 128
       OR p_idempotency_key ~ '[[:cntrl:]]' OR p_request_id IS NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'worker request rejected';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('worker.' || p_worker_kind || E'\\x1f' || p_idempotency_key, 0)
    );
    SELECT request.* INTO v_existing
      FROM ops.worker_requests AS request
     WHERE request.worker_kind = p_worker_kind
       AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.action <> p_action THEN
            RAISE EXCEPTION USING ERRCODE = 'P0003', MESSAGE = 'worker idempotency conflict';
        END IF;
        RETURN QUERY SELECT v_existing.state, true;
        RETURN;
    END IF;
    PERFORM 1 FROM ops.worker_control AS control
     WHERE control.worker_kind = p_worker_kind FOR UPDATE;
    v_now := pg_catalog.clock_timestamp();
    v_state := CASE p_action WHEN 'pause' THEN 'paused' WHEN 'cancel' THEN 'stopped' ELSE 'running' END;
    UPDATE ops.worker_control SET state = v_state, updated_at = v_now
     WHERE worker_kind = p_worker_kind;
    IF p_action = 'cancel' THEN
        IF p_worker_kind = 'media' THEN
            -- Serialize cancellation with the coordinator's per-source claim
            -- lock. After these rows are locked, every linked job is visible
            -- to the cancellation statements below.
            PERFORM 1
              FROM ops.media_requests AS request
             WHERE request.status IN ('queued', 'running')
             ORDER BY request.id
             FOR UPDATE;
        END IF;
        -- The request lock may wait for an in-flight admission transaction.
        -- Refresh the terminal timestamp after that wait so a just-committed
        -- job can never be finished before its own creation time.
        v_now := pg_catalog.clock_timestamp();
        WITH candidates AS MATERIALIZED (
            SELECT job.id, job.status AS from_status
              FROM ops.jobs AS job
             WHERE ((p_worker_kind = 'media' AND job.job_type IN ('image.download', 'system.noop'))
                    OR (p_worker_kind = 'ingest' AND (
                        job.job_type LIKE 'ingest.%' OR job.job_type IN ('admin.scan', 'admin.analyze')
                    )))
               AND job.status IN ('queued', 'retry_wait')
             FOR UPDATE
        ), cancelled AS (
            UPDATE ops.jobs AS job SET status = 'cancelled', updated_at = v_now, finished_at = v_now
              FROM candidates WHERE job.id = candidates.id RETURNING job.id
        )
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        )
        SELECT pg_catalog.gen_random_uuid(), candidates.id, 'cancelled', candidates.from_status,
               'cancelled', NULL,
               pg_catalog.jsonb_build_object('reason', p_worker_kind || '_worker_cancelled'), v_now
          FROM candidates JOIN cancelled ON cancelled.id = candidates.id;

        WITH requested AS (
            UPDATE ops.jobs AS job SET cancellation_requested = true, updated_at = v_now
             WHERE ((p_worker_kind = 'media' AND job.job_type IN ('image.download', 'system.noop'))
                    OR (p_worker_kind = 'ingest' AND (
                        job.job_type LIKE 'ingest.%' OR job.job_type IN ('admin.scan', 'admin.analyze')
                    )))
               AND job.status = 'running' AND NOT job.cancellation_requested
            RETURNING job.id
        )
        INSERT INTO ops.job_events (
            id, job_id, event_kind, from_status, to_status, worker_id, details, created_at
        )
        SELECT pg_catalog.gen_random_uuid(), requested.id, 'cancellation_requested',
               'running', 'running', NULL,
               pg_catalog.jsonb_build_object('reason', p_worker_kind || '_worker_cancelled'), v_now
          FROM requested;

        IF p_worker_kind = 'media' THEN
            UPDATE ops.media_requests
               SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL,
                   started_at = COALESCE(started_at, v_now),
                   finished_at = v_now, updated_at = v_now
             WHERE status IN ('queued', 'running');
            UPDATE ops.media_request_items AS item
               SET status = 'cancelled', updated_at = v_now
              FROM ops.media_requests AS request
             WHERE request.id = item.request_id AND request.status = 'cancelled'
               AND item.status IN ('queued', 'running');
        END IF;
    END IF;
    INSERT INTO ops.worker_requests (worker_kind, idempotency_key, action, state, request_id)
    VALUES (p_worker_kind, p_idempotency_key, p_action, v_state, p_request_id);
    RETURN QUERY SELECT v_state, false;
END
$function$;

DROP FUNCTION IF EXISTS ops.stop_worker_on_startup(text);
CREATE FUNCTION ops.start_worker_on_startup(p_worker_kind text)
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
       SET state = 'running', updated_at = pg_catalog.clock_timestamp()
     WHERE worker_kind = p_worker_kind
    RETURNING state INTO v_state;
    RETURN v_state;
END
$function$;

-- Both workers drain eligible durable work when their containers start.
UPDATE ops.worker_control SET state = 'running', updated_at = pg_catalog.clock_timestamp()
 WHERE worker_kind IN ('ingest', 'media');

-- Terminal requests retain their aggregate counters. Once their retention
-- window expires, release terminal job links so normal job-history pruning
-- does not depend on removed legacy media-scan tables.
CREATE OR REPLACE FUNCTION ops.prune_finished_jobs(p_before timestamptz, p_limit integer)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, pg_temp
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    IF p_before IS NULL
       OR p_limit NOT BETWEEN 1 AND 10000
       OR p_before >= pg_catalog.clock_timestamp()
    THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid job prune arguments';
    END IF;

    UPDATE ops.media_request_assets AS asset
       SET job_id = NULL
      FROM ops.media_request_items AS item, ops.media_requests AS request, ops.jobs AS job
     WHERE item.id = asset.request_item_id
       AND request.id = item.request_id
       AND job.id = asset.job_id
       AND request.status IN ('succeeded', 'partial', 'failed', 'cancelled')
       AND request.finished_at IS NOT NULL
       AND request.finished_at < p_before
       AND job.status IN ('succeeded', 'dead_letter', 'cancelled')
       AND job.finished_at IS NOT NULL
       AND job.finished_at < p_before;

    WITH candidates AS MATERIALIZED (
        SELECT job.id
          FROM ops.jobs AS job
         WHERE job.status IN ('succeeded', 'dead_letter', 'cancelled')
           AND job.finished_at IS NOT NULL
           AND job.finished_at < p_before
           AND NOT EXISTS (
               SELECT 1 FROM ops.admin_requests AS request
                WHERE request.job_id = job.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM ops.backup_requests AS backup
                WHERE backup.job_id = job.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM ops.media_request_assets AS asset
                WHERE asset.job_id = job.id
           )
         ORDER BY job.finished_at, job.id
         LIMIT p_limit
         FOR UPDATE OF job SKIP LOCKED
    ), deleted_events AS (
        DELETE FROM ops.job_events AS event
         USING candidates
         WHERE event.job_id = candidates.id
    ), deleted_jobs AS (
        DELETE FROM ops.jobs AS job
         USING candidates
         WHERE job.id = candidates.id
        RETURNING 1
    )
    SELECT pg_catalog.count(*)::integer INTO deleted_count FROM deleted_jobs;
    RETURN deleted_count;
END
$function$;

ALTER FUNCTION ops.submit_media_request(uuid, text, text) OWNER TO migrator;
ALTER FUNCTION ops.lock_media_request_claim(uuid, text) OWNER TO migrator;
ALTER FUNCTION ops.media_image_job_source(uuid) OWNER TO migrator;
ALTER FUNCTION ops.claim_media_request(text, bigint) OWNER TO migrator;
ALTER FUNCTION assets.select_media_request_sources(uuid, bigint, integer) OWNER TO migrator;
ALTER FUNCTION assets.media_image_configuration() OWNER TO migrator;
ALTER FUNCTION ops.advance_media_request(uuid, text, bigint, boolean, integer) OWNER TO migrator;
ALTER FUNCTION ops.link_media_request_asset(uuid, text, bigint, bigint, smallint, bigint, text, integer, text, uuid, boolean, boolean) OWNER TO migrator;
ALTER FUNCTION ops.refresh_media_request(uuid) OWNER TO migrator;
ALTER FUNCTION assets.queue_obsolete_media_request_files(uuid) OWNER TO migrator;
ALTER FUNCTION assets.queue_image_asset_replacements(smallint, bigint, text, integer, text, text) OWNER TO migrator;
ALTER FUNCTION assets.pending_media_file_deletions(integer) OWNER TO migrator;
ALTER FUNCTION assets.complete_media_file_deletion(bigint) OWNER TO migrator;
ALTER FUNCTION ops.submit_scheduled_catalog_scan(text, timestamptz, date, date) OWNER TO migrator;
ALTER FUNCTION ops.complete_catalog_sync(text, date) OWNER TO migrator;
ALTER FUNCTION ops.mark_catalog_full_sweep_required() OWNER TO migrator;
ALTER FUNCTION ops.media_request_status(uuid) OWNER TO migrator;
ALTER FUNCTION ops.submit_manual_catalog_scan(uuid, text, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.set_worker_state(text, text, text, uuid) OWNER TO migrator;
ALTER FUNCTION ops.start_worker_on_startup(text) OWNER TO migrator;
ALTER FUNCTION ops.prune_finished_jobs(timestamptz, integer) OWNER TO migrator;

ALTER TABLE ops.catalog_schedule_slots OWNER TO migrator;
ALTER TABLE ops.catalog_sync_state OWNER TO migrator;

REVOKE ALL ON TABLE ops.media_requests, ops.media_request_items, ops.media_request_assets
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.media_requests, ops.media_request_items, ops.media_request_assets TO monitor;
REVOKE ALL ON TABLE assets.pending_file_deletions
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE assets.pending_file_deletions TO monitor;
REVOKE ALL ON TABLE ops.catalog_schedule_slots, ops.catalog_sync_state
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.catalog_schedule_slots, ops.catalog_sync_state TO ingest_writer, monitor;
GRANT SELECT ON TABLE ops.catalog_sync_state TO api_reader;

REVOKE ALL ON FUNCTION
    ops.submit_media_request(uuid, text, text),
    ops.lock_media_request_claim(uuid, text),
    ops.media_image_job_source(uuid),
    ops.claim_media_request(text, bigint),
    assets.select_media_request_sources(uuid, bigint, integer),
    assets.media_image_configuration(),
    ops.advance_media_request(uuid, text, bigint, boolean, integer),
    ops.link_media_request_asset(uuid, text, bigint, bigint, smallint, bigint, text, integer, text, uuid, boolean, boolean),
    ops.refresh_media_request(uuid),
    assets.queue_obsolete_media_request_files(uuid),
    assets.queue_image_asset_replacements(smallint, bigint, text, integer, text, text),
    assets.pending_media_file_deletions(integer),
    assets.complete_media_file_deletion(bigint)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.submit_media_request(uuid, text, text)
    TO api_job_submitter;
GRANT EXECUTE ON FUNCTION
    ops.claim_media_request(text, bigint),
    ops.lock_media_request_claim(uuid, text),
    ops.media_image_job_source(uuid),
    assets.select_media_request_sources(uuid, bigint, integer),
    assets.media_image_configuration(),
    ops.advance_media_request(uuid, text, bigint, boolean, integer),
    ops.link_media_request_asset(uuid, text, bigint, bigint, smallint, bigint, text, integer, text, uuid, boolean, boolean),
    ops.refresh_media_request(uuid),
    assets.queue_obsolete_media_request_files(uuid),
    assets.queue_image_asset_replacements(smallint, bigint, text, integer, text, text),
    assets.pending_media_file_deletions(integer),
    assets.complete_media_file_deletion(bigint)
    TO image_writer;
REVOKE ALL ON FUNCTION
    ops.submit_scheduled_catalog_scan(text, timestamptz, date, date),
    ops.complete_catalog_sync(text, date),
    ops.mark_catalog_full_sweep_required(),
    ops.submit_manual_catalog_scan(uuid, text, text, uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION
    ops.submit_scheduled_catalog_scan(text, timestamptz, date, date),
    ops.complete_catalog_sync(text, date),
    ops.mark_catalog_full_sweep_required()
    TO ingest_writer;
GRANT EXECUTE ON FUNCTION ops.submit_manual_catalog_scan(uuid, text, text, uuid)
    TO api_job_submitter;
REVOKE ALL ON FUNCTION ops.media_request_status(uuid)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.media_request_status(uuid) TO api_reader, monitor;
REVOKE ALL ON FUNCTION ops.start_worker_on_startup(text)
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.start_worker_on_startup(text) TO ingest_writer, image_writer;
REVOKE ALL ON FUNCTION ops.prune_finished_jobs(timestamptz, integer)
    FROM PUBLIC, api_reader, api_job_submitter, image_writer, monitor;
GRANT EXECUTE ON FUNCTION ops.prune_finished_jobs(timestamptz, integer)
    TO ingest_writer;

UPDATE ops.service_metadata
SET value = pg_catalog.jsonb_build_object(
        'revision', '0052',
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
  AND metadata.value ->> 'revision' = '0052'
  AND (SELECT pg_catalog.count(*) FROM ops._sqlx_migrations) = 52
  AND (
      SELECT pg_catalog.array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52
  ]::bigint[]
  AND NOT EXISTS (
      SELECT 1 FROM ops._sqlx_migrations AS migration WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_reader, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
