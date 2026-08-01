-- Mirror-critical relational entities for people, credits, seasons, episodes, and images.
-- SQLx migrations are immutable once published; changes belong in a later migration.

CREATE TABLE catalog.people (
    id bigint PRIMARY KEY,
    name text,
    normalized_name text NOT NULL DEFAULT '',
    original_name text,
    known_for_department text,
    gender smallint,
    biography text,
    birthday date,
    deathday date,
    place_of_birth text,
    homepage text,
    imdb_id text,
    adult boolean NOT NULL DEFAULT false,
    popularity double precision,
    profile_path text,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT people_id_check CHECK (id > 0),
    CONSTRAINT people_name_check CHECK (name IS NULL OR btrim(name) <> ''),
    CONSTRAINT people_gender_check CHECK (gender IS NULL OR gender BETWEEN 0 AND 3),
    CONSTRAINT people_popularity_check CHECK (popularity IS NULL OR popularity >= 0),
    CONSTRAINT people_popularity_finite_check CHECK (
        popularity IS NULL
        OR (popularity > '-Infinity'::double precision
            AND popularity < 'Infinity'::double precision)
    ),
    CONSTRAINT people_life_dates_check CHECK (
        deathday IS NULL OR birthday IS NULL OR deathday >= birthday
    )
);

CREATE OR REPLACE FUNCTION catalog.refresh_person_search()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, catalog
AS $function$
BEGIN
    NEW.normalized_name := lower(public.unaccent(coalesce(NEW.name, '')));
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$function$;

ALTER FUNCTION catalog.refresh_person_search() OWNER TO migrator;
REVOKE ALL ON FUNCTION catalog.refresh_person_search() FROM PUBLIC;

CREATE TRIGGER people_refresh_search
BEFORE INSERT OR UPDATE OF name ON catalog.people
FOR EACH ROW EXECUTE FUNCTION catalog.refresh_person_search();

CREATE TABLE catalog.title_credits (
    title_id bigint NOT NULL REFERENCES catalog.titles (id) ON DELETE CASCADE,
    person_id bigint NOT NULL REFERENCES catalog.people (id) ON DELETE RESTRICT,
    credit_id text NOT NULL,
    credit_type text NOT NULL DEFAULT 'cast',
    department text,
    job text,
    character text,
    cast_order integer,
    episode_count integer,
    adult boolean NOT NULL DEFAULT false,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT title_credits_credit_id_check CHECK (btrim(credit_id) <> ''),
    CONSTRAINT title_credits_type_check CHECK (credit_type IN ('cast', 'crew')),
    CONSTRAINT title_credits_cast_order_check CHECK (cast_order IS NULL OR cast_order >= 0),
    CONSTRAINT title_credits_episode_count_check
        CHECK (episode_count IS NULL OR episode_count >= 0),
    PRIMARY KEY (title_id, person_id, credit_id)
);

CREATE TABLE catalog.seasons (
    id bigint PRIMARY KEY,
    title_id bigint NOT NULL,
    media_type text NOT NULL DEFAULT 'tv',
    season_number integer NOT NULL,
    name text,
    overview text,
    air_date date,
    episode_count integer,
    poster_path text,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT seasons_id_check CHECK (id > 0),
    CONSTRAINT seasons_media_type_check CHECK (media_type = 'tv'),
    CONSTRAINT seasons_number_check CHECK (season_number >= 0),
    CONSTRAINT seasons_episode_count_check CHECK (episode_count IS NULL OR episode_count >= 0),
    CONSTRAINT seasons_title_fkey
        FOREIGN KEY (title_id, media_type)
        REFERENCES catalog.titles (id, media_type)
        ON DELETE CASCADE,
    CONSTRAINT seasons_title_number_unique UNIQUE (title_id, season_number),
    CONSTRAINT seasons_id_title_unique UNIQUE (id, title_id)
);

CREATE TABLE catalog.episodes (
    id bigint PRIMARY KEY,
    season_id bigint NOT NULL,
    title_id bigint NOT NULL,
    episode_number integer NOT NULL,
    name text,
    overview text,
    air_date date,
    runtime_minutes integer,
    still_path text,
    vote_average double precision,
    vote_count bigint,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT episodes_id_check CHECK (id > 0),
    CONSTRAINT episodes_number_check CHECK (episode_number >= 0),
    CONSTRAINT episodes_runtime_check CHECK (runtime_minutes IS NULL OR runtime_minutes >= 0),
    CONSTRAINT episodes_vote_average_check CHECK (
        vote_average IS NULL OR vote_average BETWEEN 0 AND 10
    ),
    CONSTRAINT episodes_vote_average_finite_check CHECK (
        vote_average IS NULL
        OR (vote_average > '-Infinity'::double precision
            AND vote_average < 'Infinity'::double precision)
    ),
    CONSTRAINT episodes_vote_count_check CHECK (vote_count IS NULL OR vote_count >= 0),
    CONSTRAINT episodes_season_fkey
        FOREIGN KEY (season_id, title_id)
        REFERENCES catalog.seasons (id, title_id)
        ON DELETE CASCADE,
    CONSTRAINT episodes_title_fkey
        FOREIGN KEY (title_id)
        REFERENCES catalog.titles (id)
        ON DELETE CASCADE,
    CONSTRAINT episodes_season_number_unique UNIQUE (season_id, episode_number),
    CONSTRAINT episodes_id_title_unique UNIQUE (id, title_id)
);

CREATE TABLE catalog.episode_credits (
    episode_id bigint NOT NULL,
    title_id bigint NOT NULL,
    person_id bigint NOT NULL REFERENCES catalog.people (id) ON DELETE RESTRICT,
    credit_id text NOT NULL,
    credit_type text NOT NULL DEFAULT 'cast',
    department text,
    job text,
    character text,
    cast_order integer,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT episode_credits_credit_id_check CHECK (btrim(credit_id) <> ''),
    CONSTRAINT episode_credits_type_check CHECK (credit_type IN ('cast', 'crew')),
    CONSTRAINT episode_credits_cast_order_check CHECK (cast_order IS NULL OR cast_order >= 0),
    CONSTRAINT episode_credits_episode_fkey
        FOREIGN KEY (episode_id, title_id)
        REFERENCES catalog.episodes (id, title_id)
        ON DELETE CASCADE,
    PRIMARY KEY (episode_id, person_id, credit_id)
);

CREATE TABLE assets.image_assets (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    title_id bigint REFERENCES catalog.titles (id) ON DELETE CASCADE,
    person_id bigint REFERENCES catalog.people (id) ON DELETE CASCADE,
    company_id bigint REFERENCES catalog.companies (id) ON DELETE CASCADE,
    network_id bigint REFERENCES catalog.networks (id) ON DELETE CASCADE,
    collection_id bigint REFERENCES catalog.collections (id) ON DELETE CASCADE,
    season_id bigint REFERENCES catalog.seasons (id) ON DELETE CASCADE,
    episode_id bigint REFERENCES catalog.episodes (id) ON DELETE CASCADE,
    image_kind text NOT NULL,
    source text NOT NULL DEFAULT 'tmdb',
    source_key text NOT NULL,
    source_url text,
    storage_path text,
    mime_type text,
    width integer,
    height integer,
    file_size_bytes bigint,
    sha256 text,
    status text NOT NULL DEFAULT 'pending',
    iso_639_1 text REFERENCES catalog.languages (iso_639_1) ON DELETE RESTRICT,
    source_updated_at timestamptz,
    downloaded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT image_assets_owner_check CHECK (
        num_nonnulls(title_id, person_id, company_id, network_id, collection_id, season_id, episode_id) = 1
    ),
    CONSTRAINT image_assets_kind_check CHECK (
        image_kind IN (
            'poster', 'backdrop', 'logo', 'profile', 'still', 'avatar', 'banner', 'other'
        )
    ),
    CONSTRAINT image_assets_source_check CHECK (btrim(source) <> ''),
    CONSTRAINT image_assets_key_check CHECK (btrim(source_key) <> ''),
    CONSTRAINT image_assets_dimensions_check CHECK (
        (width IS NULL OR width > 0) AND (height IS NULL OR height > 0)
    ),
    CONSTRAINT image_assets_file_size_check CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0),
    CONSTRAINT image_assets_sha256_check CHECK (
        sha256 IS NULL OR sha256 ~ '^[0-9a-fA-F]{64}$'
    ),
    CONSTRAINT image_assets_status_check CHECK (status IN ('pending', 'downloading', 'ready', 'failed')),
    CONSTRAINT image_assets_downloaded_status_check CHECK (
        status <> 'ready' OR downloaded_at IS NOT NULL
    ),
    CONSTRAINT image_assets_source_key_unique UNIQUE (source, source_key)
);

CREATE INDEX people_normalized_name_trgm_idx
    ON catalog.people USING gist (normalized_name gist_trgm_ops);
CREATE INDEX people_popularity_idx
    ON catalog.people (popularity DESC NULLS LAST, id DESC)
    WHERE popularity IS NOT NULL;

CREATE INDEX title_credits_person_idx ON catalog.title_credits (person_id, title_id, cast_order, credit_id);
CREATE INDEX title_credits_title_cast_idx
    ON catalog.title_credits (title_id, cast_order, person_id, credit_id)
    WHERE credit_type = 'cast';
CREATE INDEX title_credits_title_crew_idx
    ON catalog.title_credits (title_id, department, job, person_id, credit_id)
    WHERE credit_type = 'crew';
CREATE INDEX episode_credits_person_idx
    ON catalog.episode_credits (person_id, title_id, episode_id, cast_order, credit_id);
CREATE INDEX episode_credits_episode_cast_idx
    ON catalog.episode_credits (episode_id, cast_order, person_id, credit_id)
    WHERE credit_type = 'cast';
CREATE INDEX seasons_title_idx ON catalog.seasons (title_id, season_number, id);
CREATE INDEX episodes_title_air_idx
    ON catalog.episodes (title_id, air_date DESC NULLS LAST, season_id, episode_number, id);
CREATE INDEX episodes_season_idx
    ON catalog.episodes (season_id, episode_number, id);

CREATE INDEX image_assets_title_kind_idx
    ON assets.image_assets (title_id, image_kind, id)
    WHERE title_id IS NOT NULL;
CREATE INDEX image_assets_person_kind_idx
    ON assets.image_assets (person_id, image_kind, id)
    WHERE person_id IS NOT NULL;
CREATE INDEX image_assets_company_kind_idx
    ON assets.image_assets (company_id, image_kind, id)
    WHERE company_id IS NOT NULL;
CREATE INDEX image_assets_network_kind_idx
    ON assets.image_assets (network_id, image_kind, id)
    WHERE network_id IS NOT NULL;
CREATE INDEX image_assets_collection_kind_idx
    ON assets.image_assets (collection_id, image_kind, id)
    WHERE collection_id IS NOT NULL;
CREATE INDEX image_assets_season_kind_idx
    ON assets.image_assets (season_id, image_kind, id)
    WHERE season_id IS NOT NULL;
CREATE INDEX image_assets_episode_kind_idx
    ON assets.image_assets (episode_id, image_kind, id)
    WHERE episode_id IS NOT NULL;
CREATE INDEX image_assets_pending_idx
    ON assets.image_assets (status, updated_at, id)
    WHERE status IN ('pending', 'failed');

ALTER TABLE catalog.people OWNER TO migrator;
ALTER TABLE catalog.title_credits OWNER TO migrator;
ALTER TABLE catalog.seasons OWNER TO migrator;
ALTER TABLE catalog.episodes OWNER TO migrator;
ALTER TABLE catalog.episode_credits OWNER TO migrator;
ALTER TABLE assets.image_assets OWNER TO migrator;

GRANT SELECT ON catalog.people, catalog.title_credits, catalog.seasons,
    catalog.episodes, catalog.episode_credits TO api_reader;
GRANT INSERT, UPDATE, DELETE ON catalog.people, catalog.title_credits,
    catalog.seasons, catalog.episodes, catalog.episode_credits TO ingest_writer;
GRANT SELECT ON assets.image_assets TO api_reader, image_writer;
GRANT INSERT, UPDATE, DELETE ON assets.image_assets TO image_writer;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE assets.image_assets_id_seq TO image_writer;

ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
    GRANT SELECT ON TABLES TO api_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO image_writer;

UPDATE ops.service_metadata
SET value = jsonb_build_object(
        'revision', '0008',
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
  AND metadata.value ->> 'revision' = '0008'
  AND (SELECT count(*) FROM ops._sqlx_migrations) = 8
  AND (
      SELECT array_agg(migration.version ORDER BY migration.version)
      FROM ops._sqlx_migrations AS migration
      WHERE migration.success
  ) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8]::bigint[]
  AND NOT EXISTS (
      SELECT 1
      FROM ops._sqlx_migrations AS migration
      WHERE NOT migration.success
  );

ALTER VIEW ops.readiness OWNER TO migrator;
REVOKE ALL ON TABLE ops.readiness
    FROM PUBLIC, api_job_submitter, ingest_writer, image_writer, monitor;
GRANT SELECT ON TABLE ops.readiness TO api_reader, monitor;
