\set ON_ERROR_STOP on
\set seed_count 100000
\set seed_base 900000000

BEGIN;

-- Only the reserved synthetic ID range is touched. This script never deletes
-- real TMDB rows, making repeatable stress runs safe inside the disposable
-- project.
DELETE FROM catalog.titles
 WHERE tmdb_id >= (:seed_base + 1)
   AND tmdb_id < (:seed_base + :seed_count + 1);
-- Deleting titles first cascades title credits; people are RESTRICTed while
-- those credits still exist.
DELETE FROM catalog.people
 WHERE id >= :seed_base
   AND id < (:seed_base + 1000);

INSERT INTO catalog.genres (id, name)
SELECT :seed_base + value, 'Stress Genre ' || value
FROM generate_series(1, 20) AS value
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO catalog.keywords (id, name)
VALUES (:seed_base + 1, 'stress keyword'),
       (210024, 'anime')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO catalog.tags (name)
VALUES ('stress-tag'), ('stress-accent')
ON CONFLICT (name) DO NOTHING;

INSERT INTO catalog.companies (id, name, origin_country, logo_path)
SELECT :seed_base + value, 'Stress Studio ' || value, 'US', '/stress/studio-' || value || '.png'
FROM generate_series(1, 8) AS value
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    origin_country = EXCLUDED.origin_country,
    logo_path = EXCLUDED.logo_path;

INSERT INTO catalog.networks (id, name, origin_country, logo_path)
SELECT :seed_base + value, 'Stress Network ' || value, 'US', '/stress/network-' || value || '.png'
FROM generate_series(1, 4) AS value
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    origin_country = EXCLUDED.origin_country,
    logo_path = EXCLUDED.logo_path;

INSERT INTO catalog.languages (iso_639_1, english_name, name)
VALUES ('en', 'English', 'English'),
       ('ja', 'Japanese', '日本語'),
       ('es', 'Spanish', 'Español'),
       ('fr', 'French', 'Français')
ON CONFLICT (iso_639_1) DO UPDATE SET
    english_name = EXCLUDED.english_name,
    name = EXCLUDED.name;

INSERT INTO catalog.people (id, name, original_name, known_for_department, popularity, profile_path)
SELECT :seed_base + value,
       CASE WHEN value % 19 = 0 THEN 'Beyoncé Stress Actor ' || value
            ELSE 'Stress Actor ' || value END,
       'Stress Actor Original ' || value,
       'Acting',
       (value % 1000)::double precision / 10.0,
       '/stress/person-' || value || '.jpg'
FROM generate_series(1, 1000) AS value
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    original_name = EXCLUDED.original_name,
    known_for_department = EXCLUDED.known_for_department,
    popularity = EXCLUDED.popularity,
    profile_path = EXCLUDED.profile_path;

INSERT INTO catalog.titles (
    id, media_type, tmdb_id, display_title, original_title, overview, tagline,
    status, original_language, release_date, first_air_date, last_air_date,
    popularity, vote_average, vote_count, runtime_minutes, adult, video,
    homepage, poster_path, backdrop_path, is_anime, active, source_updated_at
)
SELECT :seed_base + value,
       CASE WHEN value % 2 = 0 THEN 'tv' ELSE 'movie' END,
       :seed_base + value,
       CASE WHEN value % 101 = 0 THEN 'Café One Piece Stress ' || value
            ELSE 'Café Stress Title ' || value END,
       'Cafe Original Stress ' || value,
       'A searchable stress fixture with accented words and stable facets.',
       'Fast deterministic fixture',
       CASE WHEN value % 11 = 0 THEN 'Ended' ELSE 'Released' END,
       CASE WHEN value % 5 = 0 THEN 'ja' WHEN value % 3 = 0 THEN 'es' ELSE 'en' END,
       (DATE '2000-01-01' + (value % 9000)::integer),
       CASE WHEN value % 2 = 0 THEN (DATE '2000-01-01' + (value % 9000)::integer) END,
       CASE WHEN value % 2 = 0 THEN (DATE '2001-01-01' + (value % 9000)::integer) END,
       (value % 10000)::double precision / 100.0,
       (value % 1001)::double precision / 100.0,
       value * 3,
       45 + (value % 180),
       (value % 97 = 0),
       (value % 13 = 0),
       'https://stress.invalid/title/' || (:seed_base + value),
       '/stress/poster-' || value || '.jpg',
       '/stress/backdrop-' || value || '.jpg',
       (value % 17 = 0),
       (value % 113 <> 0),
       clock_timestamp()
FROM generate_series(1, :seed_count) AS value
ON CONFLICT (media_type, tmdb_id) DO UPDATE SET
    display_title = EXCLUDED.display_title,
    original_title = EXCLUDED.original_title,
    overview = EXCLUDED.overview,
    status = EXCLUDED.status,
    original_language = EXCLUDED.original_language,
    release_date = EXCLUDED.release_date,
    first_air_date = EXCLUDED.first_air_date,
    last_air_date = EXCLUDED.last_air_date,
    popularity = EXCLUDED.popularity,
    vote_average = EXCLUDED.vote_average,
    vote_count = EXCLUDED.vote_count,
    runtime_minutes = EXCLUDED.runtime_minutes,
    is_anime = EXCLUDED.is_anime,
    active = EXCLUDED.active,
    updated_at = clock_timestamp();

INSERT INTO catalog.movie_details (title_id, runtime_minutes)
SELECT id, runtime_minutes
FROM catalog.titles
WHERE media_type = 'movie'
  AND tmdb_id >= (:seed_base + 1)
  AND tmdb_id < (:seed_base + :seed_count + 1)
ON CONFLICT (title_id) DO UPDATE SET runtime_minutes = EXCLUDED.runtime_minutes;

INSERT INTO catalog.tv_details (title_id, in_production, number_of_episodes, number_of_seasons, series_type)
SELECT id, (id % 11 <> 0), 12 + (id % 200), 1 + (id % 8), 'Scripted'
FROM catalog.titles
WHERE media_type = 'tv'
  AND tmdb_id >= (:seed_base + 1)
  AND tmdb_id < (:seed_base + :seed_count + 1)
ON CONFLICT (title_id) DO UPDATE SET
    in_production = EXCLUDED.in_production,
    number_of_episodes = EXCLUDED.number_of_episodes,
    number_of_seasons = EXCLUDED.number_of_seasons,
    series_type = EXCLUDED.series_type;

INSERT INTO catalog.title_genres (title_id, genre_id)
SELECT :seed_base + value, :seed_base + (1 + (value % 20))
FROM generate_series(1, :seed_count) AS value
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_keywords (title_id, keyword_id)
SELECT :seed_base + value, :seed_base + 1
FROM generate_series(1, :seed_count) AS value
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_keywords (title_id, keyword_id)
SELECT :seed_base + value, 210024
FROM generate_series(1, :seed_count) AS value
WHERE value % 17 = 0
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_tags (title_id, tag_id)
SELECT :seed_base + value, tag.id
FROM generate_series(1, :seed_count) AS value
CROSS JOIN LATERAL (SELECT id FROM catalog.tags WHERE name = 'stress-tag') AS tag
WHERE value % 5 = 0
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_companies (title_id, company_id, company_role)
SELECT :seed_base + value, :seed_base + (1 + (value % 8)), 'production'
FROM generate_series(1, :seed_count) AS value
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_networks (title_id, network_id)
SELECT :seed_base + value, :seed_base + (1 + (value % 4))
FROM generate_series(1, :seed_count) AS value
WHERE value % 2 = 0
ON CONFLICT DO NOTHING;

INSERT INTO catalog.title_languages (title_id, language_id, is_original)
SELECT :seed_base + value,
       CASE WHEN value % 5 = 0 THEN 'ja' WHEN value % 3 = 0 THEN 'es' ELSE 'en' END,
       true
FROM generate_series(1, :seed_count) AS value
ON CONFLICT (title_id, language_id) DO UPDATE SET is_original = EXCLUDED.is_original;

INSERT INTO catalog.people (id, name, original_name, known_for_department, popularity)
SELECT :seed_base + 1000 + value, 'Stress Crew ' || value, 'Stress Crew Original ' || value,
       'Crew', 1.0
FROM generate_series(1, 10) AS value
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.title_credits (title_id, person_id, credit_id, credit_type, department, character, cast_order)
SELECT :seed_base + value,
       :seed_base + (1 + (value % 1000)),
       'stress-cast-' || (:seed_base + value),
       'cast', 'Acting', 'Character ' || value, 0
FROM generate_series(1, :seed_count) AS value
ON CONFLICT DO NOTHING;

INSERT INTO catalog.seasons (id, title_id, season_number, name, overview, air_date, episode_count)
SELECT :seed_base + :seed_count + value,
       :seed_base + value,
       1,
       'Season 1', 'Stress season', DATE '2020-01-01', 10
FROM generate_series(1, :seed_count) AS value
WHERE value % 2 = 0
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO catalog.episodes (
    id, season_id, title_id, episode_number, name, overview, air_date,
    runtime_minutes, vote_average, vote_count
)
SELECT :seed_base + (:seed_count * 2) + value,
       :seed_base + :seed_count + value,
       :seed_base + value,
       1, 'Episode 1', 'Stress episode', DATE '2020-01-02', 45, 8.0, 10
FROM generate_series(1, :seed_count) AS value
WHERE value % 2 = 0
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO assets.image_assets (
    title_id, image_kind, source, source_key, source_url, status
)
SELECT :seed_base + value, 'poster', 'stress', 'poster-' || (:seed_base + value),
       'https://image.tmdb.org/t/p/w500/stress-' || value || '.jpg', 'pending'
FROM generate_series(1, :seed_count) AS value
WHERE value % 1000 = 0
ON CONFLICT (source, source_key) DO NOTHING;

COMMIT;

ANALYZE catalog.titles;
ANALYZE catalog.people;
ANALYZE catalog.title_credits;
ANALYZE catalog.title_genres;
ANALYZE catalog.title_keywords;
ANALYZE catalog.title_tags;
ANALYZE catalog.title_companies;
ANALYZE catalog.title_networks;
ANALYZE catalog.title_languages;
ANALYZE search.search_documents;
