use std::{collections::BTreeSet, path::Path, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use tmdb_db::TmdbDocumentRepository;
use tmdb_domain::MediaType;
use tmdb_jobs::JobExecutionError;
use tmdb_upstream::{
    ChangePage, TmdbAlternateTitle, TmdbCollection, TmdbCompany, TmdbContentRating, TmdbCredit,
    TmdbCredits, TmdbEpisode, TmdbExternalIds, TmdbGenre, TmdbImage, TmdbImages, TmdbKeyword,
    TmdbMovie, TmdbNetwork, TmdbReleaseDateCountry, TmdbSeason, TmdbSeasonSummary, TmdbTranslation,
    TmdbTv, TmdbVideo,
};
use uuid::Uuid;

const IMAGE_JOB_TYPE: &str = "image.download";
const IMAGE_JOB_PAYLOAD_VERSION: i32 = 1;
const MAX_ACTIVE_IMAGE_JOBS: i64 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogWriteOptions {
    enqueue_media: bool,
    enqueue_enrichment: bool,
    enqueue_seasons: bool,
}

impl CatalogWriteOptions {
    pub(crate) const CATALOG_ONLY: Self = Self {
        enqueue_media: false,
        enqueue_enrichment: false,
        enqueue_seasons: false,
    };

    pub(crate) const fn title_refresh(enqueue_media: bool) -> Self {
        Self {
            enqueue_media,
            enqueue_enrichment: true,
            enqueue_seasons: true,
        }
    }

    pub(crate) const fn title_enrichment(enqueue_media: bool) -> Self {
        Self {
            enqueue_media,
            enqueue_enrichment: false,
            enqueue_seasons: true,
        }
    }

    pub(crate) const fn season_refresh(enqueue_media: bool) -> Self {
        Self {
            enqueue_media,
            enqueue_enrichment: false,
            enqueue_seasons: false,
        }
    }
}

/// Persists the exact upstream JSON used for a detail refresh.
pub(crate) async fn persist_tmdb_document(
    pool: &PgPool,
    endpoint_path: &str,
    query_string: &str,
    response: &serde_json::Value,
) -> Result<(), JobExecutionError> {
    TmdbDocumentRepository::new(pool.clone())
        .upsert(endpoint_path, query_string, response)
        .await
        .map_err(database_error)
}

use super::catalog_locks::{
    changes_write_resources, movie_write_resources, prelock_catalog_write_resources,
    season_write_resources, tv_write_resources,
};
use super::{normalize_language, parse_source_date, source_id};

/// Persists a movie and optionally creates local-media jobs in the same
/// catalog transaction.
#[allow(
    clippy::too_many_lines,
    reason = "one transaction must keep title, facets, artwork jobs, and the sorted lock set atomic"
)]
pub(crate) async fn persist_movie_with_options(
    pool: &PgPool,
    movie: &TmdbMovie,
    options: CatalogWriteOptions,
) -> Result<(), JobExecutionError> {
    let tmdb_id = source_id(movie.id)?;
    let release_date = parse_source_date(movie.release_date.as_deref())?;
    let runtime_minutes = movie.runtime.map(i32::from);
    let vote_count = movie
        .vote_count
        .map(|value| {
            i64::try_from(value).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .transpose()?;
    let resources = movie_write_resources(movie, tmdb_id)?;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    prelock_catalog_write_resources(&mut transaction, resources).await?;
    let title_id = upsert_title(
        &mut transaction,
        "movie",
        tmdb_id,
        movie.title.as_deref(),
        movie.original_title.as_deref(),
        movie.overview.as_deref(),
        movie.original_language.as_deref(),
        release_date,
        None,
        movie.popularity,
        movie.vote_average,
        vote_count,
        runtime_minutes,
        false,
        false,
    )
    .await?;

    sqlx::query(
        "INSERT INTO catalog.movie_details (title_id, media_type, runtime_minutes)
         VALUES ($1, 'movie', $2)
         ON CONFLICT (title_id) DO UPDATE SET
             runtime_minutes = COALESCE(EXCLUDED.runtime_minutes, catalog.movie_details.runtime_minutes)",
    )
    .bind(title_id)
    .bind(runtime_minutes)
    .execute(&mut *transaction)
    .await
    .map_err(database_error)?;

    replace_genres(&mut transaction, title_id, &movie.genres).await?;
    replace_keywords(&mut transaction, title_id, &movie.keywords).await?;
    replace_credits(&mut transaction, title_id, &movie.credits).await?;
    replace_companies(
        &mut transaction,
        title_id,
        &movie.production_companies,
        "production",
    )
    .await?;
    replace_original_language(
        &mut transaction,
        title_id,
        movie.original_language.as_deref(),
    )
    .await?;
    replace_collection(
        &mut transaction,
        title_id,
        movie.belongs_to_collection.as_ref(),
    )
    .await?;
    replace_common_parity_facets(
        &mut transaction,
        title_id,
        movie.translations.translations.as_slice(),
        movie.alternate_titles.as_slice(),
        &movie.external_ids,
        movie.videos.results.as_slice(),
    )
    .await?;
    replace_movie_release_dates(
        &mut transaction,
        title_id,
        movie.release_dates.results.as_slice(),
    )
    .await?;
    enqueue_credit_images(&mut transaction, &movie.credits, options.enqueue_media).await?;
    enqueue_company_images(
        &mut transaction,
        &movie.production_companies,
        options.enqueue_media,
    )
    .await?;
    enqueue_collection_images(
        &mut transaction,
        movie.belongs_to_collection.as_ref(),
        options.enqueue_media,
    )
    .await?;
    enqueue_title_images(
        &mut transaction,
        "movie",
        tmdb_id,
        movie.poster_path.as_deref(),
        movie.backdrop_path.as_deref(),
        &movie.images,
        options.enqueue_media,
    )
    .await?;
    if options.enqueue_enrichment {
        enqueue_title_enrichment(&mut transaction, super::ENRICH_MOVIE_JOB, tmdb_id).await?;
    }
    transaction.commit().await.map_err(database_error)
}

/// Persists a TV title and optionally creates local-media jobs in the same
/// catalog transaction.
#[allow(clippy::too_many_lines)]
pub(crate) async fn persist_tv_with_options(
    pool: &PgPool,
    series: &TmdbTv,
    options: CatalogWriteOptions,
) -> Result<(), JobExecutionError> {
    let tmdb_id = source_id(series.id)?;
    let first_air_date = parse_source_date(series.first_air_date.as_deref())?;
    let vote_count = series
        .vote_count
        .map(|value| {
            i64::try_from(value).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .transpose()?;
    let number_of_episodes = series
        .number_of_episodes
        .map(|value| {
            i32::try_from(value).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .transpose()?;
    let number_of_seasons = series.number_of_seasons.map(i32::from);
    let resources = tv_write_resources(series, tmdb_id)?;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    prelock_catalog_write_resources(&mut transaction, resources).await?;
    let title_id = upsert_title(
        &mut transaction,
        "tv",
        tmdb_id,
        series.name.as_deref(),
        series.original_name.as_deref(),
        series.overview.as_deref(),
        series.original_language.as_deref(),
        None,
        first_air_date,
        series.popularity,
        series.vote_average,
        vote_count,
        None,
        false,
        false,
    )
    .await?;

    sqlx::query(
        "INSERT INTO catalog.tv_details (
             title_id, media_type, number_of_episodes, number_of_seasons
         ) VALUES ($1, 'tv', $2, $3)
         ON CONFLICT (title_id) DO UPDATE SET
             number_of_episodes = EXCLUDED.number_of_episodes,
             number_of_seasons = EXCLUDED.number_of_seasons",
    )
    .bind(title_id)
    .bind(number_of_episodes)
    .bind(number_of_seasons)
    .execute(&mut *transaction)
    .await
    .map_err(database_error)?;

    replace_genres(&mut transaction, title_id, &series.genres).await?;
    replace_keywords(&mut transaction, title_id, &series.keywords).await?;
    replace_credits(&mut transaction, title_id, &series.credits).await?;
    replace_season_summaries(&mut transaction, title_id, series.seasons.as_slice()).await?;
    replace_companies(
        &mut transaction,
        title_id,
        &series.production_companies,
        "production",
    )
    .await?;
    replace_networks(&mut transaction, title_id, &series.networks).await?;
    replace_original_language(
        &mut transaction,
        title_id,
        series.original_language.as_deref(),
    )
    .await?;
    replace_common_parity_facets(
        &mut transaction,
        title_id,
        series.translations.translations.as_slice(),
        series.alternate_titles.as_slice(),
        &series.external_ids,
        series.videos.results.as_slice(),
    )
    .await?;
    replace_tv_certifications(
        &mut transaction,
        title_id,
        series.content_ratings.results.as_slice(),
    )
    .await?;
    enqueue_credit_images(&mut transaction, &series.credits, options.enqueue_media).await?;
    enqueue_season_summary_jobs(
        &mut transaction,
        tmdb_id,
        series.seasons.as_slice(),
        options,
    )
    .await?;
    enqueue_company_images(
        &mut transaction,
        &series.production_companies,
        options.enqueue_media,
    )
    .await?;
    enqueue_network_images(&mut transaction, &series.networks, options.enqueue_media).await?;
    enqueue_title_images(
        &mut transaction,
        "tv",
        tmdb_id,
        series.poster_path.as_deref(),
        series.backdrop_path.as_deref(),
        &series.images,
        options.enqueue_media,
    )
    .await?;
    if options.enqueue_enrichment {
        enqueue_title_enrichment(&mut transaction, super::ENRICH_TV_JOB, tmdb_id).await?;
    }
    transaction.commit().await.map_err(database_error)
}

/// Persists one TV season and its episodes, credits, and image jobs in one
/// transaction. The season job is intentionally separate from the TV detail
/// request because a series can contain hundreds of episodes.
pub(crate) async fn persist_season_with_options(
    pool: &PgPool,
    tv_id: u32,
    season: &TmdbSeason,
    options: CatalogWriteOptions,
) -> Result<(), JobExecutionError> {
    let tv_id = source_id(u64::from(tv_id))?;
    let season_id = source_id(season.id)?;
    let season_number = i32::try_from(season.season_number)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let air_date = parse_source_date(season.air_date.as_deref())?;
    let resources = season_write_resources(tv_id, season, season_id)?;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    prelock_catalog_write_resources(&mut transaction, resources).await?;
    let parent: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM catalog.titles
         WHERE media_type = 'tv' AND tmdb_id = $1 AND active
         FOR UPDATE",
    )
    .bind(tv_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(database_error)?;
    let Some(title_id) = parent else {
        return Err(JobExecutionError::retry(
            "entity_not_ready",
            Duration::from_secs(5),
        ));
    };
    let episode_count = i32::try_from(season.episodes.len()).ok();
    sqlx::query(
        "INSERT INTO catalog.seasons (
             id, title_id, media_type, season_number, name, overview, air_date,
             episode_count, poster_path, source_updated_at
         ) VALUES ($1, $2, 'tv', $3, $4, $5, $6, $7, $8, clock_timestamp())
         ON CONFLICT (id) DO UPDATE SET
             title_id = EXCLUDED.title_id,
             season_number = EXCLUDED.season_number,
             name = EXCLUDED.name,
             overview = EXCLUDED.overview,
             air_date = EXCLUDED.air_date,
             episode_count = EXCLUDED.episode_count,
             poster_path = EXCLUDED.poster_path,
             source_updated_at = EXCLUDED.source_updated_at,
             updated_at = clock_timestamp()",
    )
    .bind(season_id)
    .bind(title_id)
    .bind(season_number)
    .bind(season.name.as_deref())
    .bind(season.overview.as_deref())
    .bind(air_date)
    .bind(episode_count)
    .bind(season.poster_path.as_deref())
    .execute(&mut *transaction)
    .await
    .map_err(database_error)?;

    for episode in &season.episodes {
        persist_episode(&mut transaction, title_id, season_id, episode).await?;
    }

    enqueue_gallery_images_with_position(
        &mut transaction,
        "season",
        season_id,
        "poster",
        season.poster_path.as_deref(),
        &season.images.posters,
        Some(season.season_number),
        None,
        Some(tv_id),
        options.enqueue_media,
    )
    .await?;
    for episode in &season.episodes {
        enqueue_gallery_images_with_position(
            &mut transaction,
            "episode",
            source_id(episode.id)?,
            "still",
            episode.still_path.as_deref(),
            &episode.images.stills,
            Some(season.season_number),
            Some(episode.episode_number),
            Some(tv_id),
            options.enqueue_media,
        )
        .await?;
        enqueue_credit_images(&mut transaction, &episode.credits, options.enqueue_media).await?;
    }
    transaction.commit().await.map_err(database_error)
}

/// Materializes changed IDs as active title identities for bounded refresh work.
pub(crate) async fn persist_changes(
    pool: &PgPool,
    media_type: MediaType,
    page: &ChangePage,
) -> Result<(), JobExecutionError> {
    let resources = changes_write_resources(media_type, page)?;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    prelock_catalog_write_resources(&mut transaction, resources).await?;
    for changed in &page.results {
        let tmdb_id = source_id(changed.id)?;
        upsert_changed_title(
            &mut transaction,
            &media_type.to_string(),
            tmdb_id,
            changed.popularity,
            changed.adult,
            changed.video,
        )
        .await?;
    }
    transaction.commit().await.map_err(database_error)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_title(
    transaction: &mut Transaction<'_, Postgres>,
    media_type: &str,
    tmdb_id: i64,
    display_title: Option<&str>,
    original_title: Option<&str>,
    overview: Option<&str>,
    original_language: Option<&str>,
    release_date: Option<NaiveDate>,
    first_air_date: Option<NaiveDate>,
    popularity: Option<f64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    runtime_minutes: Option<i32>,
    adult: bool,
    video: bool,
) -> Result<i64, JobExecutionError> {
    sqlx::query_scalar(
        "INSERT INTO catalog.titles (
             media_type, tmdb_id, display_title, original_title, overview,
             original_language, release_date, first_air_date, popularity,
             vote_average, vote_count, runtime_minutes, adult, video,
             active, source_updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, true, $15)
         ON CONFLICT (media_type, tmdb_id) DO UPDATE SET
             display_title = EXCLUDED.display_title,
             original_title = EXCLUDED.original_title,
             overview = EXCLUDED.overview,
             original_language = EXCLUDED.original_language,
             release_date = EXCLUDED.release_date,
             first_air_date = EXCLUDED.first_air_date,
             popularity = EXCLUDED.popularity,
             vote_average = EXCLUDED.vote_average,
             vote_count = EXCLUDED.vote_count,
             runtime_minutes = COALESCE(EXCLUDED.runtime_minutes, catalog.titles.runtime_minutes),
             adult = EXCLUDED.adult,
             video = EXCLUDED.video,
             active = true,
             source_updated_at = EXCLUDED.source_updated_at,
             updated_at = clock_timestamp()
         RETURNING id",
    )
    .bind(media_type)
    .bind(tmdb_id)
    .bind(display_title)
    .bind(original_title)
    .bind(overview)
    .bind(original_language)
    .bind(release_date)
    .bind(first_air_date)
    .bind(popularity)
    .bind(vote_average)
    .bind(vote_count)
    .bind(runtime_minutes)
    .bind(adult)
    .bind(video)
    .bind(Utc::now())
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)
}

async fn upsert_changed_title(
    transaction: &mut Transaction<'_, Postgres>,
    media_type: &str,
    tmdb_id: i64,
    popularity: Option<f64>,
    adult: bool,
    video: bool,
) -> Result<(), JobExecutionError> {
    sqlx::query(
        "INSERT INTO catalog.titles (
             media_type, tmdb_id, popularity, adult, video, active, source_updated_at
         ) VALUES ($1, $2, $3, $4, $5, true, $6)
         ON CONFLICT (media_type, tmdb_id) DO UPDATE SET
             popularity = COALESCE(EXCLUDED.popularity, catalog.titles.popularity),
             adult = EXCLUDED.adult,
             video = EXCLUDED.video,
             active = true,
             source_updated_at = EXCLUDED.source_updated_at,
             updated_at = clock_timestamp()",
    )
    .bind(media_type)
    .bind(tmdb_id)
    .bind(popularity)
    .bind(adult)
    .bind(video)
    .bind(Utc::now())
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn replace_genres(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    genres: &[TmdbGenre],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_genres WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let mut genres = genres.iter().collect::<Vec<_>>();
    genres.sort_unstable_by_key(|genre| genre.id);
    for genre in genres {
        let genre_id = source_id(genre.id)?;
        sqlx::query(
            "WITH updated AS (
                 UPDATE catalog.genres
                    SET name = $2
                  WHERE id = $1
                    AND $2 IS NOT NULL
                    AND name IS DISTINCT FROM $2
                 RETURNING id
             )
             INSERT INTO catalog.genres (id, name)
             SELECT $1, $2
              WHERE NOT EXISTS (
                    SELECT 1 FROM catalog.genres WHERE id = $1
              )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(genre_id)
        .bind(genre.name.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO catalog.title_genres (title_id, genre_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(title_id)
        .bind(genre_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_keywords(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    keywords: &[TmdbKeyword],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_keywords WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let mut keywords = keywords.iter().collect::<Vec<_>>();
    keywords.sort_unstable_by_key(|keyword| keyword.id);
    for keyword in keywords {
        let keyword_id = source_id(keyword.id)?;
        sqlx::query(
            "WITH updated AS (
                 UPDATE catalog.keywords
                    SET name = $2
                  WHERE id = $1
                    AND $2 IS NOT NULL
                    AND name IS DISTINCT FROM $2
                 RETURNING id
             )
             INSERT INTO catalog.keywords (id, name)
             SELECT $1, $2
              WHERE NOT EXISTS (
                    SELECT 1 FROM catalog.keywords WHERE id = $1
              )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(keyword_id)
        .bind(keyword.name.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO catalog.title_keywords (title_id, keyword_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(title_id)
        .bind(keyword_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_credits(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    credits: &TmdbCredits,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_credits WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for (credit_type, position, credit) in ordered_credits(credits) {
        let person_id = source_id(credit.id)?;
        let credit_id = stable_credit_id(credit, credit_type, position);
        upsert_person(transaction, person_id, credit).await?;
        sqlx::query(
            "INSERT INTO catalog.title_credits (
                 title_id, person_id, credit_id, credit_type, department, job,
                 character, cast_order, episode_count, adult, source_updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())
             ON CONFLICT (title_id, person_id, credit_id) DO UPDATE SET
                 credit_type = EXCLUDED.credit_type,
                 department = EXCLUDED.department,
                 job = EXCLUDED.job,
                 character = EXCLUDED.character,
                 cast_order = EXCLUDED.cast_order,
                 episode_count = EXCLUDED.episode_count,
                 adult = EXCLUDED.adult,
                 source_updated_at = EXCLUDED.source_updated_at,
                 updated_at = clock_timestamp()",
        )
        .bind(title_id)
        .bind(person_id)
        .bind(credit_id)
        .bind(credit_type)
        .bind(credit.department.as_deref())
        .bind(credit.job.as_deref())
        .bind(credit.character.as_deref())
        .bind(if credit_type == "cast" {
            credit.order
        } else {
            None
        })
        .bind(credit.total_episode_count)
        .bind(credit.adult)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_season_summaries(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    seasons: &[TmdbSeasonSummary],
) -> Result<(), JobExecutionError> {
    let mut seasons = seasons.iter().collect::<Vec<_>>();
    seasons.sort_unstable_by_key(|season| season.id);
    for season in seasons {
        let season_id = source_id(season.id)?;
        let season_number = i32::try_from(season.season_number)
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        let air_date = parse_source_date(season.air_date.as_deref())?;
        sqlx::query(
            "INSERT INTO catalog.seasons (
                 id, title_id, media_type, season_number, name, overview, air_date,
                 episode_count, poster_path, source_updated_at
             ) VALUES ($1, $2, 'tv', $3, $4, $5, $6, $7, $8, clock_timestamp())
             ON CONFLICT (id) DO UPDATE SET
                 title_id = EXCLUDED.title_id,
                 season_number = EXCLUDED.season_number,
                 name = EXCLUDED.name,
                 overview = EXCLUDED.overview,
                 air_date = EXCLUDED.air_date,
                 episode_count = EXCLUDED.episode_count,
                 poster_path = EXCLUDED.poster_path,
                 source_updated_at = EXCLUDED.source_updated_at,
                 updated_at = clock_timestamp()",
        )
        .bind(season_id)
        .bind(title_id)
        .bind(season_number)
        .bind(season.name.as_deref())
        .bind(season.overview.as_deref())
        .bind(air_date)
        .bind(season.episode_count.map(i32::from))
        .bind(season.poster_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn enqueue_credit_images(
    transaction: &mut Transaction<'_, Postgres>,
    credits: &TmdbCredits,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    for (_, _, credit) in ordered_credits(credits) {
        enqueue_gallery_images(
            transaction,
            "person",
            source_id(credit.id)?,
            "profile",
            credit.profile_path.as_deref(),
            &credit.images.profiles,
            allow_local_media,
        )
        .await?;
    }
    Ok(())
}

async fn enqueue_season_summary_jobs(
    transaction: &mut Transaction<'_, Postgres>,
    tv_id: i64,
    seasons: &[TmdbSeasonSummary],
    options: CatalogWriteOptions,
) -> Result<(), JobExecutionError> {
    let mut seasons = seasons.iter().collect::<Vec<_>>();
    seasons.sort_unstable_by_key(|season| season.id);
    for season in seasons {
        enqueue_image_job_with_position(
            transaction,
            "season",
            source_id(season.id)?,
            "poster",
            season.poster_path.as_deref(),
            Some(season.season_number),
            None,
            Some(tv_id),
            options.enqueue_media,
        )
        .await?;
        if options.enqueue_seasons {
            enqueue_season_refresh(transaction, tv_id, season.season_number).await?;
        }
    }
    Ok(())
}

async fn enqueue_season_refresh(
    transaction: &mut Transaction<'_, Postgres>,
    tv_id: i64,
    season_number: u32,
) -> Result<(), JobExecutionError> {
    if !season_refresh_queue_has_capacity(transaction).await? {
        tracing::debug!(
            event = "season_refresh_queue_capacity_deferred",
            tv_id,
            season_number,
            max_active_jobs = super::MAX_PENDING_REFRESH_JOBS,
            "deferring season refresh until a later explicit scan"
        );
        return Ok(());
    }
    let payload = serde_json::json!({
        "tv_id": tv_id,
        "season_number": season_number,
    });
    let dedup_key = format!("{}:{tv_id}:{season_number}", super::REFRESH_SEASON_JOB);
    sqlx::query(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(Uuid::now_v7())
    .bind(super::REFRESH_SEASON_JOB)
    .bind(super::INGEST_PAYLOAD_VERSION)
    .bind(payload.to_string())
    .bind(0_i16)
    .bind(8_i32)
    .bind(Option::<chrono::DateTime<Utc>>::None)
    .bind(dedup_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn enqueue_title_enrichment(
    transaction: &mut Transaction<'_, Postgres>,
    job_type: &str,
    tmdb_id: i64,
) -> Result<(), JobExecutionError> {
    let payload = serde_json::json!({"tmdb_id": tmdb_id});
    let dedup_key = format!("{job_type}:{tmdb_id}");
    sqlx::query(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(Uuid::now_v7())
    .bind(job_type)
    .bind(super::INGEST_PAYLOAD_VERSION)
    .bind(payload.to_string())
    .bind(super::ENRICHMENT_PRIORITY)
    .bind(8_i32)
    .bind(Option::<chrono::DateTime<Utc>>::None)
    .bind(dedup_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn season_refresh_queue_has_capacity(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<bool, JobExecutionError> {
    sqlx::query(
        "SELECT pg_catalog.pg_advisory_xact_lock(
             pg_catalog.hashtextextended('queue:capacity', 0)
         )",
    )
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;

    let active_jobs: i64 = sqlx::query_scalar(
        "SELECT pg_catalog.count(*)::bigint
           FROM ops.jobs
          WHERE job_type = $1
            AND status IN ('queued', 'running', 'retry_wait')",
    )
    .bind(super::REFRESH_SEASON_JOB)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;

    Ok(active_jobs < super::MAX_PENDING_REFRESH_JOBS)
}

async fn upsert_person(
    transaction: &mut Transaction<'_, Postgres>,
    person_id: i64,
    credit: &TmdbCredit,
) -> Result<(), JobExecutionError> {
    let original_name = non_empty_text(credit.original_name.as_deref());
    let name = non_empty_text(credit.name.as_deref()).or(original_name);
    sqlx::query(
        "WITH updated AS (
             UPDATE catalog.people
                SET name = COALESCE($2, name),
                    original_name = COALESCE($3, original_name),
                    known_for_department = COALESCE($4, known_for_department),
                    profile_path = COALESCE($5, profile_path),
                    adult = $6,
                    source_updated_at = clock_timestamp(),
                    updated_at = clock_timestamp()
              WHERE id = $1
                AND (($2 IS NOT NULL AND name IS DISTINCT FROM $2)
                  OR ($3 IS NOT NULL AND original_name IS DISTINCT FROM $3)
                  OR ($4 IS NOT NULL AND known_for_department IS DISTINCT FROM $4)
                  OR ($5 IS NOT NULL AND profile_path IS DISTINCT FROM $5)
                  OR adult IS DISTINCT FROM $6)
             RETURNING id
         )
         INSERT INTO catalog.people (
             id, name, original_name, known_for_department, profile_path,
             adult, source_updated_at
         )
         SELECT $1, $2, $3, $4, $5, $6, clock_timestamp()
          WHERE NOT EXISTS (SELECT 1 FROM catalog.people WHERE id = $1)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(person_id)
    .bind(name)
    .bind(original_name)
    .bind(credit.department.as_deref())
    .bind(credit.profile_path.as_deref())
    .bind(credit.adult)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

fn non_empty_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn stable_credit_id(credit: &TmdbCredit, credit_type: &str, position: usize) -> String {
    credit
        .credit_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(
            || format!("tmdb-{credit_type}-{}-{position}", credit.id),
            str::to_owned,
        )
}

fn ordered_credits(credits: &TmdbCredits) -> Vec<(&'static str, usize, &TmdbCredit)> {
    let mut rows = credits
        .cast
        .iter()
        .enumerate()
        .map(|(position, credit)| ("cast", position, credit))
        .chain(
            credits
                .crew
                .iter()
                .enumerate()
                .map(|(position, credit)| ("crew", position, credit)),
        )
        .collect::<Vec<_>>();
    rows.sort_unstable_by(|left, right| {
        left.2
            .id
            .cmp(&right.2.id)
            .then_with(|| left.0.cmp(right.0))
            .then_with(|| left.1.cmp(&right.1))
    });
    rows
}

async fn persist_episode(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    season_id: i64,
    episode: &TmdbEpisode,
) -> Result<(), JobExecutionError> {
    let episode_id = source_id(episode.id)?;
    let air_date = parse_source_date(episode.air_date.as_deref())?;
    let runtime = episode.runtime.map(i32::from);
    let vote_count = episode
        .vote_count
        .map(|value| {
            i64::try_from(value).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .transpose()?;
    sqlx::query(
        "INSERT INTO catalog.episodes (
             id, season_id, title_id, episode_number, name, overview, air_date,
             runtime_minutes, still_path, vote_average, vote_count, source_updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, clock_timestamp())
         ON CONFLICT (id) DO UPDATE SET
             season_id = EXCLUDED.season_id,
             title_id = EXCLUDED.title_id,
             episode_number = EXCLUDED.episode_number,
             name = EXCLUDED.name,
             overview = EXCLUDED.overview,
             air_date = EXCLUDED.air_date,
             runtime_minutes = EXCLUDED.runtime_minutes,
             still_path = EXCLUDED.still_path,
             vote_average = EXCLUDED.vote_average,
             vote_count = EXCLUDED.vote_count,
             source_updated_at = EXCLUDED.source_updated_at,
             updated_at = clock_timestamp()",
    )
    .bind(episode_id)
    .bind(season_id)
    .bind(title_id)
    .bind(i32::from(episode.episode_number))
    .bind(episode.name.as_deref())
    .bind(episode.overview.as_deref())
    .bind(air_date)
    .bind(runtime)
    .bind(episode.still_path.as_deref())
    .bind(episode.vote_average)
    .bind(vote_count)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    replace_episode_credits(transaction, episode_id, title_id, &episode.credits).await
}

async fn replace_episode_credits(
    transaction: &mut Transaction<'_, Postgres>,
    episode_id: i64,
    title_id: i64,
    credits: &TmdbCredits,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.episode_credits WHERE episode_id = $1 AND title_id = $2")
        .bind(episode_id)
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for (credit_type, position, credit) in ordered_credits(credits) {
        let person_id = source_id(credit.id)?;
        let credit_id = stable_credit_id(credit, credit_type, position);
        upsert_person(transaction, person_id, credit).await?;
        sqlx::query(
            "INSERT INTO catalog.episode_credits (
                 episode_id, title_id, person_id, credit_id, credit_type,
                 department, job, character, cast_order, source_updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp())
             ON CONFLICT (episode_id, person_id, credit_id) DO UPDATE SET
                 credit_type = EXCLUDED.credit_type,
                 department = EXCLUDED.department,
                 job = EXCLUDED.job,
                 character = EXCLUDED.character,
                 cast_order = EXCLUDED.cast_order,
                 source_updated_at = EXCLUDED.source_updated_at",
        )
        .bind(episode_id)
        .bind(title_id)
        .bind(person_id)
        .bind(credit_id)
        .bind(credit_type)
        .bind(credit.department.as_deref())
        .bind(credit.job.as_deref())
        .bind(credit.character.as_deref())
        .bind(if credit_type == "cast" {
            credit.order
        } else {
            None
        })
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_companies(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    companies: &[TmdbCompany],
    role: &str,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_companies WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let mut companies = companies.iter().collect::<Vec<_>>();
    companies.sort_unstable_by_key(|company| company.id);
    for company in companies {
        let company_id = source_id(company.id)?;
        sqlx::query(
            "WITH updated AS (
                 UPDATE catalog.companies
                    SET name = COALESCE($2, name),
                        origin_country = COALESCE($3, origin_country),
                        logo_path = COALESCE($4, logo_path)
                  WHERE id = $1
                    AND (($2 IS NOT NULL AND name IS DISTINCT FROM $2)
                      OR ($3 IS NOT NULL AND origin_country IS DISTINCT FROM $3)
                      OR ($4 IS NOT NULL AND logo_path IS DISTINCT FROM $4))
                 RETURNING id
             )
             INSERT INTO catalog.companies (id, name, origin_country, logo_path)
             SELECT $1, $2, $3, $4
              WHERE NOT EXISTS (SELECT 1 FROM catalog.companies WHERE id = $1)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(company_id)
        .bind(company.name.as_deref())
        .bind(company.origin_country.as_deref())
        .bind(company.logo_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO catalog.title_companies (title_id, company_id, company_role)
             VALUES ($1, $2, $3)
             ON CONFLICT (title_id, company_id) DO UPDATE SET company_role = EXCLUDED.company_role",
        )
        .bind(title_id)
        .bind(company_id)
        .bind(role)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_networks(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    networks: &[TmdbNetwork],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_networks WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let mut networks = networks.iter().collect::<Vec<_>>();
    networks.sort_unstable_by_key(|network| network.id);
    for network in networks {
        let network_id = source_id(network.id)?;
        sqlx::query(
            "WITH updated AS (
                 UPDATE catalog.networks
                    SET name = COALESCE($2, name),
                        origin_country = COALESCE($3, origin_country),
                        logo_path = COALESCE($4, logo_path)
                  WHERE id = $1
                    AND (($2 IS NOT NULL AND name IS DISTINCT FROM $2)
                      OR ($3 IS NOT NULL AND origin_country IS DISTINCT FROM $3)
                      OR ($4 IS NOT NULL AND logo_path IS DISTINCT FROM $4))
                 RETURNING id
             )
             INSERT INTO catalog.networks (id, name, origin_country, logo_path)
             SELECT $1, $2, $3, $4
              WHERE NOT EXISTS (
                    SELECT 1 FROM catalog.networks WHERE id = $1
              )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(network_id)
        .bind(network.name.as_deref())
        .bind(network.origin_country.as_deref())
        .bind(network.logo_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO catalog.title_networks (title_id, network_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(title_id)
        .bind(network_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_original_language(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    language: Option<&str>,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_languages WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let Some(language) = language.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let language = normalize_language(language)?;
    sqlx::query(
        "INSERT INTO catalog.languages (iso_639_1) VALUES ($1)
         ON CONFLICT (iso_639_1) DO NOTHING",
    )
    .bind(&language)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO catalog.title_languages (title_id, language_id, is_original)
         VALUES ($1, $2, true)
         ON CONFLICT (title_id, language_id) DO UPDATE SET is_original = true",
    )
    .bind(title_id)
    .bind(language)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn replace_collection(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    collection: Option<&TmdbCollection>,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_collections WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let Some(collection) = collection else {
        return Ok(());
    };
    let collection_id = source_id(collection.id)?;
    sqlx::query(
        "WITH updated AS (
             UPDATE catalog.collections
                SET name = COALESCE($2, name),
                    poster_path = COALESCE($3, poster_path),
                    backdrop_path = COALESCE($4, backdrop_path)
              WHERE id = $1
                AND (($2 IS NOT NULL AND name IS DISTINCT FROM $2)
                  OR ($3 IS NOT NULL AND poster_path IS DISTINCT FROM $3)
                  OR ($4 IS NOT NULL AND backdrop_path IS DISTINCT FROM $4))
             RETURNING id
         )
         INSERT INTO catalog.collections (id, name, poster_path, backdrop_path)
         SELECT $1, $2, $3, $4
          WHERE NOT EXISTS (SELECT 1 FROM catalog.collections WHERE id = $1)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(collection_id)
    .bind(collection.name.as_deref())
    .bind(collection.poster_path.as_deref())
    .bind(collection.backdrop_path.as_deref())
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO catalog.title_collections (title_id, collection_id)
         VALUES ($1, $2)
         ON CONFLICT (title_id) DO UPDATE SET collection_id = EXCLUDED.collection_id",
    )
    .bind(title_id)
    .bind(collection_id)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn enqueue_company_images(
    transaction: &mut Transaction<'_, Postgres>,
    companies: &[TmdbCompany],
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    let mut companies = companies.iter().collect::<Vec<_>>();
    companies.sort_unstable_by_key(|company| company.id);
    for company in companies {
        enqueue_gallery_images(
            transaction,
            "company",
            source_id(company.id)?,
            "logo",
            company.logo_path.as_deref(),
            &company.images.logos,
            allow_local_media,
        )
        .await?;
    }
    Ok(())
}

async fn enqueue_network_images(
    transaction: &mut Transaction<'_, Postgres>,
    networks: &[TmdbNetwork],
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    let mut networks = networks.iter().collect::<Vec<_>>();
    networks.sort_unstable_by_key(|network| network.id);
    for network in networks {
        enqueue_gallery_images(
            transaction,
            "network",
            source_id(network.id)?,
            "logo",
            network.logo_path.as_deref(),
            &network.images.logos,
            allow_local_media,
        )
        .await?;
    }
    Ok(())
}

async fn enqueue_collection_images(
    transaction: &mut Transaction<'_, Postgres>,
    collection: Option<&TmdbCollection>,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    let Some(collection) = collection else {
        return Ok(());
    };
    let collection_id = source_id(collection.id)?;
    enqueue_gallery_images(
        transaction,
        "collection",
        collection_id,
        "poster",
        collection.poster_path.as_deref(),
        &collection.images.posters,
        allow_local_media,
    )
    .await?;
    enqueue_gallery_images(
        transaction,
        "collection",
        collection_id,
        "backdrop",
        collection.backdrop_path.as_deref(),
        &collection.images.backdrops,
        allow_local_media,
    )
    .await
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared parity facets intentionally use one transaction so replacement cannot expose a partial title"
)]
async fn replace_common_parity_facets(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    translations: &[TmdbTranslation],
    alternate_titles: &[TmdbAlternateTitle],
    external_ids: &TmdbExternalIds,
    videos: &[TmdbVideo],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_translations WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for translation in translations {
        let Some(language_code) = normalized_language(translation.iso_639_1.as_deref()) else {
            continue;
        };
        let country_code =
            normalized_country(translation.iso_3166_1.as_deref()).unwrap_or_default();
        let name = bounded_text(
            translation
                .data
                .title
                .as_deref()
                .or(translation.data.name.as_deref()),
            2_048,
        );
        let overview = bounded_text(translation.data.overview.as_deref(), 32_768);
        let tagline = bounded_text(translation.data.tagline.as_deref(), 2_048);
        let homepage = bounded_text(translation.data.homepage.as_deref(), 2_048);
        sqlx::query(
            "INSERT INTO catalog.title_translations (
                 title_id, language_code, country_code, name, overview, tagline, homepage
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (title_id, language_code, country_code) DO UPDATE SET
                 name = EXCLUDED.name,
                 overview = EXCLUDED.overview,
                 tagline = EXCLUDED.tagline,
                 homepage = EXCLUDED.homepage,
                 updated_at = clock_timestamp()",
        )
        .bind(title_id)
        .bind(language_code)
        .bind(country_code)
        .bind(name)
        .bind(overview)
        .bind(tagline)
        .bind(homepage)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }

    sqlx::query("DELETE FROM catalog.title_alternate_titles WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for alternate in alternate_titles {
        let Some(title) = bounded_text(alternate.title.as_deref(), 2_048) else {
            continue;
        };
        let country_code = normalized_country(alternate.iso_3166_1.as_deref()).unwrap_or_default();
        let title_type = bounded_text(alternate.title_type.as_deref(), 128).unwrap_or_default();
        sqlx::query(
            "INSERT INTO catalog.title_alternate_titles (title_id, title, country_code, title_type)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (title_id, title, country_code, title_type) DO UPDATE
             SET updated_at = clock_timestamp()",
        )
        .bind(title_id)
        .bind(title)
        .bind(country_code)
        .bind(title_type)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }

    sqlx::query(
        "INSERT INTO catalog.title_external_ids (
             title_id, imdb_id, tvdb_id, wikidata_id, facebook_id, instagram_id, twitter_id
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (title_id) DO UPDATE SET
             imdb_id = EXCLUDED.imdb_id,
             tvdb_id = EXCLUDED.tvdb_id,
             wikidata_id = EXCLUDED.wikidata_id,
             facebook_id = EXCLUDED.facebook_id,
             instagram_id = EXCLUDED.instagram_id,
             twitter_id = EXCLUDED.twitter_id,
             updated_at = clock_timestamp()",
    )
    .bind(title_id)
    .bind(bounded_text(external_ids.imdb_id.as_deref(), 128))
    .bind(bounded_text(external_ids.tvdb_id.as_deref(), 128))
    .bind(bounded_text(external_ids.wikidata_id.as_deref(), 128))
    .bind(bounded_text(external_ids.facebook_id.as_deref(), 128))
    .bind(bounded_text(external_ids.instagram_id.as_deref(), 128))
    .bind(bounded_text(external_ids.twitter_id.as_deref(), 128))
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;

    sqlx::query("DELETE FROM catalog.title_videos WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    let mut seen_videos = BTreeSet::new();
    for video in videos {
        let (Some(video_key), Some(site)) = (
            bounded_text(video.key.as_deref(), 128),
            bounded_text(video.site.as_deref(), 64),
        ) else {
            continue;
        };
        if !seen_videos.insert((site.clone(), video_key.clone())) {
            continue;
        }
        sqlx::query(
            "INSERT INTO catalog.title_videos (
                 title_id, video_key, site, video_type, name, official,
                 language_code, country_code, published_at, size
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(title_id)
        .bind(video_key)
        .bind(site)
        .bind(bounded_text(video.video_type.as_deref(), 128))
        .bind(bounded_text(video.name.as_deref(), 2_048))
        .bind(video.official)
        .bind(normalized_language(video.iso_639_1.as_deref()))
        .bind(normalized_country(video.iso_3166_1.as_deref()))
        .bind(parse_source_timestamp(video.published_at.as_deref()))
        .bind(video.size.map(i32::from))
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

async fn replace_movie_release_dates(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    countries: &[TmdbReleaseDateCountry],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_release_dates WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for country in countries {
        let Some(country_code) = normalized_country(country.iso_3166_1.as_deref()) else {
            continue;
        };
        for release in &country.release_dates {
            let release_type = release.release_type.map(i16::from);
            if release_type.is_some_and(|value| !(1..=16).contains(&value)) {
                continue;
            }
            sqlx::query(
                "INSERT INTO catalog.title_release_dates (
                     title_id, country_code, release_date, certification, release_type, note
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT ON CONSTRAINT title_release_dates_identity_unique DO UPDATE
                 SET updated_at = clock_timestamp()",
            )
            .bind(title_id)
            .bind(&country_code)
            .bind(parse_source_timestamp(release.release_date.as_deref()))
            .bind(bounded_text(release.certification.as_deref(), 64))
            .bind(release_type)
            .bind(bounded_text(release.note.as_deref(), 2_048))
            .execute(&mut **transaction)
            .await
            .map_err(database_error)?;
        }
    }
    Ok(())
}

async fn replace_tv_certifications(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    ratings: &[TmdbContentRating],
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_release_dates WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for rating in ratings {
        let Some(country_code) = normalized_country(rating.iso_3166_1.as_deref()) else {
            continue;
        };
        sqlx::query(
            "INSERT INTO catalog.title_release_dates (title_id, country_code, certification)
             VALUES ($1, $2, $3)
             ON CONFLICT ON CONSTRAINT title_release_dates_identity_unique DO UPDATE
             SET updated_at = clock_timestamp()",
        )
        .bind(title_id)
        .bind(country_code)
        .bind(bounded_text(rating.rating.as_deref(), 64))
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

fn bounded_text(value: Option<&str>, maximum: usize) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()
            && value.chars().count() <= maximum
            && !value.chars().any(char::is_control))
        .then(|| value.to_owned())
    })
}

fn normalized_language(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    ((2..=3).contains(&value.len())
        && value.is_ascii()
        && value.bytes().all(|byte| byte.is_ascii_alphabetic()))
    .then(|| value.to_ascii_lowercase())
}

fn normalized_country(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (value.len() == 2 && value.is_ascii() && value.bytes().all(|byte| byte.is_ascii_alphabetic()))
        .then(|| value.to_ascii_uppercase())
}

fn parse_source_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value.trim()).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn database_error(_: sqlx::Error) -> JobExecutionError {
    JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
}

/// Enqueues the current gallery for an existing reusable catalog entity.
/// The entity row supplies the detail endpoint's primary path; the dedicated
/// gallery response supplies the remaining paths.
#[allow(
    clippy::too_many_lines,
    reason = "the four reusable entity kinds share one transaction and one gallery ordering path"
)]
pub(crate) async fn enqueue_reusable_gallery(
    pool: &PgPool,
    entity_type: &str,
    entity_id: i64,
    images: &TmdbImages,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    if !allow_local_media {
        return Ok(());
    }
    if entity_id <= 0 {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    let primary_paths: Option<(Option<String>, Option<String>)> = match entity_type {
        "person" => sqlx::query_as(
            "SELECT profile_path, NULL::text
               FROM catalog.people
              WHERE id = $1",
        )
        .bind(entity_id)
        .fetch_optional(pool)
        .await
        .map_err(database_error)?,
        "company" => sqlx::query_as(
            "SELECT logo_path, NULL::text
               FROM catalog.companies
              WHERE id = $1",
        )
        .bind(entity_id)
        .fetch_optional(pool)
        .await
        .map_err(database_error)?,
        "network" => sqlx::query_as(
            "SELECT logo_path, NULL::text
               FROM catalog.networks
              WHERE id = $1",
        )
        .bind(entity_id)
        .fetch_optional(pool)
        .await
        .map_err(database_error)?,
        "collection" => sqlx::query_as(
            "SELECT poster_path, backdrop_path
               FROM catalog.collections
              WHERE id = $1",
        )
        .bind(entity_id)
        .fetch_optional(pool)
        .await
        .map_err(database_error)?,
        _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
    };
    let Some((primary, secondary)) = primary_paths else {
        return Err(JobExecutionError::retry(
            "entity_not_ready",
            Duration::from_secs(5),
        ));
    };

    let mut transaction = pool.begin().await.map_err(database_error)?;
    match entity_type {
        "person" => {
            enqueue_gallery_images(
                &mut transaction,
                entity_type,
                entity_id,
                "profile",
                primary.as_deref(),
                &images.profiles,
                allow_local_media,
            )
            .await?;
        }
        "company" | "network" => {
            enqueue_gallery_images(
                &mut transaction,
                entity_type,
                entity_id,
                "logo",
                primary.as_deref(),
                &images.logos,
                allow_local_media,
            )
            .await?;
        }
        "collection" => {
            enqueue_gallery_images(
                &mut transaction,
                entity_type,
                entity_id,
                "poster",
                primary.as_deref(),
                &images.posters,
                allow_local_media,
            )
            .await?;
            enqueue_gallery_images(
                &mut transaction,
                entity_type,
                entity_id,
                "backdrop",
                secondary.as_deref(),
                &images.backdrops,
                allow_local_media,
            )
            .await?;
        }
        _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
    }
    transaction.commit().await.map_err(database_error)
}

#[allow(
    clippy::too_many_arguments,
    reason = "title gallery metadata is passed explicitly to preserve the shared enqueue contract"
)]
async fn enqueue_title_images(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    poster_path: Option<&str>,
    backdrop_path: Option<&str>,
    images: &TmdbImages,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    enqueue_gallery_images(
        transaction,
        entity_type,
        entity_id,
        "poster",
        poster_path,
        &images.posters,
        allow_local_media,
    )
    .await?;
    enqueue_gallery_images(
        transaction,
        entity_type,
        entity_id,
        "backdrop",
        backdrop_path,
        &images.backdrops,
        allow_local_media,
    )
    .await?;
    enqueue_gallery_images(
        transaction,
        entity_type,
        entity_id,
        "logo",
        None,
        &images.logos,
        allow_local_media,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "gallery ownership and naming inputs are explicit at the database boundary"
)]
async fn enqueue_gallery_images(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    primary_path: Option<&str>,
    images: &[TmdbImage],
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    enqueue_gallery_images_with_position(
        transaction,
        entity_type,
        entity_id,
        kind,
        primary_path,
        images,
        None,
        None,
        None,
        allow_local_media,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the low-level image job payload maps one-to-one to the normalized gallery columns"
)]
async fn enqueue_gallery_images_with_position(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    primary_path: Option<&str>,
    images: &[TmdbImage],
    season_number: Option<u32>,
    episode_number: Option<u16>,
    title_tmdb_id: Option<i64>,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    if !allow_local_media {
        return Ok(());
    }
    let paths = ordered_gallery_paths(primary_path, images);
    if paths.is_empty() {
        return Ok(());
    }
    if !image_queue_has_capacity(transaction, paths.len()).await? {
        tracing::debug!(
            event = "image_queue_capacity_deferred",
            entity_type,
            entity_id,
            image_kind = kind,
            candidate_count = paths.len(),
            max_active_jobs = MAX_ACTIVE_IMAGE_JOBS,
            "deferring image jobs until a later media scan"
        );
        return Ok(());
    }
    for (offset, path) in paths.into_iter().enumerate() {
        let asset_index = u16::try_from(offset + 1)
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        enqueue_image_job_with_position_and_index(
            transaction,
            entity_type,
            entity_id,
            kind,
            Some(path),
            season_number,
            episode_number,
            title_tmdb_id,
            asset_index,
            allow_local_media,
        )
        .await?;
    }
    Ok(())
}

async fn image_queue_has_capacity(
    transaction: &mut Transaction<'_, Postgres>,
    candidate_count: usize,
) -> Result<bool, JobExecutionError> {
    sqlx::query(
        "SELECT pg_catalog.pg_advisory_xact_lock(
             pg_catalog.hashtextextended('queue:capacity', 0)
         )",
    )
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;

    let active_jobs: i64 = sqlx::query_scalar(
        "SELECT pg_catalog.count(*)::bigint
           FROM ops.jobs
          WHERE job_type = $1
            AND status IN ('queued', 'running', 'retry_wait')",
    )
    .bind(IMAGE_JOB_TYPE)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;

    Ok(
        active_jobs.saturating_add(i64::try_from(candidate_count).unwrap_or(i64::MAX))
            <= MAX_ACTIVE_IMAGE_JOBS,
    )
}

fn ordered_gallery_paths<'a>(
    primary_path: Option<&'a str>,
    images: &'a [TmdbImage],
) -> Vec<&'a str> {
    let primary_path = primary_path.filter(|path| valid_image_path(path));
    let mut remaining = images
        .iter()
        .map(|image| image.file_path.as_str())
        .filter(|path| valid_image_path(path) && Some(*path) != primary_path)
        .collect::<Vec<_>>();
    remaining.sort_unstable();
    remaining.dedup();
    remaining.truncate(if primary_path.is_some() { 98 } else { 99 });
    let mut paths = Vec::with_capacity(1 + remaining.len());
    if let Some(primary_path) = primary_path {
        paths.push(primary_path);
    }
    paths.extend(remaining);
    paths
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_image_job_with_position(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    tmdb_path: Option<&str>,
    season_number: Option<u32>,
    episode_number: Option<u16>,
    title_tmdb_id: Option<i64>,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    if !allow_local_media || !tmdb_path.is_some_and(valid_image_path) {
        return Ok(());
    }
    if !image_queue_has_capacity(transaction, 1).await? {
        tracing::debug!(
            event = "image_queue_capacity_deferred",
            entity_type,
            entity_id,
            image_kind = kind,
            candidate_count = 1,
            max_active_jobs = MAX_ACTIVE_IMAGE_JOBS,
            "deferring image job until a later media scan"
        );
        return Ok(());
    }
    enqueue_image_job_with_position_and_index(
        transaction,
        entity_type,
        entity_id,
        kind,
        tmdb_path,
        season_number,
        episode_number,
        title_tmdb_id,
        1,
        allow_local_media,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_image_job_with_position_and_index(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    tmdb_path: Option<&str>,
    season_number: Option<u32>,
    episode_number: Option<u16>,
    title_tmdb_id: Option<i64>,
    asset_index: u16,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    if !allow_local_media {
        return Ok(());
    }
    let Some(tmdb_path) = tmdb_path.filter(|path| valid_image_path(path)) else {
        return Ok(());
    };
    let source_path = if kind == "logo"
        && Path::new(tmdb_path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("svg"))
    {
        format!("{}png", tmdb_path.trim_end_matches("svg"))
    } else {
        tmdb_path.to_owned()
    };
    let source_url = format!("https://image.tmdb.org/t/p/original{source_path}");
    let payload = serde_json::json!({
        "schemaVersion": IMAGE_JOB_PAYLOAD_VERSION,
        "entityType": entity_type,
        "entityId": entity_id,
        "kind": kind,
        "tmdbPath": tmdb_path,
        "sourceUrl": source_url,
        "language": serde_json::Value::Null,
        "sourceRevision": serde_json::Value::Null,
        "seasonNumber": season_number,
        "episodeNumber": episode_number,
        "titleTmdbId": title_tmdb_id,
        "assetIndex": asset_index,
    });
    let payload = serde_json::to_string(&payload)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let dedup_key = format!(
        "image:{entity_type}:{entity_id}:{kind}:{asset_index}:{}",
        digest_hex(tmdb_path)
    );
    sqlx::query(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(Uuid::now_v7())
    .bind(IMAGE_JOB_TYPE)
    .bind(IMAGE_JOB_PAYLOAD_VERSION)
    .bind(payload)
    .bind(image_job_priority(entity_type, kind))
    .bind(8_i32)
    .bind(Option::<chrono::DateTime<Utc>>::None)
    .bind(dedup_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
}

fn valid_image_path(path: &str) -> bool {
    !path.is_empty()
        && path.chars().count() <= 512
        && path.starts_with('/')
        && !path.chars().any(char::is_control)
        && !path.contains('\\')
        && !path.split('/').any(|part| matches!(part, "." | ".."))
}

fn image_job_priority(entity_type: &str, kind: &str) -> i16 {
    match (entity_type, kind) {
        ("movie" | "tv", "poster" | "backdrop") => 100,
        ("season" | "episode", _) => 50,
        ("person" | "company" | "network" | "collection", _) => 25,
        _ => 0,
    }
}

fn digest_hex(value: &str) -> String {
    use std::fmt::Write;

    Sha256::digest(value.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::Value;
    use sqlx::PgPool;
    use tmdb_upstream::{
        TmdbCompany, TmdbCredit, TmdbCredits, TmdbEpisode, TmdbGenre, TmdbImage, TmdbMovie,
        TmdbSeason, TmdbSeasonSummary, TmdbTv,
    };
    use tokio::sync::Barrier;

    use super::*;

    fn as_sqlx_error(error: &JobExecutionError) -> sqlx::Error {
        sqlx::Error::Protocol(error.to_string())
    }

    #[test]
    fn primary_title_artwork_outranks_related_artwork() {
        let title_priority = image_job_priority("movie", "poster");
        assert_eq!(title_priority, image_job_priority("tv", "backdrop"));
        assert!(title_priority > image_job_priority("season", "poster"));
        assert!(title_priority > image_job_priority("episode", "still"));
        assert!(title_priority > image_job_priority("person", "profile"));
        assert!(title_priority > image_job_priority("network", "logo"));
        assert!(title_priority > image_job_priority("collection", "poster"));
    }

    #[test]
    fn gallery_paths_are_primary_first_unique_and_lexically_stable() {
        let images = [
            TmdbImage {
                file_path: "/z.jpg".to_owned(),
                ..TmdbImage::default()
            },
            TmdbImage {
                file_path: "/a.jpg".to_owned(),
                ..TmdbImage::default()
            },
            TmdbImage {
                file_path: "/z.jpg".to_owned(),
                ..TmdbImage::default()
            },
            TmdbImage {
                file_path: "/primary.jpg".to_owned(),
                ..TmdbImage::default()
            },
        ];
        assert_eq!(
            ordered_gallery_paths(Some("/primary.jpg"), &images),
            ["/primary.jpg", "/a.jpg", "/z.jpg"]
        );
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn movie_persistence_enqueues_idempotent_image_jobs(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 42,
            title: Some("Image queue fixture".to_owned()),
            poster_path: Some("/poster-fixture.jpg".to_owned()),
            backdrop_path: Some("/backdrop-fixture.jpg".to_owned()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::title_refresh(true))
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::title_refresh(true))
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let rows: Vec<(String, Value, String)> = sqlx::query_as(
            "SELECT job_type, payload, dedup_key
               FROM ops.jobs
              WHERE job_type = 'image.download'
              ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, IMAGE_JOB_TYPE);
        assert_eq!(rows[1].0, IMAGE_JOB_TYPE);
        assert_eq!(rows[0].1["entityType"], "movie");
        assert_eq!(rows[0].1["entityId"], 42);
        assert!(rows.iter().all(|(_, payload, _)| {
            payload["sourceUrl"]
                .as_str()
                .is_some_and(|url| url.starts_with("https://image.tmdb.org/t/p/original/"))
        }));
        assert!(rows.iter().any(|(_, _, dedup_key)| {
            dedup_key
                == &format!(
                    "image:movie:42:poster:1:{}",
                    digest_hex("/poster-fixture.jpg")
                )
        }));
        let enrichment_jobs: i64 = sqlx::query_scalar(
            "SELECT count(*)
               FROM ops.jobs
              WHERE job_type = 'ingest.enrich_movie'
                AND status IN ('queued', 'running', 'retry_wait')",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(enrichment_jobs, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_refresh_defers_image_fanout_when_queue_is_full(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO ops.jobs (id, job_type, payload_version, payload, status, dedup_key)
             SELECT gen_random_uuid(), 'image.download', 1, '{}'::jsonb, 'queued',
                    'capacity-fixture-' || series::text
               FROM generate_series(1, 10000) AS series",
        )
        .execute(&pool)
        .await?;

        let movie = TmdbMovie {
            id: 9001,
            title: Some("Queue capacity fixture".to_owned()),
            poster_path: Some("/capacity-poster.jpg".to_owned()),
            backdrop_path: Some("/capacity-backdrop.jpg".to_owned()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::title_refresh(true))
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let mut transaction = pool.begin().await?;
        enqueue_image_job_with_position(
            &mut transaction,
            "season",
            9002,
            "poster",
            Some("/capacity-season-poster.jpg"),
            Some(1),
            None,
            Some(9001),
            true,
        )
        .await
        .map_err(|error| as_sqlx_error(&error))?;
        transaction.commit().await?;

        let active_images: i64 = sqlx::query_scalar(
            "SELECT count(*)
               FROM ops.jobs
              WHERE job_type = 'image.download'
                AND status IN ('queued', 'running', 'retry_wait')",
        )
        .fetch_one(&pool)
        .await?;
        let catalog_title: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM catalog.titles
                  WHERE tmdb_id = 9001 AND media_type = 'movie'
             )",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(active_images, MAX_ACTIVE_IMAGE_JOBS);
        assert!(catalog_title);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_refresh_defers_season_fanout_when_queue_is_full(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO ops.jobs (id, job_type, payload_version, payload, status, dedup_key)
             SELECT gen_random_uuid(), 'ingest.refresh_season', 1,
                    jsonb_build_object('tv_id', series, 'season_number', 1),
                    'queued', 'season-capacity-fixture-' || series::text
               FROM generate_series(1, 1000) AS series",
        )
        .execute(&pool)
        .await?;

        let mut transaction = pool.begin().await?;
        enqueue_season_refresh(&mut transaction, 9001, 1)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        transaction.commit().await?;

        let active_seasons: i64 = sqlx::query_scalar(
            "SELECT count(*)
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_season'
                AND status IN ('queued', 'running', 'retry_wait')",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_seasons, 1_000);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn movie_and_tv_persistence_use_the_same_media_surface(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 44,
            title: Some("Movie fixture".to_owned()),
            ..TmdbMovie::default()
        };
        let tv = TmdbTv {
            id: 45,
            name: Some("TV fixture".to_owned()),
            ..TmdbTv::default()
        };

        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::title_refresh(false))
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        persist_tv_with_options(&pool, &tv, CatalogWriteOptions::title_refresh(false))
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let rows: Vec<(String, bool)> = sqlx::query_as(
            "SELECT media_type, active
               FROM catalog.titles
              WHERE tmdb_id IN (44, 45)
              ORDER BY tmdb_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows, [("movie".to_owned(), true), ("tv".to_owned(), true)]);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_only_title_persistence_does_not_enqueue_child_jobs(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 46,
            title: Some("Catalog-only movie".to_owned()),
            poster_path: Some("/movie.jpg".to_owned()),
            ..TmdbMovie::default()
        };
        let tv = TmdbTv {
            id: 47,
            name: Some("Catalog-only TV".to_owned()),
            poster_path: Some("/tv.jpg".to_owned()),
            seasons: vec![TmdbSeasonSummary {
                id: 48,
                season_number: 1,
                poster_path: Some("/season.jpg".to_owned()),
                ..TmdbSeasonSummary::default()
            }],
            ..TmdbTv::default()
        };

        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        persist_tv_with_options(&pool, &tv, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let child_jobs: i64 = sqlx::query_scalar(
            "SELECT count(*)
               FROM ops.jobs
              WHERE job_type IN (
                    'image.download', 'ingest.enrich_movie',
                    'ingest.enrich_tv', 'ingest.refresh_season'
              )",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(child_jobs, 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn disabled_local_media_does_not_create_download_jobs(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 43,
            title: Some("Remote image fixture".to_owned()),
            poster_path: Some("/poster-remote.jpg".to_owned()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::title_refresh(false))
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")
                .fetch_one(&pool)
                .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn concurrent_tv_and_season_writes_with_shared_resources_finish_without_lock_errors(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let series = concurrent_tv_fixture();
        persist_tv_with_options(&pool, &series, CatalogWriteOptions::title_refresh(false))
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let season = concurrent_season_fixture();
        let start = Arc::new(Barrier::new(9));
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0_usize..8 {
            let writer_pool = pool.clone();
            let writer_start = Arc::clone(&start);
            if writer.is_multiple_of(2) {
                let series = series.clone();
                writers.spawn(async move {
                    writer_start.wait().await;
                    persist_tv_with_options(
                        &writer_pool,
                        &series,
                        CatalogWriteOptions::title_refresh(false),
                    )
                    .await
                });
            } else {
                let season = season.clone();
                writers.spawn(async move {
                    writer_start.wait().await;
                    persist_season_with_options(
                        &writer_pool,
                        800_001,
                        &season,
                        CatalogWriteOptions::season_refresh(false),
                    )
                    .await
                });
            }
        }
        start.wait().await;

        tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(result) = writers.join_next().await {
                result
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
                    .map_err(|error| as_sqlx_error(&error))?;
            }
            Ok::<(), sqlx::Error>(())
        })
        .await
        .map_err(|_| sqlx::Error::Protocol("catalog writers exceeded 30 seconds".to_owned()))??;

        let persisted: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM catalog.titles
                   WHERE media_type = 'tv' AND tmdb_id = 800001),
                 (SELECT count(*) FROM catalog.seasons WHERE id = 800011),
                 (SELECT count(*) FROM catalog.episodes WHERE id = 800012)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(persisted, (1, 1, 1));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn concurrent_movies_with_reversed_shared_resource_order_finish_without_lock_errors(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        // This mirrors the production failure: separate titles begin their
        // catalog writes at the same time but touch the same genre and person
        // rows in opposite TMDB payload order. The resource prelock must make
        // that source order irrelevant without changing the persisted order.
        let forward = concurrent_movie_fixture(810_001, false);
        let reverse = concurrent_movie_fixture(810_002, true);
        let start = Arc::new(Barrier::new(9));
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0_usize..8 {
            let writer_pool = pool.clone();
            let writer_start = Arc::clone(&start);
            let movie = if writer.is_multiple_of(2) {
                forward.clone()
            } else {
                reverse.clone()
            };
            writers.spawn(async move {
                writer_start.wait().await;
                persist_movie_with_options(
                    &writer_pool,
                    &movie,
                    CatalogWriteOptions::title_refresh(false),
                )
                .await
            });
        }
        start.wait().await;

        tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(result) = writers.join_next().await {
                result
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
                    .map_err(|error| as_sqlx_error(&error))?;
            }
            Ok::<(), sqlx::Error>(())
        })
        .await
        .map_err(|_| sqlx::Error::Protocol("catalog writers exceeded 30 seconds".to_owned()))??;

        let persisted: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM catalog.titles
                   WHERE media_type = 'movie' AND tmdb_id IN (810001, 810002)),
                 (SELECT count(*) FROM catalog.genres WHERE id IN (810021, 810022)),
                 (SELECT count(*) FROM catalog.people WHERE id IN (810031, 810032))",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(persisted, (2, 2, 2));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn concurrent_catalog_and_media_fanout_use_one_lock_order(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let series = TmdbTv {
            id: 811_001,
            name: Some("Media lock-order TV fixture".to_owned()),
            seasons: vec![TmdbSeasonSummary {
                id: 811_011,
                season_number: 1,
                ..TmdbSeasonSummary::default()
            }],
            ..TmdbTv::default()
        };
        persist_tv_with_options(&pool, &series, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let shared_person = TmdbCredit {
            id: 811_021,
            name: Some("Shared media person".to_owned()),
            profile_path: Some("/shared-media-person.jpg".to_owned()),
            ..TmdbCredit::default()
        };
        let movie = TmdbMovie {
            id: 811_002,
            title: Some("Media lock-order movie fixture".to_owned()),
            poster_path: Some("/media-lock-order-movie.jpg".to_owned()),
            credits: TmdbCredits {
                cast: vec![shared_person.clone()],
                ..TmdbCredits::default()
            },
            production_companies: vec![TmdbCompany {
                id: 811_031,
                name: Some("Media lock-order company".to_owned()),
                logo_path: Some("/media-lock-order-company.png".to_owned()),
                ..TmdbCompany::default()
            }],
            ..TmdbMovie::default()
        };
        let season = TmdbSeason {
            id: 811_011,
            season_number: 1,
            poster_path: Some("/media-lock-order-season.jpg".to_owned()),
            episodes: vec![TmdbEpisode {
                id: 811_012,
                episode_number: 1,
                still_path: Some("/media-lock-order-episode.jpg".to_owned()),
                credits: TmdbCredits {
                    crew: vec![shared_person],
                    ..TmdbCredits::default()
                },
                ..TmdbEpisode::default()
            }],
            ..TmdbSeason::default()
        };

        let start = Arc::new(Barrier::new(17));
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0_usize..16 {
            let writer_pool = pool.clone();
            let writer_start = Arc::clone(&start);
            if writer.is_multiple_of(2) {
                let movie = movie.clone();
                writers.spawn(async move {
                    writer_start.wait().await;
                    persist_movie_with_options(
                        &writer_pool,
                        &movie,
                        CatalogWriteOptions::title_refresh(true),
                    )
                    .await
                });
            } else {
                let season = season.clone();
                writers.spawn(async move {
                    writer_start.wait().await;
                    persist_season_with_options(
                        &writer_pool,
                        811_001,
                        &season,
                        CatalogWriteOptions::season_refresh(true),
                    )
                    .await
                });
            }
        }
        start.wait().await;

        tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(result) = writers.join_next().await {
                result
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
                    .map_err(|error| as_sqlx_error(&error))?;
            }
            Ok::<(), sqlx::Error>(())
        })
        .await
        .map_err(|_| {
            sqlx::Error::Protocol("media fanout writers exceeded 30 seconds".to_owned())
        })??;

        let persisted: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM catalog.titles
                   WHERE tmdb_id IN (811001, 811002)),
                 (SELECT count(*) FROM catalog.episodes WHERE id = 811012),
                 (SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download')",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!((persisted.0, persisted.1), (2, 1));
        assert!(persisted.2 >= 5);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unchanged_shared_genre_is_not_rewritten(pool: PgPool) -> sqlx::Result<()> {
        let first = TmdbMovie {
            id: 820_001,
            title: Some("Genre seed fixture".to_owned()),
            genres: vec![TmdbGenre {
                id: 28,
                name: Some("Action".to_owned()),
            }],
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &first, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let before: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.genres WHERE id = 28")
                .fetch_one(&pool)
                .await?;
        let second = TmdbMovie {
            id: 820_002,
            title: Some("Independent title fixture".to_owned()),
            genres: first.genres.clone(),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &second, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let after: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.genres WHERE id = 28")
                .fetch_one(&pool)
                .await?;

        assert_eq!(after, before);
        let renamed = TmdbMovie {
            id: 820_003,
            title: Some("Genre rename fixture".to_owned()),
            genres: vec![TmdbGenre {
                id: 28,
                name: Some("Action Updated".to_owned()),
            }],
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &renamed, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let genre_name: String =
            sqlx::query_scalar("SELECT name FROM catalog.genres WHERE id = 28")
                .fetch_one(&pool)
                .await?;
        assert_eq!(genre_name, "Action Updated");
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unchanged_shared_keyword_is_not_rewritten(pool: PgPool) -> sqlx::Result<()> {
        let first = TmdbMovie {
            id: 825_001,
            title: Some("Keyword seed fixture".to_owned()),
            keywords: vec![TmdbKeyword {
                id: 42,
                name: Some("based on novel".to_owned()),
            }],
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &first, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let before: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.keywords WHERE id = 42")
                .fetch_one(&pool)
                .await?;
        let second = TmdbMovie {
            id: 825_002,
            title: Some("Independent keyword fixture".to_owned()),
            keywords: first.keywords.clone(),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &second, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let after: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.keywords WHERE id = 42")
                .fetch_one(&pool)
                .await?;

        assert_eq!(after, before);
        let renamed = TmdbMovie {
            id: 825_003,
            title: Some("Keyword rename fixture".to_owned()),
            keywords: vec![TmdbKeyword {
                id: 42,
                name: Some("novel adaptation".to_owned()),
            }],
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &renamed, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let keyword_name: String =
            sqlx::query_scalar("SELECT name FROM catalog.keywords WHERE id = 42")
                .fetch_one(&pool)
                .await?;
        assert_eq!(keyword_name, "novel adaptation");
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unchanged_shared_network_is_not_rewritten(pool: PgPool) -> sqlx::Result<()> {
        let first = TmdbTv {
            id: 830_001,
            name: Some("Network seed fixture".to_owned()),
            networks: vec![TmdbNetwork {
                id: 6,
                name: Some("NBC".to_owned()),
                origin_country: Some("US".to_owned()),
                logo_path: Some("/nbc.png".to_owned()),
                ..TmdbNetwork::default()
            }],
            ..TmdbTv::default()
        };
        persist_tv_with_options(&pool, &first, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let before: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.networks WHERE id = 6")
                .fetch_one(&pool)
                .await?;
        let second = TmdbTv {
            id: 830_002,
            name: Some("Independent network fixture".to_owned()),
            networks: first.networks.clone(),
            ..TmdbTv::default()
        };
        persist_tv_with_options(&pool, &second, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let after: String =
            sqlx::query_scalar("SELECT xmin::text FROM catalog.networks WHERE id = 6")
                .fetch_one(&pool)
                .await?;

        assert_eq!(after, before);
        let renamed = TmdbTv {
            id: 830_003,
            name: Some("Network rename fixture".to_owned()),
            networks: vec![TmdbNetwork {
                id: 6,
                name: Some("NBC Updated".to_owned()),
                origin_country: Some("US".to_owned()),
                logo_path: Some("/nbc.png".to_owned()),
                ..TmdbNetwork::default()
            }],
            ..TmdbTv::default()
        };
        persist_tv_with_options(&pool, &renamed, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let network_name: String =
            sqlx::query_scalar("SELECT name FROM catalog.networks WHERE id = 6")
                .fetch_one(&pool)
                .await?;
        assert_eq!(network_name, "NBC Updated");
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unchanged_shared_people_companies_and_collections_are_not_rewritten(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let shared_credit = TmdbCredit {
            id: 850_101,
            name: Some("Shared Person".to_owned()),
            original_name: Some("Shared Person".to_owned()),
            department: Some("Acting".to_owned()),
            profile_path: Some("/shared-person.jpg".to_owned()),
            ..TmdbCredit::default()
        };
        let shared_company = TmdbCompany {
            id: 850_102,
            name: Some("Shared Company".to_owned()),
            origin_country: Some("US".to_owned()),
            logo_path: Some("/shared-company.png".to_owned()),
            ..TmdbCompany::default()
        };
        let shared_collection = TmdbCollection {
            id: 850_103,
            name: Some("Shared Collection".to_owned()),
            poster_path: Some("/shared-collection.jpg".to_owned()),
            backdrop_path: Some("/shared-collection-backdrop.jpg".to_owned()),
            ..TmdbCollection::default()
        };
        let first = TmdbMovie {
            id: 850_001,
            title: Some("Shared resource seed".to_owned()),
            credits: TmdbCredits {
                cast: vec![shared_credit.clone()],
                ..TmdbCredits::default()
            },
            production_companies: vec![shared_company.clone()],
            belongs_to_collection: Some(shared_collection.clone()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &first, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let before: (String, String, String) = sqlx::query_as(
            "SELECT
                 (SELECT xmin::text FROM catalog.people WHERE id = 850101),
                 (SELECT xmin::text FROM catalog.companies WHERE id = 850102),
                 (SELECT xmin::text FROM catalog.collections WHERE id = 850103)",
        )
        .fetch_one(&pool)
        .await?;

        let second = TmdbMovie {
            id: 850_002,
            title: Some("Independent shared resource title".to_owned()),
            credits: first.credits.clone(),
            production_companies: first.production_companies.clone(),
            belongs_to_collection: first.belongs_to_collection.clone(),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &second, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let after: (String, String, String) = sqlx::query_as(
            "SELECT
                 (SELECT xmin::text FROM catalog.people WHERE id = 850101),
                 (SELECT xmin::text FROM catalog.companies WHERE id = 850102),
                 (SELECT xmin::text FROM catalog.collections WHERE id = 850103)",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(after, before);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unchanged_title_search_document_is_not_rewritten(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 850_201,
            title: Some("Stable search title".to_owned()),
            original_title: Some("Stable original title".to_owned()),
            overview: Some("Stable overview".to_owned()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let before: String = sqlx::query_scalar(
            "SELECT xmin::text FROM search.search_documents
              WHERE title_id = (SELECT id FROM catalog.titles
                                  WHERE media_type = 'movie' AND tmdb_id = 850201)
                AND locale = ''",
        )
        .fetch_one(&pool)
        .await?;

        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let after: String = sqlx::query_scalar(
            "SELECT xmin::text FROM search.search_documents
              WHERE title_id = (SELECT id FROM catalog.titles
                                  WHERE media_type = 'movie' AND tmdb_id = 850201)
                AND locale = ''",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(after, before);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn blank_credit_name_uses_the_original_person_name(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 840_001,
            title: Some("Blank credit name fixture".to_owned()),
            credits: TmdbCredits {
                crew: vec![TmdbCredit {
                    id: 4_153_033,
                    name: Some(" ".to_owned()),
                    original_name: Some("Murielle La Ferrière".to_owned()),
                    ..TmdbCredit::default()
                }],
                ..TmdbCredits::default()
            },
            ..TmdbMovie::default()
        };

        persist_movie_with_options(&pool, &movie, CatalogWriteOptions::CATALOG_ONLY)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        let persisted: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT name, original_name FROM catalog.people WHERE id = $1")
                .bind(4_153_033_i64)
                .fetch_one(&pool)
                .await?;

        assert_eq!(persisted.0.as_deref(), Some("Murielle La Ferrière"));
        assert_eq!(persisted.1.as_deref(), Some("Murielle La Ferrière"));
        Ok(())
    }

    fn concurrent_tv_fixture() -> TmdbTv {
        TmdbTv {
            id: 800_001,
            name: Some("Concurrent TV fixture".to_owned()),
            genres: vec![
                TmdbGenre {
                    id: 28,
                    name: Some("Action".to_owned()),
                },
                TmdbGenre {
                    id: 18,
                    name: Some("Drama".to_owned()),
                },
            ],
            credits: TmdbCredits {
                cast: vec![TmdbCredit {
                    id: 800_021,
                    name: Some("Shared person".to_owned()),
                    ..TmdbCredit::default()
                }],
                ..TmdbCredits::default()
            },
            seasons: vec![TmdbSeasonSummary {
                id: 800_011,
                season_number: 1,
                name: Some("Season one".to_owned()),
                ..TmdbSeasonSummary::default()
            }],
            ..TmdbTv::default()
        }
    }

    fn concurrent_movie_fixture(tmdb_id: u64, reversed: bool) -> TmdbMovie {
        let genres = [
            TmdbGenre {
                id: 810_021,
                name: Some("First shared genre".to_owned()),
            },
            TmdbGenre {
                id: 810_022,
                name: Some("Second shared genre".to_owned()),
            },
        ];
        let cast = [
            TmdbCredit {
                id: 810_031,
                name: Some("First shared person".to_owned()),
                ..TmdbCredit::default()
            },
            TmdbCredit {
                id: 810_032,
                name: Some("Second shared person".to_owned()),
                ..TmdbCredit::default()
            },
        ];
        let (genres, cast) = if reversed {
            (
                vec![genres[1].clone(), genres[0].clone()],
                vec![cast[1].clone(), cast[0].clone()],
            )
        } else {
            (genres.to_vec(), cast.to_vec())
        };
        TmdbMovie {
            id: tmdb_id,
            title: Some(format!("Concurrent movie fixture {tmdb_id}")),
            genres,
            credits: TmdbCredits {
                cast,
                ..TmdbCredits::default()
            },
            ..TmdbMovie::default()
        }
    }

    fn concurrent_season_fixture() -> TmdbSeason {
        TmdbSeason {
            id: 800_011,
            season_number: 1,
            episodes: vec![TmdbEpisode {
                id: 800_012,
                episode_number: 1,
                name: Some("Concurrent episode".to_owned()),
                credits: TmdbCredits {
                    crew: vec![TmdbCredit {
                        id: 800_021,
                        name: Some("Shared person".to_owned()),
                        ..TmdbCredit::default()
                    }],
                    ..TmdbCredits::default()
                },
                ..TmdbEpisode::default()
            }],
            ..TmdbSeason::default()
        }
    }
}
