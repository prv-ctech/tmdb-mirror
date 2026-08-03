use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use tmdb_domain::{MediaType, classify_anime};
use tmdb_jobs::JobExecutionError;
use tmdb_upstream::{
    ChangePage, TmdbAlternateTitle, TmdbCollection, TmdbCompany, TmdbContentRating, TmdbCredit,
    TmdbCredits, TmdbEpisode, TmdbExternalIds, TmdbGenre, TmdbKeyword, TmdbMovie, TmdbNetwork,
    TmdbReleaseDateCountry, TmdbSeason, TmdbSeasonSummary, TmdbTranslation, TmdbTv, TmdbVideo,
};
use uuid::Uuid;

const IMAGE_JOB_TYPE: &str = "image.download";
const IMAGE_JOB_PAYLOAD_VERSION: i32 = 1;

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
    allow_local_media: bool,
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
    let is_anime = classify_anime(
        movie.keywords.iter().map(|keyword| keyword.id),
        movie.genres.iter().map(|genre| genre.id),
    );
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
        is_anime,
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
    replace_credits(
        &mut transaction,
        title_id,
        &movie.credits,
        allow_local_media,
    )
    .await?;
    replace_companies(
        &mut transaction,
        title_id,
        &movie.production_companies,
        "production",
        allow_local_media,
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
        allow_local_media,
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
    enqueue_title_images(
        &mut transaction,
        "movie",
        tmdb_id,
        movie.poster_path.as_deref(),
        movie.backdrop_path.as_deref(),
        is_anime,
        allow_local_media,
    )
    .await?;
    transaction.commit().await.map_err(database_error)
}

/// Persists a TV title and optionally creates local-media jobs in the same
/// catalog transaction.
#[allow(clippy::too_many_lines)]
pub(crate) async fn persist_tv_with_options(
    pool: &PgPool,
    series: &TmdbTv,
    allow_local_media: bool,
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
    let is_anime = classify_anime(
        series.keywords.iter().map(|keyword| keyword.id),
        series.genres.iter().map(|genre| genre.id),
    );
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
        is_anime,
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
    replace_credits(
        &mut transaction,
        title_id,
        &series.credits,
        allow_local_media,
    )
    .await?;
    replace_season_summaries(
        &mut transaction,
        title_id,
        tmdb_id,
        series.seasons.as_slice(),
        is_anime,
        allow_local_media,
    )
    .await?;
    replace_companies(
        &mut transaction,
        title_id,
        &series.production_companies,
        "production",
        allow_local_media,
    )
    .await?;
    replace_networks(
        &mut transaction,
        title_id,
        &series.networks,
        allow_local_media,
    )
    .await?;
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
    enqueue_title_images(
        &mut transaction,
        "tv",
        tmdb_id,
        series.poster_path.as_deref(),
        series.backdrop_path.as_deref(),
        is_anime,
        allow_local_media,
    )
    .await?;
    transaction.commit().await.map_err(database_error)
}

/// Persists one TV season and its episodes, credits, and image jobs in one
/// transaction. The season job is intentionally separate from the TV detail
/// request because a series can contain hundreds of episodes.
pub(crate) async fn persist_season_with_options(
    pool: &PgPool,
    tv_id: u32,
    season: &TmdbSeason,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    let tv_id = source_id(u64::from(tv_id))?;
    let season_id = source_id(season.id)?;
    let season_number = i32::from(season.season_number);
    let air_date = parse_source_date(season.air_date.as_deref())?;
    let resources = season_write_resources(tv_id, season, season_id)?;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    prelock_catalog_write_resources(&mut transaction, resources).await?;
    let parent: Option<(i64, bool)> = sqlx::query_as(
        "SELECT id, is_anime FROM catalog.titles
         WHERE media_type = 'tv' AND tmdb_id = $1 AND active
         FOR UPDATE",
    )
    .bind(tv_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(database_error)?;
    let Some((title_id, anime)) = parent else {
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

    if season_number > 0 {
        enqueue_image_job_with_position(
            &mut transaction,
            "season",
            season_id,
            "still",
            season.poster_path.as_deref(),
            anime,
            Some(season.season_number),
            None,
            Some(tv_id),
            allow_local_media,
        )
        .await?;
    }
    for episode in &season.episodes {
        persist_episode(
            &mut transaction,
            title_id,
            season_id,
            tv_id,
            season.season_number,
            episode,
            anime,
            allow_local_media,
        )
        .await?;
    }
    transaction.commit().await.map_err(database_error)
}

/// Materializes changed IDs as active title identities for the refresh scheduler.
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
    is_anime: bool,
) -> Result<i64, JobExecutionError> {
    sqlx::query_scalar(
        "INSERT INTO catalog.titles (
             media_type, tmdb_id, display_title, original_title, overview,
             original_language, release_date, first_air_date, popularity,
             vote_average, vote_count, runtime_minutes, adult, video,
             active, source_updated_at, is_anime
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, true, $15, $16)
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
             is_anime = EXCLUDED.is_anime,
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
    .bind(is_anime)
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
    for genre in genres {
        let genre_id = source_id(genre.id)?;
        sqlx::query(
            "INSERT INTO catalog.genres (id, name) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, catalog.genres.name)",
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
    for keyword in keywords {
        let keyword_id = source_id(keyword.id)?;
        sqlx::query(
            "INSERT INTO catalog.keywords (id, name) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, catalog.keywords.name)",
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
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_credits WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for (credit_type, rows) in [
        ("cast", credits.cast.as_slice()),
        ("crew", credits.crew.as_slice()),
    ] {
        for (position, credit) in rows.iter().enumerate() {
            let person_id = source_id(credit.id)?;
            let credit_id = stable_credit_id(credit, credit_type, position);
            upsert_person(transaction, person_id, credit).await?;
            enqueue_image_job(
                transaction,
                "person",
                person_id,
                "profile",
                credit.profile_path.as_deref(),
                false,
                allow_local_media,
            )
            .await?;
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
    }
    Ok(())
}

async fn replace_season_summaries(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    tv_id: i64,
    seasons: &[TmdbSeasonSummary],
    anime: bool,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    for season in seasons {
        let season_id = source_id(season.id)?;
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
        .bind(i32::from(season.season_number))
        .bind(season.name.as_deref())
        .bind(season.overview.as_deref())
        .bind(air_date)
        .bind(season.episode_count.map(i32::from))
        .bind(season.poster_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        if season.season_number > 0 {
            enqueue_image_job_with_position(
                transaction,
                "season",
                season_id,
                "still",
                season.poster_path.as_deref(),
                anime,
                Some(season.season_number),
                None,
                Some(tv_id),
                allow_local_media,
            )
            .await?;
        }
        enqueue_season_refresh(transaction, tv_id, season.season_number).await?;
    }
    Ok(())
}

async fn enqueue_season_refresh(
    transaction: &mut Transaction<'_, Postgres>,
    tv_id: i64,
    season_number: u16,
) -> Result<(), JobExecutionError> {
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

async fn upsert_person(
    transaction: &mut Transaction<'_, Postgres>,
    person_id: i64,
    credit: &TmdbCredit,
) -> Result<(), JobExecutionError> {
    sqlx::query(
        "INSERT INTO catalog.people (
             id, name, original_name, known_for_department, profile_path,
             adult, source_updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
         ON CONFLICT (id) DO UPDATE SET
             name = COALESCE(EXCLUDED.name, catalog.people.name),
             original_name = COALESCE(EXCLUDED.original_name, catalog.people.original_name),
             known_for_department = COALESCE(EXCLUDED.known_for_department, catalog.people.known_for_department),
             profile_path = COALESCE(EXCLUDED.profile_path, catalog.people.profile_path),
             adult = EXCLUDED.adult,
             source_updated_at = EXCLUDED.source_updated_at,
             updated_at = clock_timestamp()",
    )
    .bind(person_id)
    .bind(credit.name.as_deref())
    .bind(credit.original_name.as_deref())
    .bind(credit.department.as_deref())
    .bind(credit.profile_path.as_deref())
    .bind(credit.adult)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(())
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

#[allow(clippy::too_many_arguments)]
async fn persist_episode(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    season_id: i64,
    tv_id: i64,
    season_number: u16,
    episode: &TmdbEpisode,
    anime: bool,
    allow_local_media: bool,
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
    enqueue_image_job_with_position(
        transaction,
        "episode",
        episode_id,
        "still",
        episode.still_path.as_deref(),
        anime,
        Some(season_number),
        Some(episode.episode_number),
        Some(tv_id),
        allow_local_media,
    )
    .await?;
    replace_episode_credits(
        transaction,
        episode_id,
        title_id,
        &episode.credits,
        allow_local_media,
    )
    .await
}

async fn replace_episode_credits(
    transaction: &mut Transaction<'_, Postgres>,
    episode_id: i64,
    title_id: i64,
    credits: &TmdbCredits,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.episode_credits WHERE episode_id = $1 AND title_id = $2")
        .bind(episode_id)
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for (credit_type, rows) in [
        ("cast", credits.cast.as_slice()),
        ("crew", credits.crew.as_slice()),
    ] {
        for (position, credit) in rows.iter().enumerate() {
            let person_id = source_id(credit.id)?;
            let credit_id = stable_credit_id(credit, credit_type, position);
            upsert_person(transaction, person_id, credit).await?;
            enqueue_image_job(
                transaction,
                "person",
                person_id,
                "profile",
                credit.profile_path.as_deref(),
                false,
                allow_local_media,
            )
            .await?;
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
    }
    Ok(())
}

async fn replace_companies(
    transaction: &mut Transaction<'_, Postgres>,
    title_id: i64,
    companies: &[TmdbCompany],
    role: &str,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_companies WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for company in companies {
        let company_id = source_id(company.id)?;
        sqlx::query(
            "INSERT INTO catalog.companies (id, name, origin_country, logo_path)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE SET
                 name = COALESCE(EXCLUDED.name, catalog.companies.name),
                 origin_country = COALESCE(EXCLUDED.origin_country, catalog.companies.origin_country),
                 logo_path = COALESCE(EXCLUDED.logo_path, catalog.companies.logo_path)",
        )
        .bind(company_id)
        .bind(company.name.as_deref())
        .bind(company.origin_country.as_deref())
        .bind(company.logo_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        enqueue_image_job(
            transaction,
            "company",
            company_id,
            "logo",
            company.logo_path.as_deref(),
            false,
            allow_local_media,
        )
        .await?;
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
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    sqlx::query("DELETE FROM catalog.title_networks WHERE title_id = $1")
        .bind(title_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    for network in networks {
        let network_id = source_id(network.id)?;
        sqlx::query(
            "INSERT INTO catalog.networks (id, name, origin_country, logo_path)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE SET
                 name = COALESCE(EXCLUDED.name, catalog.networks.name),
                 origin_country = COALESCE(EXCLUDED.origin_country, catalog.networks.origin_country),
                 logo_path = COALESCE(EXCLUDED.logo_path, catalog.networks.logo_path)",
        )
        .bind(network_id)
        .bind(network.name.as_deref())
        .bind(network.origin_country.as_deref())
        .bind(network.logo_path.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        enqueue_image_job(
            transaction,
            "network",
            network_id,
            "logo",
            network.logo_path.as_deref(),
            false,
            allow_local_media,
        )
        .await?;
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
    allow_local_media: bool,
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
        "INSERT INTO catalog.collections (id, name, poster_path, backdrop_path)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET
             name = COALESCE(EXCLUDED.name, catalog.collections.name),
             poster_path = COALESCE(EXCLUDED.poster_path, catalog.collections.poster_path),
             backdrop_path = COALESCE(EXCLUDED.backdrop_path, catalog.collections.backdrop_path)",
    )
    .bind(collection_id)
    .bind(collection.name.as_deref())
    .bind(collection.poster_path.as_deref())
    .bind(collection.backdrop_path.as_deref())
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    enqueue_image_job(
        transaction,
        "collection",
        collection_id,
        "poster",
        collection.poster_path.as_deref(),
        false,
        allow_local_media,
    )
    .await?;
    enqueue_image_job(
        transaction,
        "collection",
        collection_id,
        "backdrop",
        collection.backdrop_path.as_deref(),
        false,
        allow_local_media,
    )
    .await?;
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
    for video in videos {
        let (Some(video_key), Some(site)) = (
            bounded_text(video.key.as_deref(), 128),
            bounded_text(video.site.as_deref(), 64),
        ) else {
            continue;
        };
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

async fn enqueue_title_images(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    poster_path: Option<&str>,
    backdrop_path: Option<&str>,
    anime: bool,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    enqueue_image_job(
        transaction,
        entity_type,
        entity_id,
        "poster",
        poster_path,
        anime,
        allow_local_media,
    )
    .await?;
    enqueue_image_job(
        transaction,
        entity_type,
        entity_id,
        "backdrop",
        backdrop_path,
        anime,
        allow_local_media,
    )
    .await
}

async fn enqueue_image_job(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    tmdb_path: Option<&str>,
    anime: bool,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    enqueue_image_job_with_position(
        transaction,
        entity_type,
        entity_id,
        kind,
        tmdb_path,
        anime,
        None,
        None,
        None,
        allow_local_media,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_image_job_with_position(
    transaction: &mut Transaction<'_, Postgres>,
    entity_type: &str,
    entity_id: i64,
    kind: &str,
    tmdb_path: Option<&str>,
    anime: bool,
    season_number: Option<u16>,
    episode_number: Option<u16>,
    title_tmdb_id: Option<i64>,
    allow_local_media: bool,
) -> Result<(), JobExecutionError> {
    if !allow_local_media {
        return Ok(());
    }
    let Some(tmdb_path) = tmdb_path.filter(|path| valid_image_path(path)) else {
        return Ok(());
    };
    let source_url = format!("https://image.tmdb.org/t/p/original{tmdb_path}");
    let payload = serde_json::json!({
        "schemaVersion": IMAGE_JOB_PAYLOAD_VERSION,
        "entityType": entity_type,
        "entityId": entity_id,
        "kind": kind,
        "tmdbPath": tmdb_path,
        "sourceUrl": source_url,
        "language": serde_json::Value::Null,
        "sourceRevision": serde_json::Value::Null,
        "anime": anime,
        "seasonNumber": season_number,
        "episodeNumber": episode_number,
        "titleTmdbId": title_tmdb_id,
    });
    let payload = serde_json::to_string(&payload)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let dedup_key = format!(
        "image:{entity_type}:{entity_id}:{kind}:{}",
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
        TmdbCredit, TmdbCredits, TmdbEpisode, TmdbGenre, TmdbKeyword, TmdbMovie, TmdbSeason,
        TmdbSeasonSummary, TmdbTv,
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

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn movie_persistence_enqueues_idempotent_image_jobs(pool: PgPool) -> sqlx::Result<()> {
        let movie = TmdbMovie {
            id: 42,
            title: Some("Image queue fixture".to_owned()),
            poster_path: Some("/poster-fixture.jpg".to_owned()),
            backdrop_path: Some("/backdrop-fixture.jpg".to_owned()),
            ..TmdbMovie::default()
        };
        persist_movie_with_options(&pool, &movie, true)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        persist_movie_with_options(&pool, &movie, true)
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
                    "image:movie:42:poster:{}",
                    digest_hex("/poster-fixture.jpg")
                )
        }));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn movie_and_tv_persistence_require_keyword_and_animation_genre(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let keyword_only_movie = TmdbMovie {
            id: 44,
            title: Some("Keyword-only live adaptation".to_owned()),
            keywords: vec![TmdbKeyword {
                id: 210_024,
                name: Some("anime".to_owned()),
            }],
            genres: vec![TmdbGenre {
                id: 28,
                name: Some("Action".to_owned()),
            }],
            ..TmdbMovie::default()
        };
        let anime_tv = TmdbTv {
            id: 45,
            name: Some("Strict anime TV fixture".to_owned()),
            keywords: vec![TmdbKeyword {
                id: 210_024,
                name: Some("anime".to_owned()),
            }],
            genres: vec![TmdbGenre {
                id: 16,
                name: Some("Animation".to_owned()),
            }],
            ..TmdbTv::default()
        };

        persist_movie_with_options(&pool, &keyword_only_movie, false)
            .await
            .map_err(|error| as_sqlx_error(&error))?;
        persist_tv_with_options(&pool, &anime_tv, false)
            .await
            .map_err(|error| as_sqlx_error(&error))?;

        let rows: Vec<(String, bool)> = sqlx::query_as(
            "SELECT media_type, is_anime
               FROM catalog.titles
              WHERE tmdb_id IN (44, 45)
              ORDER BY tmdb_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows, [("movie".to_owned(), false), ("tv".to_owned(), true)]);
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
        persist_movie_with_options(&pool, &movie, false)
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
        persist_tv_with_options(&pool, &series, false)
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
                    persist_tv_with_options(&writer_pool, &series, false).await
                });
            } else {
                let season = season.clone();
                writers.spawn(async move {
                    writer_start.wait().await;
                    persist_season_with_options(&writer_pool, 800_001, &season, false).await
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
                persist_movie_with_options(&writer_pool, &movie, false).await
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
