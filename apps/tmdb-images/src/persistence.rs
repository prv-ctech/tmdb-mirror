use std::path::{Component, Path};

use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

use crate::image::{ImageEntityType, ImageJobPayload, ImageKind, ImageMetadata};

/// Sanitized failures from the image metadata transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub(crate) enum PersistError {
    /// The payload and downloaded metadata did not agree or contain unsafe values.
    #[error("image metadata is invalid")]
    InvalidPayload,
    /// The catalog owner has not been materialized yet.
    #[error("image catalog owner is not ready")]
    OwnerNotFound,
    /// A supplied image language is not present in the catalog dictionary.
    #[error("image language is not ready")]
    LanguageNotFound,
    /// The database rejected the transaction.
    #[error("image metadata transaction failed")]
    Database,
}

#[derive(Clone, Copy, Debug, Default)]
struct OwnerIds {
    title: Option<i64>,
    person: Option<i64>,
    company: Option<i64>,
    network: Option<i64>,
    collection: Option<i64>,
    season: Option<i64>,
    episode: Option<i64>,
}

/// Persists a successfully published image as a ready catalog asset.
///
/// The filesystem publication happens before this function is called.  This
/// transaction resolves the owner through the catalog's canonical identity,
/// reconciles a source path when TMDB gallery ordering changes, and upserts
/// the current metadata. A missing owner is retryable because ingest may be
/// committing the entity concurrently.
pub(crate) async fn persist_ready(
    pool: &PgPool,
    payload: &ImageJobPayload,
    metadata: &ImageMetadata,
) -> Result<(), PersistError> {
    validate_metadata(payload, metadata)?;
    let mut transaction = pool.begin().await.map_err(|_| PersistError::Database)?;
    let owner = resolve_owner(&mut transaction, payload).await?;
    let (owner_type, owner_id) = owner_identity(payload.entity_type, owner)?;
    let language = resolve_language(&mut transaction, payload.language.as_deref()).await?;
    let width = i32::try_from(metadata.width).map_err(|_| PersistError::InvalidPayload)?;
    let height = i32::try_from(metadata.height).map_err(|_| PersistError::InvalidPayload)?;
    let file_size = i64::try_from(metadata.byte_size).map_err(|_| PersistError::InvalidPayload)?;
    let gallery_index =
        i32::try_from(payload.asset_index).map_err(|_| PersistError::InvalidPayload)?;
    let image_kind = db_image_kind(payload.kind);

    let replacement_queued: bool =
        sqlx::query_scalar("SELECT assets.queue_image_asset_replacements($1, $2, $3, $4, $5, $6)")
            .bind(owner_type)
            .bind(owner_id)
            .bind(image_kind)
            .bind(gallery_index)
            .bind(&payload.tmdb_path)
            .bind(&metadata.storage_path)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| PersistError::Database)?;
    if !replacement_queued {
        return Err(PersistError::Database);
    }

    sqlx::query(
        "INSERT INTO assets.image_assets (
             title_id, person_id, company_id, network_id, collection_id, season_id, episode_id,
             image_kind, source, source_key, source_url, storage_path, mime_type,
             width, height, file_size_bytes, sha256, gallery_index, status, iso_639_1,
             downloaded_at, verified_at, updated_at
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, 'tmdb', $9, $10, $11, $12,
             $13, $14, $15, $16, $17, 'ready', $18,
             clock_timestamp(), clock_timestamp(), clock_timestamp()
         )
         ON CONFLICT (source, source_key, owner_type, owner_id) DO UPDATE SET
             title_id = EXCLUDED.title_id,
             person_id = EXCLUDED.person_id,
             company_id = EXCLUDED.company_id,
             network_id = EXCLUDED.network_id,
             collection_id = EXCLUDED.collection_id,
             season_id = EXCLUDED.season_id,
             episode_id = EXCLUDED.episode_id,
             image_kind = EXCLUDED.image_kind,
             source_url = EXCLUDED.source_url,
             storage_path = EXCLUDED.storage_path,
             mime_type = EXCLUDED.mime_type,
             width = EXCLUDED.width,
             height = EXCLUDED.height,
             file_size_bytes = EXCLUDED.file_size_bytes,
             sha256 = EXCLUDED.sha256,
             gallery_index = EXCLUDED.gallery_index,
             status = 'ready',
             iso_639_1 = EXCLUDED.iso_639_1,
             downloaded_at = clock_timestamp(),
             verified_at = clock_timestamp(),
             updated_at = clock_timestamp()
        ",
    )
    .bind(owner.title)
    .bind(owner.person)
    .bind(owner.company)
    .bind(owner.network)
    .bind(owner.collection)
    .bind(owner.season)
    .bind(owner.episode)
    .bind(db_image_kind(payload.kind))
    .bind(&payload.tmdb_path)
    .bind(&payload.source_url)
    .bind(&metadata.storage_path)
    .bind(&metadata.mime_type)
    .bind(width)
    .bind(height)
    .bind(file_size)
    .bind(&metadata.sha256)
    .bind(gallery_index)
    .bind(language)
    .execute(&mut *transaction)
    .await
    .map_err(|_| PersistError::Database)?;
    transaction
        .commit()
        .await
        .map_err(|_| PersistError::Database)
}

fn validate_metadata(
    payload: &ImageJobPayload,
    metadata: &ImageMetadata,
) -> Result<(), PersistError> {
    payload
        .validate()
        .map_err(|_| PersistError::InvalidPayload)?;
    if metadata.entity_type != payload.entity_type
        || metadata.entity_id != payload.entity_id
        || metadata.kind != payload.kind
        || metadata.tmdb_path != payload.tmdb_path
        || metadata.language != payload.language
        || metadata.source_revision != payload.source_revision
        || metadata.source_url != payload.source_url
        || metadata.byte_size == 0
        || metadata.width == 0
        || metadata.height == 0
        || !matches!(
            metadata.mime_type.as_str(),
            "image/jpeg" | "image/png" | "image/webp"
        )
        || metadata.sha256.len() != 64
        || !metadata
            .sha256
            .chars()
            .all(|value| value.is_ascii_hexdigit())
        || !safe_storage_path(&metadata.storage_path)
        || is_obsolete_layout(&metadata.storage_path)
    {
        return Err(PersistError::InvalidPayload);
    }
    Ok(())
}

fn is_obsolete_layout(path: &str) -> bool {
    path.starts_with("optimized/")
        || path.contains("/optimized/")
        || path.starts_with(".masters/")
        || path.contains("/.masters/")
}

fn safe_storage_path(value: &str) -> bool {
    !value.is_empty()
        && value.chars().count() <= 512
        && !value.chars().any(char::is_control)
        && !value.contains('\\')
        && !Path::new(value).is_absolute()
        && Path::new(value)
            .components()
            .all(|component| match component {
                Component::Normal(value) => !value.to_string_lossy().starts_with('.'),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => false,
            })
}

async fn resolve_owner(
    transaction: &mut Transaction<'_, Postgres>,
    payload: &ImageJobPayload,
) -> Result<OwnerIds, PersistError> {
    let owner_id: Option<i64> = match payload.entity_type {
        ImageEntityType::Movie | ImageEntityType::Tv => {
            let media_type = match payload.entity_type {
                ImageEntityType::Movie => "movie",
                ImageEntityType::Tv => "tv",
                _ => unreachable!(),
            };
            sqlx::query_scalar(
                "SELECT id FROM catalog.titles
                  WHERE media_type = $1 AND tmdb_id = $2 AND active",
            )
            .bind(media_type)
            .bind(payload.entity_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| PersistError::Database)?
        }
        ImageEntityType::Person => {
            sqlx::query_scalar("SELECT id FROM catalog.people WHERE id = $1")
                .bind(payload.entity_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|_| PersistError::Database)?
        }
        ImageEntityType::Company => {
            sqlx::query_scalar("SELECT id FROM catalog.companies WHERE id = $1")
                .bind(payload.entity_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|_| PersistError::Database)?
        }
        ImageEntityType::Network => {
            sqlx::query_scalar("SELECT id FROM catalog.networks WHERE id = $1")
                .bind(payload.entity_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|_| PersistError::Database)?
        }
        ImageEntityType::Collection => {
            sqlx::query_scalar("SELECT id FROM catalog.collections WHERE id = $1")
                .bind(payload.entity_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|_| PersistError::Database)?
        }
        ImageEntityType::Season => sqlx::query_scalar(
            "SELECT season.id
               FROM catalog.seasons AS season
               JOIN catalog.titles AS title ON title.id = season.title_id
              WHERE season.id = $1
                AND title.media_type = 'tv'
                AND title.tmdb_id = $2
                AND title.active",
        )
        .bind(payload.entity_id)
        .bind(payload.title_tmdb_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| PersistError::Database)?,
        ImageEntityType::Episode => sqlx::query_scalar(
            "SELECT episode.id
               FROM catalog.episodes AS episode
               JOIN catalog.titles AS title ON title.id = episode.title_id
              WHERE episode.id = $1
                AND title.media_type = 'tv'
                AND title.tmdb_id = $2
                AND title.active",
        )
        .bind(payload.entity_id)
        .bind(payload.title_tmdb_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| PersistError::Database)?,
    };
    let owner_id = owner_id.ok_or(PersistError::OwnerNotFound)?;
    let mut owner = OwnerIds::default();
    match payload.entity_type {
        ImageEntityType::Movie | ImageEntityType::Tv => owner.title = Some(owner_id),
        ImageEntityType::Person => owner.person = Some(owner_id),
        ImageEntityType::Company => owner.company = Some(owner_id),
        ImageEntityType::Network => owner.network = Some(owner_id),
        ImageEntityType::Collection => owner.collection = Some(owner_id),
        ImageEntityType::Season => owner.season = Some(owner_id),
        ImageEntityType::Episode => owner.episode = Some(owner_id),
    }
    Ok(owner)
}

fn owner_identity(
    entity_type: ImageEntityType,
    owner: OwnerIds,
) -> Result<(i16, i64), PersistError> {
    let (owner_type, owner_id): (i16, Option<i64>) = match entity_type {
        ImageEntityType::Movie | ImageEntityType::Tv => (1, owner.title),
        ImageEntityType::Person => (2, owner.person),
        ImageEntityType::Company => (3, owner.company),
        ImageEntityType::Network => (4, owner.network),
        ImageEntityType::Collection => (5, owner.collection),
        ImageEntityType::Season => (6, owner.season),
        ImageEntityType::Episode => (7, owner.episode),
    };
    owner_id
        .map(|owner_id| (owner_type, owner_id))
        .ok_or(PersistError::OwnerNotFound)
}

async fn resolve_language(
    transaction: &mut Transaction<'_, Postgres>,
    language: Option<&str>,
) -> Result<Option<String>, PersistError> {
    let Some(language) = language else {
        return Ok(None);
    };
    if !(2..=3).contains(&language.chars().count()) || !language.is_ascii() {
        return Err(PersistError::InvalidPayload);
    }
    let language = language.to_ascii_lowercase();
    let found: Option<String> =
        sqlx::query_scalar("SELECT iso_639_1 FROM catalog.languages WHERE iso_639_1 = $1")
            .bind(&language)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| PersistError::Database)?;
    found.ok_or(PersistError::LanguageNotFound).map(Some)
}

fn db_image_kind(kind: ImageKind) -> &'static str {
    match kind {
        ImageKind::Poster => "poster",
        ImageKind::Backdrop => "backdrop",
        ImageKind::Still => "still",
        ImageKind::Profile => "profile",
        ImageKind::Logo => "logo",
        ImageKind::Other => "other",
    }
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use crate::image::ImageSource;

    use super::*;

    fn persist_error(error: PersistError) -> sqlx::Error {
        sqlx::Error::Protocol(error.to_string())
    }

    fn movie_payload() -> Result<ImageJobPayload, crate::image::ImagePayloadError> {
        ImageJobPayload::new(
            ImageEntityType::Movie,
            42,
            ImageKind::Poster,
            "/poster.jpg",
            "https://image.tmdb.org/t/p/original/poster.jpg",
            None,
            None,
        )
    }

    fn metadata(payload: &ImageJobPayload, storage_path: &str) -> ImageMetadata {
        ImageMetadata {
            entity_type: payload.entity_type,
            entity_id: payload.entity_id,
            kind: payload.kind,
            tmdb_path: payload.tmdb_path.clone(),
            language: payload.language.clone(),
            source_revision: payload.source_revision.clone(),
            source_url: payload.source_url.clone(),
            mime_type: "image/png".to_owned(),
            byte_size: 70,
            width: 1,
            height: 1,
            sha256: "a".repeat(64),
            storage_path: storage_path.to_owned(),
            source: ImageSource::Direct,
        }
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn ready_asset_metadata_is_upserted_after_owner_resolution(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, active)
             VALUES ('movie', 42, 'Persistence test', true)",
        )
        .execute(&pool)
        .await?;
        let payload = movie_payload().map_err(|_| persist_error(PersistError::InvalidPayload))?;
        let first = metadata(
            &payload,
            "sha256/aa/bb/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        persist_ready(&pool, &payload, &first)
            .await
            .map_err(persist_error)?;

        let row: (i64, String, String, String, i32, i32, i64, String, bool) = sqlx::query_as(
            "SELECT asset.title_id, asset.image_kind, asset.source, asset.source_key,
                    asset.width, asset.height, asset.file_size_bytes, asset.status,
                    asset.downloaded_at IS NOT NULL
               FROM assets.image_assets AS asset
              WHERE asset.source = 'tmdb' AND asset.source_key = '/poster.jpg'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, 1);
        assert_eq!(row.1, "poster");
        assert_eq!(row.2, "tmdb");
        assert_eq!(row.3, "/poster.jpg");
        assert_eq!(row.4, 1);
        assert_eq!(row.5, 1);
        assert_eq!(row.6, 70);
        assert_eq!(row.7, "ready");
        assert!(row.8);

        let second = metadata(
            &payload,
            "sha256/cc/dd/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        persist_ready(&pool, &payload, &second)
            .await
            .map_err(persist_error)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM assets.image_assets
              WHERE source = 'tmdb' AND source_key = '/poster.jpg'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 1);
        let storage_path: String = sqlx::query_scalar(
            "SELECT storage_path FROM assets.image_assets
              WHERE source = 'tmdb' AND source_key = '/poster.jpg'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(storage_path, second.storage_path);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn source_owner_reconciliation_moves_an_asset_when_gallery_index_changes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, active)
             VALUES ('movie', 42, 'Gallery reorder fixture', true)",
        )
        .execute(&pool)
        .await?;

        let first = movie_payload().map_err(|_| persist_error(PersistError::InvalidPayload))?;
        persist_ready(
            &pool,
            &first,
            &metadata(&first, "movies/42/posters/poster.jpg"),
        )
        .await
        .map_err(persist_error)?;

        let moved = first
            .clone()
            .with_asset_index(2)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        persist_ready(
            &pool,
            &moved,
            &metadata(&moved, "movies/42/posters/poster-02.jpg"),
        )
        .await
        .map_err(persist_error)?;

        let rows: Vec<(i32, String, String)> = sqlx::query_as(
            "SELECT gallery_index, source_key, storage_path
               FROM assets.image_assets
              WHERE title_id = (SELECT id FROM catalog.titles WHERE tmdb_id = 42)
              ORDER BY gallery_index",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            rows,
            vec![(
                2,
                "/poster.jpg".to_owned(),
                "movies/42/posters/poster-02.jpg".to_owned(),
            )]
        );
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn source_owner_reconciliation_replaces_a_stale_gallery_slot(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, active)
             VALUES ('movie', 42, 'Gallery slot fixture', true)",
        )
        .execute(&pool)
        .await?;

        let first = movie_payload().map_err(|_| persist_error(PersistError::InvalidPayload))?;
        persist_ready(
            &pool,
            &first,
            &metadata(&first, "movies/42/posters/poster.jpg"),
        )
        .await
        .map_err(persist_error)?;

        let stale = ImageJobPayload::new(
            ImageEntityType::Movie,
            42,
            ImageKind::Poster,
            "/stale.jpg",
            "https://image.tmdb.org/t/p/original/stale.jpg",
            None,
            None,
        )
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
        .with_asset_index(2)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        persist_ready(
            &pool,
            &stale,
            &metadata(&stale, "movies/42/posters/poster-02.jpg"),
        )
        .await
        .map_err(persist_error)?;

        let moved = first
            .with_asset_index(2)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        persist_ready(
            &pool,
            &moved,
            &metadata(&moved, "movies/42/posters/poster-02.jpg"),
        )
        .await
        .map_err(persist_error)?;

        let rows: Vec<(i32, String)> = sqlx::query_as(
            "SELECT gallery_index, source_key
               FROM assets.image_assets
              WHERE title_id = (SELECT id FROM catalog.titles WHERE tmdb_id = 42)
              ORDER BY gallery_index",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows, vec![(2, "/poster.jpg".to_owned())]);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn legacy_variant_table_is_removed(pool: PgPool) -> sqlx::Result<()> {
        let relation: Option<String> =
            sqlx::query_scalar("SELECT pg_catalog.to_regclass('assets.image_variants')::text")
                .fetch_one(&pool)
                .await?;
        assert!(relation.is_none());
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn unknown_owner_does_not_insert_an_asset(pool: PgPool) -> sqlx::Result<()> {
        let payload = movie_payload().map_err(|_| persist_error(PersistError::InvalidPayload))?;
        let image = metadata(
            &payload,
            "sha256/aa/bb/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        assert_eq!(
            persist_ready(&pool, &payload, &image).await,
            Err(PersistError::OwnerNotFound)
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM assets.image_assets")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn shared_episode_still_is_attached_to_each_episode_owner(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let title_id: i64 = sqlx::query_scalar(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, active)
             VALUES ('tv', 100, 'Shared still fixture', true)
             RETURNING id",
        )
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.seasons (id, title_id, media_type, season_number)
             VALUES (200, $1, 'tv', 1)",
        )
        .bind(title_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.episodes (id, season_id, title_id, episode_number)
             VALUES (300, 200, $1, 1), (301, 200, $1, 2)",
        )
        .bind(title_id)
        .execute(&pool)
        .await?;

        let first = ImageJobPayload::new(
            ImageEntityType::Episode,
            300,
            ImageKind::Still,
            "/shared-still.jpg",
            "https://image.tmdb.org/t/p/original/shared-still.jpg",
            None,
            None,
        )
        .and_then(|payload| payload.with_tv_position(100, 1, Some(1)))
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let second = ImageJobPayload::new(
            ImageEntityType::Episode,
            301,
            ImageKind::Still,
            "/shared-still.jpg",
            "https://image.tmdb.org/t/p/original/shared-still.jpg",
            None,
            None,
        )
        .and_then(|payload| payload.with_tv_position(100, 1, Some(2)))
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        persist_ready(
            &pool,
            &first,
            &metadata(
                &first,
                "tv/100/thumbnails/season01-episode01-thumbnails.jpg",
            ),
        )
        .await
        .map_err(persist_error)?;
        persist_ready(
            &pool,
            &second,
            &metadata(
                &second,
                "tv/100/thumbnails/season01-episode02-thumbnails.jpg",
            ),
        )
        .await
        .map_err(persist_error)?;

        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT episode_id, storage_path
               FROM assets.image_assets
              WHERE source = 'tmdb' AND source_key = '/shared-still.jpg'
              ORDER BY episode_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            rows,
            vec![
                (
                    300,
                    "tv/100/thumbnails/season01-episode01-thumbnails.jpg".to_owned(),
                ),
                (
                    301,
                    "tv/100/thumbnails/season01-episode02-thumbnails.jpg".to_owned(),
                ),
            ]
        );
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn title_and_parent_identity_are_verified_before_persisting(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let title_id: i64 = sqlx::query_scalar(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, active)
             VALUES ('tv', 700, 'Ownership fixture', true)
             RETURNING id",
        )
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.seasons (id, title_id, media_type, season_number)
             VALUES (701, $1, 'tv', 1)",
        )
        .bind(title_id)
        .execute(&pool)
        .await?;

        let missing_title = ImageJobPayload::new(
            ImageEntityType::Tv,
            999,
            ImageKind::Poster,
            "/ownership-tv.jpg",
            "https://image.tmdb.org/t/p/original/ownership-tv.jpg",
            None,
            None,
        )
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(
            persist_ready(
                &pool,
                &missing_title,
                &metadata(&missing_title, "tv/999/posters/poster.jpg"),
            )
            .await,
            Err(PersistError::OwnerNotFound)
        );

        let wrong_parent = ImageJobPayload::new(
            ImageEntityType::Season,
            701,
            ImageKind::Still,
            "/ownership-season.jpg",
            "https://image.tmdb.org/t/p/original/ownership-season.jpg",
            None,
            None,
        )
        .and_then(|payload| payload.with_tv_position(701, 1, None))
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(
            persist_ready(
                &pool,
                &wrong_parent,
                &metadata(&wrong_parent, "tv/700/posters/season01-poster.jpg",),
            )
            .await,
            Err(PersistError::OwnerNotFound)
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM assets.image_assets")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }
}
