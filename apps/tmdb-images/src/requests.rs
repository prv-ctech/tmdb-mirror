use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use tmdb_jobs::{JobError, JobRepository, NewJob};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::image::{
    IMAGE_JOB_PAYLOAD_VERSION, IMAGE_JOB_TYPE, ImageEntityType, ImageJobPayload, ImageKind,
};

const EXPANSION_BATCH_SIZE: i32 = 250;
const CAPACITY_RETRY: Duration = Duration::from_secs(2);
const DATABASE_RETRY: Duration = Duration::from_secs(5);
const RECENT_VERIFICATION_HOURS: i32 = 24;

#[derive(Clone, Debug)]
pub(crate) struct CoordinatorConfig {
    pub(crate) worker_id: String,
    pub(crate) lease_duration: Duration,
    pub(crate) idle_poll_interval: Duration,
}

#[derive(Clone, Debug, FromRow)]
struct ClaimedRequest {
    request_id: Uuid,
    source_cursor: i64,
}

#[derive(Clone, Debug, FromRow)]
struct SelectedSource {
    source_cursor: i64,
    request_item_id: i64,
    entity_type: String,
    entity_id: i64,
    owner_id: i64,
    title_tmdb_id: i64,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    image_kind: String,
    source_path: String,
    language_code: Option<String>,
    gallery_index: i32,
    catalog_incomplete: bool,
}

#[derive(Clone, Debug, FromRow)]
struct ReadyAsset {
    id: i64,
    storage_path: String,
    file_size_bytes: i64,
    sha256: String,
    recently_verified: bool,
}

#[derive(Clone, Debug, FromRow)]
struct PendingDeletion {
    deletion_id: i64,
    storage_path: String,
    expected_directory: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ImageConfiguration {
    secure_base_url: String,
    poster_sizes: Vec<String>,
    backdrop_sizes: Vec<String>,
    still_sizes: Vec<String>,
    profile_sizes: Vec<String>,
    logo_sizes: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ExpansionCounts {
    found: u64,
    queued: u64,
    reused: u64,
}

enum SourceAdmission {
    Linked,
    CapacityWait,
    Cancelled,
}

/// Drains durable on-demand media requests directly from `PostgreSQL`.
pub(crate) async fn run(pool: PgPool, config: CoordinatorConfig, cancellation: CancellationToken) {
    loop {
        let claim = tokio::select! {
            () = cancellation.cancelled() => return,
            claim = claim_request(&pool, &config) => claim,
        };
        match claim {
            Ok(Some(request)) => {
                if let Err(reason) = process_request(&pool, &config, &request).await {
                    tracing::warn!(
                        event = "media_request_expansion_delayed",
                        request_id = %request.request_id,
                        reason,
                        retry_seconds = DATABASE_RETRY.as_secs(),
                    );
                    let _ = release_request(
                        &pool,
                        &config.worker_id,
                        request.request_id,
                        request.source_cursor,
                        false,
                        DATABASE_RETRY,
                    )
                    .await;
                }
            }
            Ok(None) => {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(config.idle_poll_interval) => {}
                }
            }
            Err(()) => {
                tracing::warn!(
                    event = "media_request_claim_failed",
                    error_code = "database_unavailable",
                    retry_seconds = DATABASE_RETRY.as_secs(),
                );
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(DATABASE_RETRY) => {}
                }
            }
        }
    }
}

async fn claim_request(
    pool: &PgPool,
    config: &CoordinatorConfig,
) -> Result<Option<ClaimedRequest>, ()> {
    let lease_micros = i64::try_from(config.lease_duration.as_micros()).map_err(|_| ())?;
    sqlx::query_as(
        "SELECT request_id, source_cursor
           FROM ops.claim_media_request($1, $2)",
    )
    .bind(&config.worker_id)
    .bind(lease_micros)
    .fetch_optional(pool)
    .await
    .map_err(|_| ())
}

async fn process_request(
    pool: &PgPool,
    config: &CoordinatorConfig,
    request: &ClaimedRequest,
) -> Result<(), &'static str> {
    let image_configuration = load_image_configuration(pool).await?;
    let sources = sqlx::query_as::<_, SelectedSource>(
        "SELECT source_cursor, request_item_id, entity_type, entity_id, owner_id,
                title_tmdb_id, season_number, episode_number, image_kind,
                source_path, language_code, gallery_index, catalog_incomplete
           FROM assets.select_media_request_sources($1, $2, $3)",
    )
    .bind(request.request_id)
    .bind(request.source_cursor)
    .bind(EXPANSION_BATCH_SIZE)
    .fetch_all(pool)
    .await
    .map_err(|_| "database_unavailable")?;

    let mut counts = ExpansionCounts::default();
    let mut cursor = request.source_cursor;
    let mut capacity_wait = false;
    for source in &sources {
        let payload = build_payload(source, &image_configuration)?;
        if ready_asset_is_valid(pool, source, &payload.source_url).await? {
            match admit_source(pool, config, request, source, None, true).await? {
                SourceAdmission::Linked => {}
                SourceAdmission::CapacityWait => {
                    capacity_wait = true;
                    break;
                }
                SourceAdmission::Cancelled => return Err("request_cancelled"),
            }
            counts.reused += 1;
            counts.found += 1;
            cursor = source.source_cursor;
            continue;
        }
        let job = image_job(source, &payload)?;
        match admit_source(pool, config, request, source, Some(job), false).await? {
            SourceAdmission::Linked => {}
            SourceAdmission::CapacityWait => {
                capacity_wait = true;
                break;
            }
            SourceAdmission::Cancelled => return Err("request_cancelled"),
        }
        counts.queued += 1;
        counts.found += 1;
        cursor = source.source_cursor;
    }

    let exhausted = !capacity_wait && sources.len() < EXPANSION_BATCH_SIZE as usize;
    let delay = if capacity_wait {
        CAPACITY_RETRY
    } else if exhausted {
        Duration::ZERO
    } else {
        config.idle_poll_interval
    };
    if !release_request(
        pool,
        &config.worker_id,
        request.request_id,
        cursor,
        exhausted,
        delay,
    )
    .await
    {
        return Err("lease_lost");
    }
    let deleted = if exhausted {
        let _: i64 = sqlx::query_scalar("SELECT assets.queue_obsolete_media_request_files($1)")
            .bind(request.request_id)
            .fetch_one(pool)
            .await
            .map_err(|_| "database_unavailable")?;
        drain_pending_deletions(pool).await?
    } else {
        0
    };
    let status: String = sqlx::query_scalar("SELECT ops.refresh_media_request($1)")
        .bind(request.request_id)
        .fetch_one(pool)
        .await
        .map_err(|_| "database_unavailable")?;
    tracing::info!(
        event = "media_request_progress",
        request_id = %request.request_id,
        status,
        source_cursor = cursor,
        source_assets_found = counts.found,
        queued = counts.queued,
        reused = counts.reused,
        deleted,
        expansion_complete = exhausted,
        capacity_wait,
    );
    Ok(())
}

async fn drain_pending_deletions(pool: &PgPool) -> Result<u64, &'static str> {
    let deletions = sqlx::query_as::<_, PendingDeletion>(
        "SELECT deletion_id, storage_path, expected_directory
           FROM assets.pending_media_file_deletions(250)",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| "database_unavailable")?;
    let root = tokio::fs::canonicalize(tmdb_media::MEDIA_ROOT)
        .await
        .map_err(|_| "media_root_unavailable")?;
    let mut deleted = 0_u64;
    for deletion in deletions {
        let Some(expected_directory) = deletion.expected_directory else {
            continue;
        };
        let expected_directory = expected_directory.trim_end_matches('/');
        if !tmdb_media::is_public_relative(&deletion.storage_path)
            || !tmdb_media::is_public_relative(expected_directory)
            || !Path::new(&deletion.storage_path).starts_with(Path::new(expected_directory))
        {
            return Err("unsafe_deletion_path");
        }
        if let Some(path) =
            safe_regular_file(&root, &deletion.storage_path, expected_directory).await?
        {
            tokio::fs::remove_file(path)
                .await
                .map_err(|_| "media_delete_failed")?;
        }
        let completed: bool = sqlx::query_scalar("SELECT assets.complete_media_file_deletion($1)")
            .bind(deletion.deletion_id)
            .fetch_one(pool)
            .await
            .map_err(|_| "database_unavailable")?;
        if completed {
            deleted = deleted.saturating_add(1);
        }
    }
    Ok(deleted)
}

async fn load_image_configuration(pool: &PgPool) -> Result<ImageConfiguration, &'static str> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT assets.media_image_configuration()")
            .fetch_one(pool)
            .await
            .map_err(|_| "database_unavailable")?;
    let value = value.ok_or("image_configuration_unavailable")?;
    serde_json::from_value(value).map_err(|_| "image_configuration_invalid")
}

fn build_payload(
    source: &SelectedSource,
    configuration: &ImageConfiguration,
) -> Result<ImageJobPayload, &'static str> {
    let entity_type = entity_type(&source.entity_type)?;
    let kind = image_kind(&source.image_kind)?;
    let rendition = configuration.rendition(kind)?;
    let source_path = cdn_path(kind, &source.source_path);
    let base =
        Url::parse(&configuration.secure_base_url).map_err(|_| "image_configuration_invalid")?;
    let source_url = base
        .join(&format!(
            "{rendition}/{}",
            source_path.trim_start_matches('/')
        ))
        .map_err(|_| "image_configuration_invalid")?;
    let mut payload = ImageJobPayload::new(
        entity_type,
        source.entity_id,
        kind,
        source.source_path.clone(),
        source_url.to_string(),
        source.language_code.clone(),
        None,
    )
    .map_err(|_| "catalog_source_invalid")?;
    if matches!(
        entity_type,
        ImageEntityType::Season | ImageEntityType::Episode
    ) {
        let season = source
            .season_number
            .and_then(|value| u32::try_from(value).ok())
            .ok_or("catalog_source_invalid")?;
        let episode = source
            .episode_number
            .map(u16::try_from)
            .transpose()
            .map_err(|_| "catalog_source_invalid")?;
        payload = payload
            .with_tv_position(source.title_tmdb_id, season, episode)
            .map_err(|_| "catalog_source_invalid")?;
    }
    payload
        .with_asset_index(
            u32::try_from(source.gallery_index).map_err(|_| "catalog_source_invalid")?,
        )
        .map_err(|_| "catalog_source_invalid")
}

impl ImageConfiguration {
    fn rendition(&self, kind: ImageKind) -> Result<&str, &'static str> {
        let (sizes, maximum) = match kind {
            ImageKind::Poster | ImageKind::Other => (&self.poster_sizes, 500_u32),
            ImageKind::Backdrop => (&self.backdrop_sizes, 1_280_u32),
            ImageKind::Still => (&self.still_sizes, 300_u32),
            ImageKind::Profile => (&self.profile_sizes, 185_u32),
            ImageKind::Logo => (&self.logo_sizes, 185_u32),
        };
        sizes
            .iter()
            .filter_map(|size| rendition_width(size).map(|width| (width, size.as_str())))
            .filter(|(width, _)| *width <= maximum)
            .max_by_key(|(width, _)| *width)
            .map(|(_, size)| size)
            .ok_or("bounded_rendition_unavailable")
    }
}

fn rendition_width(value: &str) -> Option<u32> {
    value.strip_prefix('w')?.parse().ok()
}

fn cdn_path(kind: ImageKind, source_path: &str) -> String {
    if kind == ImageKind::Logo && source_path.to_ascii_lowercase().ends_with(".svg") {
        format!("{}.png", &source_path[..source_path.len() - 4])
    } else {
        source_path.to_owned()
    }
}

fn image_job(source: &SelectedSource, payload: &ImageJobPayload) -> Result<NewJob, &'static str> {
    let payload_json = payload.to_json().map_err(|_| "catalog_source_invalid")?;
    NewJob::new(
        IMAGE_JOB_TYPE,
        IMAGE_JOB_PAYLOAD_VERSION,
        payload_json,
        &format!(
            "image:{}:{}:{}:{}",
            owner_type(&source.entity_type)?,
            source.owner_id,
            source.image_kind,
            source.gallery_index
        ),
    )
    .and_then(|job| job.with_priority(50))
    .and_then(|job| job.with_max_attempts(8))
    .map_err(|_| "catalog_source_invalid")
}

async fn ready_asset_is_valid(
    pool: &PgPool,
    source: &SelectedSource,
    source_url: &str,
) -> Result<bool, &'static str> {
    let owner_type = owner_type(&source.entity_type)?;
    let asset = sqlx::query_as::<_, ReadyAsset>(
        "SELECT id, storage_path, file_size_bytes, sha256,
                verified_at > clock_timestamp()
                    - make_interval(hours => $6) AS recently_verified
           FROM assets.image_assets
          WHERE owner_type = $1 AND owner_id = $2
            AND source = 'tmdb' AND source_key = $3
            AND source_url = $4 AND status = 'ready'
            AND storage_path IS NOT NULL AND file_size_bytes IS NOT NULL
            AND sha256 IS NOT NULL AND gallery_index = $5",
    )
    .bind(owner_type)
    .bind(source.owner_id)
    .bind(&source.source_path)
    .bind(source_url)
    .bind(source.gallery_index)
    .bind(RECENT_VERIFICATION_HOURS)
    .fetch_optional(pool)
    .await
    .map_err(|_| "database_unavailable")?;
    let Some(asset) = asset else {
        return Ok(false);
    };
    if !verify_asset_file(&asset).await {
        return Ok(false);
    }
    if !asset.recently_verified {
        sqlx::query(
            "UPDATE assets.image_assets SET verified_at = clock_timestamp(),
                    updated_at = clock_timestamp() WHERE id = $1",
        )
        .bind(asset.id)
        .execute(pool)
        .await
        .map_err(|_| "database_unavailable")?;
    }
    Ok(true)
}

async fn verify_asset_file(asset: &ReadyAsset) -> bool {
    if !tmdb_media::is_public_relative(&asset.storage_path)
        || asset.file_size_bytes <= 0
        || asset.sha256.len() != 64
        || !asset.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }
    let root = Path::new(tmdb_media::MEDIA_ROOT);
    let Ok(root) = tokio::fs::canonicalize(root).await else {
        return false;
    };
    let expected = match Path::new(&asset.storage_path).parent() {
        Some(parent) => parent.to_string_lossy().into_owned(),
        None => return false,
    };
    let Ok(Some(path)) = safe_regular_file(&root, &asset.storage_path, &expected).await else {
        return false;
    };
    let metadata = match tokio::fs::metadata(&path).await {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) | Err(_) => return false,
    };
    if metadata.len() != u64::try_from(asset.file_size_bytes).unwrap_or_default() {
        return false;
    }
    if asset.recently_verified {
        return true;
    }
    file_sha256(&path)
        .await
        .is_some_and(|digest| digest.eq_ignore_ascii_case(&asset.sha256))
}

async fn safe_regular_file(
    root: &Path,
    relative: &str,
    expected_directory: &str,
) -> Result<Option<PathBuf>, &'static str> {
    let relative = Path::new(relative);
    let expected = Path::new(expected_directory);
    if !relative.starts_with(expected)
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        || expected
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err("unsafe_media_path");
    }
    let mut current = root.to_path_buf();
    let parent = relative.parent().ok_or("unsafe_media_path")?;
    for component in parent.components() {
        let std::path::Component::Normal(component) = component else {
            return Err("unsafe_media_path");
        };
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err("unsafe_media_path");
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("media_path_unavailable"),
        }
    }
    let candidate = root.join(relative);
    match tokio::fs::symlink_metadata(&candidate).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err("unsafe_media_path")
        }
        Ok(_) => Ok(Some(candidate)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("media_path_unavailable"),
    }
}

async fn file_sha256(path: &PathBuf) -> Option<String> {
    let mut file = tokio::fs::File::open(path).await.ok()?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await.ok()?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

async fn mark_existing_asset_pending(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &SelectedSource,
) -> Result<(), &'static str> {
    sqlx::query(
        "UPDATE assets.image_assets
            SET status = 'pending', verified_at = NULL, updated_at = clock_timestamp()
          WHERE owner_type = $1 AND owner_id = $2
            AND source = 'tmdb' AND source_key = $3",
    )
    .bind(owner_type(&source.entity_type)?)
    .bind(source.owner_id)
    .bind(&source.source_path)
    .execute(&mut **transaction)
    .await
    .map_err(|_| "database_unavailable")?;
    Ok(())
}

async fn admit_source(
    pool: &PgPool,
    config: &CoordinatorConfig,
    request: &ClaimedRequest,
    source: &SelectedSource,
    job: Option<NewJob>,
    reused: bool,
) -> Result<SourceAdmission, &'static str> {
    let mut transaction = pool.begin().await.map_err(|_| "database_unavailable")?;
    let claim_active: bool = sqlx::query_scalar("SELECT ops.lock_media_request_claim($1, $2)")
        .bind(request.request_id)
        .bind(&config.worker_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| "database_unavailable")?;
    if !claim_active {
        transaction
            .rollback()
            .await
            .map_err(|_| "database_unavailable")?;
        return Ok(SourceAdmission::Cancelled);
    }
    let job_id = if let Some(job) = job {
        mark_existing_asset_pending(&mut transaction, source).await?;
        let mut outcomes =
            match JobRepository::submit_many_in_transaction(&mut transaction, &[job]).await {
                Ok(outcomes) => outcomes,
                Err(JobError::Rejected) => {
                    transaction
                        .rollback()
                        .await
                        .map_err(|_| "database_unavailable")?;
                    return Ok(SourceAdmission::CapacityWait);
                }
                Err(JobError::Validation(_)) => return Err("catalog_source_invalid"),
                Err(JobError::NotFound | JobError::LeaseLost | JobError::Database) => {
                    return Err("database_unavailable");
                }
            };
        let outcome = outcomes.pop().ok_or("database_unavailable")?;
        if outcome.was_duplicate() {
            let existing_source: Option<String> =
                sqlx::query_scalar("SELECT ops.media_image_job_source($1)")
                    .bind(outcome.job_id().as_uuid())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| "database_unavailable")?
                    .flatten();
            if existing_source.as_deref() != Some(source.source_path.as_str()) {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| "database_unavailable")?;
                return Ok(SourceAdmission::CapacityWait);
            }
        }
        Some(outcome.job_id().as_uuid())
    } else {
        None
    };
    let linked: bool = sqlx::query_scalar(
        "SELECT ops.link_media_request_asset(
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
         )",
    )
    .bind(request.request_id)
    .bind(&config.worker_id)
    .bind(source.request_item_id)
    .bind(source.source_cursor)
    .bind(owner_type(&source.entity_type)?)
    .bind(source.owner_id)
    .bind(&source.image_kind)
    .bind(source.gallery_index)
    .bind(&source.source_path)
    .bind(job_id)
    .bind(reused)
    .bind(source.catalog_incomplete)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|_| "database_unavailable")?;
    if !linked {
        transaction
            .rollback()
            .await
            .map_err(|_| "database_unavailable")?;
        return Ok(SourceAdmission::Cancelled);
    }
    transaction
        .commit()
        .await
        .map_err(|_| "database_unavailable")?;
    Ok(SourceAdmission::Linked)
}

async fn release_request(
    pool: &PgPool,
    worker_id: &str,
    request_id: Uuid,
    cursor: i64,
    expansion_complete: bool,
    delay: Duration,
) -> bool {
    let delay = i32::try_from(delay.as_secs()).unwrap_or(300).min(300);
    sqlx::query_scalar("SELECT ops.advance_media_request($1, $2, $3, $4, $5)")
        .bind(request_id)
        .bind(worker_id)
        .bind(cursor)
        .bind(expansion_complete)
        .bind(delay)
        .fetch_one(pool)
        .await
        .unwrap_or(false)
}

fn entity_type(value: &str) -> Result<ImageEntityType, &'static str> {
    match value {
        "movie" => Ok(ImageEntityType::Movie),
        "tv" => Ok(ImageEntityType::Tv),
        "season" => Ok(ImageEntityType::Season),
        "episode" => Ok(ImageEntityType::Episode),
        "person" => Ok(ImageEntityType::Person),
        "company" => Ok(ImageEntityType::Company),
        "network" => Ok(ImageEntityType::Network),
        "collection" => Ok(ImageEntityType::Collection),
        _ => Err("catalog_source_invalid"),
    }
}

fn owner_type(value: &str) -> Result<i16, &'static str> {
    match value {
        "movie" | "tv" => Ok(1),
        "person" => Ok(2),
        "company" => Ok(3),
        "network" => Ok(4),
        "collection" => Ok(5),
        "season" => Ok(6),
        "episode" => Ok(7),
        _ => Err("catalog_source_invalid"),
    }
}

fn image_kind(value: &str) -> Result<ImageKind, &'static str> {
    match value {
        "poster" => Ok(ImageKind::Poster),
        "backdrop" => Ok(ImageKind::Backdrop),
        "logo" => Ok(ImageKind::Logo),
        "profile" => Ok(ImageKind::Profile),
        "still" => Ok(ImageKind::Still),
        "other" => Ok(ImageKind::Other),
        _ => Err("catalog_source_invalid"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> ImageConfiguration {
        ImageConfiguration {
            secure_base_url: "https://image.tmdb.org/t/p/".to_owned(),
            poster_sizes: vec!["w92".to_owned(), "w500".to_owned(), "original".to_owned()],
            backdrop_sizes: vec!["w300".to_owned(), "w1280".to_owned(), "original".to_owned()],
            still_sizes: vec!["w92".to_owned(), "w300".to_owned(), "original".to_owned()],
            profile_sizes: vec!["w45".to_owned(), "h632".to_owned(), "w185".to_owned()],
            logo_sizes: vec!["w45".to_owned(), "w185".to_owned(), "w500".to_owned()],
        }
    }

    #[test]
    fn rendition_selection_never_uses_original_or_exceeds_policy() {
        let configuration = configuration();
        assert_eq!(configuration.rendition(ImageKind::Poster), Ok("w500"));
        assert_eq!(configuration.rendition(ImageKind::Backdrop), Ok("w1280"));
        assert_eq!(configuration.rendition(ImageKind::Still), Ok("w300"));
        assert_eq!(configuration.rendition(ImageKind::Profile), Ok("w185"));
        assert_eq!(configuration.rendition(ImageKind::Logo), Ok("w185"));
    }

    #[test]
    fn svg_logo_source_uses_png_cdn_representation() {
        assert_eq!(cdn_path(ImageKind::Logo, "/network.svg"), "/network.png");
        assert_eq!(cdn_path(ImageKind::Poster, "/poster.jpg"), "/poster.jpg");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deletion_validation_rejects_entity_directory_symlinks()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        tokio::fs::create_dir(root.path().join("tv")).await?;
        symlink(outside.path(), root.path().join("tv/1"))?;
        let root = tokio::fs::canonicalize(root.path()).await?;
        assert_eq!(
            safe_regular_file(&root, "tv/1/posters/poster.jpg", "tv/1").await,
            Err("unsafe_media_path")
        );
        Ok(())
    }
}
