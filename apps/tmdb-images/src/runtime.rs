use std::sync::Arc;
use std::time::Duration;
use std::{net::SocketAddr, path::Path};

use crate::image::{
    DownloadPolicy, HttpTrawlFallback, ImageDownloader, ImageError, ImageJobPayload, ImageStore,
    ImageTransport, ReqwestTransport, StorageError, TrawlFallback,
};
use crate::media_server;
use crate::persistence::persist_ready;
use anyhow::Context;
use async_trait::async_trait;
use image::GenericImageView;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use tmdb_config::{ConfigSource, EnvSource, Environment, load_database_for_role};
use tmdb_db::{PoolPolicy, connect_direct};
use tmdb_jobs::{
    ClaimedJob, JobExecutionError, JobExecutor, JobRepository, NewJob, Worker, WorkerConfig,
    WorkerId,
};
use tmdb_media::{RuntimeStorageRole, prepare_runtime_storage};
use tmdb_observability::init_tracing_from_env;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MEDIA_AUDIT_JOB: &str = "admin.media_audit";
const MEDIA_AUDIT_PAYLOAD_VERSION: i32 = 1;
const MEDIA_AUDIT_BATCH_SIZE: i64 = 500;
const MAX_AUDIT_FILE_BYTES: u64 = 32 * 1024 * 1024;
const COMPONENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const IMAGE_JOB_TYPES: &[&str] = &[crate::image::IMAGE_JOB_TYPE, MEDIA_AUDIT_JOB, "system.noop"];
const IMAGE_QUEUE_READY_RETRY: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaAuditPayload {
    repair: bool,
    #[serde(default)]
    after_asset_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ImageWorkerJob {
    Noop,
    Download(ImageJobPayload),
    MediaAudit(MediaAuditPayload),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MediaAuditSummary {
    skipped: bool,
    audited: u64,
    valid: u64,
    invalid: u64,
    repair_queued: u64,
    unrepairable: u64,
    next_audit_queued: bool,
}

fn disabled_media_audit_summary(allow_local_media: bool) -> Option<MediaAuditSummary> {
    (!allow_local_media).then_some(MediaAuditSummary {
        skipped: true,
        ..MediaAuditSummary::default()
    })
}

/// Starts the direct-database image worker shell.
pub async fn run() -> anyhow::Result<()> {
    init_tracing_from_env(env!("CARGO_PKG_NAME")).map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(event = "media_worker_starting");
    prepare_media_storage()?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let database = load_database_for_role(&source, environment, "image_writer")?;
    let worker_config = load_worker_config(source, "tmdb-images")?;
    let worker_concurrency = load_image_worker_concurrency(source)?;
    let store = load_image_store()?;
    store
        .harden_private_masters()
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("harden private image masters")?;
    let downloader = load_downloader(source)?;
    let allow_local_media = parse_or(source, "ALLOW_LOCAL_MEDIA", false)?;
    let trawl_fallback_configured =
        std::env::var("TMDB_TRAWL_BASE_URL").is_ok_and(|value| !value.trim().is_empty());
    let media_bind = parse_or(source, "TMDB_MEDIA_BIND", "0.0.0.0:8090".to_owned())?
        .parse::<SocketAddr>()
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_MEDIA_BIND is invalid"))?;
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect image database")?;
    let executor = ImageExecutor {
        downloader,
        store,
        pool: pool.clone(),
        allow_local_media,
    };
    let workers = image_worker_configs(worker_config, worker_concurrency)?
        .into_iter()
        .map(|config| Worker::new(JobRepository::new(pool.clone()), executor.clone(), config))
        .collect();
    tracing::info!(
        event = "media_worker_ready",
        download_workers = worker_concurrency,
        local_media_enabled = allow_local_media,
        trawl_fallback_configured,
    );
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if shutdown_signal().await.is_err() {
            tracing::error!(
                event = "shutdown_signal_failed",
                error_code = "signal_setup"
            );
        }
        signal_cancellation.cancel();
    });
    let media_cancellation = cancellation.clone();
    tracing::info!(event = "media_server_starting");
    let media_server = tokio::spawn(async move {
        media_server::run(
            media_bind,
            std::path::PathBuf::from(tmdb_media::MEDIA_ROOT),
            media_cancellation,
        )
        .await
    });
    if !wait_for_image_job_queue(&pool, &cancellation).await {
        cancellation.cancel();
        if let Ok(Err(error)) = media_server.await {
            tracing::error!(event = "media_server_stopped", error = %error);
        }
        pool.close().await;
        return Ok(());
    }
    let heartbeat = spawn_component_heartbeat(pool.clone(), cancellation.clone());
    let result = run_workers(workers, cancellation.clone()).await;
    cancellation.cancel();
    let _ = heartbeat.await;
    if let Ok(Err(error)) = media_server.await {
        tracing::error!(event = "media_server_stopped", error = %error);
    }
    pool.close().await;
    result
}

fn spawn_component_heartbeat(
    pool: PgPool,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(COMPONENT_HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = interval.tick() => {
                    if sqlx::query("SELECT ops.record_component_heartbeat('media', 'ready')")
                    .execute(&pool)
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            event = "component_heartbeat_failed",
                            component = "media",
                            error_code = "database_unavailable",
                        );
                    }
                }
            }
        }
    })
}

fn prepare_media_storage() -> anyhow::Result<()> {
    prepare_runtime_storage(RuntimeStorageRole::Media).map_err(|error| {
        tracing::error!(
            event = "storage_preflight_failed",
            role = RuntimeStorageRole::Media.as_str(),
            path = error.path().as_str(),
            operation = error.operation(),
            io_kind = error.io_kind().unwrap_or("not_applicable"),
        );
        anyhow::anyhow!(
            "media storage preflight failed at {} ({})",
            error.path().as_str(),
            error.operation(),
        )
    })?;
    tracing::info!(
        event = "storage_preflight_ready",
        role = RuntimeStorageRole::Media.as_str()
    );
    Ok(())
}

async fn wait_for_image_job_queue(pool: &PgPool, cancellation: &CancellationToken) -> bool {
    loop {
        match image_job_queue_ready(pool).await {
            Ok(true) => {
                tracing::info!(event = "image_job_queue_ready");
                return true;
            }
            Ok(false) => tracing::info!(
                event = "image_job_queue_not_ready",
                retry_seconds = IMAGE_QUEUE_READY_RETRY.as_secs()
            ),
            Err(_) => tracing::warn!(
                event = "image_job_queue_check_failed",
                retry_seconds = IMAGE_QUEUE_READY_RETRY.as_secs()
            ),
        }
        tokio::select! {
            () = cancellation.cancelled() => return false,
            () = tokio::time::sleep(IMAGE_QUEUE_READY_RETRY) => {}
        }
    }
}

async fn image_job_queue_ready(pool: &PgPool) -> sqlx::Result<bool> {
    sqlx::query_scalar(IMAGE_JOB_QUEUE_READY_SQL)
        .fetch_one(pool)
        .await
}

const IMAGE_JOB_QUEUE_READY_SQL: &str = concat!(
    "SELECT pg_catalog.to_regprocedure('ops.claim_job_for_types(text,bigint,text[])') IS NOT NULL ",
    "AND pg_catalog.to_regprocedure('ops.record_component_heartbeat(text,text)') IS NOT NULL ",
    "AND pg_catalog.to_regclass('assets.image_variants') IS NOT NULL",
);

async fn run_workers<E>(
    workers: Vec<Worker<E>>,
    cancellation: CancellationToken,
) -> anyhow::Result<()>
where
    E: JobExecutor + 'static,
{
    let mut tasks = JoinSet::new();
    for worker in workers {
        let worker_cancellation = cancellation.clone();
        tasks.spawn(async move { worker.run(worker_cancellation).await });
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) if cancellation.is_cancelled() => {}
            Ok(Ok(())) => {
                cancellation.cancel();
                tasks.abort_all();
                return Err(anyhow::anyhow!("image worker stopped unexpectedly"));
            }
            Ok(Err(error)) => {
                cancellation.cancel();
                tasks.abort_all();
                return Err(anyhow::anyhow!(error));
            }
            Err(error) => {
                cancellation.cancel();
                tasks.abort_all();
                return Err(anyhow::anyhow!("image worker task failed: {error}"));
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct ImageExecutor<T, F = Arc<dyn TrawlFallback>> {
    downloader: ImageDownloader<T, F>,
    store: ImageStore,
    pool: PgPool,
    allow_local_media: bool,
}

impl<T, F> ImageExecutor<T, F>
where
    T: ImageTransport,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the bounded audit loop keeps validation, repair submission, and continuation state together"
    )]
    async fn audit_media(
        &self,
        payload: MediaAuditPayload,
    ) -> Result<MediaAuditSummary, JobExecutionError> {
        if let Some(summary) = disabled_media_audit_summary(self.allow_local_media) {
            tracing::info!(
                event = "media_audit_skipped",
                reason = "local_media_disabled",
                repair_requested = payload.repair,
            );
            return Ok(summary);
        }
        let after_asset_id = payload.after_asset_id.unwrap_or_default();
        let rows = sqlx::query_as::<_, MediaAuditAssetRow>(
            "SELECT asset.id,
                    asset.source_key,
                    asset.source_url,
                    asset.storage_path,
                    asset.mime_type,
                    asset.width,
                    asset.height,
                    asset.file_size_bytes,
                    asset.sha256,
                    asset.image_kind,
                    asset.iso_639_1 AS language,
                    CASE
                        WHEN asset.title_id IS NOT NULL THEN title.media_type
                        WHEN asset.person_id IS NOT NULL THEN 'person'
                        WHEN asset.company_id IS NOT NULL THEN 'company'
                        WHEN asset.network_id IS NOT NULL THEN 'network'
                        WHEN asset.collection_id IS NOT NULL THEN 'collection'
                        WHEN asset.season_id IS NOT NULL THEN 'season'
                        WHEN asset.episode_id IS NOT NULL THEN 'episode'
                    END AS entity_type,
                    CASE
                        WHEN asset.title_id IS NOT NULL THEN title.tmdb_id
                        ELSE asset.owner_id
                    END AS entity_id,
                    COALESCE(title.is_anime, season_title.is_anime, episode_title.is_anime, false) AS anime,
                    season.season_number,
                    episode.episode_number,
                    COALESCE(season_title.tmdb_id, episode_title.tmdb_id) AS title_tmdb_id
               FROM assets.image_assets AS asset
               LEFT JOIN catalog.titles AS title ON title.id = asset.title_id
               LEFT JOIN catalog.seasons AS season ON season.id = asset.season_id
               LEFT JOIN catalog.titles AS season_title ON season_title.id = season.title_id
               LEFT JOIN catalog.episodes AS episode ON episode.id = asset.episode_id
               LEFT JOIN catalog.titles AS episode_title ON episode_title.id = episode.title_id
              WHERE asset.status = 'ready'
                AND asset.id > $1
              ORDER BY asset.id
              LIMIT $2",
        )
        .bind(after_asset_id)
        .bind(MEDIA_AUDIT_BATCH_SIZE)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;

        let canonical_root = tokio::fs::canonicalize(tmdb_media::MEDIA_ROOT)
            .await
            .map_err(|_| JobExecutionError::retry("media_unavailable", Duration::from_secs(5)))?;
        let repository = JobRepository::new(self.pool.clone());
        let mut summary = MediaAuditSummary::default();
        for row in &rows {
            summary.audited += 1;
            let primary_valid = verify_primary_asset(&canonical_root, row).await;
            let variants_valid = verify_asset_variants(&self.pool, &canonical_root, row.id).await?;
            if primary_valid && variants_valid {
                summary.valid += 1;
                continue;
            }
            summary.invalid += 1;
            if payload.repair && self.allow_local_media {
                match repair_job(row) {
                    Some(job) => match repository.submit(job).await {
                        Ok(_) => summary.repair_queued += 1,
                        Err(_) => {
                            return Err(JobExecutionError::retry(
                                "database_unavailable",
                                Duration::from_secs(5),
                            ));
                        }
                    },
                    None => summary.unrepairable += 1,
                }
            }
        }
        if i64::try_from(rows.len()).ok() == Some(MEDIA_AUDIT_BATCH_SIZE) {
            let last_asset_id = rows.last().map(|row| row.id).ok_or_else(|| {
                JobExecutionError::retry("execution_failed", Duration::from_secs(5))
            })?;
            let follow_up = NewJob::new(
                MEDIA_AUDIT_JOB,
                MEDIA_AUDIT_PAYLOAD_VERSION,
                json!({"repair": payload.repair, "afterAssetId": last_asset_id}),
                &format!("admin.media_audit:{}:{last_asset_id}", payload.repair),
            )
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            repository.submit(follow_up).await.map_err(|_| {
                JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
            })?;
            summary.next_audit_queued = true;
        }
        tracing::info!(
            event = "media_audit_completed",
            audited = summary.audited,
            valid = summary.valid,
            invalid = summary.invalid,
            repair_queued = summary.repair_queued,
            unrepairable = summary.unrepairable,
            next_audit_queued = summary.next_audit_queued,
        );
        Ok(summary)
    }
}

#[derive(Clone, Debug, FromRow)]
struct MediaAuditAssetRow {
    id: i64,
    source_key: String,
    source_url: Option<String>,
    storage_path: Option<String>,
    mime_type: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    file_size_bytes: Option<i64>,
    sha256: Option<String>,
    image_kind: String,
    language: Option<String>,
    entity_type: Option<String>,
    entity_id: Option<i64>,
    anime: bool,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    title_tmdb_id: Option<i64>,
}

#[derive(Clone, Debug, FromRow)]
struct MediaAuditVariantRow {
    storage_path: String,
    mime_type: String,
    width: i32,
    height: i32,
    file_size_bytes: i64,
    sha256: String,
}

async fn verify_asset_variants(
    pool: &PgPool,
    canonical_root: &Path,
    asset_id: i64,
) -> Result<bool, JobExecutionError> {
    let variants = sqlx::query_as::<_, MediaAuditVariantRow>(
        "SELECT storage_path, mime_type, width, height, file_size_bytes, sha256
           FROM assets.image_variants
          WHERE image_asset_id = $1
          ORDER BY variant_key",
    )
    .bind(asset_id)
    .fetch_all(pool)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    for variant in variants {
        if !verify_media_file(
            canonical_root,
            &variant.storage_path,
            &variant.mime_type,
            variant.width,
            variant.height,
            variant.file_size_bytes,
            &variant.sha256,
        )
        .await
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn verify_primary_asset(canonical_root: &Path, row: &MediaAuditAssetRow) -> bool {
    let (
        Some(storage_path),
        Some(mime_type),
        Some(width),
        Some(height),
        Some(file_size_bytes),
        Some(sha256),
    ) = (
        row.storage_path.as_deref(),
        row.mime_type.as_deref(),
        row.width,
        row.height,
        row.file_size_bytes,
        row.sha256.as_deref(),
    )
    else {
        return false;
    };
    verify_media_file(
        canonical_root,
        storage_path,
        mime_type,
        width,
        height,
        file_size_bytes,
        sha256,
    )
    .await
}

async fn verify_media_file(
    canonical_root: &Path,
    storage_path: &str,
    mime_type: &str,
    width: i32,
    height: i32,
    file_size_bytes: i64,
    sha256: &str,
) -> bool {
    if !tmdb_media::is_public_relative(storage_path)
        || !matches!(mime_type, "image/jpeg" | "image/webp")
        || width <= 0
        || height <= 0
        || file_size_bytes <= 0
        || u64::try_from(file_size_bytes)
            .ok()
            .is_none_or(|size| size > MAX_AUDIT_FILE_BYTES)
        || sha256.len() != 64
        || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }
    let candidate = canonical_root.join(storage_path);
    let canonical_file = match tokio::fs::canonicalize(candidate).await {
        Ok(file) if file.starts_with(canonical_root) => file,
        Ok(_) | Err(_) => return false,
    };
    let metadata = match tokio::fs::metadata(&canonical_file).await {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) | Err(_) => return false,
    };
    if metadata.len() != u64::try_from(file_size_bytes).unwrap_or_default()
        || mime_type_for_path(&canonical_file) != Some(mime_type)
    {
        return false;
    }
    let bytes = match tokio::fs::read(&canonical_file).await {
        Ok(bytes) if bytes.len() as u64 == metadata.len() => bytes,
        Ok(_) | Err(_) => return false,
    };
    let Ok(Ok(dimensions)) = tokio::task::spawn_blocking({
        let bytes = bytes.clone();
        move || image::load_from_memory(&bytes).map(|decoded| decoded.dimensions())
    })
    .await
    else {
        return false;
    };
    if dimensions != (width.cast_unsigned(), height.cast_unsigned()) {
        return false;
    }
    let actual = Sha256::digest(bytes);
    hex_digest_matches(&actual, sha256)
}

fn hex_digest_matches(digest: &[u8], expected: &str) -> bool {
    let mut actual = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut actual, "{byte:02x}");
    }
    actual.eq_ignore_ascii_case(expected)
}

fn mime_type_for_path(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("jpg" | "jpeg") => Some("image/jpeg"),
        Some("webp") => Some("image/webp"),
        _ => None,
    }
}

fn repair_job(row: &MediaAuditAssetRow) -> Option<NewJob> {
    let entity_type = row.entity_type.as_deref()?;
    let entity_id = row.entity_id?;
    let season_number = row
        .season_number
        .and_then(|number| u16::try_from(number).ok());
    let episode_number = row
        .episode_number
        .and_then(|number| u16::try_from(number).ok());
    let mut value = json!({
        "schemaVersion": crate::image::IMAGE_JOB_PAYLOAD_VERSION,
        "entityType": entity_type,
        "entityId": entity_id,
        "kind": row.image_kind,
        "tmdbPath": row.source_key,
        "sourceUrl": row.source_url,
        "language": row.language,
        "sourceRevision": Value::Null,
        "anime": row.anime,
        "seasonNumber": season_number,
        "episodeNumber": episode_number,
        "titleTmdbId": row.title_tmdb_id,
        "assetIndex": 1,
    });
    let payload = ImageJobPayload::from_json(&value).ok()?;
    value = payload.to_json().ok()?;
    let dedup_key = format!(
        "image:{entity_type}:{entity_id}:{}:{}",
        row.image_kind,
        source_digest(&row.source_key),
    );
    NewJob::new(
        crate::image::IMAGE_JOB_TYPE,
        crate::image::IMAGE_JOB_PAYLOAD_VERSION,
        value,
        &dedup_key,
    )
    .and_then(|job| job.with_priority(50))
    .and_then(|job| job.with_max_attempts(8))
    .ok()
}

fn source_digest(source_key: &str) -> String {
    format!("{:x}", Sha256::digest(source_key.as_bytes()))
}

#[async_trait]
impl<T> JobExecutor for ImageExecutor<T>
where
    T: ImageTransport + 'static,
{
    fn supported_job_types(&self) -> Option<&'static [&'static str]> {
        Some(IMAGE_JOB_TYPES)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one job execution boundary keeps outcome-specific structured logging adjacent to each failure path"
    )]
    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError> {
        let worker_job =
            parse_image_worker_job(job.job_type(), job.payload_version(), job.payload())?;
        let ImageWorkerJob::Download(payload) = worker_job else {
            return match worker_job {
                ImageWorkerJob::Noop => Ok(json!({"ok": true})),
                ImageWorkerJob::MediaAudit(payload) => {
                    let summary = self.audit_media(payload).await?;
                    serde_json::to_value(summary).map_err(|_| {
                        JobExecutionError::retry("execution_failed", Duration::from_secs(5))
                    })
                }
                ImageWorkerJob::Download(_) => unreachable!(),
            };
        };
        tracing::debug!(
            event = "image_job_started",
            job_id = %job.job_id().as_uuid(),
            attempt = job.attempts(),
            entity_type = image_entity_type_name(payload.entity_type),
            entity_id = payload.entity_id,
            image_kind = image_kind_name(payload.kind),
            anime = payload.anime,
        );
        if !self.allow_local_media {
            tracing::debug!(
                event = "image_job_skipped",
                job_id = %job.job_id().as_uuid(),
                reason = "local_media_disabled",
            );
            return Ok(json!({"skipped": "local_media_disabled"}));
        }
        {
            let image = match self.downloader.download(&payload).await {
                Ok(image) => image,
                Err(error) => {
                    let job_error = map_download_error(&error);
                    tracing::warn!(
                        event = "image_download_failed",
                        job_id = %job.job_id().as_uuid(),
                        entity_type = image_entity_type_name(payload.entity_type),
                        entity_id = payload.entity_id,
                        image_kind = image_kind_name(payload.kind),
                        failure_code = job_error.failure_code(),
                        failure_reason = image_download_reason(&error),
                        http_status = image_http_status(&error),
                    );
                    return Err(job_error);
                }
            };
            let stored = match self.store.publish(&payload, &image).await {
                Ok(stored) => stored,
                Err(error) => {
                    let job_error = map_storage_error(&error);
                    tracing::error!(
                        event = "image_publish_failed",
                        job_id = %job.job_id().as_uuid(),
                        entity_type = image_entity_type_name(payload.entity_type),
                        entity_id = payload.entity_id,
                        image_kind = image_kind_name(payload.kind),
                        failure_code = job_error.failure_code(),
                        storage_reason = storage_error_reason(&error),
                        storage_operation = storage_error_operation(&error),
                        io_kind = storage_io_kind(&error).unwrap_or("not_applicable"),
                    );
                    return Err(job_error);
                }
            };
            if let Err(error) = persist_ready(&self.pool, &payload, &stored.metadata).await {
                let job_error = map_persist_error(error);
                tracing::warn!(
                    event = "image_metadata_persist_failed",
                    job_id = %job.job_id().as_uuid(),
                    entity_type = image_entity_type_name(payload.entity_type),
                    entity_id = payload.entity_id,
                    image_kind = image_kind_name(payload.kind),
                    failure_code = job_error.failure_code(),
                    persistence_reason = persist_error_reason(error),
                );
                return Err(job_error);
            }
            tracing::debug!(
                event = "image_published",
                job_id = %job.job_id().as_uuid(),
                entity_type = image_entity_type_name(payload.entity_type),
                entity_id = payload.entity_id,
                image_kind = image_kind_name(payload.kind),
                source = image_source_name(image.source),
                deduplicated = stored.deduplicated,
                bytes = stored.metadata.byte_size,
            );
            serde_json::to_value(stored.metadata)
                .map(|metadata| json!({"metadata": metadata, "deduplicated": stored.deduplicated}))
                .map_err(|_| {
                    tracing::error!(
                        event = "image_result_serialization_failed",
                        job_id = %job.job_id().as_uuid(),
                        error_code = "execution_failed",
                    );
                    JobExecutionError::retry("execution_failed", Duration::from_secs(5))
                })
        }
    }
}

fn map_persist_error(error: crate::persistence::PersistError) -> JobExecutionError {
    match error {
        crate::persistence::PersistError::InvalidPayload => {
            JobExecutionError::dead_letter("invalid_payload")
        }
        crate::persistence::PersistError::OwnerNotFound
        | crate::persistence::PersistError::LanguageNotFound => {
            JobExecutionError::retry("entity_not_ready", Duration::from_secs(5))
        }
        crate::persistence::PersistError::Database => {
            JobExecutionError::retry("execution_failed", Duration::from_secs(5))
        }
    }
}

fn parse_image_worker_job(
    job_type: &str,
    payload_version: i32,
    payload: &Value,
) -> Result<ImageWorkerJob, JobExecutionError> {
    if job_type == "system.noop" && payload_version == 1 {
        return Ok(ImageWorkerJob::Noop);
    }
    if job_type == MEDIA_AUDIT_JOB && payload_version == MEDIA_AUDIT_PAYLOAD_VERSION {
        let parsed: MediaAuditPayload = serde_json::from_value(payload.clone())
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        if parsed.after_asset_id.is_some_and(|id| id <= 0) {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
        return Ok(ImageWorkerJob::MediaAudit(parsed));
    }
    if job_type != crate::image::IMAGE_JOB_TYPE
        || payload_version != crate::image::IMAGE_JOB_PAYLOAD_VERSION
    {
        return Err(JobExecutionError::retry(
            "invalid_payload",
            Duration::from_secs(5),
        ));
    }
    ImageJobPayload::from_json(payload)
        .map(ImageWorkerJob::Download)
        .map_err(|_| JobExecutionError::retry("invalid_payload", Duration::from_secs(5)))
}

fn map_download_error(error: &ImageError) -> JobExecutionError {
    let code = match error {
        ImageError::HttpStatus(429) => "rate_limited",
        ImageError::InvalidPolicy
        | ImageError::InvalidTrawlUrl
        | ImageError::DisallowedHost
        | ImageError::InvalidRedirect
        | ImageError::RedirectLimit
        | ImageError::UnsupportedMime
        | ImageError::InvalidImage
        | ImageError::ImageTooLarge
        | ImageError::TooLarge
        | ImageError::Truncated
        | ImageError::HttpStatus(400..=499) => "invalid_payload",
        ImageError::ChallengeDetected
        | ImageError::FallbackUnavailable
        | ImageError::BodyRead
        | ImageError::Transport(_)
        | ImageError::HttpStatus(_) => "upstream_unavailable",
    };
    JobExecutionError::retry(code, Duration::from_secs(5))
}

fn map_storage_error(_: &StorageError) -> JobExecutionError {
    JobExecutionError::retry("execution_failed", Duration::from_secs(5))
}

fn image_entity_type_name(entity_type: crate::image::ImageEntityType) -> &'static str {
    match entity_type {
        crate::image::ImageEntityType::Movie => "movie",
        crate::image::ImageEntityType::Tv => "tv",
        crate::image::ImageEntityType::Season => "season",
        crate::image::ImageEntityType::Episode => "episode",
        crate::image::ImageEntityType::Person => "person",
        crate::image::ImageEntityType::Collection => "collection",
        crate::image::ImageEntityType::Company => "company",
        crate::image::ImageEntityType::Network => "network",
    }
}

fn image_kind_name(kind: crate::image::ImageKind) -> &'static str {
    match kind {
        crate::image::ImageKind::Poster => "poster",
        crate::image::ImageKind::Backdrop => "backdrop",
        crate::image::ImageKind::Still => "still",
        crate::image::ImageKind::Profile => "profile",
        crate::image::ImageKind::Logo => "logo",
        crate::image::ImageKind::Banner => "banner",
        crate::image::ImageKind::Other => "other",
    }
}

fn image_source_name(source: crate::image::ImageSource) -> &'static str {
    match source {
        crate::image::ImageSource::Direct => "direct",
        crate::image::ImageSource::Trawl => "trawl",
    }
}

fn image_download_reason(error: &ImageError) -> &'static str {
    match error {
        ImageError::InvalidPolicy => "invalid_policy",
        ImageError::InvalidTrawlUrl => "invalid_trawl_url",
        ImageError::DisallowedHost => "disallowed_host",
        ImageError::InvalidRedirect => "invalid_redirect",
        ImageError::RedirectLimit => "redirect_limit",
        ImageError::ChallengeDetected => "challenge_detected",
        ImageError::FallbackUnavailable => "trawl_unavailable",
        ImageError::HttpStatus(_) => "http_status",
        ImageError::UnsupportedMime => "unsupported_mime",
        ImageError::InvalidImage => "invalid_image",
        ImageError::ImageTooLarge => "image_too_large",
        ImageError::TooLarge => "body_too_large",
        ImageError::Truncated => "body_truncated",
        ImageError::BodyRead => "body_read_failed",
        ImageError::Transport(_) => "transport_failed",
    }
}

fn image_http_status(error: &ImageError) -> u16 {
    match error {
        ImageError::HttpStatus(status) => *status,
        _ => 0,
    }
}

fn storage_error_reason(error: &StorageError) -> &'static str {
    match error {
        StorageError::InvalidRoot => "invalid_root",
        StorageError::InvalidPayload => "invalid_payload",
        StorageError::DigestMismatch => "digest_mismatch",
        StorageError::Io { .. } => "io",
        StorageError::DestinationConflict => "destination_conflict",
        StorageError::Derivative => "derivative_failed",
    }
}

fn storage_io_kind(error: &StorageError) -> Option<&'static str> {
    let StorageError::Io { source, .. } = error else {
        return None;
    };
    Some(match source.kind() {
        std::io::ErrorKind::PermissionDenied => "permission_denied",
        std::io::ErrorKind::ReadOnlyFilesystem => "read_only_filesystem",
        std::io::ErrorKind::StorageFull => "storage_full",
        std::io::ErrorKind::QuotaExceeded => "quota_exceeded",
        std::io::ErrorKind::NotFound => "not_found",
        std::io::ErrorKind::AlreadyExists => "already_exists",
        _ => "io_error",
    })
}

fn storage_error_operation(error: &StorageError) -> &'static str {
    match error {
        StorageError::Io { operation, .. } => operation.as_str(),
        _ => "not_applicable",
    }
}

fn persist_error_reason(error: crate::persistence::PersistError) -> &'static str {
    match error {
        crate::persistence::PersistError::InvalidPayload => "invalid_payload",
        crate::persistence::PersistError::OwnerNotFound => "owner_not_found",
        crate::persistence::PersistError::LanguageNotFound => "language_not_found",
        crate::persistence::PersistError::Database => "database",
    }
}

fn load_image_store() -> anyhow::Result<ImageStore> {
    ImageStore::fixed().map_err(|_| anyhow::anyhow!("image storage roots are invalid"))
}

fn load_downloader(source: EnvSource) -> anyhow::Result<ImageDownloader<ReqwestTransport>> {
    let max_bytes = parse_or(source, "TMDB_IMAGE_MAX_BYTES", 20_usize * 1024 * 1024)?;
    let max_redirects = parse_or(source, "TMDB_IMAGE_MAX_REDIRECTS", 3_usize)?;
    let timeout_seconds = parse_or(source, "TMDB_IMAGE_TIMEOUT_SECONDS", 30_u64)?;
    let hosts = match source.get("TMDB_IMAGE_ALLOWED_HOSTS") {
        Some(value) => value
            .into_string()
            .map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_IMAGE_ALLOWED_HOSTS is not valid Unicode")
            })?
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        None => vec![
            "image.tmdb.org".to_owned(),
            "media.themoviedb.org".to_owned(),
        ],
    };
    let policy = DownloadPolicy::new(
        max_bytes,
        max_redirects,
        Duration::from_secs(timeout_seconds),
        hosts,
    )
    .map_err(|_| anyhow::anyhow!("image download policy is invalid"))?;
    let transport = ReqwestTransport::new()
        .map_err(|_| anyhow::anyhow!("image HTTP client could not start"))?;
    let downloader = ImageDownloader::new(transport, policy);
    let trawl_base_url = source
        .get("TMDB_TRAWL_BASE_URL")
        .map(|value| {
            value.into_string().map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is not valid Unicode")
            })
        })
        .transpose()?;
    let Some(value) = trawl_base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(downloader);
    };

    let base = Url::parse(value)
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is invalid"))?;
    let fallback = HttpTrawlFallback::new(base)
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is invalid"))?;
    Ok(downloader.with_fallback(Arc::new(fallback)))
}

fn load_environment(source: EnvSource) -> anyhow::Result<Environment> {
    required(source, "TMDB_ENVIRONMENT")?
        .parse()
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_ENVIRONMENT is invalid"))
}

fn load_worker_config(source: EnvSource, default_id: &str) -> anyhow::Result<WorkerConfig> {
    let worker_id = match source
        .get("TMDB_IMAGE_WORKER_ID")
        .or_else(|| source.get("TMDB_WORKER_ID"))
    {
        Some(value) => value.into_string().map_err(|_| {
            anyhow::anyhow!("configuration field TMDB_WORKER_ID is not valid Unicode")
        })?,
        None => format!("{default_id}-{}", Uuid::now_v7()),
    };
    let lease = parse_or(source, "TMDB_WORKER_LEASE_SECONDS", 60_u64)?;
    let heartbeat = parse_or(source, "TMDB_WORKER_HEARTBEAT_SECONDS", 15_u64)?;
    let poll = parse_or(source, "TMDB_WORKER_IDLE_POLL_MS", 500_u64)?;
    WorkerConfig::try_new(
        WorkerId::new(&worker_id).map_err(|error| anyhow::anyhow!(error))?,
        Duration::from_secs(lease),
        Duration::from_secs(heartbeat),
        Duration::from_millis(poll),
    )
    .map_err(|error| anyhow::anyhow!(error))
}

fn load_image_worker_concurrency(source: EnvSource) -> anyhow::Result<usize> {
    let concurrency = parse_or(source, "TMDB_IMAGE_WORKER_CONCURRENCY", 4_usize)?;
    if !(1..=32).contains(&concurrency) {
        return Err(anyhow::anyhow!(
            "TMDB_IMAGE_WORKER_CONCURRENCY must be between 1 and 32"
        ));
    }
    Ok(concurrency)
}

fn image_worker_configs(
    base: WorkerConfig,
    concurrency: usize,
) -> anyhow::Result<Vec<WorkerConfig>> {
    if concurrency == 1 {
        return Ok(vec![base]);
    }
    (0..concurrency)
        .map(|index| {
            let worker_id = WorkerId::new(&format!("{}-{}", base.worker_id.as_str(), index + 1))
                .map_err(|error| anyhow::anyhow!(error))?;
            WorkerConfig::try_new(
                worker_id,
                base.lease_duration,
                base.heartbeat_interval,
                base.idle_poll_interval,
            )
            .map_err(|error| anyhow::anyhow!(error))
        })
        .collect()
}

fn required(source: EnvSource, name: &str) -> anyhow::Result<String> {
    source
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing configuration field {name}"))?
        .into_string()
        .map_err(|_| anyhow::anyhow!("configuration field {name} is not valid Unicode"))
}

fn parse_or<T>(source: EnvSource, name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
{
    match source.get(name) {
        Some(value) => value
            .into_string()
            .map_err(|_| anyhow::anyhow!("configuration field {name} is not valid Unicode"))?
            .parse()
            .map_err(|_| anyhow::anyhow!("configuration field {name} is invalid")),
        None => Ok(default),
    }
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::{ImageEntityType, ImageKind};
    use std::time::Duration;

    #[test]
    fn image_queue_readiness_sql_keeps_boolean_terms_separated() {
        assert!(!IMAGE_JOB_QUEUE_READY_SQL.contains("NULLAND"));
        assert_eq!(IMAGE_JOB_QUEUE_READY_SQL.matches(" AND ").count(), 2);
    }

    #[test]
    fn image_job_dispatch_validates_version_and_payload() -> Result<(), Box<dyn std::error::Error>>
    {
        let payload = ImageJobPayload::new(
            ImageEntityType::Movie,
            42,
            ImageKind::Poster,
            "/poster.jpg",
            "https://image.tmdb.org/t/p/original/poster.jpg",
            None,
            None,
        )?;
        let value = payload.to_json()?;
        assert!(matches!(
            parse_image_worker_job(crate::image::IMAGE_JOB_TYPE, 1, &value)?,
            ImageWorkerJob::Download(_)
        ));
        assert!(matches!(
            parse_image_worker_job("system.noop", 1, &json!({}))?,
            ImageWorkerJob::Noop
        ));
        assert!(matches!(
            parse_image_worker_job(MEDIA_AUDIT_JOB, 1, &json!({"repair": true}))?,
            ImageWorkerJob::MediaAudit(MediaAuditPayload {
                repair: true,
                after_asset_id: None
            })
        ));
        assert!(parse_image_worker_job(crate::image::IMAGE_JOB_TYPE, 2, &value).is_err());
        assert!(parse_image_worker_job(crate::image::IMAGE_JOB_TYPE, 1, &json!({})).is_err());
        assert!(
            parse_image_worker_job(
                MEDIA_AUDIT_JOB,
                1,
                &json!({"repair": true, "rawSql": "DROP"})
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn media_audit_verifies_exact_file_metadata_without_paths_outside_media()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let public_dir = root.path().join("movies/1");
        tokio::fs::create_dir_all(&public_dir).await?;
        let mut encoded = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(8, 4).write_to(&mut encoded, image::ImageFormat::Jpeg)?;
        let bytes = encoded.into_inner();
        let path = public_dir.join("cover.jpg");
        tokio::fs::write(&path, &bytes).await?;
        let root = tokio::fs::canonicalize(root.path()).await?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        assert!(
            verify_media_file(
                &root,
                "movies/1/cover.jpg",
                "image/jpeg",
                8,
                4,
                i64::try_from(bytes.len())?,
                &digest,
            )
            .await
        );
        assert!(
            !verify_media_file(
                &root,
                "../cover.jpg",
                "image/jpeg",
                8,
                4,
                i64::try_from(bytes.len())?,
                &digest,
            )
            .await
        );
        assert!(
            !verify_media_file(
                &root,
                "movies/1/cover.jpg",
                "image/jpeg",
                8,
                4,
                i64::try_from(bytes.len())?,
                &"0".repeat(64),
            )
            .await
        );
        Ok(())
    }

    #[test]
    fn media_audit_is_skipped_when_local_media_is_disabled() {
        let maybe_summary = disabled_media_audit_summary(false);
        assert!(
            maybe_summary.is_some(),
            "disabled media must produce an audit summary"
        );
        let Some(summary) = maybe_summary else {
            return;
        };
        assert!(summary.skipped);
        assert_eq!(summary.audited, 0);
        assert_eq!(summary.repair_queued, 0);
    }

    #[test]
    fn parallel_image_workers_receive_distinct_lease_ids() -> Result<(), Box<dyn std::error::Error>>
    {
        let base = WorkerConfig::try_new(
            WorkerId::new("tmdb-media")?,
            Duration::from_mins(1),
            Duration::from_secs(15),
            Duration::from_millis(500),
        )?;
        let configs = image_worker_configs(base, 4)?;
        let worker_ids = configs
            .iter()
            .map(|config| config.worker_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            worker_ids,
            vec![
                "tmdb-media-1",
                "tmdb-media-2",
                "tmdb-media-3",
                "tmdb-media-4",
            ]
        );
        Ok(())
    }

    #[test]
    fn image_worker_concurrency_one_preserves_the_configured_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = WorkerConfig::try_new(
            WorkerId::new("tmdb-media")?,
            Duration::from_mins(1),
            Duration::from_secs(15),
            Duration::from_millis(500),
        )?;
        let configs = image_worker_configs(base, 1)?;
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].worker_id.as_str(), "tmdb-media");
        Ok(())
    }

    #[test]
    fn image_storage_diagnostics_keep_permission_failures_actionable_and_bounded() {
        let error = StorageError::Io {
            operation: crate::image::StorageOperation::PrepareDestinationDirectory,
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert_eq!(storage_error_reason(&error), "io");
        assert_eq!(
            storage_error_operation(&error),
            "prepare_destination_directory"
        );
        assert_eq!(storage_io_kind(&error), Some("permission_denied"));
        assert_eq!(map_storage_error(&error).failure_code(), "execution_failed");
        assert_eq!(
            image_download_reason(&ImageError::HttpStatus(503)),
            "http_status"
        );
        assert_eq!(image_http_status(&ImageError::HttpStatus(503)), 503);
    }

    #[sqlx::test(migrations = false)]
    async fn image_worker_reports_queue_unready_before_migrations(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        assert!(!image_job_queue_ready(&pool).await?);
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn image_worker_waits_for_the_current_media_schema(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query("CREATE SCHEMA ops").execute(&pool).await?;
        sqlx::query(
            "CREATE FUNCTION ops.claim_job_for_types(text, bigint, text[]) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$",
        )
        .execute(&pool)
        .await?;

        assert!(!image_job_queue_ready(&pool).await?);

        sqlx::query("CREATE SCHEMA assets").execute(&pool).await?;
        sqlx::query("CREATE TABLE assets.image_variants (id bigint PRIMARY KEY)")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE FUNCTION ops.record_component_heartbeat(text, text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$",
        )
        .execute(&pool)
        .await?;

        assert!(image_job_queue_ready(&pool).await?);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn image_worker_reports_queue_ready_after_migrations(pool: PgPool) -> sqlx::Result<()> {
        assert!(image_job_queue_ready(&pool).await?);
        Ok(())
    }
}
