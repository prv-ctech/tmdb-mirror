use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::image::{
    DownloadPolicy, HttpTrawlFallback, ImageDownloader, ImageError, ImageJobPayload, ImageStore,
    ImageTransport, ReqwestTransport, StorageError, TrawlFallback,
};
use crate::media_server;
use crate::persistence::persist_ready;
use crate::requests::{self, CoordinatorConfig};
use anyhow::Context;
use async_trait::async_trait;
use reqwest::Url;
use serde_json::{Value, json};
use sqlx::PgPool;
use tmdb_config::{ConfigSource, EnvSource, Environment, load_database_for_role};
use tmdb_db::{PoolPolicy, connect_direct_for_startup};
use tmdb_jobs::{
    ClaimedJob, JobExecutionError, JobExecutor, JobRepository, Worker, WorkerConfig, WorkerId,
};
use tmdb_media::{RuntimeStorageRole, prepare_runtime_storage};
use tmdb_observability::init_tracing_from_env;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const COMPONENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const IMAGE_JOB_TYPES: &[&str] = &[crate::image::IMAGE_JOB_TYPE, "system.noop"];
const IMAGE_QUEUE_READY_RETRY: Duration = Duration::from_secs(1);
const DEFAULT_MEDIA_BIND: &str = "0.0.0.0:9002";

#[derive(Clone, Debug, Eq, PartialEq)]
enum ImageWorkerJob {
    Noop,
    Download(ImageJobPayload),
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
    let coordinator_config = CoordinatorConfig {
        worker_id: format!("{}-requests", worker_config.worker_id.as_str()),
        lease_duration: worker_config.lease_duration,
        idle_poll_interval: worker_config.idle_poll_interval,
    };
    let worker_concurrency = load_image_worker_concurrency(source)?;
    let store = load_image_store()?;
    let downloader = load_downloader(source)?;
    let allow_local_media = parse_or(source, "ALLOW_LOCAL_MEDIA", false)?;
    let trawl_fallback_configured =
        std::env::var("TMDB_TRAWL_BASE_URL").is_ok_and(|value| !value.trim().is_empty());
    let media_bind = parse_or(source, "TMDB_MEDIA_BIND", DEFAULT_MEDIA_BIND.to_owned())?
        .parse::<SocketAddr>()
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_MEDIA_BIND is invalid"))?;
    let pool = connect_direct_for_startup(&database, PoolPolicy::ReadWrite)
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
    let startup_state: String = sqlx::query_scalar("SELECT ops.start_worker_on_startup('media')")
        .fetch_one(&pool)
        .await
        .context("start media worker queue")?;
    tracing::info!(event = "media_worker_control_ready", startup_state);
    let heartbeat = spawn_component_heartbeat(pool.clone(), cancellation.clone());
    let coordinator = tokio::spawn(requests::run(
        pool.clone(),
        coordinator_config,
        cancellation.clone(),
    ));
    let result = run_workers(workers, cancellation.clone()).await;
    cancellation.cancel();
    let _ = heartbeat.await;
    let _ = coordinator.await;
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
    "AND pg_catalog.to_regprocedure('ops.claim_media_request(text,bigint)') IS NOT NULL ",
    "AND pg_catalog.to_regprocedure('assets.select_media_request_sources(uuid,bigint,integer)') IS NOT NULL",
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
            return Ok(json!({"ok": true}));
        };
        tracing::debug!(
            event = "image_job_started",
            job_id = %job.job_id().as_uuid(),
            attempt = job.attempts(),
            entity_type = image_entity_type_name(payload.entity_type),
            entity_id = payload.entity_id,
            image_kind = image_kind_name(payload.kind),
        );
        if !self.allow_local_media {
            tracing::error!(
                event = "image_job_rejected",
                job_id = %job.job_id().as_uuid(),
                reason = "local_media_disabled",
            );
            return Err(JobExecutionError::dead_letter("local_media_disabled"));
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
    fn media_listener_default_is_the_container_contract() {
        assert_eq!(DEFAULT_MEDIA_BIND, "0.0.0.0:9002");
    }

    #[test]
    fn image_queue_readiness_sql_keeps_boolean_terms_separated() {
        assert!(!IMAGE_JOB_QUEUE_READY_SQL.contains("NULLAND"));
        assert_eq!(IMAGE_JOB_QUEUE_READY_SQL.matches(" AND ").count(), 3);
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
        assert!(parse_image_worker_job(crate::image::IMAGE_JOB_TYPE, 2, &value).is_err());
        assert!(parse_image_worker_job(crate::image::IMAGE_JOB_TYPE, 1, &json!({})).is_err());
        assert!(parse_image_worker_job("media.audit", 1, &json!({})).is_err());
        assert!(parse_image_worker_job("admin.media_scan", 1, &json!({})).is_err());
        Ok(())
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
        sqlx::query(
            "CREATE FUNCTION ops.claim_media_request(text, bigint) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE FUNCTION assets.select_media_request_sources(uuid, bigint, integer) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$",
        )
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
