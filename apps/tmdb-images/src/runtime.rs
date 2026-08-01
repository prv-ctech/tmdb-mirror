use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::image::{
    DownloadPolicy, HttpTrawlFallback, ImageDownloader, ImageError, ImageJobPayload, ImageStore,
    ImageTransport, ReqwestTransport, TrawlFallback,
};
use crate::media_server;
use crate::persistence::persist_ready;
use anyhow::{Context, bail};
use async_trait::async_trait;
use reqwest::Url;
use serde_json::{Value, json};
use sqlx::PgPool;
use tmdb_config::{
    ConfigSource, DatabaseConfig, EnvSource, Environment, load_secret_for_environment,
};
use tmdb_db::{PoolPolicy, connect_direct};
use tmdb_jobs::{
    ClaimedJob, JobExecutionError, JobExecutor, JobRepository, Worker, WorkerConfig, WorkerId,
};
use tmdb_observability::{LogFormat, init_tracing};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const IMAGE_JOB_TYPES: &[&str] = &[crate::image::IMAGE_JOB_TYPE, "system.noop"];

/// Starts the direct-database image worker shell.
pub async fn run() -> anyhow::Result<()> {
    init_tracing(env!("CARGO_PKG_NAME"), LogFormat::Json)
        .map_err(|error| anyhow::anyhow!(error))?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let database = load_database(source, "image_writer", environment)?;
    let worker_config = load_worker_config(source, "tmdb-images")?;
    let store = load_image_store()?;
    let downloader = load_downloader(source)?;
    let allow_local_media = parse_or(source, "ALLOW_LOCAL_MEDIA", false)?;
    let media_bind = parse_or(source, "TMDB_MEDIA_BIND", "0.0.0.0:8090".to_owned())?
        .parse::<SocketAddr>()
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_MEDIA_BIND is invalid"))?;
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect image database")?;
    let worker = Worker::new(
        JobRepository::new(pool.clone()),
        ImageExecutor {
            downloader,
            store,
            pool: pool.clone(),
            allow_local_media,
        },
        worker_config,
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
    let media_server = tokio::spawn(async move {
        media_server::run(
            media_bind,
            std::path::PathBuf::from(tmdb_media::MEDIA_ROOT),
            media_cancellation,
        )
        .await
    });
    let result = worker.run(cancellation.clone()).await;
    cancellation.cancel();
    if let Ok(Err(error)) = media_server.await {
        tracing::error!(event = "media_server_stopped", error = %error);
    }
    pool.close().await;
    result.map_err(|error| anyhow::anyhow!(error))
}

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

    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError> {
        let Some(payload) = parse_image_job(job.job_type(), job.payload_version(), job.payload())?
        else {
            return Ok(json!({"ok": true}));
        };
        if !self.allow_local_media {
            return Ok(json!({"skipped": "local_media_disabled"}));
        }
        {
            let image = self
                .downloader
                .download(&payload)
                .await
                .map_err(|error| map_download_error(&error))?;
            let stored = self.store.publish(&payload, &image).await.map_err(|_| {
                JobExecutionError::retry("execution_failed", Duration::from_secs(5))
            })?;
            persist_ready(&self.pool, &payload, &stored.metadata)
                .await
                .map_err(map_persist_error)?;
            serde_json::to_value(stored.metadata)
                .map(|metadata| json!({"metadata": metadata, "deduplicated": stored.deduplicated}))
                .map_err(|_| JobExecutionError::retry("execution_failed", Duration::from_secs(5)))
        }
    }
}

fn map_persist_error(error: crate::persistence::PersistError) -> JobExecutionError {
    match error {
        crate::persistence::PersistError::InvalidPayload
        | crate::persistence::PersistError::OwnerConflict => {
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

fn parse_image_job(
    job_type: &str,
    payload_version: i32,
    payload: &Value,
) -> Result<Option<ImageJobPayload>, JobExecutionError> {
    if job_type == "system.noop" && payload_version == 1 {
        return Ok(None);
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
        .map(Some)
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
    match source.get("TMDB_TRAWL_BASE_URL") {
        Some(value) => {
            let value = value.into_string().map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is not valid Unicode")
            })?;
            let base = Url::parse(&value).map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is invalid")
            })?;
            let fallback = HttpTrawlFallback::new(base).map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_TRAWL_BASE_URL is invalid")
            })?;
            Ok(downloader.with_fallback(Arc::new(fallback)))
        }
        None => Ok(downloader),
    }
}

fn load_database(
    source: EnvSource,
    expected_role: &str,
    environment: Environment,
) -> anyhow::Result<DatabaseConfig> {
    let username = required(source, "TMDB_DIRECT_DB_USER")?;
    if username != expected_role {
        bail!("TMDB_DIRECT_DB_USER must select the {expected_role} role");
    }
    Ok(DatabaseConfig {
        host: required(source, "TMDB_DIRECT_DB_HOST")?,
        port: parse(source, "TMDB_DIRECT_DB_PORT")?,
        database: required(source, "TMDB_DIRECT_DB_NAME")?,
        username,
        password: load_secret_for_environment(&source, "TMDB_DIRECT_DB_PASSWORD", environment)?,
    })
}

fn load_environment(source: EnvSource) -> anyhow::Result<Environment> {
    required(source, "TMDB_ENVIRONMENT")?
        .parse()
        .map_err(|_| anyhow::anyhow!("configuration field TMDB_ENVIRONMENT is invalid"))
}

fn load_worker_config(source: EnvSource, default_id: &str) -> anyhow::Result<WorkerConfig> {
    let worker_id = match source.get("TMDB_WORKER_ID") {
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

fn required(source: EnvSource, name: &str) -> anyhow::Result<String> {
    source
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing configuration field {name}"))?
        .into_string()
        .map_err(|_| anyhow::anyhow!("configuration field {name} is not valid Unicode"))
}

fn parse<T>(source: EnvSource, name: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
{
    required(source, name)?
        .parse()
        .map_err(|_| anyhow::anyhow!("configuration field {name} is invalid"))
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
        assert!(parse_image_job(crate::image::IMAGE_JOB_TYPE, 1, &value)?.is_some());
        assert!(parse_image_job("system.noop", 1, &json!({}))?.is_none());
        assert!(parse_image_job(crate::image::IMAGE_JOB_TYPE, 2, &value).is_err());
        assert!(parse_image_job(crate::image::IMAGE_JOB_TYPE, 1, &json!({})).is_err());
        Ok(())
    }
}
