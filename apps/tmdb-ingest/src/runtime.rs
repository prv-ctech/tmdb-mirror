use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tmdb_config::{
    ConfigSource, EnvSource, Environment, load_database_for_role, load_secret_for_environment,
};
use tmdb_db::{PoolPolicy, connect_direct, migrate};
use tmdb_jobs::{JobRepository, Worker, WorkerConfig, WorkerId};
use tmdb_media::{RAW_ROOT, RuntimeStorageRole, prepare_runtime_storage};
use tmdb_observability::init_tracing_from_env;
use tmdb_upstream::{MAX_DAILY_EXPORT_BYTES, RateLimitPolicy, RetryPolicy, TmdbClient};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::jobs::IngestExecutor;

const MAX_INGEST_WORKER_CONCURRENCY: usize = 64;
const COMPONENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Starts the direct-database ingestion worker shell.
///
/// # Errors
///
/// Returns an error when configuration, database connectivity, or the worker
/// loop cannot be started.
pub async fn run() -> anyhow::Result<()> {
    init_tracing_from_env(env!("CARGO_PKG_NAME")).map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(event = "ingest_worker_starting");
    prepare_worker_storage()?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let database = load_database_for_role(&source, environment, "ingest_writer")?;
    let tmdb_client = load_tmdb_client(source, environment)?;
    let export_root = std::path::PathBuf::from(RAW_ROOT);
    let export_max_bytes = parse_or(
        source,
        "TMDB_DAILY_EXPORT_MAX_BYTES",
        MAX_DAILY_EXPORT_BYTES,
    )?;
    let ingest_executor = IngestExecutor::with_export_root(tmdb_client, export_root)
        .with_local_media(parse_or(source, "ALLOW_LOCAL_MEDIA", false)?)
        .with_export_max_bytes(export_max_bytes)
        .map_err(|error| anyhow::anyhow!(error))?;
    let worker_config = load_worker_config(source, "tmdb-ingest")?;
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect ingest database")?;
    let ingest_executor = ingest_executor.with_database(pool.clone());
    let worker = Worker::new(
        JobRepository::new(pool.clone()),
        ingest_executor,
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
    let heartbeat = spawn_component_heartbeat(pool.clone(), "worker", cancellation.clone());
    let result = worker.run(cancellation).await;
    let _ = heartbeat.await;
    pool.close().await;
    result.map_err(|error| anyhow::anyhow!(error))
}

/// Starts the consolidated main worker. It applies migrations under the
/// database-level migration lock, then waits for explicitly submitted jobs.
///
/// # Errors
///
/// Returns an error when configuration, migration, database connectivity, or
/// the worker loop cannot be started.
pub async fn run_worker() -> anyhow::Result<()> {
    init_tracing_from_env(env!("CARGO_PKG_NAME")).map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(event = "main_worker_starting");
    prepare_worker_storage()?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let migrator = load_database_for_role(&source, environment, "migrator")?;
    let migration_pool = connect_direct(&migrator, PoolPolicy::Migrator)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect migration database")?;
    tracing::info!(event = "database_migration_starting");
    migrate(&migration_pool, &migrator.username)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("apply database migrations")?;
    tracing::info!(event = "database_migration_complete");
    migration_pool.close().await;

    let database = load_database_for_role(&source, environment, "ingest_writer")?;
    let tmdb_client = load_tmdb_client(source, environment)?;
    let export_max_bytes = parse_or(
        source,
        "TMDB_DAILY_EXPORT_MAX_BYTES",
        MAX_DAILY_EXPORT_BYTES,
    )?;
    let ingest_executor =
        IngestExecutor::with_export_root(tmdb_client, std::path::PathBuf::from(RAW_ROOT))
            .with_local_media(parse_or(source, "ALLOW_LOCAL_MEDIA", false)?)
            .with_export_max_bytes(export_max_bytes)
            .map_err(|error| anyhow::anyhow!(error))?;
    let worker_config = load_worker_config(source, "tmdb-worker")?;
    let worker_concurrency = load_ingest_worker_concurrency(&source)?;
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect worker database")?;
    let startup_state: String = sqlx::query_scalar("SELECT ops.stop_worker_on_startup('ingest')")
        .fetch_one(&pool)
        .await
        .context("reset ingest worker state after restart")?;
    // A restart must not create an implicit scan or synchronization run.
    // Operators submit those operations through the authenticated admin API.
    tracing::info!(event = "catalog_seed_not_automatic", startup_state);
    let executor = ingest_executor.with_database(pool.clone());
    let workers = ingest_worker_configs(worker_config, worker_concurrency)?
        .into_iter()
        .map(|config| Worker::new(JobRepository::new(pool.clone()), executor.clone(), config))
        .collect();
    tracing::info!(
        event = "main_worker_ready",
        ingest_workers = worker_concurrency,
        automatic_work_disabled = true,
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
    let heartbeat = spawn_component_heartbeat(pool.clone(), "worker", cancellation.clone());
    let result = run_ingest_workers(workers, cancellation.clone()).await;
    cancellation.cancel();
    let _ = heartbeat.await;
    pool.close().await;
    result.map_err(|error| anyhow::anyhow!(error))
}

fn spawn_component_heartbeat(
    pool: sqlx::PgPool,
    component: &'static str,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(COMPONENT_HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = interval.tick() => {
                    if sqlx::query_scalar::<_, chrono::DateTime<Utc>>(
                        "SELECT ops.record_component_heartbeat($1, 'ready')",
                    )
                    .bind(component)
                    .fetch_one(&pool)
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            event = "component_heartbeat_failed",
                            component,
                            error_code = "database_unavailable",
                        );
                    }
                }
            }
        }
    })
}

fn prepare_worker_storage() -> anyhow::Result<()> {
    prepare_runtime_storage(RuntimeStorageRole::Worker).map_err(|error| {
        tracing::error!(
            event = "storage_preflight_failed",
            role = RuntimeStorageRole::Worker.as_str(),
            path = error.path().as_str(),
            operation = error.operation(),
            io_kind = error.io_kind().unwrap_or("not_applicable"),
        );
        anyhow::anyhow!(
            "worker storage preflight failed at {} ({})",
            error.path().as_str(),
            error.operation(),
        )
    })?;
    tracing::info!(
        event = "storage_preflight_ready",
        role = RuntimeStorageRole::Worker.as_str()
    );
    Ok(())
}

async fn run_ingest_workers(
    workers: Vec<Worker<IngestExecutor>>,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
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
                return Err(anyhow::anyhow!("ingest worker stopped unexpectedly"));
            }
            Ok(Err(error)) => {
                cancellation.cancel();
                tasks.abort_all();
                return Err(anyhow::anyhow!(error));
            }
            Err(error) => {
                cancellation.cancel();
                tasks.abort_all();
                return Err(anyhow::anyhow!("ingest worker task failed: {error}"));
            }
        }
    }
    Ok(())
}

fn load_tmdb_client(source: EnvSource, environment: Environment) -> anyhow::Result<TmdbClient> {
    let base_url = required(source, "TMDB_API_BASE_URL")?;
    let token = load_secret_for_environment(&source, "TMDB_READ_ACCESS_TOKEN", environment)
        .map_err(|error| anyhow::anyhow!(error))?;
    let rate = parse_or(source, "TMDB_RATE_LIMIT", 35_u32)?;
    let concurrency = parse_or(source, "TMDB_MAX_CONNECTIONS", 20_u32)?;
    let attempts = parse_or(source, "TMDB_MAX_ATTEMPTS", 4_u8)?;
    let timeout_seconds = parse_or(source, "TMDB_REQUEST_TIMEOUT_SECONDS", 30_u64)?;
    let rate_limit =
        RateLimitPolicy::try_new(rate, concurrency).map_err(|error| anyhow::anyhow!(error))?;
    let policy = RetryPolicy::try_new(
        attempts,
        rate_limit,
        Duration::from_secs(timeout_seconds),
        Duration::from_millis(250),
        Duration::from_secs(15),
        16 * 1024 * 1024,
    )
    .map_err(|error| anyhow::anyhow!(error))?;
    TmdbClient::new(&base_url, token, policy).map_err(|error| anyhow::anyhow!(error))
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

fn load_ingest_worker_concurrency(source: &impl ConfigSource) -> anyhow::Result<usize> {
    let upstream_connections = match source.get("TMDB_MAX_CONNECTIONS") {
        Some(value) => value
            .into_string()
            .map_err(|_| {
                anyhow::anyhow!("configuration field TMDB_MAX_CONNECTIONS is not valid Unicode")
            })?
            .parse::<u32>()
            .map_err(|_| anyhow::anyhow!("configuration field TMDB_MAX_CONNECTIONS is invalid"))?,
        None => 64,
    };
    let upstream_connections = usize::try_from(upstream_connections)
        .map_err(|_| anyhow::anyhow!("TMDB_MAX_CONNECTIONS is too large"))?;
    Ok(upstream_connections.clamp(1, MAX_INGEST_WORKER_CONCURRENCY))
}

fn ingest_worker_configs(
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
    use tmdb_config::MapSource;

    #[test]
    fn ingest_concurrency_can_hide_upstream_latency_without_raising_the_rate_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MapSource::from([("TMDB_MAX_CONNECTIONS", "64")]);

        assert_eq!(load_ingest_worker_concurrency(&source)?, 64);
        Ok(())
    }

    #[test]
    fn parallel_ingest_workers_receive_distinct_lease_ids() -> Result<(), Box<dyn std::error::Error>>
    {
        let base = WorkerConfig::try_new(
            WorkerId::new("tmdb-worker")?,
            Duration::from_mins(1),
            Duration::from_secs(15),
            Duration::from_millis(500),
        )?;
        let configs = ingest_worker_configs(base, 4)?;
        let worker_ids = configs
            .iter()
            .map(|config| config.worker_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            worker_ids,
            vec![
                "tmdb-worker-1",
                "tmdb-worker-2",
                "tmdb-worker-3",
                "tmdb-worker-4"
            ]
        );
        Ok(())
    }
}
