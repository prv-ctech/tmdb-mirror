use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{Days, Utc};
use tmdb_config::{
    ConfigSource, DatabaseConfig, EnvSource, Environment, load_secret_for_environment,
};
use tmdb_db::{PoolPolicy, connect_direct, migrate};
use tmdb_jobs::{JobRepository, NewJob, Worker, WorkerConfig, WorkerId};
use tmdb_media::RAW_ROOT;
use tmdb_observability::{LogFormat, init_tracing};
use tmdb_upstream::{MAX_DAILY_EXPORT_BYTES, RateLimitPolicy, RetryPolicy, TmdbClient};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::jobs::IngestExecutor;

/// Starts the direct-database ingestion worker shell.
///
/// # Errors
///
/// Returns an error when configuration, database connectivity, or the worker
/// loop cannot be started.
pub async fn run() -> anyhow::Result<()> {
    init_tracing(env!("CARGO_PKG_NAME"), LogFormat::Json)
        .map_err(|error| anyhow::anyhow!(error))?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let database = load_database(source, "ingest_writer", environment)?;
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
    let result = worker.run(cancellation).await;
    pool.close().await;
    result.map_err(|error| anyhow::anyhow!(error))
}

/// Starts the consolidated main worker.  It applies migrations under the
/// database-level migration lock, then runs ingestion and the durable
/// scheduler in one process.
///
/// # Errors
///
/// Returns an error when configuration, migration, database connectivity, or
/// the worker loop cannot be started.
pub async fn run_worker() -> anyhow::Result<()> {
    init_tracing(env!("CARGO_PKG_NAME"), LogFormat::Json)
        .map_err(|error| anyhow::anyhow!(error))?;
    let source = EnvSource;
    let environment = load_environment(source)?;
    let migrator = load_database_with_prefix(source, "TMDB_MIGRATOR_DB", "migrator", environment)?;
    let migration_pool = connect_direct(&migrator, PoolPolicy::Migrator)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect migration database")?;
    migrate(&migration_pool)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("apply database migrations")?;
    migration_pool.close().await;

    let database = load_database(source, "ingest_writer", environment)?;
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
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect worker database")?;
    let worker = Worker::new(
        JobRepository::new(pool.clone()),
        ingest_executor.with_database(pool.clone()),
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
    let scheduler_cancellation = cancellation.clone();
    let scheduler_pool = pool.clone();
    let scheduler =
        tokio::spawn(
            async move { run_scheduler(scheduler_pool, scheduler_cancellation, source).await },
        );
    let result = worker.run(cancellation.clone()).await;
    cancellation.cancel();
    let _ = scheduler.await;
    pool.close().await;
    result.map_err(|error| anyhow::anyhow!(error))
}

async fn run_scheduler(
    pool: sqlx::PgPool,
    cancellation: CancellationToken,
    source: EnvSource,
) -> anyhow::Result<()> {
    if !parse_or(source, "TMDB_ENABLE_SCHEDULER", true)? {
        cancellation.cancelled().await;
        return Ok(());
    }
    let interval_seconds = parse_or(source, "TMDB_SCHEDULER_INTERVAL_SECONDS", 60_u64)?;
    if interval_seconds == 0 {
        bail!("TMDB_SCHEDULER_INTERVAL_SECONDS must be positive");
    }
    let daily_export_enabled = parse_or(source, "TMDB_ENABLE_DAILY_EXPORT", true)?;
    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            _ = interval.tick() => scheduler_tick(&pool, interval_seconds, daily_export_enabled).await?,
        }
    }
}

async fn scheduler_tick(
    pool: &sqlx::PgPool,
    interval_seconds: u64,
    daily_export_enabled: bool,
) -> anyhow::Result<()> {
    let bucket = Utc::now().timestamp().div_euclid(
        i64::try_from(interval_seconds)
            .map_err(|_| anyhow::anyhow!("scheduler interval is too large"))?,
    );
    for media_type in ["movie", "tv"] {
        let schedule_key = format!("changes_sync:{media_type}");
        let run_key = bucket.to_string();
        if !claim_schedule_slot(pool, &schedule_key, &run_key).await? {
            continue;
        }
        let payload = serde_json::json!({"media_type": media_type, "page": 1});
        let job = NewJob::new(
            "ingest.changes_sync",
            1,
            payload,
            &format!("ingest.changes_sync:{media_type}:{bucket}"),
        )?;
        JobRepository::new(pool.clone()).submit(job).await?;
    }
    if daily_export_enabled {
        // TMDB publishes the previous UTC day's exports.  The schedule key is
        // date-based, so a short scheduler interval cannot duplicate a job.
        let export_date = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(1))
            .ok_or_else(|| anyhow::anyhow!("scheduler date underflow"))?;
        let date_text = export_date.format("%m_%d_%Y").to_string();
        for media_type in ["movie", "tv"] {
            let schedule_key = format!("daily_export:{media_type}");
            if !claim_schedule_slot(pool, &schedule_key, &date_text).await? {
                continue;
            }
            let file_prefix = if media_type == "movie" {
                "movie_ids"
            } else {
                "tv_series_ids"
            };
            let url = format!("https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz");
            let job = NewJob::new(
                "ingest.daily_export",
                1,
                serde_json::json!({"media_type": media_type, "url": url}),
                &format!("ingest.daily_export:{media_type}:{date_text}"),
            )?;
            JobRepository::new(pool.clone()).submit(job).await?;
        }
    }
    // Scheduler checkpoints are bounded retention state, not the catalog.
    sqlx::query(
        "DELETE FROM ops.scheduler_runs WHERE created_at < clock_timestamp() - interval '90 days'",
    )
    .execute(pool)
    .await?;
    // Terminal job rows and their immutable events are pruned through the
    // security-definer function installed by migration 0014.  The worker has
    // no direct table-write privilege for ops.jobs.
    sqlx::query_scalar::<_, i32>(
        "SELECT ops.prune_finished_jobs(clock_timestamp() - interval '90 days', 1000)",
    )
    .fetch_one(pool)
    .await?;
    Ok(())
}

async fn claim_schedule_slot(
    pool: &sqlx::PgPool,
    schedule_key: &str,
    run_key: &str,
) -> anyhow::Result<bool> {
    let inserted: Option<bool> = sqlx::query_scalar(
        "INSERT INTO ops.scheduler_runs (schedule_key, run_key)
         VALUES ($1, $2)
         ON CONFLICT DO NOTHING
         RETURNING true",
    )
    .bind(schedule_key)
    .bind(run_key)
    .fetch_optional(pool)
    .await?;
    Ok(inserted.is_some())
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

fn load_database_with_prefix(
    source: EnvSource,
    prefix: &str,
    expected_role: &str,
    environment: Environment,
) -> anyhow::Result<DatabaseConfig> {
    let username = required(source, &format!("{prefix}_USER"))?;
    if username != expected_role {
        bail!("{prefix}_USER must select the {expected_role} role");
    }
    Ok(DatabaseConfig {
        host: required(source, &format!("{prefix}_HOST"))?,
        port: parse(source, &format!("{prefix}_PORT"))?,
        database: required(source, &format!("{prefix}_NAME"))?,
        username,
        password: load_secret_for_environment(&source, &format!("{prefix}_PASSWORD"), environment)?,
    })
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
