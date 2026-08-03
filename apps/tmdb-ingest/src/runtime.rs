use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Days, NaiveDate, Utc};
use tmdb_config::{
    ConfigSource, EnvSource, Environment, load_database_for_role, load_secret_for_environment,
};
use tmdb_db::{PoolPolicy, connect_direct, migrate};
use tmdb_jobs::{JobRepository, NewJob, Worker, WorkerConfig, WorkerId};
use tmdb_media::{RAW_ROOT, RuntimeStorageRole, prepare_runtime_storage};
use tmdb_observability::init_tracing_from_env;
use tmdb_upstream::{MAX_DAILY_EXPORT_BYTES, RateLimitPolicy, RetryPolicy, TmdbClient};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::jobs::{DAILY_EXPORT_JOB, INGEST_PAYLOAD_VERSION, IngestExecutor, TRENDING_REFRESH_JOB};

const DAILY_EXPORT_REFRESH_PRIORITY: i16 = -100;
const MAX_INGEST_WORKER_CONCURRENCY: usize = 8;
const COMPONENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const TRENDING_DAY_REFRESH_SECONDS: i64 = 30 * 60;
const TRENDING_WEEK_REFRESH_SECONDS: i64 = 6 * 60 * 60;

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

/// Starts the consolidated main worker.  It applies migrations under the
/// database-level migration lock, then runs ingestion and the durable
/// scheduler in one process.
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
    let worker_concurrency = load_ingest_worker_concurrency(source)?;
    let pool = connect_direct(&database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect worker database")?;
    let daily_export_enabled = parse_or(source, "TMDB_ENABLE_DAILY_EXPORT", true)?;
    // A restart must not create an implicit full scan. Operators request one
    // through POST /admin/v1/scans; the scheduler still handles its explicitly
    // configured incremental/daily export cadence after the worker is ready.
    tracing::info!(event = "catalog_seed_not_automatic", daily_export_enabled,);
    let executor = ingest_executor.with_database(pool.clone());
    let workers = ingest_worker_configs(worker_config, worker_concurrency)?
        .into_iter()
        .map(|config| Worker::new(JobRepository::new(pool.clone()), executor.clone(), config))
        .collect();
    tracing::info!(
        event = "main_worker_ready",
        ingest_workers = worker_concurrency,
        daily_export_enabled,
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
    let scheduler_cancellation = cancellation.clone();
    let scheduler_pool = pool.clone();
    let scheduler =
        tokio::spawn(
            async move { run_scheduler(scheduler_pool, scheduler_cancellation, source).await },
        );
    let result = run_ingest_workers(workers, cancellation.clone()).await;
    cancellation.cancel();
    let _ = scheduler.await;
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

async fn run_scheduler(
    pool: sqlx::PgPool,
    cancellation: CancellationToken,
    source: EnvSource,
) -> anyhow::Result<()> {
    if !parse_or(source, "TMDB_ENABLE_SCHEDULER", true)? {
        tracing::info!(event = "scheduler_disabled");
        cancellation.cancelled().await;
        return Ok(());
    }
    let interval_seconds = parse_or(source, "TMDB_SCHEDULER_INTERVAL_SECONDS", 60_u64)?;
    if interval_seconds == 0 {
        bail!("TMDB_SCHEDULER_INTERVAL_SECONDS must be positive");
    }
    let daily_export_enabled = parse_or(source, "TMDB_ENABLE_DAILY_EXPORT", true)?;
    tracing::info!(
        event = "scheduler_started",
        interval_seconds,
        daily_export_enabled,
    );
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
    scheduler_tick_at(pool, interval_seconds, daily_export_enabled, Utc::now()).await
}

async fn scheduler_tick_at(
    pool: &sqlx::PgPool,
    interval_seconds: u64,
    daily_export_enabled: bool,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let bucket = now.timestamp().div_euclid(
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
    schedule_trending_jobs(pool, now).await?;
    if daily_export_enabled {
        // TMDB publishes the previous UTC day's exports.  The schedule key is
        // date-based, so a short scheduler interval cannot duplicate a job.
        let export_date = previous_export_date()?;
        let date_text = export_date.format("%m_%d_%Y").to_string();
        for media_type in ["movie", "tv"] {
            let schedule_key = format!("daily_export:{media_type}");
            if !claim_schedule_slot(pool, &schedule_key, &date_text).await? {
                continue;
            }
            let job = daily_export_job(media_type, &date_text)?;
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

async fn schedule_trending_jobs(pool: &sqlx::PgPool, now: DateTime<Utc>) -> anyhow::Result<()> {
    for (trend_window, refresh_seconds) in [
        ("day", TRENDING_DAY_REFRESH_SECONDS),
        ("week", TRENDING_WEEK_REFRESH_SECONDS),
    ] {
        let bucket = now.timestamp().div_euclid(refresh_seconds);
        for media_type in ["movie", "tv"] {
            let schedule_key = format!("trending:{media_type}:{trend_window}");
            if !claim_schedule_slot(pool, &schedule_key, &bucket.to_string()).await? {
                continue;
            }
            let job = NewJob::new(
                TRENDING_REFRESH_JOB,
                INGEST_PAYLOAD_VERSION,
                serde_json::json!({"media_type": media_type, "trend_window": trend_window}),
                &format!("{TRENDING_REFRESH_JOB}:{media_type}:{trend_window}:{bucket}"),
            )?;
            JobRepository::new(pool.clone()).submit(job).await?;
        }
    }
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

fn load_ingest_worker_concurrency(source: EnvSource) -> anyhow::Result<usize> {
    let upstream_connections = parse_or(source, "TMDB_MAX_CONNECTIONS", 20_u32)?;
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

fn previous_export_date() -> anyhow::Result<NaiveDate> {
    Utc::now()
        .date_naive()
        .checked_sub_days(Days::new(1))
        .ok_or_else(|| anyhow::anyhow!("scheduler date underflow"))
}

#[cfg(test)]
async fn ensure_catalog_seed(pool: &sqlx::PgPool, export_date: NaiveDate) -> anyhow::Result<usize> {
    let date_text = export_date.format("%m_%d_%Y").to_string();
    let repository = JobRepository::new(pool.clone());
    let mut submitted = 0_usize;
    for media_type in ["movie", "tv"] {
        if !catalog_seed_needed(pool, media_type).await? {
            continue;
        }
        let outcome = repository
            .submit(daily_export_job(media_type, &date_text)?)
            .await?;
        if !outcome.was_duplicate() {
            submitted = submitted.saturating_add(1);
            tracing::info!(event = "catalog_seed_queued", media_type);
        }
    }
    Ok(submitted)
}

#[cfg(test)]
async fn catalog_seed_needed(pool: &sqlx::PgPool, media_type: &str) -> anyhow::Result<bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1
               FROM ops.jobs
              WHERE job_type = $1
                AND status = 'succeeded'
                AND payload ->> 'media_type' = $2
                AND result_summary ? 'detail_refresh_candidates'
         )",
    )
    .bind(DAILY_EXPORT_JOB)
    .bind(media_type)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

fn daily_export_job(media_type: &str, date_text: &str) -> anyhow::Result<NewJob> {
    let file_prefix = match media_type {
        "movie" => "movie_ids",
        "tv" => "tv_series_ids",
        _ => bail!("invalid daily export media type"),
    };
    let url = format!("https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz");
    NewJob::new(
        DAILY_EXPORT_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({"media_type": media_type, "url": url}),
        &format!("{DAILY_EXPORT_JOB}:{media_type}:{date_text}"),
    )?
    .with_priority(DAILY_EXPORT_REFRESH_PRIORITY)
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use sqlx::PgPool;

    use super::*;

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_seed_queues_each_media_type_once_until_completed(
        pool: PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let export_date = NaiveDate::from_ymd_opt(2026, 7, 30).ok_or("date")?;

        assert_eq!(ensure_catalog_seed(&pool, export_date).await?, 2);
        let jobs: Vec<(Value, String)> = sqlx::query_as(
            "SELECT payload, dedup_key
               FROM ops.jobs
              WHERE job_type = 'ingest.daily_export'
              ORDER BY payload ->> 'media_type'",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].0["media_type"], "movie");
        assert_eq!(
            jobs[0].0["url"],
            "https://files.tmdb.org/p/exports/movie_ids_07_30_2026.json.gz"
        );
        assert_eq!(jobs[1].0["media_type"], "tv");
        assert_eq!(
            jobs[1].0["url"],
            "https://files.tmdb.org/p/exports/tv_series_ids_07_30_2026.json.gz"
        );
        assert_eq!(ensure_catalog_seed(&pool, export_date).await?, 0);

        sqlx::query(
            "UPDATE ops.jobs
                SET status = 'succeeded',
                    attempts = 1,
                    result_summary = '{\"detail_refresh_candidates\": 1}'::jsonb,
                    finished_at = clock_timestamp(),
                    updated_at = clock_timestamp()
              WHERE job_type = 'ingest.daily_export'",
        )
        .execute(&pool)
        .await?;
        assert_eq!(ensure_catalog_seed(&pool, export_date).await?, 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn scheduler_enqueues_each_trending_scope_once_per_refresh_window(
        pool: PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-02T12:00:00Z")?.to_utc();

        scheduler_tick_at(&pool, 60, false, now).await?;
        scheduler_tick_at(&pool, 60, false, now).await?;

        let jobs: Vec<(Value, String)> = sqlx::query_as(
            "SELECT payload, dedup_key
               FROM ops.jobs
              WHERE job_type = 'ingest.trending'
              ORDER BY payload ->> 'media_type', payload ->> 'trend_window'",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(jobs.len(), 4);
        assert_eq!(
            jobs[0].0,
            serde_json::json!({"media_type": "movie", "trend_window": "day"})
        );
        assert_eq!(
            jobs[1].0,
            serde_json::json!({"media_type": "movie", "trend_window": "week"})
        );
        assert_eq!(
            jobs[2].0,
            serde_json::json!({"media_type": "tv", "trend_window": "day"})
        );
        assert_eq!(
            jobs[3].0,
            serde_json::json!({"media_type": "tv", "trend_window": "week"})
        );
        assert_ne!(jobs[0].1, jobs[1].1);
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
