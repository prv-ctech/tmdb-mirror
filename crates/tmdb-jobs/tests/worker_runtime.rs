use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::PgPool;
use tmdb_jobs::{ClaimedJob, JobExecutor, JobRepository, Worker, WorkerConfig, WorkerId};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

struct NoopExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl JobExecutor for NoopExecutor {
    async fn execute(&self, _job: ClaimedJob) -> Result<Value, tmdb_jobs::JobExecutionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"ok": true}))
    }
}

struct BlockingExecutor;

#[async_trait]
impl JobExecutor for BlockingExecutor {
    async fn execute(&self, _job: ClaimedJob) -> Result<Value, tmdb_jobs::JobExecutionError> {
        std::future::pending::<()>().await;
        Ok(json!({}))
    }
}

struct AlwaysFailExecutor;

#[async_trait]
impl JobExecutor for AlwaysFailExecutor {
    async fn execute(&self, _job: ClaimedJob) -> Result<Value, tmdb_jobs::JobExecutionError> {
        Err(tmdb_jobs::JobExecutionError::retry(
            "execution_failed",
            Duration::from_millis(10),
        ))
    }
}

struct PermanentFailExecutor;

#[async_trait]
impl JobExecutor for PermanentFailExecutor {
    async fn execute(&self, _job: ClaimedJob) -> Result<Value, tmdb_jobs::JobExecutionError> {
        Err(tmdb_jobs::JobExecutionError::dead_letter("invalid_payload"))
    }
}

struct PanickingExecutor;

#[async_trait]
impl JobExecutor for PanickingExecutor {
    #[allow(clippy::panic)]
    async fn execute(&self, _job: ClaimedJob) -> Result<Value, tmdb_jobs::JobExecutionError> {
        panic!("worker-executor-test-sentinel");
    }
}

fn config(worker_id: &str) -> Result<WorkerConfig, sqlx::Error> {
    let worker_id = WorkerId::new(worker_id).map_err(|error| test_error(&error.to_string()))?;
    WorkerConfig::try_new(
        worker_id,
        Duration::from_millis(250),
        Duration::from_millis(50),
        Duration::from_millis(10),
    )
    .map_err(|error| test_error(&error.to_string()))
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn noop_worker_completes_claimed_job(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(
            tmdb_jobs::NewJob::noop("worker-runtime-noop")
                .map_err(|error| test_error(&error.to_string()))?,
        )
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = Worker::new(
        repository,
        NoopExecutor {
            calls: calls.clone(),
        },
        config("worker-runtime-noop")?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "succeeded" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not complete noop in time"))??;
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn cancellation_does_not_claim_new_jobs(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(
            tmdb_jobs::NewJob::noop("worker-runtime-cancel")
                .map_err(|error| test_error(&error.to_string()))?,
        )
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        NoopExecutor {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        config("worker-runtime-cancel")?,
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    worker
        .run(cancellation)
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(submitted.job_id().as_uuid())
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "queued");
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn cancellation_bounds_a_blocking_executor(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(
            tmdb_jobs::NewJob::noop("worker-runtime-block")
                .map_err(|error| test_error(&error.to_string()))?,
        )
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        BlockingExecutor,
        config("worker-runtime-block")?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "running" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not claim blocking job in time"))??;
    cancellation.cancel();
    timeout(Duration::from_secs(1), worker_task)
        .await
        .map_err(|_| test_error("worker did not drain in time"))?
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn executor_failures_retry_then_dead_letter(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let job = tmdb_jobs::NewJob::noop("worker-runtime-retry")
        .map_err(|error| test_error(&error.to_string()))?
        .with_max_attempts(2)
        .map_err(|error| test_error(&error.to_string()))?;
    let submitted = repository
        .submit(job)
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        AlwaysFailExecutor,
        config("worker-runtime-retry")?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "dead_letter" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not dead-letter retry fixture in time"))??;
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM ops.jobs WHERE id = $1")
        .bind(submitted.job_id().as_uuid())
        .fetch_one(&pool)
        .await?;
    assert_eq!(attempts, 2);
    let retry_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.job_events
          WHERE job_id = $1 AND event_kind IN ('retry_scheduled', 'dead_lettered')",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert_eq!(retry_events, 2);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn permanent_executor_failure_dead_letters_without_retry(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let job = tmdb_jobs::NewJob::noop("worker-runtime-terminal")
        .map_err(|error| test_error(&error.to_string()))?
        .with_max_attempts(5)
        .map_err(|error| test_error(&error.to_string()))?;
    let submitted = repository
        .submit(job)
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        PermanentFailExecutor,
        config("worker-runtime-terminal")?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "dead_letter" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not terminally fail fixture in time"))??;
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM ops.jobs WHERE id = $1")
        .bind(submitted.job_id().as_uuid())
        .fetch_one(&pool)
        .await?;
    assert_eq!(attempts, 1);
    let error_code: Option<String> =
        sqlx::query_scalar("SELECT error_code FROM ops.jobs WHERE id = $1")
            .bind(submitted.job_id().as_uuid())
            .fetch_one(&pool)
            .await?;
    assert_eq!(error_code.as_deref(), Some("invalid_payload"));
    let terminal_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'dead_lettered'",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert_eq!(terminal_events, 1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn executor_panic_is_sanitized_into_a_dead_letter(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let job = tmdb_jobs::NewJob::noop("worker-runtime-panic")
        .map_err(|error| test_error(&error.to_string()))?
        .with_max_attempts(1)
        .map_err(|error| test_error(&error.to_string()))?;
    let submitted = repository
        .submit(job)
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        PanickingExecutor,
        config("worker-runtime-panic")?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "dead_letter" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not dead-letter panic fixture in time"))??;
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    let error_code: Option<String> =
        sqlx::query_scalar("SELECT error_code FROM ops.jobs WHERE id = $1")
            .bind(submitted.job_id().as_uuid())
            .fetch_one(&pool)
            .await?;
    assert_eq!(error_code.as_deref(), Some("execution_failed"));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn heartbeat_keeps_a_long_running_claim_owned(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(
            tmdb_jobs::NewJob::noop("worker-runtime-heartbeat")
                .map_err(|error| test_error(&error.to_string()))?,
        )
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        BlockingExecutor,
        WorkerConfig::try_new(
            WorkerId::new("worker-runtime-heartbeat")
                .map_err(|error| test_error(&error.to_string()))?,
            Duration::from_millis(120),
            Duration::from_millis(20),
            Duration::from_millis(5),
        )
        .map_err(|error| test_error(&error.to_string()))?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "running" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("worker did not claim heartbeat fixture in time"))??;
    sleep(Duration::from_millis(220)).await;
    let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(submitted.job_id().as_uuid())
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "running");
    let heartbeats: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'heartbeat'",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert!(heartbeats >= 3, "heartbeat count was {heartbeats}");
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn cancelled_worker_lease_is_reclaimed_by_a_new_worker(pool: PgPool) -> sqlx::Result<()> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(
            tmdb_jobs::NewJob::noop("worker-runtime-reclaim")
                .map_err(|error| test_error(&error.to_string()))?,
        )
        .await
        .map_err(|error| test_error(&error.to_string()))?;
    let worker = Worker::new(
        repository,
        BlockingExecutor,
        WorkerConfig::try_new(
            WorkerId::new("worker-runtime-reclaim-one")
                .map_err(|error| test_error(&error.to_string()))?,
            Duration::from_millis(120),
            Duration::from_millis(20),
            Duration::from_millis(5),
        )
        .map_err(|error| test_error(&error.to_string()))?,
    );
    let cancellation = CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "running" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("first worker did not claim reclaim fixture in time"))??;
    cancellation.cancel();
    worker_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    sleep(Duration::from_millis(180)).await;

    let second_worker = Worker::new(
        JobRepository::new(pool.clone()),
        NoopExecutor {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        WorkerConfig::try_new(
            WorkerId::new("worker-runtime-reclaim-two")
                .map_err(|error| test_error(&error.to_string()))?,
            Duration::from_millis(120),
            Duration::from_millis(20),
            Duration::from_millis(5),
        )
        .map_err(|error| test_error(&error.to_string()))?,
    );
    let second_cancellation = CancellationToken::new();
    let second_task = tokio::spawn(second_worker.run(second_cancellation.clone()));
    timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
                .bind(submitted.job_id().as_uuid())
                .fetch_one(&pool)
                .await?;
            if status == "succeeded" {
                break Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| test_error("second worker did not reclaim fixture in time"))??;
    second_cancellation.cancel();
    second_task
        .await
        .map_err(|error| test_error(&error.to_string()))?
        .map_err(|error| test_error(&error.to_string()))?;
    let reclaimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'reclaimed'",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert_eq!(reclaimed, 1);
    Ok(())
}

fn test_error(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}
