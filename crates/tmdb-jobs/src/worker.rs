use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{ClaimedJob, JobError, JobRepository, WorkerId};

const MAX_LEASE_DURATION: Duration = Duration::from_hours(1);

/// A bounded failure returned by a job executor.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("job execution failed")]
pub struct JobExecutionError {
    code: String,
    retry_delay: Duration,
    terminal: bool,
}

impl JobExecutionError {
    /// Creates a failure that is recorded using the supplied canonical code and retry delay.
    #[must_use]
    pub fn retry(code: impl Into<String>, retry_delay: Duration) -> Self {
        Self {
            code: code.into(),
            retry_delay,
            terminal: false,
        }
    }

    /// Creates a permanent failure that is immediately recorded as a dead letter.
    #[must_use]
    pub fn dead_letter(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            retry_delay: Duration::ZERO,
            terminal: true,
        }
    }

    /// Returns the sanitized failure code passed to the durable-job boundary.
    #[must_use]
    pub fn failure_code(&self) -> &str {
        &self.code
    }

    /// Returns the requested retry delay.
    #[must_use]
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    /// Reports whether the worker must dead-letter without scheduling a retry.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }
}

/// Application code that executes one claimed job.
#[async_trait]
pub trait JobExecutor: Send + Sync {
    /// Returns the registered job types this worker is allowed to claim.
    /// `None` preserves the generic worker behavior used by callers that
    /// intentionally consume every enabled type.
    fn supported_job_types(&self) -> Option<&'static [&'static str]> {
        None
    }

    /// Executes one leased job and returns a bounded JSON result.
    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError>;
}

#[async_trait]
impl<T> JobExecutor for Arc<T>
where
    T: JobExecutor + ?Sized,
{
    fn supported_job_types(&self) -> Option<&'static [&'static str]> {
        (**self).supported_job_types()
    }

    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError> {
        (**self).execute(job).await
    }
}

/// Validated worker timing and lease identity.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Stable identity recorded in every lease event.
    pub worker_id: WorkerId,
    /// Duration for which a claim remains owned without a heartbeat.
    pub lease_duration: Duration,
    /// Interval between lease heartbeats.
    pub heartbeat_interval: Duration,
    /// Delay between empty queue polls.
    pub idle_poll_interval: Duration,
}

impl WorkerConfig {
    /// Validates worker timings and creates a runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerConfigError::Timing`] when an interval is zero, cannot be represented by
    /// the `PostgreSQL` lease boundary, or the heartbeat interval is not shorter than the lease.
    pub fn try_new(
        worker_id: WorkerId,
        lease_duration: Duration,
        heartbeat_interval: Duration,
        idle_poll_interval: Duration,
    ) -> Result<Self, WorkerConfigError> {
        if lease_duration.is_zero()
            || lease_duration > MAX_LEASE_DURATION
            || heartbeat_interval.is_zero()
            || heartbeat_interval >= lease_duration
            || idle_poll_interval.is_zero()
            || !lease_duration.as_nanos().is_multiple_of(1_000)
            || !heartbeat_interval.as_nanos().is_multiple_of(1_000)
            || !idle_poll_interval.as_nanos().is_multiple_of(1_000)
        {
            return Err(WorkerConfigError::Timing);
        }
        Ok(Self {
            worker_id,
            lease_duration,
            heartbeat_interval,
            idle_poll_interval,
        })
    }

    /// Alias for [`WorkerConfig::try_new`] for callers that prefer a conventional constructor.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerConfigError::Timing`] when the supplied intervals are invalid.
    pub fn new(
        worker_id: WorkerId,
        lease_duration: Duration,
        heartbeat_interval: Duration,
        idle_poll_interval: Duration,
    ) -> Result<Self, WorkerConfigError> {
        Self::try_new(
            worker_id,
            lease_duration,
            heartbeat_interval,
            idle_poll_interval,
        )
    }
}

/// Configuration validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum WorkerConfigError {
    /// Lease and polling intervals are not positive or heartbeat is not shorter than the lease.
    #[error("invalid worker timing configuration")]
    Timing,
}

/// Sanitized worker runtime failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum WorkerError {
    /// A durable queue operation failed.
    #[error(transparent)]
    Repository(#[from] JobError),
}

/// Polling worker that owns one bounded leased-job execution at a time.
#[derive(Debug)]
pub struct Worker<E> {
    repository: JobRepository,
    executor: Arc<E>,
    config: WorkerConfig,
    claim_gate: Arc<Semaphore>,
}

impl<E> Worker<E>
where
    E: JobExecutor + 'static,
{
    /// Creates a worker over a direct `PostgreSQL` repository.
    #[must_use]
    pub fn new(repository: JobRepository, executor: E, config: WorkerConfig) -> Self {
        Self {
            repository,
            executor: Arc::new(executor),
            config,
            claim_gate: Arc::new(Semaphore::new(1)),
        }
    }

    /// Runs until cancellation. New claims stop immediately after cancellation; a running
    /// executor is dropped at the cancellation boundary so shutdown remains bounded.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Repository`] when a queue operation fails for a reason other than
    /// a lost or expired lease.
    pub async fn run(self, cancellation: CancellationToken) -> Result<(), WorkerError> {
        let supported_job_types = self.executor.supported_job_types();
        loop {
            let permit = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                permit = self.claim_gate.clone().acquire_owned() => permit.map_err(|_| WorkerError::Repository(JobError::Database))?,
            };
            let claim = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                result = self.repository.claim_for_types(
                    &self.config.worker_id,
                    self.config.lease_duration,
                    supported_job_types,
                ) => result?,
            };
            drop(permit);

            let Some(job) = claim else {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = time::sleep(self.config.idle_poll_interval) => {}
                }
                continue;
            };

            self.execute_claimed(job, &cancellation).await?;
        }
    }

    async fn execute_claimed(
        &self,
        job: ClaimedJob,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkerError> {
        if job.cancellation_requested() {
            self.repository
                .complete(job.job_id(), &self.config.worker_id, json!({}))
                .await
                .map(|_| ())
                .or_else(ignore_lost_lease)
        } else {
            let heartbeat_stop = CancellationToken::new();
            let lease_lost = CancellationToken::new();
            let heartbeat =
                self.spawn_heartbeat(job.job_id(), heartbeat_stop.clone(), lease_lost.clone());
            let executor = Arc::clone(&self.executor);
            let job_for_execution = job.clone();
            let mut execution =
                tokio::spawn(async move { executor.execute(job_for_execution).await });
            let outcome = tokio::select! {
                () = cancellation.cancelled() => {
                    execution.abort();
                    let _ = execution.await;
                    Ok(())
                },
                () = lease_lost.cancelled() => {
                    execution.abort();
                    let _ = execution.await;
                    Ok(())
                },
                result = &mut execution => {
                    let result = result.unwrap_or_else(|_| {
                        Err(JobExecutionError::retry(
                            "execution_failed",
                            Duration::from_secs(1),
                        ))
                    });
                    self.record_outcome(&job, result).await
                },
            };
            heartbeat_stop.cancel();
            heartbeat.abort();
            let _ = heartbeat.await;
            outcome
        }
    }

    fn spawn_heartbeat(
        &self,
        job_id: crate::JobId,
        stop: CancellationToken,
        lease_lost: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let repository = self.repository.clone();
        let worker_id = self.config.worker_id.clone();
        let lease_duration = self.config.lease_duration;
        let heartbeat_interval = self.config.heartbeat_interval;
        tokio::spawn(async move {
            let mut ticker = time::interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = ticker.tick() => {
                        if repository.heartbeat(job_id, &worker_id, lease_duration).await.is_err() {
                            lease_lost.cancel();
                            break;
                        }
                    }
                }
            }
        })
    }

    async fn record_outcome(
        &self,
        job: &ClaimedJob,
        result: Result<Value, JobExecutionError>,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(value) => self
                .repository
                .complete(job.job_id(), &self.config.worker_id, value)
                .await
                .map(|_| ())
                .or_else(ignore_lost_lease),
            Err(error) if error.is_terminal() => self
                .repository
                .dead_letter(job.job_id(), &self.config.worker_id, error.failure_code())
                .await
                .map(|_| ())
                .or_else(ignore_lost_lease),
            Err(error) => self
                .repository
                .fail(
                    job.job_id(),
                    &self.config.worker_id,
                    error.failure_code(),
                    error.retry_delay(),
                )
                .await
                .map(|_| ())
                .or_else(ignore_lost_lease),
        }
    }
}

fn ignore_lost_lease(error: JobError) -> Result<(), WorkerError> {
    match error {
        JobError::LeaseLost | JobError::NotFound => Ok(()),
        other => Err(WorkerError::Repository(other)),
    }
}
