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
        tracing::info!(
            event = "job_worker_started",
            worker_id = self.config.worker_id.as_str(),
            claim_scope = if supported_job_types.is_some() {
                "filtered"
            } else {
                "all"
            },
        );
        loop {
            let permit = tokio::select! {
                () = cancellation.cancelled() => {
                    tracing::info!(event = "job_worker_stopped", worker_id = self.config.worker_id.as_str());
                    return Ok(());
                },
                permit = self.claim_gate.clone().acquire_owned() => permit.map_err(|_| WorkerError::Repository(JobError::Database))?,
            };
            let claim = tokio::select! {
                () = cancellation.cancelled() => {
                    tracing::info!(event = "job_worker_stopped", worker_id = self.config.worker_id.as_str());
                    return Ok(());
                },
                result = self.repository.claim_for_types(
                    &self.config.worker_id,
                    self.config.lease_duration,
                    supported_job_types,
                ) => result?,
            };
            drop(permit);

            let Some(job) = claim else {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        tracing::info!(event = "job_worker_stopped", worker_id = self.config.worker_id.as_str());
                        return Ok(());
                    },
                    () = time::sleep(self.config.idle_poll_interval) => {}
                }
                continue;
            };

            tracing::debug!(
                event = "job_claimed",
                worker_id = self.config.worker_id.as_str(),
                job_id = %job.job_id().as_uuid(),
                job_type = job.job_type(),
                attempt = job.attempts(),
                max_attempts = job.max_attempts(),
            );
            self.execute_claimed(job, &cancellation).await?;
        }
    }

    async fn execute_claimed(
        &self,
        job: ClaimedJob,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkerError> {
        if job.cancellation_requested() {
            let outcome = self
                .repository
                .complete(job.job_id(), &self.config.worker_id, json!({}))
                .await
                .map(|_| ())
                .or_else(ignore_lost_lease);
            if outcome.is_ok() {
                tracing::info!(
                    event = "job_cancelled",
                    worker_id = self.config.worker_id.as_str(),
                    job_id = %job.job_id().as_uuid(),
                    job_type = job.job_type(),
                );
            }
            outcome
        } else {
            let heartbeat_stop = CancellationToken::new();
            let lease_lost = CancellationToken::new();
            let job_cancelled = CancellationToken::new();
            let heartbeat = self.spawn_heartbeat(
                job.job_id(),
                heartbeat_stop.clone(),
                lease_lost.clone(),
                job_cancelled.clone(),
            );
            let executor = Arc::clone(&self.executor);
            let job_for_execution = job.clone();
            let mut execution =
                tokio::spawn(async move { executor.execute(job_for_execution).await });
            let outcome = tokio::select! {
                () = cancellation.cancelled() => {
                    execution.abort();
                    let _ = execution.await;
                    tracing::info!(
                        event = "job_execution_cancelled",
                        worker_id = self.config.worker_id.as_str(),
                        job_id = %job.job_id().as_uuid(),
                        job_type = job.job_type(),
                    );
                    Ok(())
                },
                () = lease_lost.cancelled() => {
                    execution.abort();
                    let _ = execution.await;
                    tracing::warn!(
                        event = "job_lease_lost",
                        worker_id = self.config.worker_id.as_str(),
                        job_id = %job.job_id().as_uuid(),
                        job_type = job.job_type(),
                    );
                    Ok(())
                },
                () = job_cancelled.cancelled() => {
                    execution.abort();
                    let _ = execution.await;
                    let outcome = self
                        .repository
                        .complete(job.job_id(), &self.config.worker_id, json!({}))
                        .await
                        .map(|_| ())
                        .or_else(ignore_lost_lease);
                    if outcome.is_ok() {
                        tracing::info!(
                            event = "job_cancelled",
                            worker_id = self.config.worker_id.as_str(),
                            job_id = %job.job_id().as_uuid(),
                            job_type = job.job_type(),
                            reason = "cancellation_requested",
                        );
                    }
                    outcome
                },
                result = &mut execution => {
                    let result = result.unwrap_or_else(|_| {
                        tracing::error!(
                            event = "job_execution_panicked",
                            worker_id = self.config.worker_id.as_str(),
                            job_id = %job.job_id().as_uuid(),
                            job_type = job.job_type(),
                            failure_code = "execution_failed",
                        );
                        Err(JobExecutionError::retry("execution_failed", Duration::from_secs(1)))
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
        job_cancelled: CancellationToken,
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
                            tracing::warn!(
                                event = "job_lease_heartbeat_failed",
                                worker_id = worker_id.as_str(),
                                job_id = %job_id.as_uuid(),
                                error_code = "database_or_lease",
                            );
                            lease_lost.cancel();
                            break;
                        }
                        match repository.cancellation_requested(job_id, &worker_id).await {
                            Ok(true) => {
                                job_cancelled.cancel();
                                break;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                tracing::warn!(
                                    event = "job_cancellation_poll_failed",
                                    worker_id = worker_id.as_str(),
                                    job_id = %job_id.as_uuid(),
                                    error_code = match error {
                                        JobError::NotFound => "job_not_found",
                                        _ => "database_unavailable",
                                    },
                                );
                                lease_lost.cancel();
                                break;
                            }
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
            Ok(value) => self.record_success(job, value).await,
            Err(error) if error.is_terminal() => self.record_terminal_failure(job, error).await,
            Err(error) => self.record_retryable_failure(job, error).await,
        }
    }

    async fn record_success(&self, job: &ClaimedJob, value: Value) -> Result<(), WorkerError> {
        match self
            .repository
            .complete(job.job_id(), &self.config.worker_id, value)
            .await
        {
            Ok(_) => {
                tracing::debug!(
                    event = "job_succeeded",
                    worker_id = self.config.worker_id.as_str(),
                    job_id = %job.job_id().as_uuid(),
                    job_type = job.job_type(),
                    attempt = job.attempts(),
                );
                Ok(())
            }
            Err(error) => record_repository_error(job, &self.config.worker_id, error),
        }
    }

    async fn record_terminal_failure(
        &self,
        job: &ClaimedJob,
        error: JobExecutionError,
    ) -> Result<(), WorkerError> {
        let failure_code = log_failure_code(error.failure_code());
        log_job_execution_failure(job, &self.config.worker_id, failure_code, true);
        match self
            .repository
            .dead_letter(job.job_id(), &self.config.worker_id, error.failure_code())
            .await
        {
            Ok(crate::FailureDisposition::DeadLettered) => {
                log_dead_letter(job, &self.config.worker_id, failure_code);
                Ok(())
            }
            Ok(crate::FailureDisposition::Cancelled) => {
                log_cancelled(job, &self.config.worker_id);
                Ok(())
            }
            Ok(crate::FailureDisposition::RetryScheduled { .. }) => {
                Err(WorkerError::Repository(JobError::Database))
            }
            Err(repository_error) => {
                record_repository_error(job, &self.config.worker_id, repository_error)
            }
        }
    }

    async fn record_retryable_failure(
        &self,
        job: &ClaimedJob,
        error: JobExecutionError,
    ) -> Result<(), WorkerError> {
        let failure_code = log_failure_code(error.failure_code());
        log_job_execution_failure(job, &self.config.worker_id, failure_code, false);
        match self
            .repository
            .fail(
                job.job_id(),
                &self.config.worker_id,
                error.failure_code(),
                error.retry_delay(),
            )
            .await
        {
            Ok(crate::FailureDisposition::RetryScheduled { .. }) => {
                tracing::warn!(
                    event = "job_retry_scheduled",
                    worker_id = self.config.worker_id.as_str(),
                    job_id = %job.job_id().as_uuid(),
                    job_type = job.job_type(),
                    attempt = job.attempts(),
                    max_attempts = job.max_attempts(),
                    failure_code,
                    retry_seconds = error.retry_delay().as_secs_f64(),
                );
                Ok(())
            }
            Ok(crate::FailureDisposition::DeadLettered) => {
                log_dead_letter(job, &self.config.worker_id, failure_code);
                Ok(())
            }
            Ok(crate::FailureDisposition::Cancelled) => {
                log_cancelled(job, &self.config.worker_id);
                Ok(())
            }
            Err(repository_error) => {
                record_repository_error(job, &self.config.worker_id, repository_error)
            }
        }
    }
}

fn log_job_execution_failure(
    job: &ClaimedJob,
    worker_id: &WorkerId,
    failure_code: &str,
    terminal: bool,
) {
    if terminal {
        tracing::error!(
            event = "job_execution_failed",
            worker_id = worker_id.as_str(),
            job_id = %job.job_id().as_uuid(),
            job_type = job.job_type(),
            attempt = job.attempts(),
            max_attempts = job.max_attempts(),
            failure_code,
            terminal,
        );
    } else {
        tracing::warn!(
            event = "job_execution_failed",
            worker_id = worker_id.as_str(),
            job_id = %job.job_id().as_uuid(),
            job_type = job.job_type(),
            attempt = job.attempts(),
            max_attempts = job.max_attempts(),
            failure_code,
            terminal,
        );
    }
}

fn log_dead_letter(job: &ClaimedJob, worker_id: &WorkerId, failure_code: &str) {
    tracing::error!(
        event = "job_dead_lettered",
        worker_id = worker_id.as_str(),
        job_id = %job.job_id().as_uuid(),
        job_type = job.job_type(),
        attempt = job.attempts(),
        max_attempts = job.max_attempts(),
        failure_code,
    );
}

fn log_cancelled(job: &ClaimedJob, worker_id: &WorkerId) {
    tracing::info!(
        event = "job_cancelled",
        worker_id = worker_id.as_str(),
        job_id = %job.job_id().as_uuid(),
        job_type = job.job_type(),
    );
}

fn record_repository_error(
    job: &ClaimedJob,
    worker_id: &WorkerId,
    error: JobError,
) -> Result<(), WorkerError> {
    match error {
        JobError::LeaseLost | JobError::NotFound => {
            tracing::warn!(
                event = "job_outcome_not_recorded",
                worker_id = worker_id.as_str(),
                job_id = %job.job_id().as_uuid(),
                job_type = job.job_type(),
                error_code = "lease_lost",
            );
            Ok(())
        }
        other => {
            tracing::error!(
                event = "job_outcome_record_failed",
                worker_id = worker_id.as_str(),
                job_id = %job.job_id().as_uuid(),
                job_type = job.job_type(),
                error_code = "database_unavailable",
            );
            Err(WorkerError::Repository(other))
        }
    }
}

fn log_failure_code(code: &str) -> &str {
    match code {
        "execution_failed"
        | "upstream_unavailable"
        | "upstream_unauthorized"
        | "rate_limited"
        | "invalid_payload"
        | "lease_expired"
        | "attempts_exhausted"
        | "entity_not_ready"
        | "export_storage"
        | "database_unavailable"
        | "export_queue_incomplete" => code,
        _ => "custom_failure",
    }
}

fn ignore_lost_lease(error: JobError) -> Result<(), WorkerError> {
    match error {
        JobError::LeaseLost | JobError::NotFound => Ok(()),
        other => Err(WorkerError::Repository(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_logs_do_not_echo_arbitrary_failure_text() {
        assert_eq!(
            log_failure_code("upstream_unavailable"),
            "upstream_unavailable"
        );
        assert_eq!(
            log_failure_code("https://example.invalid/?token=secret"),
            "custom_failure"
        );
    }
}
