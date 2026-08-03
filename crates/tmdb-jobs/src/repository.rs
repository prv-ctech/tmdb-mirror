use std::borrow::Cow;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::model::{
    MAX_CANCEL_MESSAGE_CHARS, MAX_LEASE_MICROS, MAX_RETRY_MICROS, validate_failure_code,
    validate_json_object, validate_message,
};
use crate::{
    ClaimedJob, FailureDisposition, Job, JobError, JobId, JobStatus, NewJob, SubmitOutcome,
    ValidationError, WorkerId,
};

const MAX_SUBMIT_BATCH_SIZE: usize = 500;

/// PostgreSQL-backed durable leased-job repository.
#[derive(Clone, Debug)]
pub struct JobRepository {
    pool: PgPool,
}

impl JobRepository {
    /// Creates a repository over a direct or narrowly scoped submission pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Submits or deduplicates an active job atomically.
    ///
    /// # Errors
    ///
    /// Returns a sanitized validation, registry-rejection, or database error.
    pub async fn submit(&self, job: NewJob) -> Result<SubmitOutcome, JobError> {
        let requested_id = JobId::new();
        let payload = serde_json::to_string(&job.payload).map_err(|_| JobError::Database)?;
        for attempt in 0..3 {
            let result = sqlx::query_as::<_, SubmitRow>(
                "SELECT job_id, was_duplicate
                   FROM ops.submit_job($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(requested_id.as_uuid())
            .bind(&job.job_type)
            .bind(job.payload_version)
            .bind(&payload)
            .bind(job.priority)
            .bind(job.max_attempts)
            .bind(job.available_at)
            .bind(&job.dedup_key)
            .fetch_one(&self.pool)
            .await;
            match result {
                Ok(row) => {
                    return Ok(SubmitOutcome {
                        job_id: row.job_id.into(),
                        duplicate: row.was_duplicate,
                    });
                }
                Err(error) if sqlstate(&error).as_deref() == Some("40001") && attempt < 2 => {}
                Err(error) => return Err(map_database_error(&error)),
            }
        }
        Err(JobError::Database)
    }

    /// Submits a bounded group of jobs through the same idempotent database boundary.
    ///
    /// The returned outcomes preserve input order. One statement invokes the existing
    /// security-definer submission function for every request, avoiding a network round trip
    /// per exported TMDB ID while retaining per-job validation, deduplication, and audit events.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an oversized batch or a sanitized database error when the
    /// durable submission boundary cannot accept the requested jobs.
    pub async fn submit_many(&self, jobs: &[NewJob]) -> Result<Vec<SubmitOutcome>, JobError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        if jobs.len() > MAX_SUBMIT_BATCH_SIZE {
            return Err(JobError::Validation(ValidationError::BatchSize));
        }

        let payloads: Vec<String> = jobs
            .iter()
            .map(|job| serde_json::to_string(&job.payload).map_err(|_| JobError::Database))
            .collect::<Result<_, _>>()?;
        let positions: Vec<i32> = (0..jobs.len())
            .map(|position| {
                i32::try_from(position)
                    .map_err(|_| JobError::Validation(ValidationError::BatchSize))
            })
            .collect::<Result<_, _>>()?;

        for attempt in 0..3 {
            let mut builder = QueryBuilder::<Postgres>::new(
                "SELECT submitted.job_id, submitted.was_duplicate\n                   FROM (",
            );
            builder.push_values(
                jobs.iter().zip(payloads.iter()).zip(positions.iter()),
                |mut values, ((job, payload), position)| {
                    values
                        .push_bind(*position)
                        .push_bind(JobId::new().as_uuid())
                        .push_bind(&job.job_type)
                        .push_bind(job.payload_version)
                        .push_bind(payload)
                        .push_bind(job.priority)
                        .push_bind(job.max_attempts)
                        .push_bind(job.available_at)
                        .push_bind(&job.dedup_key);
                },
            );
            builder.push(
                ") AS requested(\n                     position, id, job_type, payload_version, payload, priority, max_attempts,\n                     available_at, dedup_key\n                 )\n                 CROSS JOIN LATERAL ops.submit_job(\n                     requested.id, requested.job_type, requested.payload_version, requested.payload,\n                     requested.priority, requested.max_attempts, requested.available_at,\n                     requested.dedup_key\n                 ) AS submitted\n                 ORDER BY requested.position",
            );

            let result = builder
                .build_query_as::<SubmitRow>()
                .fetch_all(&self.pool)
                .await;
            match result {
                Ok(rows) if rows.len() == jobs.len() => {
                    return Ok(rows
                        .into_iter()
                        .map(|row| SubmitOutcome {
                            job_id: row.job_id.into(),
                            duplicate: row.was_duplicate,
                        })
                        .collect());
                }
                Ok(_) => return Err(JobError::Database),
                Err(error) if sqlstate(&error).as_deref() == Some("40001") && attempt < 2 => {}
                Err(error) => return Err(map_database_error(&error)),
            }
        }
        Err(JobError::Database)
    }

    /// Claims the deterministic highest-ranked ready or expired job.
    ///
    /// # Errors
    ///
    /// Returns validation or sanitized database errors.
    pub async fn claim(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<Option<ClaimedJob>, JobError> {
        self.claim_for_types(worker_id, lease_duration, None).await
    }

    /// Claims one job while restricting selection to the supplied registered
    /// job types. An empty or absent list retains the legacy all-types claim.
    ///
    /// # Errors
    ///
    /// Returns validation or sanitized database errors.
    pub async fn claim_for_types(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
        supported_job_types: Option<&[&str]>,
    ) -> Result<Option<ClaimedJob>, JobError> {
        let lease_microseconds = duration_microseconds(lease_duration, MAX_LEASE_MICROS)?;
        let row = match supported_job_types {
            Some(types) if !types.is_empty() => sqlx::query_as::<_, ClaimedRow>(
                "SELECT job_id, job_type, payload_version, payload, attempts, max_attempts,
                            lease_expires_at, cancellation_requested
                       FROM ops.claim_job_for_types($1, $2, $3)",
            )
            .bind(worker_id.as_str())
            .bind(lease_microseconds)
            .bind(types)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_database_error(&error))?,
            _ => sqlx::query_as::<_, ClaimedRow>(
                "SELECT job_id, job_type, payload_version, payload, attempts, max_attempts,
                            lease_expires_at, cancellation_requested
                       FROM ops.claim_job($1, $2)",
            )
            .bind(worker_id.as_str())
            .bind(lease_microseconds)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_database_error(&error))?,
        };
        Ok(row.map(ClaimedRow::into_model))
    }

    /// Renews a live lease and records one heartbeat event.
    ///
    /// # Errors
    ///
    /// Distinguishes a missing job from a lost lease.
    pub async fn heartbeat(
        &self,
        job_id: JobId,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<DateTime<Utc>, JobError> {
        let lease_microseconds = duration_microseconds(lease_duration, MAX_LEASE_MICROS)?;
        sqlx::query_scalar("SELECT ops.heartbeat_job($1, $2, $3)")
            .bind(job_id.as_uuid())
            .bind(worker_id.as_str())
            .bind(lease_microseconds)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| map_database_error(&error))
    }

    /// Reads the durable cancellation flag for this worker's active lease.
    ///
    /// The worker polls this flag between heartbeats so an administrative
    /// media cancellation can stop an in-flight download without waiting for
    /// the lease to expire.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::NotFound`] when the job no longer exists.
    pub async fn cancellation_requested(
        &self,
        job_id: JobId,
        worker_id: &WorkerId,
    ) -> Result<bool, JobError> {
        sqlx::query_scalar(
            "SELECT cancellation_requested
               FROM ops.job_cancellation_requested($1, $2)",
        )
        .bind(job_id.as_uuid())
        .bind(worker_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .ok_or(JobError::NotFound)
    }

    /// Completes a live lease, or acknowledges a pending cancellation.
    ///
    /// # Errors
    ///
    /// Rejects an unbounded result and distinguishes a missing job from a lost lease.
    pub async fn complete(
        &self,
        job_id: JobId,
        worker_id: &WorkerId,
        result: Value,
    ) -> Result<Job, JobError> {
        validate_json_object(&result)?;
        let result = serde_json::to_string(&result).map_err(|_| JobError::Database)?;
        let _: String = sqlx::query_scalar("SELECT ops.complete_job($1, $2, $3)")
            .bind(job_id.as_uuid())
            .bind(worker_id.as_str())
            .bind(result)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| map_database_error(&error))?;
        self.get(job_id).await
    }

    /// Records a failure and atomically schedules retry, dead-letters, or acknowledges cancel.
    ///
    /// # Errors
    ///
    /// Rejects malformed failure messages or invalid durations and distinguishes a missing job
    /// from a lost lease. Canonical failure codes are preferred, while bounded legacy messages
    /// remain accepted for rolling compatibility and are sanitized by the database.
    pub async fn fail(
        &self,
        job_id: JobId,
        worker_id: &WorkerId,
        failure_code: &str,
        retry_delay: Duration,
    ) -> Result<FailureDisposition, JobError> {
        validate_failure_code(failure_code)?;
        let retry_microseconds = duration_microseconds(retry_delay, MAX_RETRY_MICROS)?;
        let row: FailureRow = sqlx::query_as(
            "SELECT disposition, next_available_at
               FROM ops.fail_job($1, $2, $3, $4)",
        )
        .bind(job_id.as_uuid())
        .bind(worker_id.as_str())
        .bind(failure_code)
        .bind(retry_microseconds)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        match row.disposition.as_str() {
            "retry_scheduled" => Ok(FailureDisposition::RetryScheduled {
                available_at: row.next_available_at,
            }),
            "dead_lettered" => Ok(FailureDisposition::DeadLettered),
            "cancelled" => Ok(FailureDisposition::Cancelled),
            _ => Err(JobError::Database),
        }
    }

    /// Immediately dead-letters a live lease without consuming another retry.
    ///
    /// # Errors
    ///
    /// Rejects malformed failure messages and distinguishes a missing job from
    /// a lost lease.
    pub async fn dead_letter(
        &self,
        job_id: JobId,
        worker_id: &WorkerId,
        failure_code: &str,
    ) -> Result<FailureDisposition, JobError> {
        validate_failure_code(failure_code)?;
        let row: FailureRow = sqlx::query_as(
            "SELECT disposition, next_available_at
               FROM ops.dead_letter_job($1, $2, $3)",
        )
        .bind(job_id.as_uuid())
        .bind(worker_id.as_str())
        .bind(failure_code)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        match row.disposition.as_str() {
            "dead_lettered" => Ok(FailureDisposition::DeadLettered),
            "cancelled" => Ok(FailureDisposition::Cancelled),
            _ => Err(JobError::Database),
        }
    }

    /// Requests cancellation through the validated submission boundary.
    ///
    /// # Errors
    ///
    /// Returns validation, not-found, registry-rejection, or sanitized database errors.
    pub async fn request_cancel(&self, job_id: JobId, message: &str) -> Result<Job, JobError> {
        validate_message(message, MAX_CANCEL_MESSAGE_CHARS)?;
        let _: (String, bool) = sqlx::query_as(
            "SELECT job_status, cancellation_requested
               FROM ops.request_job_cancel($1, $2)",
        )
        .bind(job_id.as_uuid())
        .bind(message)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        self.get(job_id).await
    }

    /// Fetches sanitized persisted status without exposing the job payload.
    ///
    /// Legacy `PostgreSQL` `infinity` availability values are represented as
    /// `None`; finite timestamps remain unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::NotFound`] or a sanitized database error.
    pub async fn get(&self, job_id: JobId) -> Result<Job, JobError> {
        let row = sqlx::query_as::<_, JobRow>(
            "SELECT id, status, attempts, max_attempts,
                    CASE WHEN pg_catalog.isfinite(available_at) THEN available_at END AS available_at,
                    cancellation_requested, error_message, created_at, updated_at, finished_at
               FROM ops.job_status
              WHERE id = $1",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .ok_or(JobError::NotFound)?;
        row.into_model()
    }
}

#[derive(Debug, FromRow)]
struct SubmitRow {
    job_id: Uuid,
    was_duplicate: bool,
}

#[derive(Debug, FromRow)]
struct ClaimedRow {
    job_id: Uuid,
    job_type: String,
    payload_version: i32,
    payload: Value,
    attempts: i32,
    max_attempts: i32,
    lease_expires_at: DateTime<Utc>,
    cancellation_requested: bool,
}

impl ClaimedRow {
    fn into_model(self) -> ClaimedJob {
        ClaimedJob {
            job_id: self.job_id.into(),
            job_type: self.job_type,
            payload_version: self.payload_version,
            payload: self.payload,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            lease_expires_at: self.lease_expires_at,
            cancellation_requested: self.cancellation_requested,
        }
    }
}

#[derive(Debug, FromRow)]
struct FailureRow {
    disposition: String,
    next_available_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct JobRow {
    id: Uuid,
    status: String,
    attempts: i32,
    max_attempts: i32,
    available_at: Option<DateTime<Utc>>,
    cancellation_requested: bool,
    error_message: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn into_model(self) -> Result<Job, JobError> {
        Ok(Job {
            id: self.id.into(),
            status: JobStatus::parse(&self.status)?,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            available_at: self.available_at,
            cancellation_requested: self.cancellation_requested,
            error_message: self.error_message,
            created_at: self.created_at,
            updated_at: self.updated_at,
            finished_at: self.finished_at,
        })
    }
}

fn duration_microseconds(duration: Duration, maximum: u128) -> Result<i64, ValidationError> {
    let microseconds = duration.as_micros();
    if microseconds == 0
        || microseconds > maximum
        || !duration.as_nanos().is_multiple_of(1_000)
        || microseconds > i64::MAX as u128
    {
        return Err(ValidationError::Duration);
    }
    i64::try_from(microseconds).map_err(|_| ValidationError::Duration)
}

fn map_database_error(error: &sqlx::Error) -> JobError {
    match sqlstate(error).as_deref() {
        Some("P0002") => JobError::NotFound,
        Some("P0001") => JobError::LeaseLost,
        Some("22023") => JobError::Rejected,
        _ => JobError::Database,
    }
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(Cow::into_owned)
}
