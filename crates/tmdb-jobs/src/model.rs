use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::ValidationError;

pub(crate) const MAX_JOB_TYPE_CHARS: usize = 128;
pub(crate) const MAX_DEDUP_KEY_CHARS: usize = 256;
pub(crate) const MAX_WORKER_ID_CHARS: usize = 128;
pub(crate) const MAX_CANCEL_MESSAGE_CHARS: usize = 1_024;
pub(crate) const MAX_FAILURE_MESSAGE_CHARS: usize = 2_048;
pub(crate) const MAX_JSON_BYTES: usize = 65_536;
pub(crate) const MAX_LEASE_MICROS: u128 = 3_600_000_000;
pub(crate) const MAX_RETRY_MICROS: u128 = 604_800_000_000;

/// Stable durable-job identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Creates a time-ordered version 7 identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Returns the database UUID value.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Uuid> for JobId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

/// Validated worker lease identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    /// Validates a worker identity.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError::WorkerId`] for empty, oversized, or control-bearing input.
    pub fn new(value: &str) -> Result<Self, ValidationError> {
        validate_text(value, MAX_WORKER_ID_CHARS, ValidationError::WorkerId)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the validated identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for WorkerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(&value).map_err(serde::de::Error::custom)
    }
}

/// Compact durable-job state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Accepted and immediately eligible or waiting for availability.
    Queued,
    /// Owned by a worker with a renewable lease.
    Running,
    /// Failed transiently and waiting for its exact retry timestamp.
    RetryWait,
    /// Completed successfully.
    Succeeded,
    /// Exhausted its configured attempts.
    DeadLetter,
    /// Cancelled before claim or acknowledged by its worker.
    Cancelled,
}

impl JobStatus {
    pub(crate) fn parse(value: &str) -> Result<Self, crate::JobError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "retry_wait" => Ok(Self::RetryWait),
            "succeeded" => Ok(Self::Succeeded),
            "dead_letter" => Ok(Self::DeadLetter),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(crate::JobError::Database),
        }
    }
}

/// Validated job submission request.
#[derive(Clone, Debug)]
pub struct NewJob {
    pub(crate) job_type: String,
    pub(crate) payload_version: i32,
    pub(crate) payload: Value,
    pub(crate) priority: i16,
    pub(crate) max_attempts: i32,
    pub(crate) available_at: Option<DateTime<Utc>>,
    pub(crate) dedup_key: String,
}

impl NewJob {
    /// Creates a validated versioned job request.
    ///
    /// # Errors
    ///
    /// Rejects invalid identifiers, versions, payloads, and deduplication keys.
    pub fn new(
        job_type: &str,
        payload_version: i32,
        payload: Value,
        dedup_key: &str,
    ) -> Result<Self, ValidationError> {
        validate_text(job_type, MAX_JOB_TYPE_CHARS, ValidationError::JobType)?;
        if payload_version <= 0 {
            return Err(ValidationError::PayloadVersion);
        }
        validate_json_object(&payload)?;
        validate_text(dedup_key, MAX_DEDUP_KEY_CHARS, ValidationError::DedupKey)?;
        Ok(Self {
            job_type: job_type.to_owned(),
            payload_version,
            payload,
            priority: 0,
            max_attempts: 3,
            available_at: None,
            dedup_key: dedup_key.to_owned(),
        })
    }

    /// Creates the foundation's registered no-op job.
    ///
    /// # Errors
    ///
    /// Rejects an invalid deduplication key.
    pub fn noop(dedup_key: &str) -> Result<Self, ValidationError> {
        Self::new("system.noop", 1, serde_json::json!({}), dedup_key)
    }

    /// Sets the scheduling priority.
    ///
    /// # Errors
    ///
    /// Rejects priorities outside `-1000..=1000`.
    pub fn with_priority(mut self, priority: i16) -> Result<Self, ValidationError> {
        if !(-1_000..=1_000).contains(&priority) {
            return Err(ValidationError::Priority);
        }
        self.priority = priority;
        Ok(self)
    }

    /// Sets the maximum number of claims, including reclaimed claims.
    ///
    /// # Errors
    ///
    /// Rejects limits outside `1..=100`.
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Result<Self, ValidationError> {
        if !(1..=100).contains(&max_attempts) {
            return Err(ValidationError::MaxAttempts);
        }
        self.max_attempts = max_attempts;
        Ok(self)
    }

    /// Schedules the earliest claim timestamp.
    ///
    /// # Errors
    ///
    /// Rejects timestamps outside the application-supported four-digit year range.
    pub fn with_available_at(
        mut self,
        available_at: DateTime<Utc>,
    ) -> Result<Self, ValidationError> {
        if !(1..=9_999).contains(&available_at.year()) {
            return Err(ValidationError::AvailableAt);
        }
        self.available_at = Some(available_at);
        Ok(self)
    }
}

/// Result of an idempotent submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmitOutcome {
    pub(crate) job_id: JobId,
    pub(crate) duplicate: bool,
}

impl SubmitOutcome {
    /// Returns the accepted job identifier, including the original identifier on deduplication.
    #[must_use]
    pub const fn job_id(self) -> JobId {
        self.job_id
    }

    /// Reports whether an already-active job satisfied this submission.
    #[must_use]
    pub const fn was_duplicate(self) -> bool {
        self.duplicate
    }
}

/// Job data returned to the worker that owns its lease.
#[derive(Clone, Debug)]
pub struct ClaimedJob {
    pub(crate) job_id: JobId,
    pub(crate) job_type: String,
    pub(crate) payload_version: i32,
    pub(crate) payload: Value,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
    pub(crate) lease_expires_at: DateTime<Utc>,
    pub(crate) cancellation_requested: bool,
}

impl ClaimedJob {
    /// Returns the claimed identifier.
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the claim count, including this claim.
    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Returns the registered job type.
    #[must_use]
    pub fn job_type(&self) -> &str {
        &self.job_type
    }

    /// Returns the payload version.
    #[must_use]
    pub const fn payload_version(&self) -> i32 {
        self.payload_version
    }

    /// Returns the immutable job payload.
    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }

    /// Returns the configured claim limit.
    #[must_use]
    pub const fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Returns the current lease expiry.
    #[must_use]
    pub const fn lease_expires_at(&self) -> DateTime<Utc> {
        self.lease_expires_at
    }

    /// Reports whether cancellation was requested after the claim.
    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }
}

/// Sanitized persisted job status.
#[derive(Clone, Debug)]
pub struct Job {
    pub(crate) id: JobId,
    pub(crate) status: JobStatus,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
    pub(crate) available_at: Option<DateTime<Utc>>,
    pub(crate) cancellation_requested: bool,
    pub(crate) error_message: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

impl Job {
    /// Returns the job identifier.
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.id
    }

    /// Returns the current state.
    #[must_use]
    pub const fn status(&self) -> JobStatus {
        self.status
    }

    /// Returns the number of claims or reclaims.
    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Returns the claim limit.
    #[must_use]
    pub const fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Returns the exact earliest claim timestamp.
    ///
    /// `None` represents an unbounded legacy `PostgreSQL` timestamp such as
    /// `infinity`, which cannot be represented by `chrono` without fabricating a date.
    #[must_use]
    pub const fn available_at(&self) -> Option<DateTime<Utc>> {
        self.available_at
    }

    /// Reports whether a running worker should acknowledge cancellation.
    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    /// Returns the bounded sanitized failure summary.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns creation time.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns last mutation time.
    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Returns terminal time, if terminal.
    #[must_use]
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }
}

/// Atomic result of recording a worker failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureDisposition {
    /// The job is waiting for the exact returned timestamp.
    RetryScheduled {
        /// Earliest next claim time.
        available_at: DateTime<Utc>,
    },
    /// The attempt limit was reached.
    DeadLettered,
    /// A pending cancellation was acknowledged instead of retrying.
    Cancelled,
}

pub(crate) fn validate_json_object(value: &Value) -> Result<(), ValidationError> {
    if !value.is_object()
        || serde_json::to_vec(value)
            .map_err(|_| ValidationError::JsonObject)?
            .len()
            > MAX_JSON_BYTES
    {
        return Err(ValidationError::JsonObject);
    }
    Ok(())
}

pub(crate) fn validate_message(value: &str, max: usize) -> Result<(), ValidationError> {
    validate_text(value, max, ValidationError::Message)
}

pub(crate) fn validate_failure_code(value: &str) -> Result<(), ValidationError> {
    if matches!(
        value,
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
            | "export_queue_incomplete"
    ) {
        Ok(())
    } else {
        validate_legacy_failure_message(value)
    }
}

fn validate_legacy_failure_message(value: &str) -> Result<(), ValidationError> {
    let length = value.chars().count();
    if value.trim().is_empty()
        || length > MAX_FAILURE_MESSAGE_CHARS
        || value.chars().any(char::is_control)
        || value != value.trim()
    {
        return Err(ValidationError::FailureCode);
    }
    Ok(())
}

fn validate_text(
    value: &str,
    max_chars: usize,
    error: ValidationError,
) -> Result<(), ValidationError> {
    let length = value.chars().count();
    if !value.is_ascii()
        || value.trim().is_empty()
        || length > max_chars
        || value.chars().any(char::is_control)
        || value != value.trim()
    {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_failure_codes_remain_canonical() {
        for code in [
            "entity_not_ready",
            "export_storage",
            "database_unavailable",
            "export_queue_incomplete",
        ] {
            assert!(validate_failure_code(code).is_ok(), "{code}");
        }
    }
}
