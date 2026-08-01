/// Validation failure at the durable-job boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ValidationError {
    /// A job type was empty, too long, or contained control characters.
    #[error("invalid job type")]
    JobType,
    /// A payload version was not positive.
    #[error("invalid payload version")]
    PayloadVersion,
    /// A payload or result was not a bounded JSON object.
    #[error("invalid job JSON object")]
    JsonObject,
    /// A deduplication key was empty, too long, or contained control characters.
    #[error("invalid deduplication key")]
    DedupKey,
    /// A worker identifier was empty, too long, or contained control characters.
    #[error("invalid worker identifier")]
    WorkerId,
    /// A priority was outside the supported range.
    #[error("invalid job priority")]
    Priority,
    /// An attempt limit was outside the supported range.
    #[error("invalid maximum attempts")]
    MaxAttempts,
    /// An availability timestamp was outside the supported range.
    #[error("invalid availability timestamp")]
    AvailableAt,
    /// A cancellation message was empty, non-ASCII, too long, or contained control characters.
    #[error("invalid job message")]
    Message,
    /// A worker failure message was empty, oversized, trimmed, or control-bearing.
    #[error("invalid job failure code")]
    FailureCode,
    /// A lease or retry duration was zero, too large, or could not be represented safely.
    #[error("invalid job duration")]
    Duration,
}

/// Sanitized durable-job repository failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobError {
    /// Caller input failed local validation.
    #[error(transparent)]
    Validation(#[from] ValidationError),
    /// No job exists with the requested identifier.
    #[error("job was not found")]
    NotFound,
    /// The caller no longer owns a live lease for the job.
    #[error("job lease was lost")]
    LeaseLost,
    /// The database boundary rejected the requested job type, version, or state.
    #[error("job request was rejected")]
    Rejected,
    /// A database operation failed without exposing its internal message.
    #[error("job database operation failed")]
    Database,
}
