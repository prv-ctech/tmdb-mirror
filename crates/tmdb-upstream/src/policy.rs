use std::num::{NonZeroU8, NonZeroU32};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

const MAX_DOCUMENTED_REQUESTS_PER_SECOND: u32 = 40;

/// Shared request-rate and concurrency bounds for upstream calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitPolicy {
    /// Maximum starts per second across all clones of one client.
    pub requests_per_second: NonZeroU32,
    /// Maximum in-flight upstream requests.
    pub max_concurrency: NonZeroU32,
}

impl RateLimitPolicy {
    /// Creates a conservative policy that cannot exceed TMDB's documented
    /// approximate upper bound without a code change and a new verification.
    ///
    /// # Errors
    ///
    /// Rejects zero values, a request rate above the documented ceiling, and
    /// unreasonably large concurrency values.
    pub fn try_new(requests_per_second: u32, max_concurrency: u32) -> Result<Self, PolicyError> {
        let Some(requests_per_second) = NonZeroU32::new(requests_per_second) else {
            return Err(PolicyError::ZeroRate);
        };
        let Some(max_concurrency) = NonZeroU32::new(max_concurrency) else {
            return Err(PolicyError::ZeroConcurrency);
        };
        if requests_per_second.get() > MAX_DOCUMENTED_REQUESTS_PER_SECOND {
            return Err(PolicyError::RateAboveDocumentedLimit);
        }
        if max_concurrency.get() > 256 {
            return Err(PolicyError::ConcurrencyTooLarge);
        }
        Ok(Self {
            requests_per_second,
            max_concurrency,
        })
    }
}

impl Default for RateLimitPolicy {
    fn default() -> Self {
        Self {
            requests_per_second: nonzero_u32_or_min(35),
            max_concurrency: nonzero_u32_or_min(20),
        }
    }
}

/// Bounded retry and HTTP timeout behavior for a TMDB client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum total attempts, including the first request.
    pub max_attempts: NonZeroU8,
    /// Shared request rate and concurrency limits.
    pub rate_limit: RateLimitPolicy,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Initial retry delay for transient failures.
    pub backoff_base: Duration,
    /// Maximum retry delay, including a server-provided `Retry-After` value.
    pub backoff_max: Duration,
    /// Maximum response body accepted by the client.
    pub max_response_bytes: usize,
}

impl RetryPolicy {
    /// Creates a fully validated transport policy.
    ///
    /// # Errors
    ///
    /// Rejects zero durations, unbounded retries, inverted backoff bounds, or
    /// an unsafe response-size limit.
    pub fn try_new(
        max_attempts: u8,
        rate_limit: RateLimitPolicy,
        request_timeout: Duration,
        backoff_base: Duration,
        backoff_max: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, PolicyError> {
        let Some(max_attempts) = NonZeroU8::new(max_attempts) else {
            return Err(PolicyError::ZeroAttempts);
        };
        if max_attempts.get() > 8 {
            return Err(PolicyError::AttemptsTooLarge);
        }
        if request_timeout.is_zero() || backoff_base.is_zero() || backoff_max.is_zero() {
            return Err(PolicyError::ZeroDuration);
        }
        if backoff_base > backoff_max {
            return Err(PolicyError::BackoffOrder);
        }
        if max_response_bytes == 0 || max_response_bytes > 64 * 1024 * 1024 {
            return Err(PolicyError::ResponseLimit);
        }
        Ok(Self {
            max_attempts,
            rate_limit,
            request_timeout,
            backoff_base,
            backoff_max,
            max_response_bytes,
        })
    }

    /// Returns a conservative production default.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            max_attempts: nonzero_u8_or_min(4),
            rate_limit: RateLimitPolicy::default(),
            request_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_millis(250),
            backoff_max: Duration::from_secs(15),
            max_response_bytes: 16 * 1024 * 1024,
        }
    }

    pub(crate) fn backoff(&self, attempt: u8) -> Duration {
        let exponent = u32::from(attempt.saturating_sub(1)).min(7);
        let multiplier = 1_u32 << exponent;
        self.backoff_base
            .checked_mul(multiplier)
            .map_or(self.backoff_max, |delay| delay.min(self.backoff_max))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::conservative()
    }
}

/// A shared token-bucket-like limiter used by all cloned clients.
#[derive(Debug)]
pub(crate) struct RequestGate {
    next_slot: Mutex<Instant>,
    slot_interval: Duration,
    concurrency: Semaphore,
}

impl RequestGate {
    pub(crate) fn new(policy: RateLimitPolicy) -> Arc<Self> {
        let interval_nanos = 1_000_000_000_u64 / u64::from(policy.requests_per_second.get());
        Arc::new(Self {
            next_slot: Mutex::new(Instant::now()),
            slot_interval: Duration::from_nanos(interval_nanos.max(1)),
            concurrency: Semaphore::new(policy.max_concurrency.get() as usize),
        })
    }

    pub(crate) async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, PolicyError> {
        let permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| PolicyError::LimiterClosed)?;
        let delay = {
            let mut next_slot = self.next_slot.lock().await;
            let now = Instant::now();
            let scheduled = (*next_slot).max(now);
            *next_slot = scheduled
                .checked_add(self.slot_interval)
                .unwrap_or_else(Instant::now);
            scheduled.saturating_duration_since(now)
        };
        if !delay.is_zero() {
            sleep(delay).await;
        }
        Ok(permit)
    }
}

fn nonzero_u8_or_min(value: u8) -> NonZeroU8 {
    match NonZeroU8::new(value) {
        Some(value) => value,
        None => NonZeroU8::MIN,
    }
}

fn nonzero_u32_or_min(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

/// Validation failure for transport policy settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyError {
    /// Request rate was zero.
    #[error("request rate must be positive")]
    ZeroRate,
    /// Concurrency was zero.
    #[error("request concurrency must be positive")]
    ZeroConcurrency,
    /// Request rate exceeded the conservative documented ceiling.
    #[error("request rate exceeds the documented TMDB ceiling")]
    RateAboveDocumentedLimit,
    /// Concurrency was outside the hard safety bound.
    #[error("request concurrency is too large")]
    ConcurrencyTooLarge,
    /// Retry attempt count was zero.
    #[error("retry attempts must be positive")]
    ZeroAttempts,
    /// Retry attempt count exceeded the hard safety bound.
    #[error("retry attempts are too large")]
    AttemptsTooLarge,
    /// A required duration was zero.
    #[error("retry durations must be positive")]
    ZeroDuration,
    /// Backoff minimum exceeded its maximum.
    #[error("retry backoff bounds are invalid")]
    BackoffOrder,
    /// Response body limit was outside the hard safety bound.
    #[error("response body limit is invalid")]
    ResponseLimit,
    /// The limiter was closed unexpectedly.
    #[error("request limiter is closed")]
    LimiterClosed,
}
