use std::future::Future;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tmdb_config::DatabaseConfig;

use crate::DbError;
use crate::options::connect_options;

const STARTUP_CONNECTION_ATTEMPTS: usize = 12;
const STARTUP_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Session policy for a bounded direct `PostgreSQL` pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolPolicy {
    /// Schema migration sessions, limited to a single connection.
    Migrator,
    /// Read-only API and monitoring sessions.
    ReadOnly,
    /// Direct worker and narrow command sessions that require writes.
    ReadWrite,
}

impl PoolPolicy {
    pub(crate) const fn application_name(self) -> &'static str {
        match self {
            Self::Migrator => "tmdb-migrator",
            Self::ReadOnly => "tmdb-read-only",
            Self::ReadWrite => "tmdb-read-write",
        }
    }

    pub(crate) const fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    const fn max_connections(self) -> u32 {
        match self {
            Self::Migrator => 1,
            // The API serves many concurrent independent reads directly from
            // PostgreSQL. Keep enough bounded slots for health probes and
            // catalog requests to progress without request serialization.
            Self::ReadOnly => 32,
            // Ingestion loops spend most of their lifetime awaiting TMDB and
            // share this smaller database pool. Keep enough write slots for
            // persistence, image work, and direct administration without
            // approaching PostgreSQL's deployment connection cap.
            Self::ReadWrite => 24,
        }
    }
}

/// Opens a conservatively bounded direct `PostgreSQL` pool without parsing a URL.
///
/// # Errors
///
/// Returns a sanitized connection error when `PostgreSQL` cannot be reached or rejects
/// the configured identity.
pub async fn connect_direct(
    config: &DatabaseConfig,
    policy: PoolPolicy,
) -> Result<PgPool, DbError> {
    PgPoolOptions::new()
        .min_connections(0)
        .max_connections(policy.max_connections())
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Some(Duration::from_mins(5)))
        .max_lifetime(Some(Duration::from_mins(30)))
        .connect_with(connect_options(config, policy))
        .await
        .map_err(|_| DbError::Connection)
}

/// Opens a direct `PostgreSQL` pool and tolerates bounded startup races.
///
/// # Errors
///
/// Returns a sanitized connection error after all startup attempts fail.
pub async fn connect_direct_for_startup(
    config: &DatabaseConfig,
    policy: PoolPolicy,
) -> Result<PgPool, DbError> {
    retry_connection(
        STARTUP_CONNECTION_ATTEMPTS,
        STARTUP_CONNECTION_RETRY_DELAY,
        || connect_direct(config, policy),
    )
    .await
}

async fn retry_connection<T, Connect, ConnectFuture>(
    max_attempts: usize,
    retry_delay: Duration,
    mut connect: Connect,
) -> Result<T, DbError>
where
    Connect: FnMut() -> ConnectFuture,
    ConnectFuture: Future<Output = Result<T, DbError>>,
{
    for attempt in 1..=max_attempts {
        match connect().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt == max_attempts => return Err(error),
            Err(_) => {
                tracing::warn!(
                    event = "database_startup_retry",
                    attempt,
                    max_attempts,
                    retry_seconds = retry_delay.as_secs_f64(),
                );
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
    Err(DbError::Connection)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[tokio::test]
    async fn startup_retry_returns_after_a_transient_connection_failure() {
        let attempts = Cell::new(0_u8);

        let result = retry_connection(3, Duration::ZERO, || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            async move {
                if attempt < 3 {
                    Err(DbError::Connection)
                } else {
                    Ok("connected")
                }
            }
        })
        .await;

        assert_eq!(result, Ok("connected"));
    }

    #[tokio::test]
    async fn startup_retry_stops_at_the_attempt_limit() {
        let attempts = Cell::new(0_u8);

        let result: Result<(), DbError> = retry_connection(3, Duration::ZERO, || {
            attempts.set(attempts.get() + 1);
            async { Err(DbError::Connection) }
        })
        .await;

        assert_eq!((attempts.get(), result), (3, Err(DbError::Connection)));
    }
}
