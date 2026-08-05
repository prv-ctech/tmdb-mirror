use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tmdb_config::DatabaseConfig;

use crate::DbError;
use crate::options::connect_options;

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
