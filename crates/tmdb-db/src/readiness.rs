use serde::Serialize;
use sqlx::{PgPool, Row};

use crate::migrate::require_role;
use crate::{DbError, MIGRATOR};

const REQUIRED_POSTGRES_MAJOR: u16 = 18;
const SCHEMA_REVISION: &str = "0050";
const REQUIRED_MIGRATION_VERSIONS: [i64; 50] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
];
const REQUIRED_EXTENSIONS: [&str; 3] = ["pg_stat_statements", "pg_trgm", "unaccent"];

/// Sanitized database readiness metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessReport {
    /// Supported `PostgreSQL` major version.
    pub postgres_major: u16,
    /// Latest application schema revision.
    pub schema_revision: String,
    /// Required installed extension names, sorted for stable output.
    pub extensions: Vec<String>,
}

/// Checks identity, session safety, server compatibility, extensions, and migrations.
///
/// # Errors
///
/// Returns a sanitized error when any readiness invariant is not met.
pub async fn readiness(pool: &PgPool, database_owner: &str) -> Result<ReadinessReport, DbError> {
    require_role(pool, "api_reader", database_owner).await?;

    let mut transaction = pool.begin().await.map_err(|_| DbError::Query)?;
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| DbError::Query)?;
    if read_only != "on" {
        return Err(DbError::Unready);
    }

    let row = sqlx::query(
        "SELECT current_setting('server_version_num')::integer / 10000 AS major,
                schema_revision
           FROM ops.readiness",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| DbError::Query)?;
    let Some(row) = row else {
        transaction.rollback().await.map_err(|_| DbError::Query)?;
        return Err(DbError::Unready);
    };
    let major: i32 = row.try_get("major").map_err(|_| DbError::Query)?;
    let schema_revision: String = row.try_get("schema_revision").map_err(|_| DbError::Query)?;

    let extensions: Vec<String> = sqlx::query_scalar(
        "SELECT extname FROM pg_extension WHERE extname = ANY($1) ORDER BY extname",
    )
    .bind(REQUIRED_EXTENSIONS)
    .fetch_all(&mut *transaction)
    .await
    .map_err(|_| DbError::Query)?;

    let embedded_versions: Vec<i64> = MIGRATOR.iter().map(|migration| migration.version).collect();

    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| DbError::Query)?;
    transaction.rollback().await.map_err(|_| DbError::Query)?;

    let postgres_major = u16::try_from(major).map_err(|_| DbError::Unready)?;
    if postgres_major != REQUIRED_POSTGRES_MAJOR
        || schema_revision != SCHEMA_REVISION
        || extensions != REQUIRED_EXTENSIONS
        || embedded_versions != REQUIRED_MIGRATION_VERSIONS
        || one != 1
    {
        return Err(DbError::Unready);
    }

    Ok(ReadinessReport {
        postgres_major,
        schema_revision,
        extensions,
    })
}
