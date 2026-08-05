use serde::Serialize;
use sqlx::{PgConnection, PgPool};

use crate::DbError;

/// Migrations embedded into every database-capable binary at build time.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

// Stable application-level lock covering migration accounting as well as SQLx's run.
const MIGRATION_REPORT_LOCK_KEY: i64 = 0x544d_4442_4d49_4752;
const ROUND_ONE_0003_CHECKSUM: [u8; 48] = [
    0xca, 0x21, 0xf9, 0x78, 0x04, 0xaf, 0x92, 0xca, 0x31, 0x4b, 0x5c, 0x9f, 0xa5, 0x4b, 0xac, 0xa0,
    0xcb, 0x33, 0xb0, 0x96, 0x39, 0x38, 0x1d, 0x9a, 0x96, 0xd7, 0x75, 0xe1, 0xf3, 0x01, 0x4a, 0x2a,
    0xf2, 0x4d, 0x80, 0x5b, 0xc7, 0xef, 0xf0, 0xf8, 0x3d, 0xfd, 0x9c, 0x2c, 0x8c, 0x1f, 0xd1, 0xf4,
];
const ROUND_TWO_0004_CHECKSUM: [u8; 48] = [
    0xef, 0xd2, 0xae, 0x05, 0x67, 0xd3, 0x57, 0x6e, 0x14, 0xac, 0x29, 0x26, 0xe3, 0x08, 0x9e, 0xb4,
    0x3e, 0xc5, 0xc5, 0x4f, 0x1d, 0x63, 0xef, 0x58, 0x22, 0xe1, 0x56, 0x96, 0x3d, 0xf3, 0xd1, 0x6b,
    0x3f, 0x54, 0x11, 0x8b, 0xd6, 0xe7, 0xde, 0x2a, 0x54, 0x3a, 0x7b, 0xc1, 0xeb, 0xdd, 0x13, 0x2e,
];
// This exact hash identifies the unrepaired administrative-operations
// migration. It is accepted only so a database that applied it before the
// correction can receive the forward-only 0025 repair without a checksum
// mismatch. New databases embed the corrected 0022 bytes.
const ADMIN_OPERATIONS_0022_CHECKSUM: [u8; 48] = [
    0x5b, 0x29, 0xdf, 0x4f, 0xa6, 0xcd, 0xa3, 0x03, 0x4d, 0xd5, 0x41, 0xc3, 0x48, 0x66, 0x9c, 0xbd,
    0x96, 0x15, 0xd5, 0x4e, 0xdf, 0xad, 0x9e, 0xae, 0x71, 0x6a, 0xca, 0x2d, 0xcf, 0xa1, 0x22, 0x7c,
    0xbf, 0x8d, 0x45, 0x24, 0x3f, 0x4d, 0xb3, 0xe0, 0x8f, 0x80, 0xef, 0x5e, 0x38, 0xad, 0x4b, 0x33,
];

/// Result of one serialized `SQLx` migration run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct MigrationReport {
    /// Number of migrations newly applied by this invocation.
    pub applied: u64,
}

/// Verifies the migration identity, then validates and applies all embedded migrations.
///
/// # Errors
///
/// Returns a sanitized role, query, or migration error. `SQLx` retains its advisory
/// lock and checksum validation behavior.
pub async fn migrate(pool: &PgPool, database_owner: &str) -> Result<MigrationReport, DbError> {
    require_role(pool, "migrator", database_owner).await?;

    let mut connection = pool.acquire().await.map_err(|_| DbError::Connection)?;
    connection.close_on_drop();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_REPORT_LOCK_KEY)
        .execute(&mut *connection)
        .await
        .map_err(|_| DbError::Migration)?;

    let result = migrate_while_locked(&mut connection).await;
    let unlocked: Result<bool, _> = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_REPORT_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await;
    match unlocked {
        Ok(true) => result,
        Ok(false) | Err(_) => Err(DbError::Migration),
    }
}

async fn migrate_while_locked(connection: &mut PgConnection) -> Result<MigrationReport, DbError> {
    let before = applied_count(connection).await?;
    reconcile_round_one_checksum(connection).await?;
    reconcile_round_two_checksum(connection).await?;
    reconcile_admin_operations_checksum(connection).await?;
    MIGRATOR
        .run(&mut *connection)
        .await
        .map_err(|_| DbError::Migration)?;
    repair_application_role_grants(connection).await?;
    let after = applied_count(connection).await?;
    let applied = after.checked_sub(before).ok_or(DbError::Migration)?;
    Ok(MigrationReport { applied })
}

/// Re-applies the role grants that are intentionally expressed as default
/// privileges in the foundation migration. A few older databases were
/// migrated by the configured owner instead of `migrator`; explicit repair
/// keeps those databases least-privilege compatible while the next migration
/// still runs under the correct role.
async fn repair_application_role_grants(connection: &mut PgConnection) -> Result<(), DbError> {
    const STATEMENTS: [&str; 19] = [
        "GRANT USAGE ON SCHEMA catalog, source, search, assets, ops TO api_reader",
        "GRANT SELECT ON ALL TABLES IN SCHEMA catalog, source, search, assets TO api_reader",
        "GRANT USAGE ON SCHEMA source, ops TO api_job_submitter",
        "GRANT USAGE ON SCHEMA catalog, source, search, assets, ops TO ingest_writer",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog, source TO ingest_writer",
        "GRANT SELECT ON ALL TABLES IN SCHEMA search TO ingest_writer",
        "GRANT SELECT ON TABLE assets.image_assets TO ingest_writer",
        "GRANT SELECT ON TABLE ops.jobs TO ingest_writer",
        "REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA search FROM ingest_writer",
        "GRANT USAGE ON SCHEMA assets, ops TO image_writer",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA assets TO image_writer",
        "GRANT EXECUTE ON FUNCTION ops.submit_job(uuid, text, integer, text, smallint, integer, timestamptz, text) TO image_writer",
        "GRANT EXECUTE ON FUNCTION ops.job_cancellation_requested(uuid, text) TO ingest_writer, image_writer",
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA catalog, source, search TO ingest_writer",
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA assets TO image_writer",
        "ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA catalog, search, assets GRANT SELECT ON TABLES TO api_reader",
        "ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA catalog, source, search GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingest_writer",
        "GRANT USAGE ON SCHEMA catalog, ops TO monitor",
        "GRANT SELECT ON TABLE catalog.titles, ops.jobs, ops.job_events, ops.backup_requests, ops.component_heartbeats, ops.readiness, ops.media_scan_runs, ops.media_scan_job_links, ops.worker_control, ops.worker_requests TO monitor",
    ];
    for statement in STATEMENTS {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .map_err(|_| DbError::Migration)?;
    }
    sqlx::query(
        "ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA search
         REVOKE INSERT, UPDATE, DELETE ON TABLES FROM ingest_writer",
    )
    .execute(&mut *connection)
    .await
    .map_err(|_| DbError::Migration)?;
    sqlx::query(
        "ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA assets
         GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO image_writer",
    )
    .execute(&mut *connection)
    .await
    .map_err(|_| DbError::Migration)?;
    Ok(())
}

/// Repairs only the checksum of the published round-one 0003 migration.
///
/// Round two was unreleased, but its 0003 bytes were already used by some development
/// databases. Those databases must acknowledge the new 0003 bytes before `SQLx` can
/// apply 0004. No other checksum is accepted or modified.
async fn reconcile_round_one_checksum(connection: &mut PgConnection) -> Result<(), DbError> {
    let has_migrations: bool =
        sqlx::query_scalar("SELECT to_regclass('ops._sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| DbError::Query)?;
    if !has_migrations {
        return Ok(());
    }

    let current_checksum = MIGRATOR
        .iter()
        .find(|migration| migration.version == 3)
        .map(|migration| migration.checksum.as_ref())
        .ok_or(DbError::Migration)?;
    let repaired = sqlx::query(
        "UPDATE ops._sqlx_migrations
            SET checksum = $1
          WHERE version = 3
            AND success
            AND checksum = $2",
    )
    .bind(current_checksum)
    .bind(ROUND_ONE_0003_CHECKSUM.as_slice())
    .execute(&mut *connection)
    .await
    .map_err(|_| DbError::Migration)?
    .rows_affected();
    if repaired > 1 {
        return Err(DbError::Migration);
    }
    Ok(())
}

/// Repairs the published round-two 0004 migration once so its audit repair is installed.
///
/// Before a later migration exists, the old checksum is removed so `SQLx` replays `0004` and
/// installs its audit repair. Once a later migration is recorded, replaying 0004 would roll
/// back metadata/readiness written by that later migration, so only the checksum is repaired.
/// This is deliberately an exact checksum allowlist rather than a general migration-table
/// bypass. A database that has already recorded the repaired 0004 checksum is untouched.
async fn reconcile_round_two_checksum(connection: &mut PgConnection) -> Result<(), DbError> {
    let has_migrations: bool =
        sqlx::query_scalar("SELECT to_regclass('ops._sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| DbError::Query)?;
    if !has_migrations {
        return Ok(());
    }

    let current_checksum = MIGRATOR
        .iter()
        .find(|migration| migration.version == 4)
        .map(|migration| migration.checksum.as_ref())
        .ok_or(DbError::Migration)?;
    if current_checksum == ROUND_TWO_0004_CHECKSUM.as_slice() {
        return Ok(());
    }

    let has_later_migration: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops._sqlx_migrations
              WHERE success AND version > 4
         )",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| DbError::Migration)?;

    if has_later_migration {
        let repaired = sqlx::query(
            "UPDATE ops._sqlx_migrations
                SET checksum = $1
              WHERE version = 4
                AND success
                AND checksum = $2",
        )
        .bind(current_checksum)
        .bind(ROUND_TWO_0004_CHECKSUM.as_slice())
        .execute(&mut *connection)
        .await
        .map_err(|_| DbError::Migration)?
        .rows_affected();
        if repaired > 1 {
            return Err(DbError::Migration);
        }
    } else {
        let removed = sqlx::query(
            "DELETE FROM ops._sqlx_migrations
              WHERE version = 4
                AND success
                AND checksum = $1",
        )
        .bind(ROUND_TWO_0004_CHECKSUM.as_slice())
        .execute(&mut *connection)
        .await
        .map_err(|_| DbError::Migration)?
        .rows_affected();
        if removed > 1 {
            return Err(DbError::Migration);
        }
    }
    Ok(())
}

/// Recognizes only the one unrepaired 0022 checksum published during this
/// implementation. Migration 0025 supplies the actual forward-only database
/// repair; this update merely lets `SQLx` continue past its immutable checksum
/// guard. Arbitrary migration history is never accepted.
async fn reconcile_admin_operations_checksum(connection: &mut PgConnection) -> Result<(), DbError> {
    let has_migrations: bool =
        sqlx::query_scalar("SELECT to_regclass('ops._sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| DbError::Query)?;
    if !has_migrations {
        return Ok(());
    }

    let current_checksum = MIGRATOR
        .iter()
        .find(|migration| migration.version == 22)
        .map(|migration| migration.checksum.as_ref())
        .ok_or(DbError::Migration)?;
    let repaired = sqlx::query(
        "UPDATE ops._sqlx_migrations
            SET checksum = $1
          WHERE version = 22
            AND success
            AND checksum = $2",
    )
    .bind(current_checksum)
    .bind(ADMIN_OPERATIONS_0022_CHECKSUM.as_slice())
    .execute(&mut *connection)
    .await
    .map_err(|_| DbError::Migration)?
    .rows_affected();
    if repaired > 1 {
        return Err(DbError::Migration);
    }
    Ok(())
}

pub(crate) async fn require_role(
    pool: &PgPool,
    expected: &str,
    database_owner: &str,
) -> Result<(), DbError> {
    let current: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(pool)
        .await
        .map_err(|_| DbError::Query)?;
    if role_is_allowed(&current, expected, database_owner) {
        Ok(())
    } else {
        Err(DbError::WrongRole)
    }
}

fn role_is_allowed(current_role: &str, expected_role: &str, database_owner: &str) -> bool {
    current_role == expected_role || current_role == database_owner
}

async fn applied_count(connection: &mut PgConnection) -> Result<u64, DbError> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('ops._sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| DbError::Query)?;
    if !exists {
        return Ok(0);
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ops._sqlx_migrations WHERE success")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| DbError::Query)?;
    u64::try_from(count).map_err(|_| DbError::Query)
}

#[cfg(test)]
mod tests {
    use super::role_is_allowed;

    #[test]
    fn configured_database_owner_is_allowed_to_migrate() {
        assert!(role_is_allowed("custom_owner", "migrator", "custom_owner"));
    }

    #[test]
    fn unrelated_role_is_not_allowed_to_migrate() {
        assert!(!role_is_allowed("api_reader", "migrator", "custom_owner"));
    }
}
