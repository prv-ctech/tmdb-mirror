use chrono::NaiveDate;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn admin_submissions_are_durable_idempotent_and_backups_are_linked(
    pool: PgPool,
) -> sqlx::Result<()> {
    let request_id = Uuid::now_v7();
    let first: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'admin.scan', $2, 'scan-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"full","mediaTypes":["movie","tv"]}"#)
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert!(!first.1);
    let duplicate: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'admin.scan', $2, 'scan-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"full","mediaTypes":["movie","tv"]}"#)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(duplicate, (first.0, true));

    let Err(mismatch) = sqlx::query(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'admin.scan', $2, 'scan-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"changes","mediaTypes":["tv"]}"#)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await
    else {
        return Err(sqlx::Error::Protocol(
            "same idempotency key with another payload was accepted".to_owned(),
        ));
    };
    assert_eq!(sqlstate(&mismatch).as_deref(), Some("P0003"));

    let backup: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'database.backup_full', $2, 'backup-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"type":"full"}"#)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let backup_request: (String, String, String) = sqlx::query_as(
        "SELECT backup_type, request_source, status
           FROM ops.backup_requests
          WHERE job_id = $1",
    )
    .bind(backup.0)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        backup_request,
        ("full".to_owned(), "manual".to_owned(), "queued".to_owned())
    );

    let event_request_id: String = sqlx::query_scalar(
        "SELECT details ->> 'request_id'
           FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'submitted'",
    )
    .bind(first.0)
    .fetch_one(&pool)
    .await?;
    assert_eq!(event_request_id, request_id.to_string());
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn admin_cancel_retry_and_dst_safe_schedule_keep_history_auditable(
    pool: PgPool,
) -> sqlx::Result<()> {
    let source: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job($1, 'system.noop', 1, '{}', 0::smallint, 3, NULL, 'retry-source')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let cancelled: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.request_admin_job_cancel($1, 'cancel-idempotency', $2)",
    )
    .bind(source)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(cancelled, (source, false));
    let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(source)
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "cancelled");

    let retried: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.retry_admin_job($1, $2, 'retry-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(source)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_ne!(retried.0, source);
    assert!(!retried.1);
    let retry_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.job_events WHERE job_id = $1 AND event_kind = 'retried'",
    )
    .bind(source)
    .fetch_one(&pool)
    .await?;
    assert_eq!(retry_event_count, 1);

    // 2026-11-01 is the DST fall-back date in America/New_York. The runner
    // keys the Sunday 05:00 schedule by local calendar date, so a restart or
    // repeated local-clock transition cannot create a second durable request.
    let Some(dst_date) = NaiveDate::from_ymd_opt(2026, 11, 1) else {
        return Err(sqlx::Error::Protocol(
            "fixed DST fixture date is invalid".to_owned(),
        ));
    };
    let scheduled_first: Uuid =
        sqlx::query_scalar("SELECT ops.submit_scheduled_backup('full', $1)")
            .bind(dst_date)
            .fetch_one(&pool)
            .await?;
    let scheduled_second: Uuid =
        sqlx::query_scalar("SELECT ops.submit_scheduled_backup('full', $1)")
            .bind(dst_date)
            .fetch_one(&pool)
            .await?;
    assert_eq!(scheduled_first, scheduled_second);
    let scheduled_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM ops.backup_requests
          WHERE request_source = 'schedule' AND scheduled_for = $1",
    )
    .bind(dst_date)
    .fetch_one(&pool)
    .await?;
    assert_eq!(scheduled_count, 1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn transient_backup_failure_returns_the_paired_request_to_the_claimable_queue(
    pool: PgPool,
) -> sqlx::Result<()> {
    let backup: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_admin_job($1, 'database.backup_full', $2, 'retryable-backup', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"type":"full"}"#)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let worker_id = "backup-retry-test";
    let claimed: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types($1, 60000000, ARRAY['database.backup_full']::text[])",
    )
    .bind(worker_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed, backup);
    sqlx::query(
        "UPDATE ops.backup_requests
            SET status = 'running',
                started_at = pg_catalog.clock_timestamp(),
                worker_id = $2
          WHERE job_id = $1",
    )
    .bind(backup)
    .bind(worker_id)
    .execute(&pool)
    .await?;

    let disposition: String = sqlx::query_scalar(
        "SELECT ops.fail_backup_request_and_job($1, $2, 'already_running', 300000000)",
    )
    .bind(backup)
    .bind(worker_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(disposition, "retry_scheduled");

    let request: (String, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT status,
                started_at IS NULL,
                finished_at IS NULL,
                worker_id IS NULL,
                error_code IS NULL,
                error_message IS NULL
           FROM ops.backup_requests
          WHERE job_id = $1",
    )
    .bind(backup)
    .fetch_one(&pool)
    .await?;
    assert_eq!(request, ("queued".to_owned(), true, true, true, true, true));
    let job_status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(backup)
        .fetch_one(&pool)
        .await?;
    assert_eq!(job_status, "retry_wait");
    Ok(())
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}
