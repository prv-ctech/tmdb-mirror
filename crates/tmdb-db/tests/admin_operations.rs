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

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn media_scan_submission_and_worker_control_are_idempotent_and_bound_claims(
    pool: PgPool,
) -> sqlx::Result<()> {
    let request_id = Uuid::now_v7();
    let first: (Uuid, Uuid, bool) = sqlx::query_as(
        "SELECT job_id, run_id, was_duplicate
           FROM ops.submit_media_scan($1, $2, 'media-scan-1', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"audit","repair":true}"#)
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert!(!first.2);

    let duplicate: (Uuid, Uuid, bool) = sqlx::query_as(
        "SELECT job_id, run_id, was_duplicate
           FROM ops.submit_media_scan($1, $2, 'media-scan-1', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"audit","repair":true}"#)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(duplicate, (first.0, first.1, true));

    let Err(conflict) = sqlx::query(
        "SELECT job_id, run_id, was_duplicate
           FROM ops.submit_media_scan($1, $2, 'media-scan-1', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"full","repair":false}"#)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await
    else {
        return Err(sqlx::Error::Protocol(
            "a reused media-scan key must reject a different payload".to_owned(),
        ));
    };
    assert_eq!(sqlstate(&conflict).as_deref(), Some("P0003"));

    let scan_row: (String, bool, String, String, serde_json::Value) = sqlx::query_as(
        "SELECT scan.mode, scan.repair, scan.phase, job.status, job.payload
           FROM ops.media_scan_runs AS scan
           JOIN ops.jobs AS job ON job.id = scan.job_id
          WHERE scan.id = $1",
    )
    .bind(first.1)
    .fetch_one(&pool)
    .await?;
    assert_eq!(scan_row.0, "audit");
    assert!(scan_row.1);
    assert_eq!(scan_row.2, "queued");
    assert_eq!(scan_row.3, "queued");
    assert_eq!(scan_row.4["runId"], first.1.to_string());

    let media_scan_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name
           FROM information_schema.columns
          WHERE table_schema = 'ops' AND table_name = 'media_scan_job_status'
          ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        media_scan_columns,
        ["id", "job_type", "status", "result_summary", "created_at"]
    );
    let media_scan_privileges: (bool, bool, bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege('ingest_writer', 'ops.media_scan_job_status', 'SELECT'),
                has_table_privilege('image_writer', 'ops.media_scan_job_status', 'SELECT'),
                has_table_privilege('api_job_submitter', 'ops.media_scan_job_status', 'SELECT'),
                has_table_privilege('monitor', 'ops.media_scan_job_status', 'SELECT')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(media_scan_privileges, (true, false, false, false));

    let initial_state: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_media_worker_state('pause', 'media-worker-pause', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(initial_state, ("paused".to_owned(), false));
    let duplicate_state: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_media_worker_state('pause', 'media-worker-pause', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(duplicate_state, ("paused".to_owned(), true));

    let media_job: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'media-worker-queued')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let paused_claim: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types('media-worker-test', 1000000, ARRAY['image.download']::text[])",
    )
    .fetch_optional(&pool)
    .await?;
    assert!(paused_claim.is_none());

    let resume: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_media_worker_state('resume', 'media-worker-resume', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(resume, ("running".to_owned(), false));
    let claimed: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types('media-worker-test', 1000000, ARRAY['image.download']::text[])",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed, media_job);

    let stopped: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_media_worker_state('cancel', 'media-worker-cancel', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(stopped, ("stopped".to_owned(), false));
    let cancellation_requested: bool =
        sqlx::query_scalar("SELECT cancellation_requested FROM ops.jobs WHERE id = $1")
            .bind(media_job)
            .fetch_one(&pool)
            .await?;
    assert!(cancellation_requested);
    Ok(())
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}
