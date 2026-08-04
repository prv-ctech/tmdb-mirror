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
    .bind(r#"{"mode":"full_sweep","mediaTypes":["movie","tv"]}"#)
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert!(!first.1);
    let duplicate: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'admin.scan', $2, 'scan-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"full_sweep","mediaTypes":["movie","tv"]}"#)
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(duplicate, (first.0, true));

    let Err(mismatch) = sqlx::query(
        "SELECT job_id, was_duplicate
           FROM ops.submit_admin_job($1, 'admin.scan', $2, 'scan-idempotency', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"mode":"daily_sync","mediaTypes":["tv"]}"#)
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

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn operator_control_gates_ingest_and_media_claims_independently(
    pool: PgPool,
) -> sqlx::Result<()> {
    let initial: Vec<(String, String)> = sqlx::query_as(
        "SELECT worker_kind, state
           FROM ops.worker_control
          ORDER BY worker_kind",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        initial,
        [
            ("ingest".to_owned(), "stopped".to_owned()),
            ("media".to_owned(), "stopped".to_owned()),
        ]
    );

    let ingest_running: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'ingest.refresh_movie', 1,
               '{\"tmdbId\":1}', 0::smallint, 3,
               clock_timestamp(), 'operator-ingest-running')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let ingest_queued: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'ingest.refresh_movie', 1,
               '{\"tmdbId\":2}', 0::smallint, 3,
               clock_timestamp(), 'operator-ingest-queued')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let media_job: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'operator-media')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;

    let paused_ingest: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-ingest-test', 1000000,
               ARRAY['ingest.refresh_movie']::text[])",
    )
    .fetch_optional(&pool)
    .await?;
    assert!(paused_ingest.is_none());

    let start_ingest: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_worker_state('ingest', 'start', 'operator-ingest-start', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(start_ingest, ("running".to_owned(), false));

    let claimed_ingest: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-ingest-test', 1000000,
               ARRAY['ingest.refresh_movie']::text[])",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed_ingest, ingest_running);

    let still_paused_media: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-media-test', 1000000,
               ARRAY['image.download']::text[])",
    )
    .fetch_optional(&pool)
    .await?;
    assert!(still_paused_media.is_none());

    let start_media: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_worker_state('media', 'start', 'operator-media-start', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(start_media, ("running".to_owned(), false));
    let claimed_media: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-media-test', 1000000,
               ARRAY['image.download']::text[])",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed_media, media_job);

    let stop_ingest: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_worker_state('ingest', 'cancel', 'operator-ingest-cancel', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(stop_ingest, ("stopped".to_owned(), false));

    let queued_status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(ingest_queued)
        .fetch_one(&pool)
        .await?;
    assert_eq!(queued_status, "cancelled");
    let running_cancel_requested: bool =
        sqlx::query_scalar("SELECT cancellation_requested FROM ops.jobs WHERE id = $1")
            .bind(ingest_running)
            .fetch_one(&pool)
            .await?;
    assert!(running_cancel_requested);

    let media_state: String =
        sqlx::query_scalar("SELECT state FROM ops.worker_control WHERE worker_kind = 'media'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(media_state, "running");
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn image_job_submission_stays_bounded_and_keeps_active_duplicates_idempotent(
    pool: PgPool,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ops.jobs (
             id, job_type, payload_version, payload, status, dedup_key,
             available_at, created_at, updated_at, finished_at
         )
         SELECT gen_random_uuid(), 'image.download', 1, '{}'::jsonb, 'succeeded',
                'backpressure-' || series::text,
                clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp()
           FROM generate_series(1, 10000) AS series",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.jobs
            SET status = 'queued', finished_at = NULL, updated_at = clock_timestamp()
          WHERE job_type = 'image.download' AND dedup_key LIKE 'backpressure-%'",
    )
    .execute(&pool)
    .await?;

    let duplicate: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'backpressure-1')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert!(duplicate.1);

    let rejected = sqlx::query(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'backpressure-new')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await
    .expect_err("a new image job must not exceed the active queue limit");
    assert_eq!(sqlstate(&rejected).as_deref(), Some("P0004"));

    sqlx::query(
        "UPDATE ops.jobs
            SET status = 'cancelled',
                finished_at = clock_timestamp(),
                updated_at = clock_timestamp()
          WHERE job_type = 'image.download'
            AND dedup_key = 'backpressure-1'",
    )
    .execute(&pool)
    .await?;

    let accepted: (Uuid, bool) = sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'backpressure-new')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert!(!accepted.1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn title_refresh_submission_stays_bounded(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ops.jobs (
             id, job_type, payload_version, payload, status, dedup_key,
             available_at, created_at, updated_at, finished_at
         )
         SELECT gen_random_uuid(), 'ingest.refresh_movie', 1,
                jsonb_build_object('tmdb_id', series), 'succeeded',
                'title-backpressure-' || series::text,
                clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp()
           FROM generate_series(1, 1000) AS series",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.jobs
            SET status = 'queued', finished_at = NULL, updated_at = clock_timestamp()
          WHERE job_type = 'ingest.refresh_movie'
            AND dedup_key LIKE 'title-backpressure-%'",
    )
    .execute(&pool)
    .await?;

    let rejected = sqlx::query(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'ingest.refresh_movie', 1, '{\"tmdb_id\":2001}', 0::smallint, 3,
               clock_timestamp(), 'title-backpressure-new')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await
    .expect_err("a new title refresh must not exceed the active queue limit");
    assert_eq!(sqlstate(&rejected).as_deref(), Some("P0004"));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn prune_finished_jobs_removes_old_history_but_keeps_scan_roots(
    pool: PgPool,
) -> sqlx::Result<()> {
    let unreferenced: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'system.noop', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'prune-unreferenced')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let admin: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_admin_job(
               $1, 'admin.analyze', '{}', 'prune-admin-reference', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let (media_scan, scan_run): (Uuid, Uuid) = sqlx::query_as(
        "SELECT job_id, run_id
           FROM ops.submit_media_scan(
               $1, '{\"mode\":\"audit\",\"repair\":false}',
               'prune-media-reference', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;

    let linked_child: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'system.noop', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'prune-linked-child')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.media_scan_job_links (run_id, job_id, phase)
         VALUES ($1, $2, 'audit')",
    )
    .bind(scan_run)
    .bind(linked_child)
    .execute(&pool)
    .await?;

    sqlx::query(
        "UPDATE ops.jobs
            SET status = 'succeeded',
                created_at = clock_timestamp() - interval '31 days',
                finished_at = clock_timestamp() - interval '31 days',
                updated_at = clock_timestamp() - interval '31 days'
          WHERE id = ANY($1)",
    )
    .bind(vec![unreferenced, admin, media_scan, linked_child])
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.media_scan_runs
            SET status = 'succeeded', phase = 'completed',
                started_at = clock_timestamp() - interval '31 days',
                finished_at = clock_timestamp() - interval '31 days'
          WHERE id = $1",
    )
    .bind(scan_run)
    .execute(&pool)
    .await?;

    let pruned: i32 = sqlx::query_scalar(
        "SELECT ops.prune_finished_jobs(
                    clock_timestamp() - interval '30 days', 100)",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(pruned, 2);

    let remaining: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id
           FROM ops.jobs
          WHERE id = ANY($1)
          ORDER BY id",
    )
    .bind(vec![unreferenced, admin, media_scan])
    .fetch_all(&pool)
    .await?;
    assert_eq!(remaining.len(), 2);
    assert!(!remaining.contains(&unreferenced));
    assert!(remaining.contains(&admin));
    assert!(remaining.contains(&media_scan));
    assert!(!remaining.contains(&linked_child));
    let link_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.media_scan_job_links
          WHERE run_id = $1 AND job_id = $2",
    )
    .bind(scan_run)
    .bind(linked_child)
    .fetch_one(&pool)
    .await?;
    assert_eq!(link_count, 0);
    Ok(())
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}
