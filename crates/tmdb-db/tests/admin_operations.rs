use chrono::{NaiveDate, Timelike};
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
async fn media_requests_are_local_durable_idempotent_and_bounded(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.titles (
             media_type, tmdb_id, display_title, poster_path, backdrop_path, enriched_at, active
         ) VALUES
             ('movie', 1, 'Identity padding', NULL, NULL, clock_timestamp(), false),
             ('movie', 550, 'Fight Club', '/primary-poster.jpg', '/primary-backdrop.jpg',
              clock_timestamp(), true)",
    )
    .execute(&pool)
    .await?;
    let title_id: i64 = sqlx::query_scalar(
        "SELECT id FROM catalog.titles WHERE media_type = 'movie' AND tmdb_id = 550",
    )
    .fetch_one(&pool)
    .await?;
    assert_ne!(title_id, 550);
    sqlx::query(
        "INSERT INTO source.tmdb_documents (endpoint_path, response)
         VALUES ('movie/550', $1::jsonb)",
    )
    .bind(
        r#"{"images":{"posters":[{"file_path":"/primary-poster.jpg","iso_639_1":"en"},{"file_path":"/poster-02.jpg","iso_639_1":null},{"file_path":"/foreign.jpg","iso_639_1":"fr"}],"backdrops":[{"file_path":"/primary-backdrop.jpg","iso_639_1":null}],"logos":[]}}"#,
    )
    .execute(&pool)
    .await?;

    let first_id = Uuid::now_v7();
    let first: (Option<Uuid>, bool, String, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT request_id, was_duplicate, outcome, invalid_items
           FROM ops.submit_media_request($1, $2, 'media-request-1')",
    )
    .bind(first_id)
    .bind(r#"{"items":[{"mediaType":"movie","tmdbId":550}]}"#)
    .fetch_one(&pool)
    .await?;
    assert_eq!(first, (Some(first_id), false, "accepted".to_owned(), None));

    let duplicate: (Option<Uuid>, bool, String) = sqlx::query_as(
        "SELECT request_id, was_duplicate, outcome
           FROM ops.submit_media_request($1, $2, 'media-request-1')",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"items":[{"mediaType":"movie","tmdbId":550}]}"#)
    .fetch_one(&pool)
    .await?;
    assert_eq!(duplicate, (Some(first_id), true, "accepted".to_owned()));

    let Err(conflict) = sqlx::query(
        "SELECT request_id
           FROM ops.submit_media_request($1, $2, 'media-request-1')",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"items":[{"mediaType":"movie","tmdbId":1}]}"#)
    .execute(&pool)
    .await
    else {
        return Err(sqlx::Error::Protocol(
            "a reused media-request key must reject a different payload".to_owned(),
        ));
    };
    assert_eq!(sqlstate(&conflict).as_deref(), Some("P0003"));

    let invalid: (Option<Uuid>, String, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT request_id, outcome, invalid_items
           FROM ops.submit_media_request($1, $2, 'media-request-invalid')",
    )
    .bind(Uuid::now_v7())
    .bind(r#"{"items":[{"mediaType":"movie","tmdbId":999999}]}"#)
    .fetch_one(&pool)
    .await?;
    assert!(invalid.0.is_none());
    assert_eq!(invalid.1, "invalid");
    assert_eq!(
        invalid.2.as_ref().map(|value| &value[0]["tmdbId"]),
        Some(&serde_json::json!(999_999))
    );
    let persisted_invalid: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.media_requests WHERE idempotency_key = 'media-request-invalid'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(persisted_invalid, 0);

    let claimed: (Uuid, i64) = sqlx::query_as(
        "SELECT request_id, source_cursor FROM ops.claim_media_request('media-test', 60000000)",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed, (first_id, 0));

    let sources: Vec<(String, i64, i64, String, String, i32)> = sqlx::query_as(
        "SELECT entity_type, entity_id, owner_id, image_kind, source_path, gallery_index
           FROM assets.select_media_request_sources($1, 0, 250)
          ORDER BY image_kind, gallery_index",
    )
    .bind(first_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(sources.len(), 3);
    assert!(sources.iter().all(|source| source.0 == "movie"));
    assert!(sources.iter().all(|source| source.1 == 550));
    assert!(sources.iter().all(|source| source.2 == title_id));
    assert!(sources.iter().any(|source| {
        source.3 == "poster" && source.4 == "/primary-poster.jpg" && source.5 == 1
    }));
    assert!(
        sources.iter().any(|source| {
            source.3 == "poster" && source.4 == "/poster-02.jpg" && source.5 == 2
        })
    );
    let image_jobs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(image_jobs, 0);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn media_source_continuations_do_not_truncate_large_local_galleries(
    pool: PgPool,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, enriched_at)
         VALUES ('movie', 700, 'Large gallery', clock_timestamp())",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO source.tmdb_documents (endpoint_path, response)
         SELECT 'movie/700/images', jsonb_build_object(
             'posters', jsonb_agg(jsonb_build_object(
                 'file_path', '/poster-' || value::text || '.jpg',
                 'iso_639_1', 'en'
             ) ORDER BY value),
             'backdrops', '[]'::jsonb,
             'logos', '[]'::jsonb
         )
           FROM generate_series(1, 300) AS value",
    )
    .execute(&pool)
    .await?;
    let request_id = Uuid::now_v7();
    let _: Option<Uuid> = sqlx::query_scalar(
        "SELECT request_id FROM ops.submit_media_request(
             $1, '{\"items\":[{\"mediaType\":\"movie\",\"tmdbId\":700}]}',
             'large-gallery')",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    let _: (Uuid, i64) = sqlx::query_as(
        "SELECT request_id, source_cursor
           FROM ops.claim_media_request('large-gallery-worker', 60000000)",
    )
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.media_request_assets (
             request_item_id, source_cursor, owner_type, owner_id, image_kind,
             gallery_index, source_key
         )
         SELECT request_item_id, source_cursor, 1, owner_id, image_kind,
                gallery_index, source_path
           FROM assets.select_media_request_sources($1, 0, 250)",
    )
    .bind(request_id)
    .execute(&pool)
    .await?;
    let remaining: (i64, i32, i32) = sqlx::query_as(
        "SELECT count(*), min(gallery_index), max(gallery_index)
           FROM assets.select_media_request_sources($1, 250, 250)",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(remaining, (50, 251, 300));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn incomplete_catalog_requests_never_queue_destructive_cleanup(
    pool: PgPool,
) -> sqlx::Result<()> {
    let title_id: i64 = sqlx::query_scalar(
        "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
         VALUES ('movie', 701, 'Incomplete title') RETURNING id",
    )
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO assets.image_assets (
             title_id, image_kind, source, source_key, source_url, storage_path,
             mime_type, width, height, file_size_bytes, sha256, gallery_index,
             status, downloaded_at, verified_at
         ) VALUES (
             $1, 'poster', 'tmdb', '/old.jpg',
             'https://image.tmdb.org/t/p/w500/old.jpg', 'movies/701/posters/poster.jpg',
             'image/jpeg', 500, 750, 3, repeat('a', 64), 1,
             'ready', clock_timestamp(), clock_timestamp()
         )",
    )
    .bind(title_id)
    .execute(&pool)
    .await?;
    let request_id = Uuid::now_v7();
    let _: Option<Uuid> = sqlx::query_scalar(
        "SELECT request_id FROM ops.submit_media_request(
             $1, '{\"items\":[{\"mediaType\":\"movie\",\"tmdbId\":701}]}',
             'incomplete-cleanup')",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    let _: (Uuid, i64) = sqlx::query_as(
        "SELECT request_id, source_cursor
           FROM ops.claim_media_request('incomplete-worker', 60000000)",
    )
    .fetch_one(&pool)
    .await?;
    let catalog_incomplete: bool = sqlx::query_scalar(
        "SELECT catalog_incomplete
           FROM ops.media_request_items
          WHERE request_id = $1",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert!(catalog_incomplete);
    let advanced: bool =
        sqlx::query_scalar("SELECT ops.advance_media_request($1, 'incomplete-worker', 0, true, 0)")
            .bind(request_id)
            .fetch_one(&pool)
            .await?;
    assert!(advanced);
    let queued: i64 = sqlx::query_scalar("SELECT assets.queue_obsolete_media_request_files($1)")
        .bind(request_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(queued, 0);
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM assets.image_assets WHERE title_id = $1")
            .bind(title_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(retained, 1);
    let status: String = sqlx::query_scalar("SELECT ops.refresh_media_request($1)")
        .bind(request_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "partial");
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn legacy_media_scan_objects_are_removed_and_request_roles_are_narrow(
    pool: PgPool,
) -> sqlx::Result<()> {
    let removed: (bool, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT to_regclass('ops.media_scan_runs') IS NULL,
                to_regclass('ops.media_scan_job_links') IS NULL,
                to_regclass('ops.media_audit_runs') IS NULL,
                to_regprocedure('ops.submit_media_scan(uuid,text,text,uuid)') IS NULL,
                to_regprocedure('ops.set_media_worker_state(text,text,uuid)') IS NULL,
                to_regprocedure('ops.media_worker_claim_enabled()') IS NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(removed, (true, true, true, true, true, true));

    let privileges: (bool, bool, bool, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT
             has_function_privilege('api_job_submitter', 'ops.submit_media_request(uuid,text,text)', 'EXECUTE'),
             has_function_privilege('image_writer', 'ops.submit_media_request(uuid,text,text)', 'EXECUTE'),
             has_function_privilege('image_writer', 'ops.claim_media_request(text,bigint)', 'EXECUTE'),
             has_function_privilege('api_job_submitter', 'ops.claim_media_request(text,bigint)', 'EXECUTE'),
             has_table_privilege('monitor', 'ops.media_requests', 'SELECT'),
             has_table_privilege('image_writer', 'ops.media_requests', 'SELECT'),
             has_function_privilege('api_reader', 'ops.media_request_status(uuid)', 'EXECUTE'),
             has_table_privilege('api_reader', 'ops.media_requests', 'SELECT')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        privileges,
        (true, false, true, false, true, false, true, false)
    );
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
            ("ingest".to_owned(), "running".to_owned()),
            ("media".to_owned(), "running".to_owned()),
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

    let pause_ingest: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_worker_state('ingest', 'pause', 'operator-ingest-pause', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(pause_ingest, ("paused".to_owned(), false));

    let paused_ingest_claim: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-ingest-test', 1000000,
               ARRAY['ingest.refresh_movie']::text[])",
    )
    .fetch_optional(&pool)
    .await?;
    assert!(paused_ingest_claim.is_none());

    let claimed_media: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-media-test', 1000000,
               ARRAY['image.download']::text[])",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed_media, media_job);

    let resume_ingest: (String, bool) = sqlx::query_as(
        "SELECT state, was_duplicate
           FROM ops.set_worker_state('ingest', 'resume', 'operator-ingest-resume', $1)",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    assert_eq!(resume_ingest, ("running".to_owned(), false));

    let claimed_ingest: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.claim_job_for_types(
               'operator-ingest-test', 1000000,
               ARRAY['ingest.refresh_movie']::text[])",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed_ingest, ingest_running);

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
async fn busy_catalog_schedule_slots_remain_pending_until_they_can_submit(
    pool: PgPool,
) -> sqlx::Result<()> {
    let blocker: Uuid = sqlx::query_scalar(
        "SELECT job_id FROM ops.submit_job(
             $1, 'ingest.refresh_movie', 1, '{\"tmdbId\":1}',
             0::smallint, 3, clock_timestamp(), 'schedule-blocker')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    let slot = chrono::Utc::now()
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| sqlx::Error::Protocol("invalid fixture timestamp".to_owned()))?;
    let pending: (Option<Uuid>, String) = sqlx::query_as(
        "SELECT job_id, outcome
           FROM ops.submit_scheduled_catalog_scan('missing_only', $1, NULL, NULL)",
    )
    .bind(slot)
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, (None, "pending".to_owned()));
    sqlx::query(
        "UPDATE ops.jobs
            SET status = 'cancelled', finished_at = clock_timestamp(),
                updated_at = clock_timestamp()
          WHERE id = $1",
    )
    .bind(blocker)
    .execute(&pool)
    .await?;
    let submitted: (Option<Uuid>, String) = sqlx::query_as(
        "SELECT job_id, outcome
           FROM ops.submit_scheduled_catalog_scan('missing_only', $1, NULL, NULL)",
    )
    .bind(slot)
    .fetch_one(&pool)
    .await?;
    assert!(submitted.0.is_some());
    assert_eq!(submitted.1, "submitted");
    let mut ingest = pool.begin().await?;
    sqlx::query("SET LOCAL ROLE ingest_writer")
        .execute(&mut *ingest)
        .await?;
    let completed: bool =
        sqlx::query_scalar("SELECT ops.complete_catalog_sync('missing_only', DATE '2026-08-05')")
            .fetch_one(&mut *ingest)
            .await?;
    assert!(completed);
    ingest.commit().await?;
    let watermark: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT last_successful_window_end
           FROM ops.catalog_sync_state
          WHERE mode = 'missing_only'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(watermark, chrono::NaiveDate::from_ymd_opt(2026, 8, 5));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn media_cancel_serializes_with_inflight_request_admission(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, enriched_at)
         VALUES ('movie', 702, 'Cancellation fixture', clock_timestamp())",
    )
    .execute(&pool)
    .await?;
    let title_id: i64 = sqlx::query_scalar(
        "SELECT id FROM catalog.titles WHERE media_type = 'movie' AND tmdb_id = 702",
    )
    .fetch_one(&pool)
    .await?;
    let request_id = Uuid::now_v7();
    let _: Option<Uuid> = sqlx::query_scalar(
        "SELECT request_id FROM ops.submit_media_request(
             $1, '{\"items\":[{\"mediaType\":\"movie\",\"tmdbId\":702}]}',
             'cancel-admission')",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    let _: (Uuid, i64) = sqlx::query_as(
        "SELECT request_id, source_cursor
           FROM ops.claim_media_request('cancel-worker', 60000000)",
    )
    .fetch_one(&pool)
    .await?;
    let request_item_id: i64 =
        sqlx::query_scalar("SELECT id FROM ops.media_request_items WHERE request_id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await?;

    let mut admission = pool.begin().await?;
    let locked: bool =
        sqlx::query_scalar("SELECT ops.lock_media_request_claim($1, 'cancel-worker')")
            .bind(request_id)
            .fetch_one(&mut *admission)
            .await?;
    assert!(locked);
    let cancel_pool = pool.clone();
    let cancel = tokio::spawn(async move {
        sqlx::query_as::<_, (String, bool)>(
            "SELECT state, was_duplicate
               FROM ops.set_worker_state('media', 'cancel', 'cancel-race', $1)",
        )
        .bind(Uuid::now_v7())
        .fetch_one(&cancel_pool)
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let image_job = Uuid::now_v7();
    let _: Uuid = sqlx::query_scalar(
        "SELECT job_id FROM ops.submit_job(
             $1, 'image.download', 1, '{\"tmdbPath\":\"/race.jpg\"}',
             0::smallint, 3, clock_timestamp(), 'cancel-race-image')",
    )
    .bind(image_job)
    .fetch_one(&mut *admission)
    .await?;
    let linked: bool = sqlx::query_scalar(
        "SELECT ops.link_media_request_asset(
             $1, 'cancel-worker', $2, 1::bigint, 1::smallint, $3, 'poster', 1,
             '/race.jpg', $4, false, false
         )",
    )
    .bind(request_id)
    .bind(request_item_id)
    .bind(title_id)
    .bind(image_job)
    .fetch_one(&mut *admission)
    .await?;
    assert!(linked);
    admission.commit().await?;
    let cancelled = cancel
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))??;
    assert_eq!(cancelled.0, "stopped");
    let job_status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1")
        .bind(image_job)
        .fetch_one(&pool)
        .await?;
    assert_eq!(job_status, "cancelled");
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

    let Err(rejected) = sqlx::query(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'backpressure-new')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await
    else {
        return Err(sqlx::Error::Protocol(
            "a new image job exceeded the active queue limit".to_owned(),
        ));
    };
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

    let Err(rejected) = sqlx::query(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'ingest.refresh_movie', 1, '{\"tmdb_id\":2001}', 0::smallint, 3,
               clock_timestamp(), 'title-backpressure-new')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await
    else {
        return Err(sqlx::Error::Protocol(
            "a new title refresh exceeded the active queue limit".to_owned(),
        ));
    };
    assert_eq!(sqlstate(&rejected).as_deref(), Some("P0004"));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn prune_finished_jobs_releases_terminal_media_request_links(
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
    sqlx::query(
        "INSERT INTO catalog.titles (media_type, tmdb_id, display_title, enriched_at)
         VALUES ('movie', 550, 'Prune fixture', clock_timestamp())",
    )
    .execute(&pool)
    .await?;
    let request_id = Uuid::now_v7();
    let submitted: Option<Uuid> = sqlx::query_scalar(
        "SELECT request_id
           FROM ops.submit_media_request(
               $1, '{\"items\":[{\"mediaType\":\"movie\",\"tmdbId\":550}]}',
               'prune-media-reference')",
    )
    .bind(request_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(submitted, Some(request_id));
    let request_item_id: i64 =
        sqlx::query_scalar("SELECT id FROM ops.media_request_items WHERE request_id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await?;
    let linked_image: Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               $1, 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'prune-linked-image')",
    )
    .bind(Uuid::now_v7())
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.media_request_assets (
             request_item_id, source_cursor, owner_type, owner_id, image_kind,
             gallery_index, source_key, job_id
         ) VALUES ($1, 1, 1, 1, 'poster', 1, '/poster.jpg', $2)",
    )
    .bind(request_item_id)
    .bind(linked_image)
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
    .bind(vec![unreferenced, admin, linked_image])
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.media_requests
            SET status = 'succeeded', expansion_complete = true,
                started_at = clock_timestamp() - interval '31 days',
                finished_at = clock_timestamp() - interval '31 days',
                updated_at = clock_timestamp() - interval '31 days'
          WHERE id = $1",
    )
    .bind(request_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.media_request_items
            SET status = 'succeeded', ready_count = 1,
                updated_at = clock_timestamp() - interval '31 days'
          WHERE request_id = $1",
    )
    .bind(request_id)
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
    .bind(vec![unreferenced, admin, linked_image])
    .fetch_all(&pool)
    .await?;
    assert_eq!(remaining.len(), 1);
    assert!(!remaining.contains(&unreferenced));
    assert!(remaining.contains(&admin));
    assert!(!remaining.contains(&linked_image));
    let released: bool = sqlx::query_scalar(
        "SELECT job_id IS NULL FROM ops.media_request_assets WHERE request_item_id = $1",
    )
    .bind(request_item_id)
    .fetch_one(&pool)
    .await?;
    assert!(released);
    let retained_ready: i64 =
        sqlx::query_scalar("SELECT ready_count FROM ops.media_request_status($1)")
            .bind(request_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(retained_ready, 1);
    Ok(())
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}
