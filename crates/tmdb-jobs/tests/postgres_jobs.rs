use std::collections::HashSet;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use secrecy::SecretString;
use serde_json::{Value, json};
use sqlx::PgPool;
use tmdb_config::DatabaseConfig;
use tmdb_db::{MIGRATOR, PoolPolicy, connect_direct};
use tmdb_jobs::{FailureDisposition, JobError, JobId, JobRepository, JobStatus, NewJob, WorkerId};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const LEASE: Duration = Duration::from_secs(30);

#[test]
fn public_types_reject_unbounded_or_ambiguous_input() -> Result<(), Box<dyn std::error::Error>> {
    assert!(NewJob::noop("").is_err());
    assert!(NewJob::noop("   ").is_err());
    assert!(NewJob::noop(&"x".repeat(257)).is_err());
    assert!(NewJob::new("", 1, json!({}), "key").is_err());
    assert!(NewJob::new(&"x".repeat(129), 1, json!({}), "key").is_err());
    assert!(NewJob::new("system.noop", 0, json!({}), "key").is_err());
    assert!(NewJob::new("system.noop", 1, json!([]), "key").is_err());
    assert!(
        NewJob::new(
            "system.noop",
            1,
            json!({ "oversized": "x".repeat(70_000) }),
            "key"
        )
        .is_err()
    );
    assert!(NewJob::noop("key")?.with_max_attempts(0).is_err());
    assert!(NewJob::noop("key")?.with_max_attempts(101).is_err());
    assert!(NewJob::noop("key")?.with_priority(1_001).is_err());
    assert!(WorkerId::new("").is_err());
    assert!(WorkerId::new("worker\nforged").is_err());
    assert!(WorkerId::new(&"w".repeat(129)).is_err());
    assert!(serde_json::from_value::<WorkerId>(json!("worker\nforged")).is_err());
    assert_eq!(
        serde_json::from_value::<WorkerId>(json!("worker-valid"))?.as_str(),
        "worker-valid"
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn jobs_migration_has_exact_readiness_indexes_and_hardened_functions(
    pool: PgPool,
) -> sqlx::Result<()> {
    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM ops._sqlx_migrations WHERE success ORDER BY version",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        versions,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
        ]
    );
    let revision: String = sqlx::query_scalar("SELECT schema_revision FROM ops.readiness")
        .fetch_one(&pool)
        .await?;
    assert_eq!(revision, "0015");

    let indexes: Vec<(String, String)> = sqlx::query_as(
        "SELECT indexname, indexdef
           FROM pg_indexes
          WHERE schemaname = 'ops' AND tablename = 'jobs'
          ORDER BY indexname",
    )
    .fetch_all(&pool)
    .await?;
    assert!(indexes.iter().any(|(name, definition)| {
        name == "jobs_active_dedup_uidx"
            && definition.contains("UNIQUE")
            && definition.contains("(job_type, dedup_key)")
            && definition.contains("status = ANY")
    }));
    assert!(indexes.iter().any(|(name, definition)| {
        name == "jobs_claim_ready_idx"
            && definition.contains("priority DESC, available_at, created_at, id")
            && definition.contains("claimable")
            && definition.contains("status = ANY")
    }));
    assert!(indexes.iter().any(|(name, definition)| {
        name == "jobs_reclaim_ready_idx"
            && definition.contains("priority DESC, available_at, created_at, id")
            && definition.contains("claimable")
            && definition.contains("attempts < max_attempts")
    }));
    assert!(indexes.iter().any(|(name, definition)| {
        name == "jobs_exhausted_expired_idx"
            && definition.contains("priority DESC, available_at, created_at, id")
            && definition.contains("attempts >= max_attempts")
    }));
    assert!(
        !indexes
            .iter()
            .any(|(name, _)| name == "jobs_lease_expiry_idx")
    );

    let functions: Vec<(String, bool, Option<Vec<String>>)> = sqlx::query_as(
        "SELECT p.proname, p.prosecdef, p.proconfig
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE n.nspname = 'ops'
            AND p.proname = ANY($1)
          ORDER BY p.proname",
    )
    .bind([
        "claim_job",
        "complete_job",
        "dead_letter_job",
        "fail_job",
        "heartbeat_job",
        "request_job_cancel",
        "submit_job",
    ])
    .fetch_all(&pool)
    .await?;
    assert_eq!(functions.len(), 7);
    for (_, security_definer, settings) in functions {
        assert!(security_definer);
        assert_eq!(
            settings,
            Some(vec!["search_path=pg_catalog, ops, pg_temp".to_owned()])
        );
    }

    let public_execute: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
           CROSS JOIN LATERAL
                aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) privilege
          WHERE n.nspname = 'ops'
            AND p.proname = ANY($1)
            AND privilege.grantee = 0
            AND privilege.privilege_type = 'EXECUTE'",
    )
    .bind([
        "claim_job",
        "complete_job",
        "dead_letter_job",
        "fail_job",
        "heartbeat_job",
        "request_job_cancel",
        "submit_job",
    ])
    .fetch_one(&pool)
    .await?;
    assert_eq!(public_execute, 0);

    let execution_matrix: Vec<(String, String)> = sqlx::query_as(
        "SELECT role.rolname, function.proname
           FROM pg_roles AS role
           CROSS JOIN pg_proc AS function
           JOIN pg_namespace AS namespace ON namespace.oid = function.pronamespace
          WHERE role.rolname = ANY($1)
            AND namespace.nspname = 'ops'
            AND function.proname = ANY($2)
            AND has_function_privilege(role.oid, function.oid, 'EXECUTE')
          ORDER BY role.rolname, function.proname",
    )
    .bind([
        "api_reader",
        "api_job_submitter",
        "ingest_writer",
        "image_writer",
        "monitor",
    ])
    .bind([
        "claim_job",
        "complete_job",
        "dead_letter_job",
        "fail_job",
        "heartbeat_job",
        "request_job_cancel",
        "submit_job",
    ])
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        execution_matrix,
        [
            (
                "api_job_submitter".to_owned(),
                "request_job_cancel".to_owned()
            ),
            ("api_job_submitter".to_owned(), "submit_job".to_owned()),
            ("image_writer".to_owned(), "claim_job".to_owned()),
            ("image_writer".to_owned(), "complete_job".to_owned()),
            ("image_writer".to_owned(), "dead_letter_job".to_owned()),
            ("image_writer".to_owned(), "fail_job".to_owned()),
            ("image_writer".to_owned(), "heartbeat_job".to_owned()),
            ("ingest_writer".to_owned(), "claim_job".to_owned()),
            ("ingest_writer".to_owned(), "complete_job".to_owned()),
            ("ingest_writer".to_owned(), "dead_letter_job".to_owned()),
            ("ingest_writer".to_owned(), "fail_job".to_owned()),
            ("ingest_writer".to_owned(), "heartbeat_job".to_owned()),
            ("ingest_writer".to_owned(), "submit_job".to_owned()),
        ]
    );

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn repository_get_reads_legacy_infinity_after_v2_to_v4_upgrade(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::raw_sql(
        "DO $grant$
         BEGIN
             EXECUTE format('GRANT CREATE ON DATABASE %I TO migrator', current_database());
         END
         $grant$;",
    )
    .execute(&owner_pool)
    .await?;
    MIGRATOR.run_to(2, &owner_pool).await?;
    let legacy_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               gen_random_uuid(), 'system.noop', 1, '{}'::text, 0::smallint, 3,
               'infinity'::timestamptz, 'repository-legacy-infinity')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let stored_available_at: String =
        sqlx::query_scalar("SELECT available_at::text FROM ops.jobs WHERE id = $1")
            .bind(legacy_id)
            .fetch_one(&owner_pool)
            .await?;
    assert_eq!(stored_available_at, "infinity");
    MIGRATOR.run(&owner_pool).await?;

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let worker_pool = role_pool(&database, "ingest_writer", PoolPolicy::ReadWrite).await?;
    let repository = JobRepository::new(worker_pool.clone());
    let result = repository.get(legacy_id.into()).await;
    let legacy = result?;
    assert_eq!(legacy.status(), JobStatus::Queued);
    assert_eq!(legacy.available_at(), None);
    worker_pool.close().await;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn duplicate_active_dedup_key_returns_original_and_terminal_state_releases_it(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let first = repo.submit(NewJob::noop("same-key")?).await?;
    let second = repo.submit(NewJob::noop("same-key")?).await?;
    assert_eq!(first.job_id(), second.job_id());
    assert!(!first.was_duplicate());
    assert!(second.was_duplicate());

    let cancelled = repo
        .request_cancel(first.job_id(), "superseded before claim")
        .await?;
    assert_eq!(cancelled.status(), JobStatus::Cancelled);
    let after_cancel = repo.submit(NewJob::noop("same-key")?).await?;
    assert_ne!(after_cancel.job_id(), first.job_id());

    let worker = WorkerId::new("dedup-worker")?;
    let claimed = repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("replacement job was not claimable")?;
    assert_eq!(claimed.job_id(), after_cancel.job_id());
    let completed = repo.complete(claimed.job_id(), &worker, json!({})).await?;
    assert_eq!(completed.status(), JobStatus::Succeeded);
    let after_success = repo.submit(NewJob::noop("same-key")?).await?;
    assert_ne!(after_success.job_id(), after_cancel.job_id());
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn batch_submission_preserves_order_and_active_deduplication(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = JobRepository::new(pool);
    let submitted = repository
        .submit_many(&[
            NewJob::noop("batch-first")?,
            NewJob::noop("batch-second")?,
            NewJob::noop("batch-third")?,
        ])
        .await?;

    assert_eq!(submitted.len(), 3);
    assert!(submitted.iter().all(|outcome| !outcome.was_duplicate()));
    assert_ne!(submitted[0].job_id(), submitted[1].job_id());
    assert_ne!(submitted[1].job_id(), submitted[2].job_id());

    let repeated = repository
        .submit_many(&[
            NewJob::noop("batch-first")?,
            NewJob::noop("batch-second")?,
            NewJob::noop("batch-third")?,
        ])
        .await?;

    assert_eq!(repeated.len(), 3);
    assert!(repeated.iter().all(|outcome| outcome.was_duplicate()));
    assert_eq!(submitted[0].job_id(), repeated[0].job_id());
    assert_eq!(submitted[1].job_id(), repeated[1].job_id());
    assert_eq!(submitted[2].job_id(), repeated[2].job_id());
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn batch_submission_accepts_the_full_bounded_export_chunk(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = JobRepository::new(pool);
    let jobs = (0..500)
        .map(|index| NewJob::noop(&format!("bounded-export-{index}")))
        .collect::<Result<Vec<_>, _>>()?;

    let outcomes = repository.submit_many(&jobs).await?;
    assert_eq!(outcomes.len(), 500);
    assert!(outcomes.iter().all(|outcome| !outcome.was_duplicate()));

    let oversized = (0..501)
        .map(|index| NewJob::noop(&format!("oversized-export-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(matches!(
        repository.submit_many(&oversized).await,
        Err(JobError::Validation(tmdb_jobs::ValidationError::BatchSize))
    ));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn simultaneous_same_key_submitters_return_one_id_and_one_new_submission(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let barrier = std::sync::Arc::new(Barrier::new(9));
    let mut tasks = JoinSet::new();
    for _ in 0..8 {
        let task_repo = repo.clone();
        let task_barrier = std::sync::Arc::clone(&barrier);
        tasks.spawn(async move {
            let job = NewJob::noop("same-key-simultaneous")?;
            task_barrier.wait().await;
            task_repo.submit(job).await
        });
    }
    barrier.wait().await;

    let mut ids = HashSet::new();
    let mut new_submissions = 0;
    while let Some(result) = tasks.join_next().await {
        let outcome = result??;
        ids.insert(outcome.job_id());
        if !outcome.was_duplicate() {
            new_submissions += 1;
        }
    }
    assert_eq!(ids.len(), 1);
    assert_eq!(new_submissions, 1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn simultaneous_submit_and_terminal_release_is_linearizable_in_both_orders(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool.clone());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let submitter_pool = role_pool(&database, "api_job_submitter", PoolPolicy::ReadWrite).await?;

    // First force submit-before-terminal. The duplicate submission finishes, then
    // its transaction holds a key-share lock so cancellation is visibly waiting
    // before the transaction commits and releases the incumbent dedup key.
    let submit_first_key = "submit-before-terminal";
    let submit_first_original = repo.submit(NewJob::noop(submit_first_key)?).await?;
    let mut submit_first_tx = pool.begin().await?;
    let (duplicate_id, was_duplicate): (String, bool) = sqlx::query_as(
        "SELECT job_id::text, was_duplicate
           FROM ops.submit_job(
               pg_catalog.gen_random_uuid(), 'system.noop', 1, '{}', 0::smallint,
               3, NULL::timestamptz, $1
           )",
    )
    .bind(submit_first_key)
    .fetch_one(&mut *submit_first_tx)
    .await?;
    assert!(was_duplicate);
    assert_eq!(
        duplicate_id,
        submit_first_original.job_id().as_uuid().to_string()
    );
    sqlx::query("SELECT id FROM ops.jobs WHERE id = $1 FOR KEY SHARE")
        .bind(submit_first_original.job_id().as_uuid())
        .fetch_one(&mut *submit_first_tx)
        .await?;

    let mut cancellation_connection = submitter_pool.acquire().await?;
    let cancellation_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *cancellation_connection)
        .await?;
    let cancellation_id = submit_first_original.job_id().as_uuid();
    let cancellation_task = tokio::spawn(async move {
        sqlx::query_as::<_, (String, bool)>(
            "SELECT job_status, cancellation_requested
               FROM ops.request_job_cancel($1, 'submit ordered first')",
        )
        .bind(cancellation_id)
        .fetch_one(&mut *cancellation_connection)
        .await
    });
    wait_until_backend_is_lock_waiting(&pool, cancellation_pid).await?;
    submit_first_tx.commit().await?;
    assert_eq!(cancellation_task.await??, ("cancelled".to_owned(), false));
    assert_eq!(active_jobs_for_key(&pool, submit_first_key).await?, 0);

    // Then force terminal-before-submit. The cancellation update remains
    // uncommitted while a new submitter blocks on the active-dedup index entry;
    // committing the terminal transaction releases that entry for one new job.
    let terminal_first_key = "terminal-before-submit";
    let terminal_first_original = repo.submit(NewJob::noop(terminal_first_key)?).await?;
    let mut terminal_first_tx = pool.begin().await?;
    let terminal_result: (String, bool) = sqlx::query_as(
        "SELECT job_status, cancellation_requested
           FROM ops.request_job_cancel($1, 'terminal ordered first')",
    )
    .bind(terminal_first_original.job_id().as_uuid())
    .fetch_one(&mut *terminal_first_tx)
    .await?;
    assert_eq!(terminal_result, ("cancelled".to_owned(), false));

    let mut submission_connection = submitter_pool.acquire().await?;
    let submission_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *submission_connection)
        .await?;
    let submission_task = tokio::spawn(async move {
        sqlx::query_as::<_, (String, bool)>(
            "SELECT job_id::text, was_duplicate
               FROM ops.submit_job(
                   pg_catalog.gen_random_uuid(), 'system.noop', 1, '{}', 0::smallint,
                   3, NULL::timestamptz, $1
               )",
        )
        .bind(terminal_first_key)
        .fetch_one(&mut *submission_connection)
        .await
    });
    wait_until_backend_is_lock_waiting(&pool, submission_pid).await?;
    terminal_first_tx.commit().await?;
    let (replacement_id, was_duplicate) = submission_task.await??;
    assert!(!was_duplicate);
    assert_ne!(
        replacement_id,
        terminal_first_original.job_id().as_uuid().to_string()
    );
    assert_eq!(active_jobs_for_key(&pool, terminal_first_key).await?, 1);
    let replacement_id: JobId = replacement_id.parse::<sqlx::types::Uuid>()?.into();
    assert_eq!(repo.get(replacement_id).await?.status(), JobStatus::Queued);
    repo.request_cancel(replacement_id, "race replacement cleanup")
        .await?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn eight_simultaneous_claimers_never_receive_the_same_job(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let mut submitted = HashSet::new();
    for index in 0..8 {
        submitted.insert(
            repo.submit(NewJob::noop(&format!("race-{index}"))?)
                .await?
                .job_id(),
        );
    }

    let barrier = std::sync::Arc::new(Barrier::new(9));
    let mut tasks = JoinSet::new();
    for index in 0..8 {
        let task_repo = repo.clone();
        let task_barrier = std::sync::Arc::clone(&barrier);
        tasks.spawn(async move {
            let worker = WorkerId::new(&format!("race-worker-{index}"))?;
            task_barrier.wait().await;
            task_repo.claim(&worker, LEASE).await
        });
    }
    barrier.wait().await;

    let mut claimed = HashSet::new();
    while let Some(result) = tasks.join_next().await {
        let job = result??.ok_or("a concurrent claimer received no job")?;
        assert!(claimed.insert(job.job_id()), "duplicate concurrent claim");
    }
    assert_eq!(claimed, submitted);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn claim_order_is_priority_then_availability_creation_and_id(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool.clone());
    let available_at = Utc::now() - ChronoDuration::minutes(1);
    let low = repo
        .submit(
            NewJob::noop("order-low")?
                .with_priority(-10)?
                .with_available_at(available_at)?,
        )
        .await?
        .job_id();
    let high = repo
        .submit(
            NewJob::noop("order-high")?
                .with_priority(10)?
                .with_available_at(available_at)?,
        )
        .await?
        .job_id();
    let middle = repo
        .submit(NewJob::noop("order-middle")?.with_available_at(available_at)?)
        .await?
        .job_id();

    let worker = WorkerId::new("ordered-worker")?;
    for expected in [high, middle, low] {
        let claimed = repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("expected ordered job was not claimable")?;
        assert_eq!(claimed.job_id(), expected);
        repo.complete(expected, &worker, json!({})).await?;
    }

    let first_tie = repo
        .submit(NewJob::noop("order-tie-first")?)
        .await?
        .job_id();
    let second_tie = repo
        .submit(NewJob::noop("order-tie-second")?)
        .await?
        .job_id();
    let third_tie = repo
        .submit(NewJob::noop("order-tie-third")?)
        .await?
        .job_id();
    let fourth_tie = repo
        .submit(NewJob::noop("order-tie-fourth")?)
        .await?
        .job_id();
    let base = Utc::now() - ChronoDuration::hours(1);
    sqlx::query(
        "UPDATE ops.jobs
            SET priority = 0, available_at = $2, created_at = $3, updated_at = $3
          WHERE id = $1",
    )
    .bind(first_tie.as_uuid())
    .bind(base)
    .bind(base + ChronoDuration::seconds(2))
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.jobs
            SET priority = 0, available_at = $2, created_at = $3, updated_at = $3
          WHERE id = $1",
    )
    .bind(second_tie.as_uuid())
    .bind(base + ChronoDuration::seconds(1))
    .bind(base)
    .execute(&pool)
    .await?;
    for id in [third_tie, fourth_tie] {
        sqlx::query(
            "UPDATE ops.jobs
                SET priority = 0, available_at = $2, created_at = $3, updated_at = $3
              WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(base + ChronoDuration::seconds(1))
        .bind(base + ChronoDuration::seconds(1))
        .execute(&pool)
        .await?;
    }
    let mut id_ties = [third_tie, fourth_tie];
    id_ties.sort_unstable();
    for expected in [first_tie, second_tie, id_ties[0], id_ties[1]] {
        let claimed = repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("expected tie-break job was not claimable")?;
        assert_eq!(claimed.job_id(), expected);
        repo.complete(expected, &worker, json!({})).await?;
    }
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn expired_lease_reclaims_and_wrong_or_expired_owners_cannot_mutate(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool.clone());
    let submitted = repo.submit(NewJob::noop("lease-ownership")?).await?;
    let first_worker = WorkerId::new("worker-first")?;
    let wrong_worker = WorkerId::new("worker-wrong")?;
    let second_worker = WorkerId::new("worker-second")?;
    let first_claim = repo
        .claim(&first_worker, LEASE)
        .await?
        .ok_or("job was not initially claimable")?;
    assert_eq!(first_claim.attempts(), 1);

    assert!(matches!(
        repo.heartbeat(submitted.job_id(), &wrong_worker, LEASE)
            .await,
        Err(JobError::LeaseLost)
    ));
    assert!(matches!(
        repo.complete(submitted.job_id(), &wrong_worker, json!({}))
            .await,
        Err(JobError::LeaseLost)
    ));
    assert!(matches!(
        repo.fail(
            submitted.job_id(),
            &wrong_worker,
            "execution_failed",
            Duration::from_secs(1)
        )
        .await,
        Err(JobError::LeaseLost)
    ));

    sqlx::query(
        "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second'
          WHERE id = $1",
    )
    .bind(submitted.job_id().as_uuid())
    .execute(&pool)
    .await?;
    assert!(matches!(
        repo.heartbeat(submitted.job_id(), &first_worker, LEASE)
            .await,
        Err(JobError::LeaseLost)
    ));
    assert!(matches!(
        repo.complete(submitted.job_id(), &first_worker, json!({}))
            .await,
        Err(JobError::LeaseLost)
    ));
    assert!(matches!(
        repo.fail(
            submitted.job_id(),
            &first_worker,
            "lease_expired",
            Duration::from_secs(1)
        )
        .await,
        Err(JobError::LeaseLost)
    ));

    let reclaimed = repo
        .claim(&second_worker, LEASE)
        .await?
        .ok_or("expired job was not reclaimed")?;
    assert_eq!(reclaimed.job_id(), submitted.job_id());
    assert_eq!(reclaimed.attempts(), 2);
    let events: Vec<(String, Option<String>, String, Option<String>)> = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, worker_id
           FROM ops.job_events WHERE job_id = $1 ORDER BY created_at, id",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        events,
        [
            ("submitted".to_owned(), None, "queued".to_owned(), None),
            (
                "claimed".to_owned(),
                Some("queued".to_owned()),
                "running".to_owned(),
                Some("worker-first".to_owned())
            ),
            (
                "reclaimed".to_owned(),
                Some("running".to_owned()),
                "running".to_owned(),
                Some("worker-second".to_owned())
            )
        ]
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn retry_timestamp_is_exact_and_max_attempts_dead_letter_atomically(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool.clone());
    let submitted = repo
        .submit(NewJob::noop("retry-exact")?.with_max_attempts(2)?)
        .await?;
    let worker = WorkerId::new("retry-worker")?;
    let first = repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("first retry attempt was not claimable")?;
    assert_eq!(first.attempts(), 1);

    let retry_delay = Duration::from_micros(37_123_456);
    let disposition = repo
        .fail(
            submitted.job_id(),
            &worker,
            "upstream_unavailable",
            retry_delay,
        )
        .await?;
    let FailureDisposition::RetryScheduled { available_at } = disposition else {
        return Err("first failure did not schedule a retry".into());
    };
    let stored_available_at = repo.get(submitted.job_id()).await?.available_at();
    assert_eq!(Some(available_at), stored_available_at);
    let exact_delay_micros: i64 = sqlx::query_scalar(
        "SELECT (extract(epoch FROM (j.available_at - e.created_at)) * 1000000)::bigint
           FROM ops.jobs j
           JOIN ops.job_events e ON e.job_id = j.id
          WHERE j.id = $1 AND e.event_kind = 'retry_scheduled'",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert_eq!(exact_delay_micros, 37_123_456);

    sqlx::query("UPDATE ops.jobs SET available_at = clock_timestamp() WHERE id = $1")
        .bind(submitted.job_id().as_uuid())
        .execute(&pool)
        .await?;
    let second = repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("scheduled retry was not claimable")?;
    assert_eq!(second.attempts(), 2);
    assert_eq!(
        repo.fail(
            submitted.job_id(),
            &worker,
            "attempts_exhausted",
            retry_delay
        )
        .await?,
        FailureDisposition::DeadLettered
    );
    let dead = repo.get(submitted.job_id()).await?;
    assert_eq!(dead.status(), JobStatus::DeadLetter);
    assert!(dead.finished_at().is_some());
    assert_eq!(dead.error_message(), Some("attempts_exhausted"));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn repository_dead_letter_terminates_a_live_lease_without_retry(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = JobRepository::new(pool.clone());
    let submitted = repository
        .submit(NewJob::noop("terminal-repository")?.with_max_attempts(5)?)
        .await?;
    let worker = WorkerId::new("terminal-repository-worker")?;
    let claimed = repository
        .claim(&worker, LEASE)
        .await?
        .ok_or("terminal fixture was not claimable")?;
    assert_eq!(claimed.attempts(), 1);
    assert_eq!(
        repository
            .dead_letter(submitted.job_id(), &worker, "invalid_payload")
            .await?,
        FailureDisposition::DeadLettered
    );
    let dead = repository.get(submitted.job_id()).await?;
    assert_eq!(dead.status(), JobStatus::DeadLetter);
    assert_eq!(dead.attempts(), 1);
    assert_eq!(dead.error_message(), Some("invalid_payload"));
    assert!(dead.finished_at().is_some());
    let terminal_event: (String, Value) = sqlx::query_as(
        "SELECT event_kind, details FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'dead_lettered'",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_one(&pool)
    .await?;
    assert_eq!(terminal_event.0, "dead_lettered");
    assert_eq!(terminal_event.1, json!({"terminal": true}));

    let auth_failure = repository
        .submit(NewJob::noop("terminal-upstream-auth")?)
        .await
        .map_err(|error| format!("submit auth fixture: {error:?}"))?;
    let claimed_auth = repository
        .claim(&worker, LEASE)
        .await?
        .ok_or("upstream auth fixture was not claimable")?;
    assert_eq!(claimed_auth.job_id(), auth_failure.job_id());
    assert_eq!(
        repository
            .dead_letter(auth_failure.job_id(), &worker, "upstream_unauthorized")
            .await
            .map_err(|error| format!("dead-letter auth fixture: {error:?}"))?,
        FailureDisposition::DeadLettered
    );
    assert_eq!(
        repository.get(auth_failure.job_id()).await?.error_message(),
        Some("upstream_unauthorized")
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn one_claim_bounds_expired_exhaustion_cleanup_to_one_job(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool.clone());
    let first = repo
        .submit(NewJob::noop("bounded-expiry-first")?.with_max_attempts(1)?)
        .await?;
    let second = repo
        .submit(NewJob::noop("bounded-expiry-second")?.with_max_attempts(1)?)
        .await?;
    let first_worker = WorkerId::new("bounded-expiry-worker-one")?;
    let second_worker = WorkerId::new("bounded-expiry-worker-two")?;
    repo.claim(&first_worker, LEASE)
        .await?
        .ok_or("first exhausted fixture was not claimed")?;
    repo.claim(&second_worker, LEASE)
        .await?
        .ok_or("second exhausted fixture was not claimed")?;
    sqlx::query(
        "UPDATE ops.jobs
            SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
          WHERE id = ANY($1)",
    )
    .bind([first.job_id().as_uuid(), second.job_id().as_uuid()])
    .execute(&pool)
    .await?;

    let ready = repo
        .submit(NewJob::noop("bounded-expiry-ready")?.with_priority(100)?)
        .await?;
    let claimant = WorkerId::new("bounded-expiry-claimant")?;
    let claimed = repo
        .claim(&claimant, LEASE)
        .await?
        .ok_or("ready job behind exhausted leases was not claimed")?;
    assert_eq!(claimed.job_id(), ready.job_id());
    let dead_letter_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.jobs
          WHERE id = ANY($1) AND status = 'dead_letter'",
    )
    .bind([first.job_id().as_uuid(), second.job_id().as_uuid()])
    .fetch_one(&pool)
    .await?;
    assert_eq!(dead_letter_count, 1);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn production_claim_skips_disabled_ready_and_reclaim_backlog_before_limit(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = pool.acquire().await?;
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
         INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('plan.disabled', 1, false), ('plan.enabled', 1, true);
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, dedup_key, created_at, updated_at
         )
         SELECT gen_random_uuid(), 'plan.disabled', 1, '{}'::jsonb, 100, 'queued', 0, 3,
                clock_timestamp() - interval '1 hour', 'disabled-ready-' || sequence,
                clock_timestamp() - interval '2 hours', clock_timestamp()
           FROM generate_series(1, 15000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, lease_owner, lease_expires_at, dedup_key,
             created_at, updated_at
         )
         SELECT gen_random_uuid(), 'plan.disabled', 1, '{}'::jsonb, 99, 'running', 1, 3,
                clock_timestamp() - interval '2 hours', 'disabled-reclaim-worker',
                clock_timestamp() - interval '1 hour', 'disabled-reclaim-' || sequence,
                clock_timestamp() - interval '3 hours', clock_timestamp()
           FROM generate_series(1, 10000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, dedup_key, created_at, updated_at
         ) VALUES (
             gen_random_uuid(), 'plan.enabled', 1, '{}'::jsonb, 1, 'queued', 0, 3,
             clock_timestamp() - interval '1 hour', 'enabled-ready',
             clock_timestamp() - interval '2 hours', clock_timestamp()
         );
         ANALYZE ops.jobs;
         SET pg_stat_statements.track = 'all';
         SELECT pg_stat_statements_reset();",
    )
    .execute(&mut *connection)
    .await?;

    let claimed: (uuid::Uuid, String) = sqlx::query_as(
        "SELECT job_id, job_type
           FROM ops.claim_job('production-backlog-worker', 1000000)",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(claimed.1, "plan.enabled");

    let disabled_remaining: (i64, i64) = sqlx::query_as(
        "SELECT
             count(*) FILTER (WHERE status = 'queued'),
             count(*) FILTER (WHERE status = 'running' AND attempts < max_attempts)
           FROM ops.jobs
          WHERE job_type = 'plan.disabled'",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(disabled_remaining, (15_000, 10_000));

    let claim_stats: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT query, calls, rows, shared_blks_hit
           FROM pg_stat_statements
          WHERE query LIKE '%ready_candidate%'
             OR query LIKE '%reclaim_candidate%'
          ORDER BY query",
    )
    .fetch_all(&mut *connection)
    .await?;
    assert!(
        !claim_stats.is_empty(),
        "production claim internals were not measured"
    );
    let total_hits: i64 = claim_stats.iter().map(|(_, _, _, hits)| *hits).sum();
    assert!(
        total_hits < 128,
        "production claim scanned disabled backlog before LIMIT: hits={total_hits}, stats={claim_stats:?}"
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn production_claim_cleans_one_expired_cancellation_backlog_before_limit(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = pool.acquire().await?;
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
         INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('plan.cancelled-disabled', 1, false), ('plan.cancelled-enabled', 1, true);
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, lease_owner, lease_expires_at, cancellation_requested,
             dedup_key, created_at, updated_at
         )
         SELECT gen_random_uuid(), 'plan.cancelled-disabled', 1, '{}'::jsonb, 100, 'running', 1, 3,
                clock_timestamp() - interval '2 hours', 'disabled-worker',
                clock_timestamp() - interval '1 hour', true,
                'disabled-cancelled-' || sequence,
                clock_timestamp() - interval '3 hours', clock_timestamp()
           FROM generate_series(1, 15000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, dedup_key, created_at, updated_at
         ) VALUES (
             gen_random_uuid(), 'plan.cancelled-enabled', 1, '{}'::jsonb, 1, 'queued', 0, 3,
             clock_timestamp() - interval '1 hour', 'enabled-after-cancel-cleanup',
             clock_timestamp() - interval '2 hours', clock_timestamp()
         );
         ANALYZE ops.jobs;
         SET pg_stat_statements.track = 'all';
         SELECT pg_stat_statements_reset();",
    )
    .execute(&mut *connection)
    .await?;

    let claimed: (uuid::Uuid, String) = sqlx::query_as(
        "SELECT job_id, job_type
           FROM ops.claim_job('cancellation-backlog-worker', 1000000)",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(claimed.1, "plan.cancelled-enabled");
    let disabled_state: (i64, i64) = sqlx::query_as(
        "SELECT
             count(*) FILTER (WHERE status = 'running'),
             count(*) FILTER (WHERE status = 'cancelled')
           FROM ops.jobs
          WHERE job_type = 'plan.cancelled-disabled'",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(disabled_state, (14_999, 1));
    let cleanup_stats: Vec<(String, i64)> = sqlx::query_as(
        "SELECT query, shared_blks_hit
           FROM pg_stat_statements
          WHERE query LIKE 'SELECT job.*%cancellation_requested%'
            AND query LIKE '%lease_expires_at%'
          ORDER BY query",
    )
    .fetch_all(&mut *connection)
    .await?;
    assert!(
        !cleanup_stats.is_empty(),
        "cancellation cleanup internals were not measured"
    );
    let total_hits: i64 = cleanup_stats.iter().map(|(_, hits)| *hits).sum();
    assert!(
        total_hits < 128,
        "cancellation cleanup scanned disabled backlog before LIMIT: hits={total_hits}, stats={cleanup_stats:?}"
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn representative_claim_and_cleanup_plans_bound_backlog_work_before_limit(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::raw_sql(
        "INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, dedup_key, created_at, updated_at
         )
         SELECT gen_random_uuid(), 'system.noop', 1, '{}'::jsonb,
                (sequence % 101)::smallint, 'queued', 0, 3,
                clock_timestamp() - interval '1 hour',
                'plan-ready-' || sequence,
                clock_timestamp() - interval '2 hours', clock_timestamp()
           FROM generate_series(1, 12000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, dedup_key, created_at, updated_at
         )
         SELECT gen_random_uuid(), 'system.noop', 1, '{}'::jsonb,
                (sequence % 101)::smallint, 'queued', 0, 3,
                clock_timestamp() + interval '1 day',
                'plan-future-' || sequence,
                clock_timestamp() - interval '1 hour', clock_timestamp()
           FROM generate_series(1, 12000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, lease_owner, lease_expires_at, dedup_key,
             created_at, updated_at
         )
         SELECT gen_random_uuid(), 'system.noop', 1, '{}'::jsonb,
                (sequence % 101)::smallint, 'running', 1, 3,
                clock_timestamp() - interval '2 hours', 'plan-worker',
                clock_timestamp() - interval '1 hour',
                'plan-reclaim-' || sequence,
                clock_timestamp() - interval '3 hours', clock_timestamp()
           FROM generate_series(1, 4000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, lease_owner, lease_expires_at, dedup_key,
             created_at, updated_at
         )
         SELECT gen_random_uuid(), 'system.noop', 1, '{}'::jsonb,
                (sequence % 101)::smallint, 'running', 1, 3,
                clock_timestamp() - interval '2 hours', 'plan-worker',
                clock_timestamp() + interval '1 day',
                'plan-live-' || sequence,
                clock_timestamp() - interval '3 hours', clock_timestamp()
           FROM generate_series(1, 4000) AS sequence;
         INSERT INTO ops.jobs(
             id, job_type, payload_version, payload, priority, status, attempts,
             max_attempts, available_at, lease_owner, lease_expires_at, dedup_key,
             created_at, updated_at
         )
         SELECT gen_random_uuid(), 'system.noop', 1, '{}'::jsonb,
                (sequence % 101)::smallint, 'running', 3, 3,
                clock_timestamp() - interval '2 hours', 'plan-worker',
                clock_timestamp() - interval '1 hour',
                'plan-exhausted-' || sequence,
                clock_timestamp() - interval '3 hours', clock_timestamp()
           FROM generate_series(1, 3000) AS sequence;
         UPDATE ops.jobs SET claimable = true WHERE job_type = 'system.noop';
         ANALYZE ops.jobs;",
    )
    .execute(&pool)
    .await?;

    let cardinality: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             count(*) FILTER (WHERE status IN ('queued', 'retry_wait')
                               AND available_at <= clock_timestamp()),
             count(*) FILTER (WHERE status = 'running' AND attempts < max_attempts
                               AND lease_expires_at <= clock_timestamp()),
             count(*) FILTER (WHERE status = 'running' AND attempts >= max_attempts
                               AND lease_expires_at <= clock_timestamp())
           FROM ops.jobs",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(cardinality, (12_000, 4_000, 3_000));

    let claim_plan: Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
         WITH ready_candidate AS MATERIALIZED (
             SELECT job.id, job.status AS from_status, job.priority,
                    job.available_at, job.created_at
               FROM ops.jobs AS job
               JOIN ops.job_type_registry AS registry
                 ON registry.job_type = job.job_type
                AND registry.payload_version = job.payload_version
                AND registry.enabled
              WHERE job.claimable
                AND job.status IN ('queued', 'retry_wait')
                AND job.available_at <= clock_timestamp()
              ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
              FOR UPDATE OF job SKIP LOCKED
              LIMIT 1
         ), reclaim_candidate AS MATERIALIZED (
             SELECT job.id, job.status AS from_status, job.priority,
                    job.available_at, job.created_at
               FROM ops.jobs AS job
               JOIN ops.job_type_registry AS registry
                 ON registry.job_type = job.job_type
                AND registry.payload_version = job.payload_version
                AND registry.enabled
              WHERE job.claimable
                AND job.status = 'running'
                AND job.attempts < job.max_attempts
                AND job.lease_expires_at <= clock_timestamp()
              ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
              FOR UPDATE OF job SKIP LOCKED
              LIMIT 1
         ), candidate AS (
             SELECT ranked.id, ranked.from_status
               FROM (
                   SELECT * FROM ready_candidate
                   UNION ALL
                   SELECT * FROM reclaim_candidate
               ) AS ranked
              ORDER BY ranked.priority DESC, ranked.available_at,
                       ranked.created_at, ranked.id
              LIMIT 1
         )
         SELECT * FROM candidate",
    )
    .fetch_one(&pool)
    .await?;
    assert_backlog_bounded_plan(&claim_plan);

    let cleanup_plan: Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
         SELECT job.id
           FROM ops.jobs AS job
          WHERE job.status = 'running'
            AND job.attempts >= job.max_attempts
            AND job.lease_expires_at <= clock_timestamp()
          ORDER BY job.priority DESC, job.available_at, job.created_at, job.id
          FOR UPDATE OF job SKIP LOCKED
          LIMIT 1",
    )
    .fetch_one(&pool)
    .await?;
    assert_backlog_bounded_plan(&cleanup_plan);
    Ok(())
}

fn assert_backlog_bounded_plan(plan: &Value) {
    fn visit(node: &Value) {
        let node_type = node["Node Type"].as_str().unwrap_or_default();
        let relation = node["Relation Name"].as_str().unwrap_or_default();
        let actual_rows = node["Actual Rows"].as_u64().unwrap_or_default();
        assert!(
            !(node_type == "Seq Scan" && relation == "jobs"),
            "backlog-wide jobs Seq Scan remained before LIMIT: {node}"
        );
        assert_ne!(
            node_type, "BitmapOr",
            "bitmap combination discards queue order before LIMIT: {node}"
        );
        assert!(
            node_type != "Sort" || actual_rows <= 2,
            "backlog-wide Sort processed {actual_rows} rows before LIMIT: {node}"
        );
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                visit(child);
            }
        }
    }

    visit(&plan[0]["Plan"]);
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn cancellation_prevents_claim_and_running_request_stays_active_until_acknowledged(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let queued = repo.submit(NewJob::noop("cancel-queued")?).await?;
    let cancelled = repo
        .request_cancel(queued.job_id(), "operator cancelled queued work")
        .await?;
    assert_eq!(cancelled.status(), JobStatus::Cancelled);

    let worker = WorkerId::new("cancel-worker")?;
    assert!(repo.claim(&worker, LEASE).await?.is_none());

    let running = repo.submit(NewJob::noop("cancel-running")?).await?;
    let claimed = repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("running cancellation fixture was not claimed")?;
    assert_eq!(claimed.job_id(), running.job_id());
    let requested = repo
        .request_cancel(running.job_id(), "stop after current safe point")
        .await?;
    assert_eq!(requested.status(), JobStatus::Running);
    assert!(requested.cancellation_requested());
    let duplicate = repo.submit(NewJob::noop("cancel-running")?).await?;
    assert_eq!(duplicate.job_id(), running.job_id());
    assert!(duplicate.was_duplicate());

    let acknowledged = repo
        .complete(running.job_id(), &worker, json!({ "ignored": true }))
        .await?;
    assert_eq!(acknowledged.status(), JobStatus::Cancelled);
    assert!(!acknowledged.cancellation_requested());
    let replacement = repo.submit(NewJob::noop("cancel-running")?).await?;
    assert_ne!(replacement.job_id(), running.job_id());
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn closing_and_recreating_the_pool_preserves_accepted_jobs(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&pool)
        .await?;
    let repo = JobRepository::new(pool.clone());
    let submitted = repo.submit(NewJob::noop("pool-recreate")?).await?;
    drop(repo);
    pool.close().await;

    let reopened = role_pool(&database, &owner, PoolPolicy::ReadWrite).await?;
    let reopened_repo = JobRepository::new(reopened);
    let persisted = reopened_repo.get(submitted.job_id()).await?;
    assert_eq!(persisted.status(), JobStatus::Queued);
    assert_eq!(persisted.attempts(), 0);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn every_repository_mutation_appends_one_explicit_immutable_event(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let repo = JobRepository::new(pool.clone());
    let submitted = repo.submit(NewJob::noop("event-history")?).await?;
    let worker = WorkerId::new("event-worker")?;
    repo.claim(&worker, LEASE)
        .await?
        .ok_or("event fixture was not claimed")?;
    repo.heartbeat(submitted.job_id(), &worker, LEASE).await?;
    repo.request_cancel(submitted.job_id(), "event audit cancellation")
        .await?;
    repo.complete(submitted.job_id(), &worker, json!({}))
        .await?;

    let events: Vec<(String, Option<String>, String, Option<String>)> = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, worker_id
           FROM ops.job_events WHERE job_id = $1 ORDER BY created_at, id",
    )
    .bind(submitted.job_id().as_uuid())
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        events,
        [
            ("submitted".to_owned(), None, "queued".to_owned(), None),
            (
                "claimed".to_owned(),
                Some("queued".to_owned()),
                "running".to_owned(),
                Some("event-worker".to_owned())
            ),
            (
                "heartbeat".to_owned(),
                Some("running".to_owned()),
                "running".to_owned(),
                Some("event-worker".to_owned())
            ),
            (
                "cancellation_requested".to_owned(),
                Some("running".to_owned()),
                "running".to_owned(),
                None
            ),
            (
                "cancelled".to_owned(),
                Some("running".to_owned()),
                "cancelled".to_owned(),
                Some("event-worker".to_owned())
            )
        ]
    );

    let ingest = role_pool(&database, "ingest_writer", PoolPolicy::ReadWrite).await?;
    assert_denied(
        &ingest,
        "UPDATE ops.job_events SET event_kind = 'heartbeat'",
    )
    .await?;
    assert_denied(&ingest, "DELETE FROM ops.job_events").await?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn worker_roles_have_effective_lifecycle_access_and_object_level_dml_denial(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let owner_repo = JobRepository::new(owner_pool);

    for role in ["ingest_writer", "image_writer"] {
        let worker_pool = role_pool(&database, role, PoolPolicy::ReadWrite).await?;
        let schema_usage: bool =
            sqlx::query_scalar("SELECT has_schema_privilege(current_user, 'ops', 'USAGE')")
                .fetch_one(&worker_pool)
                .await?;
        assert!(schema_usage, "{role} lacks effective ops schema access");

        let submitted = owner_repo
            .submit(NewJob::noop(&format!("real-role-lifecycle-{role}"))?)
            .await?;
        let worker = WorkerId::new(&format!("real-role-{role}"))?;
        let worker_repo = JobRepository::new(worker_pool.clone());
        let claimed = worker_repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("real worker role could not claim")?;
        assert_eq!(claimed.job_id(), submitted.job_id());
        worker_repo
            .heartbeat(submitted.job_id(), &worker, LEASE)
            .await?;
        assert!(matches!(
            worker_repo
                .fail(
                    submitted.job_id(),
                    &worker,
                    "execution_failed",
                    Duration::from_micros(1)
                )
                .await?,
            FailureDisposition::RetryScheduled { .. }
        ));
        let reclaimed = worker_repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("real worker role could not reclaim retry")?;
        assert_eq!(reclaimed.job_id(), submitted.job_id());
        assert_eq!(reclaimed.attempts(), 2);
        let completed = worker_repo
            .complete(submitted.job_id(), &worker, json!({ "ignored": "private" }))
            .await?;
        assert_eq!(completed.status(), JobStatus::Succeeded);
        assert_eq!(
            worker_repo.get(submitted.job_id()).await?.status(),
            JobStatus::Succeeded
        );

        assert_object_denied(
            &worker_pool,
            "UPDATE ops.jobs SET priority = priority",
            "jobs",
        )
        .await?;
        assert_object_denied(&worker_pool, "DELETE FROM ops.job_events", "job_events").await?;
    }
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn status_projection_is_minimal_and_hidden_from_reader_and_monitor(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name
           FROM information_schema.columns
          WHERE table_schema = 'ops' AND table_name = 'job_status'
          ORDER BY ordinal_position",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(
        columns,
        [
            "id",
            "status",
            "attempts",
            "max_attempts",
            "available_at",
            "cancellation_requested",
            "error_message",
            "created_at",
            "updated_at",
            "finished_at",
        ]
    );

    for role in ["api_reader", "monitor"] {
        let pool = role_pool(&database, role, PoolPolicy::ReadOnly).await?;
        let can_select: bool = sqlx::query_scalar(
            "SELECT has_table_privilege(current_user, 'ops.job_status', 'SELECT')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!can_select, "{role} retained row-level job status access");
        assert_object_denied(&pool, "SELECT id FROM ops.job_status LIMIT 1", "job_status").await?;
    }

    for role in ["api_job_submitter", "ingest_writer", "image_writer"] {
        let pool = role_pool(&database, role, PoolPolicy::ReadOnly).await?;
        let can_select: bool = sqlx::query_scalar(
            "SELECT has_table_privilege(current_user, 'ops.job_status', 'SELECT')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(
            can_select,
            "{role} cannot use the minimal status projection"
        );
    }
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn submitter_uses_only_validated_functions_and_cannot_bypass_registry_or_tables(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let submitter = role_pool(&database, "api_job_submitter", PoolPolicy::ReadWrite).await?;
    let repo = JobRepository::new(submitter.clone());
    let accepted = repo.submit(NewJob::noop("submitter-function")?).await?;
    assert_eq!(
        repo.get(accepted.job_id()).await?.status(),
        JobStatus::Queued
    );

    assert_denied(
        &submitter,
        "INSERT INTO ops.jobs(
             job_type, payload_version, payload, priority, status, attempts, max_attempts,
             available_at, dedup_key
         ) VALUES ('system.noop', 1, '{}'::jsonb, 0, 'queued', 0, 3,
                   clock_timestamp(), 'bypass')",
    )
    .await?;
    assert_denied(
        &submitter,
        "INSERT INTO ops.job_events(job_id, event_kind, to_status, details)
         SELECT id, 'submitted', 'queued', '{}'::jsonb FROM ops.jobs LIMIT 1",
    )
    .await?;
    assert_denied(&submitter, "UPDATE ops.jobs SET priority = 1000").await?;
    assert_denied(&submitter, "DELETE FROM ops.job_events").await?;

    sqlx::query(
        "UPDATE ops.job_type_registry SET enabled = false
          WHERE job_type = 'system.noop' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;
    assert!(matches!(
        repo.submit(NewJob::noop("disabled-registry")?).await,
        Err(JobError::Rejected)
    ));
    assert_eq!(
        repo.request_cancel(accepted.job_id(), "disabled registry cancellation")
            .await?
            .status(),
        JobStatus::Cancelled
    );
    sqlx::query(
        "UPDATE ops.job_type_registry SET enabled = true
          WHERE job_type = 'system.noop' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;
    assert!(matches!(
        repo.submit(NewJob::new(
            "system.noop",
            2,
            json!({}),
            "unregistered-version"
        )?)
        .await,
        Err(JobError::Rejected)
    ));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn disabled_registry_stops_new_execution_but_preserves_every_cancellation_path(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let submitter_pool = role_pool(&database, "api_job_submitter", PoolPolicy::ReadWrite).await?;
    let worker_pool = role_pool(&database, "ingest_writer", PoolPolicy::ReadWrite).await?;
    let submitter = JobRepository::new(submitter_pool);
    let worker_repo = JobRepository::new(worker_pool);
    let worker = WorkerId::new("registry-stop-worker")?;

    let retry = submitter
        .submit(NewJob::noop("disabled-retry")?.with_priority(100)?)
        .await?;
    worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("retry fixture was not claimed")?;
    worker_repo
        .fail(
            retry.job_id(),
            &worker,
            "upstream_unavailable",
            Duration::from_mins(10),
        )
        .await?;

    let live = submitter
        .submit(NewJob::noop("disabled-live")?.with_priority(90)?)
        .await?;
    assert_eq!(
        worker_repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("live fixture was not claimed")?
            .job_id(),
        live.job_id()
    );
    let live_cancel = submitter
        .submit(NewJob::noop("disabled-live-cancel")?.with_priority(80)?)
        .await?;
    assert_eq!(
        worker_repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("live cancellation fixture was not claimed")?
            .job_id(),
        live_cancel.job_id()
    );
    let expired = submitter
        .submit(NewJob::noop("disabled-expired")?.with_priority(70)?)
        .await?;
    assert_eq!(
        worker_repo
            .claim(&worker, LEASE)
            .await?
            .ok_or("expired fixture was not claimed")?
            .job_id(),
        expired.job_id()
    );
    sqlx::query(
        "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
          WHERE id = $1",
    )
    .bind(expired.job_id().as_uuid())
    .execute(&owner_pool)
    .await?;
    let queued = submitter
        .submit(NewJob::noop("disabled-queued")?.with_priority(-10)?)
        .await?;

    sqlx::query(
        "UPDATE ops.job_type_registry SET enabled = false
          WHERE job_type = 'system.noop' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;

    let cancellation_flag: bool =
        sqlx::query_scalar("SELECT cancellation_requested FROM ops.jobs WHERE id = $1")
            .bind(live.job_id().as_uuid())
            .fetch_one(&owner_pool)
            .await?;
    assert!(cancellation_flag);

    let queued_disable_event: (String, String, String, Value) = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, details
           FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'claimability_changed'
          ORDER BY created_at DESC, id DESC
          LIMIT 1",
    )
    .bind(queued.job_id().as_uuid())
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(queued_disable_event.0, "claimability_changed".to_owned());
    assert_eq!(
        (queued_disable_event.1, queued_disable_event.2),
        ("queued".to_owned(), "queued".to_owned())
    );
    assert_eq!(
        queued_disable_event.3,
        json!({
            "reason": "registry_claimability_changed",
            "type": "system.noop",
            "job_type": "system.noop",
            "payload_version": 1,
            "enabled": false
        })
    );

    let running_disable_event: (String, String, String, Value) = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, details
           FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'cancellation_requested'
          ORDER BY created_at DESC, id DESC
          LIMIT 1",
    )
    .bind(live.job_id().as_uuid())
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(
        (
            running_disable_event.0,
            running_disable_event.1,
            running_disable_event.2
        ),
        (
            "cancellation_requested".to_owned(),
            "running".to_owned(),
            "running".to_owned()
        )
    );
    assert_eq!(
        running_disable_event.3,
        json!({
            "reason": "job_type_disabled",
            "type": "system.noop",
            "job_type": "system.noop",
            "payload_version": 1,
            "enabled": false
        })
    );

    assert!(worker_repo.claim(&worker, LEASE).await?.is_none());
    assert!(matches!(
        submitter.submit(NewJob::noop("disabled-new")?).await,
        Err(JobError::Rejected)
    ));

    worker_repo.heartbeat(live.job_id(), &worker, LEASE).await?;
    assert_eq!(
        worker_repo
            .complete(
                live.job_id(),
                &worker,
                json!({ "private": "not projected" })
            )
            .await?
            .status(),
        JobStatus::Cancelled
    );

    assert!(
        submitter
            .request_cancel(live_cancel.job_id(), "disabled live cancellation")
            .await?
            .cancellation_requested()
    );
    assert_eq!(
        worker_repo
            .complete(live_cancel.job_id(), &worker, json!({}))
            .await?
            .status(),
        JobStatus::Cancelled
    );
    for job_id in [queued.job_id(), retry.job_id(), expired.job_id()] {
        let cancelled = submitter
            .request_cancel(job_id, "disabled work cancellation")
            .await?;
        assert_eq!(cancelled.status(), JobStatus::Cancelled);
        assert!(!cancelled.cancellation_requested());
    }
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn disabled_running_job_stays_cancelled_after_registry_reenable(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::raw_sql(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('plan.toggle', 1, true);",
    )
    .execute(&pool)
    .await?;
    let job_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT job_id
           FROM ops.submit_job(
               gen_random_uuid(), 'plan.toggle', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'toggle-running')",
    )
    .fetch_one(&pool)
    .await?;
    let claimed: uuid::Uuid =
        sqlx::query_scalar("SELECT job_id FROM ops.claim_job('toggle-worker', 1000000)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(claimed, job_id);
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = false
          WHERE job_type = 'plan.toggle' AND payload_version = 1",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = true
          WHERE job_type = 'plan.toggle' AND payload_version = 1",
    )
    .execute(&pool)
    .await?;
    let reenable_event: (String, String, String, Value) = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, details
           FROM ops.job_events
          WHERE job_id = $1 AND event_kind = 'claimability_changed'
          ORDER BY created_at DESC, id DESC
          LIMIT 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (reenable_event.0, reenable_event.1, reenable_event.2),
        (
            "claimability_changed".to_owned(),
            "running".to_owned(),
            "running".to_owned()
        )
    );
    assert_eq!(
        reenable_event.3,
        json!({
            "reason": "registry_claimability_changed",
            "type": "plan.toggle",
            "job_type": "plan.toggle",
            "payload_version": 1,
            "enabled": true
        })
    );
    sqlx::query(
        "UPDATE ops.jobs
            SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
          WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await?;
    let no_claim: Vec<(uuid::Uuid,)> =
        sqlx::query_as("SELECT job_id FROM ops.claim_job('toggle-worker-two', 1000000)")
            .fetch_all(&pool)
            .await?;
    assert!(no_claim.is_empty());
    let state: (String, bool) =
        sqlx::query_as("SELECT status, cancellation_requested FROM ops.jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(state, ("cancelled".to_owned(), false));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn safe_status_never_projects_worker_results_or_unbounded_failure_text(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let worker_pool = role_pool(&database, "image_writer", PoolPolicy::ReadWrite).await?;
    let worker_repo = JobRepository::new(worker_pool.clone());
    let owner_repo = JobRepository::new(owner_pool);
    let worker = WorkerId::new("safe-projection-worker")?;

    let failed = owner_repo
        .submit(NewJob::noop("safe-failure-code")?)
        .await?;
    worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("failure fixture was not claimed")?;
    assert!(matches!(
        worker_repo
            .fail(
                failed.job_id(),
                &worker,
                "postgres://db.example.invalid/token?access_token=private",
                Duration::from_secs(1),
            )
            .await?,
        FailureDisposition::RetryScheduled { .. }
    ));
    assert_eq!(
        worker_repo.get(failed.job_id()).await?.error_message(),
        Some("execution_failed")
    );
    let canonical = owner_repo
        .submit(NewJob::noop("safe-failure-code-canonical")?)
        .await?;
    worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("canonical failure fixture was not claimed")?;
    worker_repo
        .fail(
            canonical.job_id(),
            &worker,
            "execution_failed",
            Duration::from_secs(1),
        )
        .await?;
    assert_eq!(
        worker_repo.get(canonical.job_id()).await?.error_message(),
        Some("execution_failed")
    );

    let completed = owner_repo
        .submit(NewJob::noop("private-completion-result")?.with_priority(100)?)
        .await?;
    worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("completion fixture was not claimed")?;
    worker_repo
        .complete(
            completed.job_id(),
            &worker,
            json!({ "access_token": "private", "url": "postgres://private" }),
        )
        .await?;
    let projected: (String, Option<String>) =
        sqlx::query_as("SELECT status, error_message FROM ops.job_status WHERE id = $1")
            .bind(completed.job_id().as_uuid())
            .fetch_one(&worker_pool)
            .await?;
    assert_eq!(projected, ("succeeded".to_owned(), None));
    let Err(error) = sqlx::query("SELECT result_summary FROM ops.job_status WHERE id = $1")
        .bind(completed.job_id().as_uuid())
        .execute(&worker_pool)
        .await
    else {
        return Err("result_summary unexpectedly remained in the granted view".into());
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42703")
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn missing_jobs_and_lost_leases_are_distinct_sanitized_errors(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let missing = JobId::new();
    let worker = WorkerId::new("typed-error-worker")?;
    assert!(matches!(repo.get(missing).await, Err(JobError::NotFound)));
    assert!(matches!(
        repo.heartbeat(missing, &worker, LEASE).await,
        Err(JobError::NotFound)
    ));

    let submitted = repo.submit(NewJob::noop("typed-lease-lost")?).await?;
    repo.request_cancel(submitted.job_id(), "cancel before lease")
        .await?;
    let error = repo.complete(submitted.job_id(), &worker, json!({})).await;
    assert!(matches!(error, Err(JobError::LeaseLost)));
    let rendered = format!("{:?}", error.err().ok_or("expected lease error")?);
    for forbidden in ["SELECT", "UPDATE", "postgres://", "ops.jobs", "sqlx"] {
        assert!(!rendered.contains(forbidden));
    }

    assert!(matches!(
        repo.claim(&worker, Duration::ZERO).await,
        Err(JobError::Validation(_))
    ));
    assert!(matches!(
        repo.claim(&worker, Duration::from_secs(3_601)).await,
        Err(JobError::Validation(_))
    ));
    assert!(matches!(
        repo.claim(&worker, Duration::MAX).await,
        Err(JobError::Validation(_))
    ));
    assert!(matches!(
        repo.request_cancel(submitted.job_id(), "").await,
        Err(JobError::Validation(_))
    ));
    assert!(matches!(
        repo.fail(
            submitted.job_id(),
            &worker,
            "execution_failed",
            Duration::ZERO
        )
        .await,
        Err(JobError::Validation(_))
    ));
    assert!(matches!(
        repo.fail(
            submitted.job_id(),
            &worker,
            "execution_failed",
            Duration::MAX
        )
        .await,
        Err(JobError::Validation(_))
    ));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn message_and_result_boundaries_accept_the_limit_and_reject_one_more(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let worker = WorkerId::new("boundary-worker")?;

    // `{\"value\":\"\"}` is 12 compact UTF-8 bytes, derived independently by hand.
    let payload_at_limit = json!({ "value": "p".repeat(65_536 - 12) });
    let payload_too_large = json!({ "value": "p".repeat(65_537 - 12) });
    assert_eq!(serde_json::to_vec(&payload_at_limit)?.len(), 65_536);
    assert_eq!(serde_json::to_vec(&payload_too_large)?.len(), 65_537);
    assert!(NewJob::new("system.noop", 1, payload_too_large, "payload-too-large").is_err());
    let payload_boundary = repo
        .submit(NewJob::new(
            "system.noop",
            1,
            payload_at_limit,
            "payload-at-limit",
        )?)
        .await?;
    assert_eq!(
        repo.get(payload_boundary.job_id()).await?.status(),
        JobStatus::Queued
    );
    repo.request_cancel(payload_boundary.job_id(), "payload boundary checked")
        .await?;

    let invalid_cancel = repo.submit(NewJob::noop("cancel-too-large")?).await?;
    assert!(matches!(
        repo.request_cancel(invalid_cancel.job_id(), &"c".repeat(1_025))
            .await,
        Err(JobError::Validation(_))
    ));
    repo.request_cancel(invalid_cancel.job_id(), "invalid boundary checked")
        .await?;
    let valid_cancel = repo.submit(NewJob::noop("cancel-at-limit")?).await?;
    assert_eq!(
        repo.request_cancel(valid_cancel.job_id(), &"c".repeat(1_024))
            .await?
            .status(),
        JobStatus::Cancelled
    );

    let legacy_failure = repo
        .submit(NewJob::noop("failure-message-private")?)
        .await?;
    repo.claim(&worker, LEASE)
        .await?
        .ok_or("legacy failure fixture was not claimed")?;
    assert!(matches!(
        repo.fail(
            legacy_failure.job_id(),
            &worker,
            "postgres://db.example.invalid/token?access_token=private",
            Duration::from_secs(1)
        )
        .await?,
        FailureDisposition::RetryScheduled { .. }
    ));
    assert_eq!(
        repo.get(legacy_failure.job_id()).await?.error_message(),
        Some("execution_failed")
    );

    let invalid_failure = repo.submit(NewJob::noop("failure-code-private")?).await?;
    repo.claim(&worker, LEASE)
        .await?
        .ok_or("invalid failure fixture was not claimed")?;
    for invalid_message in ["", "line\nforged", &"m".repeat(2_049)] {
        assert!(matches!(
            repo.fail(
                invalid_failure.job_id(),
                &worker,
                invalid_message,
                Duration::from_secs(1)
            )
            .await,
            Err(JobError::Validation(_))
        ));
    }
    assert!(matches!(
        repo.fail(
            invalid_failure.job_id(),
            &worker,
            "execution_failed",
            Duration::from_secs(1)
        )
        .await?,
        FailureDisposition::RetryScheduled { .. }
    ));

    // The result uses the same compact UTF-8 boundary as payload submission.
    let result_at_limit = json!({ "value": "r".repeat(65_536 - 12) });
    let result_too_large = json!({ "value": "r".repeat(65_537 - 12) });
    assert_eq!(serde_json::to_vec(&result_at_limit)?.len(), 65_536);
    assert_eq!(serde_json::to_vec(&result_too_large)?.len(), 65_537);
    let invalid_result = repo.submit(NewJob::noop("result-too-large")?).await?;
    repo.claim(&worker, LEASE)
        .await?
        .ok_or("oversized result fixture was not claimed")?;
    assert!(matches!(
        repo.complete(invalid_result.job_id(), &worker, result_too_large)
            .await,
        Err(JobError::Validation(_))
    ));
    let completed = repo
        .complete(invalid_result.job_id(), &worker, result_at_limit)
        .await?;
    assert_eq!(completed.status(), JobStatus::Succeeded);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn rust_and_real_role_sql_boundaries_reject_non_ascii_text_and_invalid_time(
    owner_pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(NewJob::new("system.nóop", 1, json!({}), "ascii-key").is_err());
    assert!(NewJob::noop("non-ascii-é").is_err());
    assert!(NewJob::noop("\u{00a0}").is_err());
    assert!(WorkerId::new("worker-é").is_err());
    assert!(WorkerId::new("\u{00a0}worker").is_err());

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let submitter_pool = role_pool(&database, "api_job_submitter", PoolPolicy::ReadWrite).await?;
    let worker_pool = role_pool(&database, "image_writer", PoolPolicy::ReadWrite).await?;
    let submitter = JobRepository::new(submitter_pool.clone());
    let worker_repo = JobRepository::new(worker_pool.clone());

    for (job_type, available_at, dedup_key) in [
        ("system.noop", "infinity", "infinite-available"),
        ("system.noop", "10000-01-01T00:00:00Z", "year-10000"),
        ("system.noop", "2026-01-01T00:00:00Z", "\u{00a0}"),
        ("system.noop", "2026-01-01T00:00:00Z", "edge\u{00a0}"),
        ("system.nóop", "2026-01-01T00:00:00Z", "non-ascii-type"),
        ("system.noop\n", "2026-01-01T00:00:00Z", "control-type"),
    ] {
        assert_database_code(
            direct_submit(&submitter_pool, job_type, available_at, dedup_key).await,
            "22023",
        )?;
    }

    let maximum_job_type = "j".repeat(128);
    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ($1, 1, true)",
    )
    .bind(&maximum_job_type)
    .execute(&owner_pool)
    .await?;
    let maximum_dedup = "d".repeat(256);
    let (maximum_id, duplicate) = direct_submit(
        &submitter_pool,
        &maximum_job_type,
        "2026-01-01T00:00:00Z",
        &maximum_dedup,
    )
    .await?;
    assert!(!duplicate);
    submitter
        .request_cancel(maximum_id.into(), "maximum identifiers accepted")
        .await?;
    assert_database_code(
        direct_submit(
            &submitter_pool,
            &"j".repeat(129),
            "2026-01-01T00:00:00Z",
            "oversized-type",
        )
        .await,
        "22023",
    )?;
    assert_database_code(
        direct_submit(
            &submitter_pool,
            "system.noop",
            "2026-01-01T00:00:00Z",
            &"d".repeat(257),
        )
        .await,
        "22023",
    )?;

    for invalid_worker in [
        "\u{00a0}".to_owned(),
        "edge\u{00a0}".to_owned(),
        "worker-é".to_owned(),
        "worker\nforged".to_owned(),
        "w".repeat(129),
    ] {
        let result: Result<Vec<(uuid::Uuid,)>, sqlx::Error> =
            sqlx::query_as("SELECT job_id FROM ops.claim_job($1, 1)")
                .bind(invalid_worker)
                .fetch_all(&worker_pool)
                .await;
        assert_database_code(result, "22023")?;
    }
    let no_claim: Vec<(uuid::Uuid,)> = sqlx::query_as("SELECT job_id FROM ops.claim_job($1, 1)")
        .bind("w".repeat(128))
        .fetch_all(&worker_pool)
        .await?;
    assert!(no_claim.is_empty());

    let cancel_fixture = submitter
        .submit(NewJob::noop("direct-cancel-text")?)
        .await?;
    for invalid_message in [String::new(), "line\nforged".to_owned(), "m".repeat(1_025)] {
        let result: Result<(String, bool), sqlx::Error> = sqlx::query_as(
            "SELECT job_status, cancellation_requested
               FROM ops.request_job_cancel($1, $2)",
        )
        .bind(cancel_fixture.job_id().as_uuid())
        .bind(invalid_message)
        .fetch_one(&submitter_pool)
        .await;
        assert_database_code(result, "22023")?;
    }
    submitter
        .request_cancel(cancel_fixture.job_id(), &"m".repeat(1_024))
        .await?;

    let failure_fixture = submitter
        .submit(NewJob::noop("direct-failure-message")?)
        .await?;
    let worker = WorkerId::new("direct-failure-worker")?;
    worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("direct failure fixture was not claimed")?;
    let legacy_failure: Result<(String, chrono::DateTime<Utc>), sqlx::Error> =
        sqlx::query_as("SELECT disposition, next_available_at FROM ops.fail_job($1, $2, $3, $4)")
            .bind(failure_fixture.job_id().as_uuid())
            .bind(worker.as_str())
            .bind("token=private postgres://db.invalid")
            .bind(60_000_000_i64)
            .fetch_one(&worker_pool)
            .await;
    assert_eq!(legacy_failure?.0, "retry_scheduled");
    let projected_code: String =
        sqlx::query_scalar("SELECT error_message FROM ops.job_status WHERE id = $1")
            .bind(failure_fixture.job_id().as_uuid())
            .fetch_one(&worker_pool)
            .await?;
    assert_eq!(projected_code, "execution_failed");

    let invalid_fixture = submitter
        .submit(NewJob::noop("direct-invalid-failure")?)
        .await?;
    let claimed_invalid = worker_repo
        .claim(&worker, LEASE)
        .await?
        .ok_or("direct invalid failure fixture was not claimed")?;
    assert_eq!(claimed_invalid.job_id(), invalid_fixture.job_id());
    for invalid_message in ["", "line\nforged", &"m".repeat(2_049)] {
        let invalid_failure: Result<(String, chrono::DateTime<Utc>), sqlx::Error> = sqlx::query_as(
            "SELECT disposition, next_available_at
                   FROM ops.fail_job($1, $2, $3, $4)",
        )
        .bind(invalid_fixture.job_id().as_uuid())
        .bind(worker.as_str())
        .bind(invalid_message)
        .bind(1_i64)
        .fetch_one(&worker_pool)
        .await;
        assert_database_code(invalid_failure, "22023")?;
    }
    let lease_state: (String, Option<String>, bool) = sqlx::query_as(
        "SELECT status, lease_owner, lease_expires_at > clock_timestamp()
           FROM ops.jobs WHERE id = $1",
    )
    .bind(invalid_fixture.job_id().as_uuid())
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(
        lease_state,
        ("running".to_owned(), Some(worker.as_str().to_owned()), true)
    );
    worker_repo
        .fail(
            invalid_fixture.job_id(),
            &worker,
            "execution_failed",
            Duration::from_micros(1),
        )
        .await?;

    Ok(())
}

async fn direct_submit(
    pool: &PgPool,
    job_type: &str,
    available_at: &str,
    dedup_key: &str,
) -> Result<(uuid::Uuid, bool), sqlx::Error> {
    sqlx::query_as(
        "SELECT job_id, was_duplicate
           FROM ops.submit_job($1, $2, 1, '{}'::text, 0::smallint, 3,
                               $3::timestamptz, $4)",
    )
    .bind(JobId::new().as_uuid())
    .bind(job_type)
    .bind(available_at)
    .bind(dedup_key)
    .fetch_one(pool)
    .await
}

fn assert_database_code<T>(result: Result<T, sqlx::Error>, expected: &str) -> sqlx::Result<()> {
    let Err(error) = result else {
        return Err(test_error("database boundary accepted an invalid value"));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some(expected)
    );
    Ok(())
}

async fn active_jobs_for_key(pool: &PgPool, dedup_key: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM ops.jobs
          WHERE job_type = 'system.noop' AND dedup_key = $1
            AND status IN ('queued', 'retry_wait', 'running')",
    )
    .bind(dedup_key)
    .fetch_one(pool)
    .await
}

async fn wait_until_backend_is_lock_waiting(pool: &PgPool, pid: i32) -> sqlx::Result<()> {
    for _ in 0..500 {
        let waiting: bool = sqlx::query_scalar(
            "SELECT COALESCE((
                 SELECT wait_event_type = 'Lock'
                   FROM pg_catalog.pg_stat_activity
                  WHERE pid = $1
             ), false)",
        )
        .bind(pid)
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(test_error("backend did not enter the expected lock wait"))
}

async fn role_pool(database: &str, role: &str, policy: PoolPolicy) -> sqlx::Result<PgPool> {
    let config = DatabaseConfig {
        host: std::env::var("TMDB_TEST_DB_HOST")
            .unwrap_or_else(|_| "host.docker.internal".to_owned()),
        port: std::env::var("TMDB_TEST_DB_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(55432),
        database: database.to_owned(),
        username: role.to_owned(),
        password: SecretString::from(
            test_database_password()
                .map_err(|_| test_error("test database password was not configured"))?,
        ),
    };
    connect_direct(&config, policy)
        .await
        .map_err(|error| test_error(&error.to_string()))
}

fn test_database_password() -> std::io::Result<String> {
    std::env::var("TMDB_TEST_DB_PASSWORD")
        .or_else(|_| std::env::var("POSTGRES_PASSWORD"))
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "TMDB_TEST_DB_PASSWORD or POSTGRES_PASSWORD is required",
            )
        })
}

async fn assert_denied(pool: &PgPool, statement: &'static str) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    let Err(error) = sqlx::raw_sql(statement).execute(&mut *transaction).await else {
        return Err(test_error("operation expected to be denied was allowed"));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    transaction.rollback().await?;
    let one: i32 = sqlx::query_scalar("SELECT 1").fetch_one(pool).await?;
    assert_eq!(one, 1);
    Ok(())
}

async fn assert_object_denied(
    pool: &PgPool,
    statement: &'static str,
    object_name: &str,
) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    let Err(error) = sqlx::raw_sql(statement).execute(&mut *transaction).await else {
        return Err(test_error(
            "object-level operation expected to be denied was allowed",
        ));
    };
    let database_error = error
        .as_database_error()
        .ok_or_else(|| test_error("denial did not return a database error"))?;
    assert_eq!(database_error.code().as_deref(), Some("42501"));
    assert!(
        database_error.message().contains(object_name),
        "denial occurred before the expected object boundary: {}",
        database_error.message()
    );
    transaction.rollback().await?;
    Ok(())
}

fn test_error(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}
