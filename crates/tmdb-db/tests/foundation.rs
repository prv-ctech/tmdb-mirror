use std::sync::Arc;

use secrecy::SecretString;
use sqlx::PgPool;
use tmdb_config::DatabaseConfig;
use tmdb_db::{DbError, MIGRATOR, PoolPolicy, connect_direct, migrate, readiness};
use tokio::sync::Barrier;

const SCHEMAS: [&str; 6] = ["assets", "auth", "catalog", "ops", "search", "source"];
const TEST_SHARED_DATABASE_OWNER: &str = "test_shared_database_owner";

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn foundation_migration_installs_owned_schemas_and_bookkeeping(
    pool: PgPool,
) -> sqlx::Result<()> {
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT schema_name FROM information_schema.schemata
         WHERE schema_name = ANY($1) ORDER BY schema_name",
    )
    .bind(SCHEMAS)
    .fetch_all(&pool)
    .await?;
    assert_eq!(schemas, SCHEMAS);

    let owned_schemas: Vec<String> = sqlx::query_scalar(
        "SELECT nspname
           FROM pg_namespace
          WHERE nspname = ANY($1) AND pg_get_userbyid(nspowner) = 'migrator'
          ORDER BY nspname",
    )
    .bind(SCHEMAS)
    .fetch_all(&pool)
    .await?;
    assert_eq!(owned_schemas, SCHEMAS);

    let objects: Vec<String> = sqlx::query_scalar(
        "SELECT format('%I.%I:%s', n.nspname, c.relname,
                       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END)
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE (n.nspname, c.relname) IN
                (('ops', '_sqlx_migrations'), ('ops', 'service_metadata'),
                 ('ops', 'job_type_registry'), ('ops', 'jobs'), ('ops', 'job_events'),
                 ('ops', 'job_status'), ('ops', 'readiness'),
                 ('source', 'ingest_runs'), ('auth', 'api_keys'))
          ORDER BY 1",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        objects,
        [
            "auth.api_keys:table",
            "ops._sqlx_migrations:table",
            "ops.job_events:table",
            "ops.job_status:view",
            "ops.job_type_registry:table",
            "ops.jobs:table",
            "ops.readiness:view",
            "ops.service_metadata:table",
            "source.ingest_runs:table",
        ]
    );

    let not_owned: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = ANY($1)
            AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
            AND pg_get_userbyid(c.relowner) <> 'migrator'",
    )
    .bind(SCHEMAS)
    .fetch_one(&pool)
    .await?;
    assert_eq!(not_owned, 0);

    let migration_schema: String = sqlx::query_scalar(
        "SELECT n.nspname
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = to_regclass('ops._sqlx_migrations')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(migration_schema, "ops");
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regclass('public._sqlx_migrations')::text"
        )
        .fetch_one(&pool)
        .await?
        .is_none()
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn foundation_rows_constraints_and_readiness_projection_are_exact(
    pool: PgPool,
) -> sqlx::Result<()> {
    let noop: (i32, bool) = sqlx::query_as(
        "SELECT payload_version, enabled
           FROM ops.job_type_registry WHERE job_type = 'system.noop'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(noop, (1, true));
    let registry_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.job_type_registry")
        .fetch_one(&pool)
        .await?;
    assert_eq!(registry_count, 18);
    let metadata_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.service_metadata")
        .fetch_one(&pool)
        .await?;
    assert_eq!(metadata_count, 1);

    let readiness_row: (String, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as("SELECT schema_revision, migrated_at FROM ops.readiness")
            .fetch_one(&pool)
            .await?;
    assert_eq!(readiness_row.0, "0053");
    let readiness_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'ops' AND table_name = 'readiness'
          ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(readiness_columns, ["schema_revision", "migrated_at"]);

    let retention_indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname
           FROM pg_indexes
          WHERE schemaname = 'ops'
            AND indexname = ANY($1)
          ORDER BY indexname",
    )
    .bind(["jobs_terminal_retention_idx", "media_requests_claim_idx"])
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        retention_indexes,
        ["jobs_terminal_retention_idx", "media_requests_claim_idx"]
    );

    let removed_media_objects: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT pg_catalog.to_regclass('ops.media_scan_runs')::text,
                pg_catalog.to_regclass('assets.image_variants')::text",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(removed_media_objects, (None, None));

    let recovery_indexes: Vec<String> = sqlx::query_scalar(
        "SELECT schemaname || '.' || indexname
           FROM pg_indexes
          WHERE indexname = ANY($1)
          ORDER BY schemaname, indexname",
    )
    .bind([
        "jobs_dead_catalog_refresh_idx",
        "seasons_missing_enrichment_idx",
        "titles_missing_enrichment_idx",
    ])
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        recovery_indexes,
        [
            "catalog.seasons_missing_enrichment_idx",
            "catalog.titles_missing_enrichment_idx",
            "ops.jobs_dead_catalog_refresh_idx",
        ]
    );

    assert_sqlstate(
        &pool,
        "INSERT INTO ops.job_type_registry(job_type, payload_version) VALUES ('bad', 0)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO ops.service_metadata(key, value) VALUES ('bad-json', '[]'::jsonb)",
        "23514",
    )
    .await?;
    for invalid in ["bulkish", "repair"] {
        let Err(error) =
            sqlx::query("INSERT INTO source.ingest_runs(run_type, status) VALUES ($1, 'pending')")
                .bind(invalid)
                .execute(&pool)
                .await
        else {
            return Err(test_error("invalid run type was accepted"));
        };
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("23514")
        );
    }
    for invalid in ["queued", "complete", "errored"] {
        let Err(error) =
            sqlx::query("INSERT INTO source.ingest_runs(run_type, status) VALUES ('bulk', $1)")
                .bind(invalid)
                .execute(&pool)
                .await
        else {
            return Err(test_error("invalid run status was accepted"));
        };
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("23514")
        );
    }
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status, watermark)
         VALUES ('bulk', 'pending', '[]'::jsonb)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status, counts)
         VALUES ('incremental', 'pending', '[]'::jsonb)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status)
         VALUES ('bulk', 'running')",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status, started_at, finished_at)
         VALUES ('bulk', 'succeeded', clock_timestamp(), NULL)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status, started_at, finished_at)
         VALUES ('bulk', 'failed', clock_timestamp(), clock_timestamp() - interval '1 second')",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO source.ingest_runs(run_type, status, created_at, updated_at)
         VALUES ('bulk', 'pending', clock_timestamp(), clock_timestamp() - interval '1 second')",
        "23514",
    )
    .await?;

    sqlx::raw_sql(
        "INSERT INTO source.ingest_runs(run_type, status) VALUES ('bulk', 'pending');
         INSERT INTO source.ingest_runs(run_type, status, started_at)
             VALUES ('incremental', 'running', clock_timestamp());
         INSERT INTO source.ingest_runs(run_type, status, started_at, finished_at)
             VALUES ('bulk', 'succeeded', clock_timestamp() - interval '1 second', clock_timestamp());
         INSERT INTO source.ingest_runs(run_type, status, started_at, finished_at)
             VALUES ('incremental', 'failed', clock_timestamp() - interval '1 second', clock_timestamp());
         INSERT INTO source.ingest_runs(run_type, status, started_at, finished_at)
             VALUES ('bulk', 'cancelled', clock_timestamp() - interval '1 second', clock_timestamp());",
    )
    .execute(&pool)
    .await?;

    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('short-digest', decode('00', 'hex'), 'owner', ARRAY['catalog:read'])",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('empty-scopes', decode(repeat('00', 32), 'hex'), 'owner', ARRAY[]::text[])",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('unknown-scope', decode(repeat('01', 32), 'hex'), 'owner', ARRAY['unknown'])",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('null-scope', decode(repeat('04', 32), 'hex'), 'owner', ARRAY[NULL, 'catalog:read'])",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('duplicate-scope', decode(repeat('05', 32), 'hex'), 'owner',
                 ARRAY['catalog:read', 'catalog:read'])",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes, expires_at)
         VALUES ('bad-expiry', decode(repeat('02', 32), 'hex'), 'owner', ARRAY['catalog:read'],
                 clock_timestamp() - interval '1 second')",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes, revoked_at)
         VALUES ('bad-revocation', decode(repeat('03', 32), 'hex'), 'owner', ARRAY['catalog:read'],
                 clock_timestamp() - interval '1 second')",
        "23514",
    )
    .await?;
    sqlx::query(
        "INSERT INTO auth.api_keys(
             identifier, hmac_digest, owner, scopes, expires_at, revoked_at, created_at, updated_at
         ) VALUES (
             'valid-key', decode(repeat('06', 32), 'hex'), 'owner',
             ARRAY['catalog:read', 'jobs:read'], clock_timestamp() + interval '1 day',
             clock_timestamp(), clock_timestamp() - interval '1 second', clock_timestamp()
         )",
    )
    .execute(&pool)
    .await?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn worker_startup_enables_durable_queue_draining(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE ops.worker_control
            SET state = CASE worker_kind
                WHEN 'ingest' THEN 'running'
                WHEN 'media' THEN 'paused'
            END,
                updated_at = clock_timestamp()",
    )
    .execute(&pool)
    .await?;

    let ingest_state: String = sqlx::query_scalar("SELECT ops.start_worker_on_startup('ingest')")
        .fetch_one(&pool)
        .await?;
    let media_state: String = sqlx::query_scalar("SELECT ops.start_worker_on_startup('media')")
        .fetch_one(&pool)
        .await?;
    assert_eq!(ingest_state, "running");
    assert_eq!(media_state, "running");

    let states: Vec<(String, String)> = sqlx::query_as(
        "SELECT worker_kind, state
           FROM ops.worker_control
          ORDER BY worker_kind",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        states,
        [
            ("ingest".to_owned(), "running".to_owned()),
            ("media".to_owned(), "running".to_owned())
        ]
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn ingest_writer_can_use_only_the_locked_child_submission_gate(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let ingest = role_pool(&database, "ingest_writer", PoolPolicy::ReadWrite).await?;

    let can_read_control: bool = sqlx::query_scalar(
        "SELECT has_table_privilege(current_user, 'ops.worker_control', 'SELECT')",
    )
    .fetch_one(&ingest)
    .await?;
    let can_execute_gate: bool = sqlx::query_scalar(
        "SELECT has_function_privilege(
            current_user, 'ops.ingest_child_submissions_enabled()', 'EXECUTE'
        )",
    )
    .fetch_one(&ingest)
    .await?;
    assert!(!can_read_control);
    assert!(can_execute_gate);
    sqlx::query(
        "UPDATE ops.worker_control
            SET state = 'stopped', updated_at = clock_timestamp()
          WHERE worker_kind = 'ingest'",
    )
    .execute(&owner_pool)
    .await?;
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT ops.ingest_child_submissions_enabled()")
            .fetch_one(&ingest)
            .await?
    );

    sqlx::query(
        "UPDATE ops.worker_control
            SET state = 'running', updated_at = clock_timestamp()
          WHERE worker_kind = 'ingest'",
    )
    .execute(&owner_pool)
    .await?;
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT ops.ingest_child_submissions_enabled()")
            .fetch_one(&ingest)
            .await?
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn media_restart_immediately_drains_durable_work(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE ops.worker_control
            SET state = 'running', updated_at = clock_timestamp()
          WHERE worker_kind = 'media'",
    )
    .execute(&pool)
    .await?;
    let media_job: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'image.download', 1, '{}', 0::smallint, 3,
               clock_timestamp(), 'media-restart-claim-gate'
           )",
    )
    .fetch_one(&pool)
    .await?;

    let startup_state: String = sqlx::query_scalar("SELECT ops.start_worker_on_startup('media')")
        .fetch_one(&pool)
        .await?;
    assert_eq!(startup_state, "running");
    let claimed_job: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job_for_types(
               'media-restart-test', 1000000, ARRAY['image.download']::text[]
           )",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed_job, media_job);
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn actual_roles_obey_the_permission_matrix_and_recover_after_denial(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;

    let reader = role_pool(&database, "api_reader", PoolPolicy::ReadOnly).await?;
    assert_eq!(current_user(&reader).await?, "api_reader");
    assert_session_policy(&reader, "tmdb-read-only", "on").await?;
    assert!(schema_usage(&reader, "catalog").await?);
    let _: String = sqlx::query_scalar("SELECT schema_revision FROM ops.readiness")
        .fetch_one(&reader)
        .await?;
    denied_then_recovers(&reader, "SELECT * FROM ops.service_metadata").await?;
    denied_then_recovers(&reader, "SELECT * FROM ops._sqlx_migrations").await?;

    // Test-only ACL probe: read-write session policy removes read-only enforcement
    // as a confounder. Production api_reader sessions remain read-only above.
    let reader_acl_probe = role_pool(&database, "api_reader", PoolPolicy::ReadWrite).await?;
    assert_eq!(current_user(&reader_acl_probe).await?, "api_reader");
    assert_session_policy(&reader_acl_probe, "tmdb-read-write", "off").await?;
    assert_api_reader_metadata_insert_is_denied_by_acl(&reader_acl_probe, &owner_pool).await?;
    denied_then_recovers(
        &reader_acl_probe,
        "SELECT ops.lock_catalog_write_resources(ARRAY['catalog:genre:2', 'catalog:genre:1'])",
    )
    .await?;
    reader_acl_probe.close().await;

    let submitter = role_pool(&database, "api_job_submitter", PoolPolicy::ReadWrite).await?;
    assert_eq!(current_user(&submitter).await?, "api_job_submitter");
    assert_session_policy(&submitter, "tmdb-read-write", "off").await?;
    assert!(schema_usage(&submitter, "ops").await?);
    assert!(schema_usage(&submitter, "source").await?);
    sqlx::query(
        "INSERT INTO source.tmdb_v3_request_tokens (token, expires_at)
         VALUES ('submitter-role-fixture', clock_timestamp() + interval '1 minute')",
    )
    .execute(&submitter)
    .await?;
    sqlx::query("DELETE FROM source.tmdb_v3_request_tokens WHERE token = 'submitter-role-fixture'")
        .execute(&submitter)
        .await?;
    denied_then_recovers(
        &submitter,
        "INSERT INTO ops.service_metadata(key, value) VALUES ('denied-submitter', '{}'::jsonb)",
    )
    .await?;

    let ingest = role_pool(&database, "ingest_writer", PoolPolicy::ReadWrite).await?;
    assert_eq!(current_user(&ingest).await?, "ingest_writer");
    assert!(schema_usage(&ingest, "assets").await?);
    sqlx::query("INSERT INTO source.ingest_runs(run_type, status) VALUES ('bulk', 'pending')")
        .execute(&ingest)
        .await?;
    sqlx::query(
        "SELECT ops.lock_catalog_write_resources(ARRAY['catalog:genre:2', 'catalog:genre:1'])",
    )
    .execute(&ingest)
    .await?;
    denied_then_recovers(&ingest, "CREATE SCHEMA denied_ingest_schema").await?;
    denied_then_recovers(&ingest, "CREATE ROLE denied_ingest_role").await?;
    denied_then_recovers(&ingest, "CREATE EXTENSION hstore").await?;
    denied_then_recovers(
        &ingest,
        "CREATE TABLE assets.denied_ingest_cross_worker(id integer)",
    )
    .await?;
    denied_then_recovers(
        &ingest,
        "INSERT INTO ops.service_metadata(key, value) VALUES ('denied-ingest', '{}'::jsonb)",
    )
    .await?;

    let image = role_pool(&database, "image_writer", PoolPolicy::ReadWrite).await?;
    assert_eq!(current_user(&image).await?, "image_writer");
    assert!(schema_usage(&image, "assets").await?);
    assert!(schema_usage(&image, "catalog").await?);
    let can_read_titles: bool =
        sqlx::query_scalar("SELECT has_table_privilege(current_user, 'catalog.titles', 'SELECT')")
            .fetch_one(&image)
            .await?;
    assert!(can_read_titles);
    let can_submit_jobs: bool = sqlx::query_scalar(
        "SELECT has_function_privilege(
                    current_user,
                    'ops.submit_job(uuid,text,integer,text,smallint,integer,timestamptz,text)',
                    'EXECUTE'
                )",
    )
    .fetch_one(&image)
    .await?;
    assert!(can_submit_jobs);
    denied_then_recovers(&image, "UPDATE catalog.titles SET display_title = 'denied'").await?;
    denied_then_recovers(&image, "CREATE SCHEMA denied_image_schema").await?;
    denied_then_recovers(&image, "CREATE ROLE denied_image_role").await?;
    denied_then_recovers(&image, "CREATE EXTENSION hstore").await?;
    denied_then_recovers(
        &image,
        "INSERT INTO source.ingest_runs(run_type, status) VALUES ('bulk', 'pending')",
    )
    .await?;

    let monitor = role_pool(&database, "monitor", PoolPolicy::ReadOnly).await?;
    assert_eq!(current_user(&monitor).await?, "monitor");
    let _: String = sqlx::query_scalar("SELECT schema_revision FROM ops.readiness")
        .fetch_one(&monitor)
        .await?;
    denied_then_recovers(&monitor, "SELECT * FROM ops.service_metadata").await?;
    denied_then_recovers(&monitor, "SELECT * FROM ops.job_type_registry").await?;

    let public_privileges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.role_table_grants
          WHERE grantee = 'PUBLIC' AND table_schema = ANY($1)",
    )
    .bind(SCHEMAS)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(public_privileges, 0);
    let public_schema_privileges: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM pg_namespace n, LATERAL aclexplode(n.nspacl) privilege
          WHERE n.nspname = ANY($1) AND privilege.grantee = 0",
    )
    .bind(SCHEMAS)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(public_schema_privileges, 0);
    let public_function_privileges: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace,
                LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) privilege
          WHERE n.nspname = ANY($1) AND privilege.grantee = 0",
    )
    .bind(SCHEMAS)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(public_function_privileges, 0);

    let default_acl: Vec<String> = sqlx::query_scalar(
        "SELECT n.nspname || '|' || d.defaclobjtype::text || '|'
                    || CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                            ELSE coalesce(role.rolname, 'oid:' || acl.grantee::text) END
                    || '|' || acl.privilege_type
           FROM pg_default_acl d
           JOIN pg_namespace n ON n.oid = d.defaclnamespace,
                LATERAL aclexplode(d.defaclacl) acl
           LEFT JOIN pg_roles role ON role.oid = acl.grantee
          WHERE n.nspname = ANY($1)
            AND CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                     ELSE coalesce(role.rolname, 'oid:' || acl.grantee::text) END <> 'migrator'
          ORDER BY 1",
    )
    .bind(SCHEMAS)
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(
        default_acl,
        [
            "assets|S|image_writer|SELECT",
            "assets|S|image_writer|UPDATE",
            "assets|S|image_writer|USAGE",
            "assets|r|api_reader|SELECT",
            "assets|r|image_writer|DELETE",
            "assets|r|image_writer|INSERT",
            "assets|r|image_writer|SELECT",
            "assets|r|image_writer|UPDATE",
            "catalog|S|ingest_writer|SELECT",
            "catalog|S|ingest_writer|UPDATE",
            "catalog|S|ingest_writer|USAGE",
            "catalog|r|api_reader|SELECT",
            "catalog|r|ingest_writer|DELETE",
            "catalog|r|ingest_writer|INSERT",
            "catalog|r|ingest_writer|SELECT",
            "catalog|r|ingest_writer|UPDATE",
            "search|S|ingest_writer|SELECT",
            "search|S|ingest_writer|UPDATE",
            "search|S|ingest_writer|USAGE",
            "search|r|api_reader|SELECT",
            "search|r|ingest_writer|SELECT",
            "source|S|ingest_writer|SELECT",
            "source|S|ingest_writer|UPDATE",
            "source|S|ingest_writer|USAGE",
            "source|r|ingest_writer|DELETE",
            "source|r|ingest_writer|INSERT",
            "source|r|ingest_writer|SELECT",
            "source|r|ingest_writer|UPDATE",
        ]
    );
    let effective_function_default_acl: Vec<String> = sqlx::query_scalar(
        "WITH migrator AS (SELECT oid FROM pg_roles WHERE rolname = 'migrator')
         SELECT CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                     ELSE coalesce(role.rolname, 'oid:' || acl.grantee::text) END
                || '|' || acl.privilege_type
           FROM migrator m
           LEFT JOIN pg_default_acl d
             ON d.defaclrole = m.oid
            AND d.defaclnamespace = 0
            AND d.defaclobjtype = 'f',
                LATERAL aclexplode(coalesce(d.defaclacl, acldefault('f', m.oid))) acl
           LEFT JOIN pg_roles role ON role.oid = acl.grantee
          ORDER BY 1",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(effective_function_default_acl, ["migrator|EXECUTE"]);
    for role in [
        "api_reader",
        "api_job_submitter",
        "ingest_writer",
        "image_writer",
        "monitor",
    ] {
        let can_create: bool = sqlx::query_scalar(
            "SELECT bool_or(has_schema_privilege($1, schema_name, 'CREATE'))
               FROM information_schema.schemata WHERE schema_name = ANY($2)",
        )
        .bind(role)
        .bind(SCHEMAS)
        .fetch_one(&owner_pool)
        .await?;
        assert!(!can_create, "{role} unexpectedly has schema CREATE");
    }

    let wrong_secret = "wrong-test-password-not-present-in-errors";
    let bad_config = DatabaseConfig {
        host: std::env::var("TMDB_TEST_DB_HOST").unwrap_or_else(|_| "host.docker.internal".into()),
        port: 55432,
        database,
        username: "api_reader".to_owned(),
        password: SecretString::from(wrong_secret.to_owned()),
    };
    let Err(connection_error) = connect_direct(&bad_config, PoolPolicy::ReadOnly).await else {
        return Err(test_error("invalid password was accepted"));
    };
    assert!(!format!("{connection_error:?}").contains(wrong_secret));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn readiness_is_sanitized_read_only_and_requires_api_reader(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
         CREATE EXTENSION IF NOT EXISTS pg_trgm;
         CREATE EXTENSION IF NOT EXISTS unaccent;",
    )
    .execute(&owner_pool)
    .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let reader = role_pool(&database, "api_reader", PoolPolicy::ReadOnly).await?;
    let report = readiness(&reader, TEST_SHARED_DATABASE_OWNER)
        .await
        .map_err(db_error)?;
    assert_eq!(report.postgres_major, 18);
    assert_eq!(report.schema_revision, "0053");
    assert_eq!(
        report.extensions,
        ["pg_stat_statements", "pg_trgm", "unaccent"]
    );
    let json =
        serde_json::to_value(&report).map_err(|_| test_error("readiness did not serialize"))?;
    assert_eq!(json.as_object().map(serde_json::Map::len), Some(3));
    let rendered = json.to_string();
    for forbidden in ["password", "host", "port", "database", "path", "user"] {
        assert!(!rendered.contains(forbidden));
    }

    let Err(wrong_role) = readiness(&owner_pool, TEST_SHARED_DATABASE_OWNER).await else {
        return Err(test_error("readiness accepted a non-api_reader session"));
    };
    assert!(!format!("{wrong_role:?}").contains("postgres://"));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn readiness_rejects_an_extra_successful_migration(owner_pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ops._sqlx_migrations(
             version, description, installed_on, success, checksum, execution_time
         ) VALUES (54, 'unexpected', clock_timestamp(), true, decode('00', 'hex'), 0)",
    )
    .execute(&owner_pool)
    .await?;
    assert_readiness_drift(&owner_pool).await
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn readiness_rejects_a_failed_migration_row(owner_pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ops._sqlx_migrations(
             version, description, installed_on, success, checksum, execution_time
         ) VALUES (54, 'failed', clock_timestamp(), false, decode('00', 'hex'), 0)",
    )
    .execute(&owner_pool)
    .await?;
    assert_readiness_drift(&owner_pool).await
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn readiness_rejects_a_missing_expected_migration(owner_pool: PgPool) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM ops._sqlx_migrations WHERE version = 10")
        .execute(&owner_pool)
        .await?;
    assert_readiness_drift(&owner_pool).await
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn readiness_rejects_metadata_revision_drift(owner_pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE ops.service_metadata
            SET value = jsonb_set(value, '{revision}', to_jsonb('0003'::text))
          WHERE key = 'schema'",
    )
    .execute(&owner_pool)
    .await?;
    assert_readiness_drift(&owner_pool).await
}

#[sqlx::test(migrations = false)]
async fn representative_version_one_database_upgrades_through_two_three_and_four(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    MIGRATOR
        .run_to(2, &owner_pool)
        .await
        .map_err(|_| test_error("migration 0002 fixture setup failed"))?;
    let initial_versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM ops._sqlx_migrations WHERE success ORDER BY version",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(initial_versions, [1, 2]);

    let ingest_id: String = sqlx::query_scalar(
        "INSERT INTO source.ingest_runs(run_type, status, watermark, counts)
         VALUES ('incremental', 'pending', '{\"cursor\":42}'::jsonb, '{\"titles\":7}'::jsonb)
         RETURNING id::text",
    )
    .fetch_one(&owner_pool)
    .await?;
    let api_key_id: String = sqlx::query_scalar(
        "INSERT INTO auth.api_keys(identifier, hmac_digest, owner, scopes)
         VALUES ('upgrade-fixture-key', decode(repeat('01', 32), 'hex'),
                 'upgrade-owner', ARRAY['jobs:read']::text[])
         RETURNING id::text",
    )
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('upgrade.fixture', 7, false)",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('legacy.nóop', 1, true)",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('legacy.disabled', 1, true)",
    )
    .execute(&owner_pool)
    .await?;

    let legacy_infinity_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'system.noop', 1, '{}'::text, 0::smallint, 3,
               'infinity'::timestamptz, 'legacy-infinity')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let legacy_failure_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'system.noop', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'legacy-failure')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let _: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('legacy-upgrade-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&legacy_failure_id)
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "SELECT disposition
           FROM ops.fail_job($1::uuid, 'legacy-upgrade-worker',
                            'legacy upstream says nóop is unavailable', 1000000)",
    )
    .bind(&legacy_failure_id)
    .execute(&owner_pool)
    .await?;
    let legacy_cancel_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'system.noop', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'legacy-cancel')",
    )
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "SELECT job_status
           FROM ops.request_job_cancel($1::uuid, 'legacy cancellation requested by opérator')",
    )
    .bind(&legacy_cancel_id)
    .execute(&owner_pool)
    .await?;
    let legacy_unicode_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'legacy.nóop', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'clé')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let legacy_disabled_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'legacy.disabled', 1, '{}'::text, 100::smallint, 3,
               clock_timestamp(), 'legacy-disabled-running')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let _: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('legacy-disabled-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&legacy_disabled_id)
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = false
          WHERE job_type = 'legacy.disabled' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;

    MIGRATOR
        .run(&owner_pool)
        .await
        .map_err(|_| test_error("0001 fixture did not upgrade through 0053"))?;
    let upgraded_versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM ops._sqlx_migrations WHERE success ORDER BY version",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(
        upgraded_versions,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53
        ]
    );

    let preserved: (String, String, Option<String>, String, String) = sqlx::query_as(
        "SELECT
             (SELECT status FROM ops.jobs WHERE id = $1::uuid),
             (SELECT available_at::text FROM ops.jobs WHERE id = $1::uuid),
             (SELECT error_message FROM ops.jobs WHERE id = $2::uuid),
             (SELECT status FROM ops.jobs WHERE id = $3::uuid),
             (SELECT job_type || ':' || dedup_key FROM ops.jobs WHERE id = $4::uuid)",
    )
    .bind(&legacy_infinity_id)
    .bind(&legacy_failure_id)
    .bind(&legacy_cancel_id)
    .bind(&legacy_unicode_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(preserved.0, "queued");
    assert_eq!(preserved.1, "infinity");
    assert_eq!(
        preserved.2.as_deref(),
        Some("legacy upstream says nóop is unavailable")
    );
    assert_eq!(preserved.3, "cancelled");
    assert_eq!(preserved.4, "legacy.nóop:clé");
    let disabled_state: (String, bool, bool) = sqlx::query_as(
        "SELECT status, cancellation_requested, claimable
           FROM ops.jobs WHERE id = $1::uuid",
    )
    .bind(&legacy_disabled_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(disabled_state, ("running".to_owned(), true, false));
    let disabled_audit_events: Vec<(String, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT event_kind, from_status, to_status, details
               FROM ops.job_events
              WHERE job_id = $1::uuid
                AND details ->> 'reason' = 'migration_reconcile'
              ORDER BY created_at, id",
    )
    .bind(&legacy_disabled_id)
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(disabled_audit_events.len(), 1);
    assert_eq!(
        (
            disabled_audit_events[0].0.as_str(),
            disabled_audit_events[0].1.as_str(),
            disabled_audit_events[0].2.as_str()
        ),
        ("cancellation_requested", "running", "running")
    );
    assert_eq!(
        disabled_audit_events[0].3,
        serde_json::json!({
            "reason": "migration_reconcile",
            "type": "legacy.disabled",
            "job_type": "legacy.disabled",
            "payload_version": 1,
            "enabled": false
        })
    );

    let cancelled_infinity: String = sqlx::query_scalar(
        "SELECT job_status
           FROM ops.request_job_cancel($1::uuid, 'cancelación legacy infinity')",
    )
    .bind(&legacy_infinity_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(cancelled_infinity, "cancelled");
    sqlx::query(
        "UPDATE ops.jobs
            SET priority = 100, available_at = clock_timestamp()
          WHERE id = $1::uuid",
    )
    .bind(&legacy_failure_id)
    .execute(&owner_pool)
    .await?;
    let _: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('legacy-upgrade-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&legacy_failure_id)
    .fetch_one(&owner_pool)
    .await
    .map_err(|error| test_error(&format!("legacy lifecycle claim failed: {error:?}")))?;
    let legacy_retry_disposition: String = sqlx::query_scalar(
        "SELECT disposition
           FROM ops.fail_job($1::uuid, 'legacy-upgrade-worker',
                            'legacy second failure nóop', 1000000)",
    )
    .bind(&legacy_failure_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(legacy_retry_disposition, "retry_scheduled");
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = true
          WHERE job_type = 'legacy.disabled' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "UPDATE ops.jobs
            SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
          WHERE id = $1::uuid",
    )
    .bind(&legacy_disabled_id)
    .execute(&owner_pool)
    .await?;
    sqlx::query("SELECT * FROM ops.claim_job('legacy-cleanup-worker', 1000000)")
        .execute(&owner_pool)
        .await?;
    let disabled_terminal: (String, bool) = sqlx::query_as(
        "SELECT status, cancellation_requested
           FROM ops.jobs WHERE id = $1::uuid",
    )
    .bind(&legacy_disabled_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(disabled_terminal, ("cancelled".to_owned(), false));
    MIGRATOR
        .run(&owner_pool)
        .await
        .map_err(|_| test_error("repeat upgraded migration run failed"))?;
    let repeat_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ops._sqlx_migrations WHERE success")
            .fetch_one(&owner_pool)
            .await?;
    assert_eq!(repeat_count, 53);
    let scheduler_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('ops.scheduler_runs')::text")
            .fetch_one(&owner_pool)
            .await?;
    assert!(scheduler_table.is_none());

    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('upgrade.fixture', 8, true)",
    )
    .execute(&owner_pool)
    .await?;
    let registry_versions: Vec<(i32, bool)> = sqlx::query_as(
        "SELECT payload_version, enabled
           FROM ops.job_type_registry
          WHERE job_type = 'upgrade.fixture'
          ORDER BY payload_version",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(registry_versions, [(7, false), (8, true)]);
    let watermark: serde_json::Value =
        sqlx::query_scalar("SELECT watermark FROM source.ingest_runs WHERE id = $1::uuid")
            .bind(ingest_id)
            .fetch_one(&owner_pool)
            .await?;
    assert_eq!(watermark, serde_json::json!({ "cursor": 42 }));
    let key_owner: String =
        sqlx::query_scalar("SELECT owner FROM auth.api_keys WHERE id = $1::uuid")
            .bind(api_key_id)
            .fetch_one(&owner_pool)
            .await?;
    assert_eq!(key_owner, "upgrade-owner");

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    for role in ["ingest_writer", "image_writer"] {
        let pool = role_pool(&database, role, PoolPolicy::ReadWrite).await?;
        assert!(schema_usage(&pool, "ops").await?);
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT has_function_privilege(current_user, 'ops.claim_job(text,bigint)', 'EXECUTE')",
            )
            .fetch_one(&pool)
            .await?
        );
    }
    for role in ["api_reader", "monitor"] {
        let pool = role_pool(&database, role, PoolPolicy::ReadOnly).await?;
        let can_read_jobs: bool = sqlx::query_scalar(
            "SELECT has_table_privilege(current_user, 'ops.job_status', 'SELECT')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!can_read_jobs);
    }
    install_required_extensions(&owner_pool).await?;
    let reader = role_pool(&database, "api_reader", PoolPolicy::ReadOnly).await?;
    assert_eq!(
        readiness(&reader, TEST_SHARED_DATABASE_OWNER)
            .await
            .map_err(db_error)?
            .schema_revision,
        "0053"
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn round_one_three_database_is_repaired_by_four(owner_pool: PgPool) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    MIGRATOR
        .run_to(3, &owner_pool)
        .await
        .map_err(|_| test_error("round-one simulation setup failed"))?;

    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('rollout.audit-queued', 1, true),
                ('rollout.audit-running', 1, true),
                ('rollout.audit-running-cancelled', 1, true)",
    )
    .execute(&owner_pool)
    .await?;
    let stale_queued_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'rollout.audit-queued', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'stale-audit-queued')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let stale_running_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'rollout.audit-running', 1, '{}'::text, 100::smallint, 3,
               clock_timestamp(), 'stale-audit-running')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let claimed_stale_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('stale-audit-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&stale_running_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(claimed_stale_id, stale_running_id);
    let stale_cancelled_running_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'rollout.audit-running-cancelled', 1, '{}'::text,
               200::smallint, 3, clock_timestamp(), 'stale-audit-running-cancelled')",
    )
    .fetch_one(&owner_pool)
    .await?;
    let claimed_stale_cancelled_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('stale-audit-cancel-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&stale_cancelled_running_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(claimed_stale_cancelled_id, stale_cancelled_running_id);
    sqlx::query(
        "SELECT job_status
           FROM ops.request_job_cancel($1::uuid, 'prior user cancellation')",
    )
    .bind(&stale_cancelled_running_id)
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = false
          WHERE (job_type, payload_version) IN (
              ('rollout.audit-queued', 1),
              ('rollout.audit-running', 1),
              ('rollout.audit-running-cancelled', 1)
          )",
    )
    .execute(&owner_pool)
    .await?;

    sqlx::raw_sql(
        "ALTER TABLE ops.job_type_registry
             DROP CONSTRAINT job_type_registry_job_type_check,
             ADD CONSTRAINT job_type_registry_job_type_check CHECK (
                 pg_catalog.char_length(job_type) BETWEEN 1 AND 128
                 AND pg_catalog.octet_length(job_type) = pg_catalog.char_length(job_type)
                 AND job_type = pg_catalog.btrim(job_type)
                 AND job_type !~ '[[:cntrl:]]'
             );
         ALTER TABLE ops.jobs
             DROP CONSTRAINT jobs_job_type_check,
             ADD CONSTRAINT jobs_job_type_check CHECK (
                 pg_catalog.char_length(job_type) BETWEEN 1 AND 128
                 AND pg_catalog.octet_length(job_type) = pg_catalog.char_length(job_type)
                 AND job_type = pg_catalog.btrim(job_type)
                 AND job_type !~ '[[:cntrl:]]'
             ),
             DROP CONSTRAINT jobs_dedup_key_check,
             ADD CONSTRAINT jobs_dedup_key_check CHECK (
                 pg_catalog.char_length(dedup_key) BETWEEN 1 AND 256
                 AND pg_catalog.octet_length(dedup_key) = pg_catalog.char_length(dedup_key)
                 AND dedup_key = pg_catalog.btrim(dedup_key)
                 AND dedup_key !~ '[[:cntrl:]]'
             ),
             DROP CONSTRAINT jobs_lease_owner_check,
             ADD CONSTRAINT jobs_lease_owner_check CHECK (
                 lease_owner IS NULL
                 OR (
                     pg_catalog.char_length(lease_owner) BETWEEN 1 AND 128
                     AND pg_catalog.octet_length(lease_owner) = pg_catalog.char_length(lease_owner)
                     AND lease_owner = pg_catalog.btrim(lease_owner)
                     AND lease_owner !~ '[[:cntrl:]]'
                 )
             ),
             ADD CONSTRAINT jobs_available_at_check CHECK (
                 pg_catalog.isfinite(available_at)
                 AND EXTRACT(year FROM available_at) BETWEEN 1 AND 9999
             );
         ALTER TABLE ops.job_events
             DROP CONSTRAINT job_events_worker_check,
             ADD CONSTRAINT job_events_worker_check CHECK (
                 worker_id IS NULL
                 OR (
                     pg_catalog.char_length(worker_id) BETWEEN 1 AND 128
                     AND pg_catalog.octet_length(worker_id) = pg_catalog.char_length(worker_id)
                     AND worker_id = pg_catalog.btrim(worker_id)
                     AND worker_id !~ '[[:cntrl:]]'
                 )
             );
         DROP TRIGGER IF EXISTS jobs_claimable_sync ON ops.jobs;
         DROP TRIGGER IF EXISTS registry_job_claimability_sync ON ops.job_type_registry;
         DROP FUNCTION IF EXISTS ops.sync_job_claimable();
         DROP FUNCTION IF EXISTS ops.sync_registry_job_claimability();
         DROP INDEX IF EXISTS ops.jobs_claim_ready_idx;
         DROP INDEX IF EXISTS ops.jobs_reclaim_ready_idx;
         DROP INDEX IF EXISTS ops.jobs_cancel_requested_expired_idx;
         ALTER TABLE ops.jobs DROP COLUMN claimable;
         DELETE FROM ops._sqlx_migrations WHERE version = 4;
         UPDATE ops._sqlx_migrations
            SET checksum = decode(
                'CA21F97804AF92CA314B5C9FA54BACA0CB33B09639381D9A96D775E1F3014A2AF24D805BC7EFF0F83DFD9C2C8C1FD1F4',
                'hex'
            )
          WHERE version = 3",
    )
    .execute(&owner_pool)
    .await?;

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let migrator_pool = role_pool(&database, "migrator", PoolPolicy::Migrator).await?;
    let report = migrate(&migrator_pool, TEST_SHARED_DATABASE_OWNER)
        .await
        .map_err(db_error)?;
    assert_eq!(report.applied, 50);

    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM ops._sqlx_migrations WHERE success ORDER BY version",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(
        versions,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53
        ]
    );
    let claimable_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'ops' AND table_name = 'jobs'
                AND column_name = 'claimable'
         )",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert!(claimable_exists);
    for constraint_name in [
        "job_type_registry_job_type_check",
        "jobs_job_type_check",
        "jobs_dedup_key_check",
        "jobs_lease_owner_check",
        "job_events_worker_check",
    ] {
        let definition: String = sqlx::query_scalar(
            "SELECT pg_catalog.pg_get_constraintdef(c.oid)
               FROM pg_catalog.pg_constraint AS c
               JOIN pg_catalog.pg_class AS relation ON relation.oid = c.conrelid
               JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
              WHERE namespace.nspname = 'ops' AND c.conname = $1",
        )
        .bind(constraint_name)
        .fetch_one(&owner_pool)
        .await?;
        assert!(
            !definition.contains("octet_length"),
            "stale strict constraint {constraint_name} was not relaxed: {definition}"
        );
    }
    let finite_available_check: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM pg_catalog.pg_constraint AS c
               JOIN pg_catalog.pg_class AS relation ON relation.oid = c.conrelid
               JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
              WHERE namespace.nspname = 'ops'
                AND relation.relname = 'jobs'
                AND c.conname = 'jobs_available_at_check'
         )",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert!(!finite_available_check);
    for index_name in [
        "ops.jobs_claim_ready_idx",
        "ops.jobs_reclaim_ready_idx",
        "ops.jobs_cancel_requested_expired_idx",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(index_name)
            .fetch_one(&owner_pool)
            .await?;
        assert!(exists, "repaired index {index_name} is missing");
    }

    let stale_audit_events: Vec<(String, String, String, String, serde_json::Value)> =
        sqlx::query_as(
            "SELECT job.id::text, event_kind, from_status, to_status, details
               FROM ops.job_events AS event
               JOIN ops.jobs AS job ON job.id = event.job_id
              WHERE event.details ->> 'reason' = 'migration_reconcile'
                AND job.job_type LIKE 'rollout.audit-%'
              ORDER BY job.job_type, event.created_at, event.id",
        )
        .fetch_all(&owner_pool)
        .await?;
    assert_eq!(stale_audit_events.len(), 3);
    assert_eq!(stale_audit_events[0].0, stale_queued_id);
    assert_eq!(
        (
            stale_audit_events[0].1.as_str(),
            stale_audit_events[0].2.as_str(),
            stale_audit_events[0].3.as_str()
        ),
        ("claimability_changed", "queued", "queued")
    );
    assert_eq!(stale_audit_events[0].4["reason"], "migration_reconcile");
    assert_eq!(stale_audit_events[0].4["type"], "rollout.audit-queued");
    assert_eq!(stale_audit_events[1].0, stale_running_id);
    assert_eq!(
        (
            stale_audit_events[1].1.as_str(),
            stale_audit_events[1].2.as_str(),
            stale_audit_events[1].3.as_str()
        ),
        ("cancellation_requested", "running", "running")
    );
    assert_eq!(stale_audit_events[1].4["reason"], "migration_reconcile");
    assert_eq!(stale_audit_events[1].4["type"], "rollout.audit-running");
    assert_eq!(stale_audit_events[2].0, stale_cancelled_running_id);
    assert_eq!(
        (
            stale_audit_events[2].1.as_str(),
            stale_audit_events[2].2.as_str(),
            stale_audit_events[2].3.as_str()
        ),
        ("claimability_changed", "running", "running")
    );
    assert_eq!(stale_audit_events[2].4["reason"], "migration_reconcile");
    assert_eq!(
        stale_audit_events[2].4["type"],
        "rollout.audit-running-cancelled"
    );
    let prior_user_cancellation_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM ops.job_events
          WHERE job_id = $1::uuid
            AND event_kind = 'cancellation_requested'
            AND details ->> 'message' = 'prior user cancellation'",
    )
    .bind(&stale_cancelled_running_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(prior_user_cancellation_count, 1);

    sqlx::query(
        "INSERT INTO ops.job_type_registry(job_type, payload_version, enabled)
         VALUES ('rollout.disabled', 1, true)",
    )
    .execute(&owner_pool)
    .await?;
    let job_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'rollout.disabled', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'rollout-disabled')",
    )
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "UPDATE ops.job_type_registry
            SET enabled = false
          WHERE job_type = 'rollout.disabled' AND payload_version = 1",
    )
    .execute(&owner_pool)
    .await?;
    let disabled_state: (bool, bool) = sqlx::query_as(
        "SELECT cancellation_requested, claimable
           FROM ops.jobs WHERE id = $1::uuid",
    )
    .bind(&job_id)
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(disabled_state, (false, false));

    let claimed: Option<String> = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.claim_job('rollout-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&job_id)
    .fetch_optional(&owner_pool)
    .await?;
    assert!(claimed.is_none());
    let status: String = sqlx::query_scalar("SELECT status FROM ops.jobs WHERE id = $1::uuid")
        .bind(&job_id)
        .fetch_one(&owner_pool)
        .await?;
    assert_eq!(status, "queued");

    let legacy_id: String = sqlx::query_scalar(
        "SELECT job_id::text
           FROM ops.submit_job(
               gen_random_uuid(), 'system.noop', 1, '{}'::text, 0::smallint, 3,
               clock_timestamp(), 'rollout-legacy-failure')",
    )
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query_scalar::<_, String>(
        "SELECT job_id::text
           FROM ops.claim_job('rollout-worker', 1000000)
          WHERE job_id = $1::uuid",
    )
    .bind(&legacy_id)
    .fetch_one(&owner_pool)
    .await?;
    sqlx::query(
        "SELECT disposition
           FROM ops.fail_job($1::uuid, 'rollout-worker', 'legacy rollout failure', 60000000)",
    )
    .bind(&legacy_id)
    .execute(&owner_pool)
    .await?;
    let error_code: Option<String> =
        sqlx::query_scalar("SELECT error_code FROM ops.jobs WHERE id = $1::uuid")
            .bind(&legacy_id)
            .fetch_one(&owner_pool)
            .await?;
    assert_eq!(error_code.as_deref(), Some("execution_failed"));

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT schema_revision FROM ops.readiness")
            .fetch_one(&owner_pool)
            .await?,
        "0053"
    );
    migrator_pool.close().await;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn actual_migrator_applies_once_then_preserves_snapshot(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let migrator_pool = role_pool(&database, "migrator", PoolPolicy::Migrator).await?;

    assert_eq!(
        migrate(&migrator_pool, TEST_SHARED_DATABASE_OWNER)
            .await
            .map_err(db_error)?
            .applied,
        53
    );
    let first = foundation_snapshot(&migrator_pool).await?;
    sqlx::query(
        "REVOKE EXECUTE ON FUNCTION ops.submit_job(
             uuid, text, integer, text, smallint, integer, timestamptz, text
         ) FROM image_writer",
    )
    .execute(&owner_pool)
    .await?;
    assert_eq!(
        migrate(&migrator_pool, TEST_SHARED_DATABASE_OWNER)
            .await
            .map_err(db_error)?
            .applied,
        0
    );
    let repaired_submit_job_privilege: bool = sqlx::query_scalar(
        "SELECT has_function_privilege(
                    'image_writer',
                    'ops.submit_job(uuid,text,integer,text,smallint,integer,timestamptz,text)',
                    'EXECUTE'
                )",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert!(repaired_submit_job_privilege);
    let second = foundation_snapshot(&migrator_pool).await?;
    if second != first {
        let only_second: Vec<&String> =
            second.iter().filter(|line| !first.contains(line)).collect();
        let only_first: Vec<&String> = first.iter().filter(|line| !second.contains(line)).collect();
        return Err(test_error(&format!(
            "migration snapshot changed; only_second={only_second:?}; only_first={only_first:?}"
        )));
    }

    sqlx::query(
        "UPDATE ops._sqlx_migrations
            SET checksum = decode(
                'EFD2AE0567D3576E14AC2926E3089EB43EC5C54F1D63EF5822E156963DF3D16B3F54118BD6E7DE2A543A7BC1EBDD132E',
                'hex'
            )
          WHERE version = 4",
    )
    .execute(&owner_pool)
    .await?;
    assert_eq!(
        migrate(&migrator_pool, TEST_SHARED_DATABASE_OWNER)
            .await
            .map_err(db_error)?
            .applied,
        0
    );
    let repaired = foundation_snapshot(&migrator_pool).await?;
    assert_eq!(repaired, first);

    // A database that applied the short-lived unrepaired administrative
    // migration must be accepted only through its exact historical checksum;
    // the forward-only migration keeps its database effects intact.
    sqlx::query(
        "UPDATE ops._sqlx_migrations
            SET checksum = decode(
                '5B29DF4FA6CDA3034DD541C348669CBD9615D54EDFAD9EAE716ACA2DCFA1227CBF8D45243F4DB3E08F80EF5E38AD4B33',
                'hex'
            )
          WHERE version = 22",
    )
    .execute(&owner_pool)
    .await?;
    assert_eq!(
        migrate(&migrator_pool, TEST_SHARED_DATABASE_OWNER)
            .await
            .map_err(db_error)?
            .applied,
        0
    );
    assert_eq!(foundation_snapshot(&migrator_pool).await?, first);
    assert!(first.iter().any(|line| line.starts_with("migration|1|")));
    assert!(first.iter().any(|line| line.starts_with("migration|2|")));
    assert!(first.iter().any(|line| line.starts_with("migration|3|")));
    assert!(first.iter().any(|line| line.starts_with("migration|4|")));
    assert!(
        first
            .iter()
            .any(|line| line == "default_acl|*|f|migrator|EXECUTE")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|system.noop|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|image.download|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|ingest.refresh_movie|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|ingest.refresh_tv|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|ingest.changes_sync|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|ingest.daily_export|1|true")
    );
    assert!(
        first
            .iter()
            .any(|line| line == "seed|job|ingest.trending|1|true")
    );
    assert!(first.iter().any(|line| line == "seed|metadata|schema|0053"));

    migrator_pool.close().await;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn catalog_recovery_migration_preserves_completed_enrichment(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    MIGRATOR
        .run_to(50, &owner_pool)
        .await
        .map_err(|_| test_error("migration 0050 fixture setup failed"))?;

    sqlx::query(
        "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
         VALUES ('movie', 51, 'Complete movie'),
                ('movie', 52, 'Incomplete movie'),
                ('tv', 700, 'Complete TV')",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.seasons (id, title_id, media_type, season_number)
         SELECT 1000 + season_number, id, 'tv', season_number
           FROM catalog.titles
          CROSS JOIN generate_series(1, 2) AS season_number
          WHERE media_type = 'tv' AND tmdb_id = 700",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "INSERT INTO ops.jobs (
             id, job_type, payload_version, payload, status, attempts,
             max_attempts, available_at, dedup_key, result_summary, created_at,
             updated_at, finished_at
         ) VALUES
             (gen_random_uuid(), 'ingest.enrich_movie', 1,
              '{\"tmdb_id\":51}'::jsonb, 'succeeded', 1, 8,
              statement_timestamp() - interval '1 second',
              'migration-enrich-movie', '{}'::jsonb,
              statement_timestamp() - interval '1 second', statement_timestamp(),
              statement_timestamp()),
             (gen_random_uuid(), 'ingest.enrich_tv', 1,
              '{\"tmdb_id\":700}'::jsonb, 'succeeded', 1, 8,
              statement_timestamp() - interval '1 second',
              'migration-enrich-tv', '{}'::jsonb,
              statement_timestamp() - interval '1 second', statement_timestamp(),
              statement_timestamp()),
             (gen_random_uuid(), 'ingest.refresh_season', 1,
              '{\"tv_id\":700,\"season_number\":1}'::jsonb,
              'succeeded', 1, 8, statement_timestamp() - interval '1 second',
              'migration-refresh-season', '{}'::jsonb,
              statement_timestamp() - interval '1 second', statement_timestamp(),
              statement_timestamp())",
    )
    .execute(&owner_pool)
    .await?;

    MIGRATOR
        .run_to(51, &owner_pool)
        .await
        .map_err(|_| test_error("migration 0051 application failed"))?;
    sqlx::query(
        "INSERT INTO source.tmdb_documents (endpoint_path, response)
         VALUES ('movie/51', '{\"id\":51,\"title\":\"Complete movie\"}')",
    )
    .execute(&owner_pool)
    .await?;
    sqlx::query(
        "WITH asset AS (
             INSERT INTO assets.image_assets (
                 title_id, image_kind, source, source_key, source_url,
                 source_mime_type, source_width, source_height,
                 source_file_size_bytes, source_sha256, source_storage_path,
                 storage_path, mime_type, width, height, file_size_bytes,
                 sha256, status, downloaded_at
             )
             SELECT id, 'poster', 'tmdb', '/legacy.jpg',
                    'https://image.tmdb.org/t/p/original/legacy.jpg',
                    'image/jpeg', 1000, 1500, 3, repeat('a', 64),
                    'movies/51/posters/poster.jpg',
                    'movies/51/optimized/posters/poster-w640.jpg',
                    'image/jpeg', 640, 960, 3, repeat('b', 64),
                    'ready', clock_timestamp()
               FROM catalog.titles WHERE tmdb_id = 51
             RETURNING id
         )
         INSERT INTO assets.image_variants (
             image_asset_id, variant_key, storage_path, mime_type,
             width, height, file_size_bytes, sha256
         )
         SELECT id, 'jpeg_w640',
                'movies/51/optimized/posters/poster-w640.jpg',
                'image/jpeg', 640, 960, 3, repeat('b', 64)
           FROM asset",
    )
    .execute(&owner_pool)
    .await?;

    MIGRATOR
        .run(&owner_pool)
        .await
        .map_err(|_| test_error("migrations through 0053 failed"))?;

    let title_state: Vec<(i64, bool)> = sqlx::query_as(
        "SELECT tmdb_id, enriched_at IS NOT NULL
           FROM catalog.titles
          ORDER BY tmdb_id",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(title_state, [(51, true), (52, false), (700, true)]);
    let season_state: Vec<(i32, bool)> = sqlx::query_as(
        "SELECT season_number, enriched_at IS NOT NULL
           FROM catalog.seasons
          ORDER BY season_number",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(season_state, [(1, true), (2, false)]);
    let preserved_document: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM source.tmdb_documents WHERE endpoint_path = 'movie/51'",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(preserved_document, 1);
    let removed_media: (i64, bool) = sqlx::query_as(
        "SELECT count(*), to_regclass('assets.image_variants') IS NULL
           FROM assets.image_assets",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(removed_media, (0, true));
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn queue_slot_migration_preserves_and_backfills_active_production_jobs(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    MIGRATOR
        .run_to(52, &owner_pool)
        .await
        .map_err(|_| test_error("schema 0052 setup failed"))?;

    sqlx::query(
        "INSERT INTO ops.jobs (
             id, job_type, payload_version, payload, status, dedup_key
         ) VALUES
             (gen_random_uuid(), 'image.download', 1, '{}'::jsonb, 'queued',
              'upgrade-image'),
             (gen_random_uuid(), 'ingest.refresh_movie', 1, '{}'::jsonb, 'queued',
              'upgrade-refresh-movie'),
             (gen_random_uuid(), 'ingest.refresh_tv', 1, '{}'::jsonb, 'queued',
              'upgrade-refresh-tv'),
             (gen_random_uuid(), 'ingest.enrich_movie', 1, '{}'::jsonb, 'queued',
              'upgrade-enrich-movie'),
             (gen_random_uuid(), 'ingest.enrich_tv', 1, '{}'::jsonb, 'queued',
              'upgrade-enrich-tv'),
             (gen_random_uuid(), 'ingest.refresh_season', 1, '{}'::jsonb, 'queued',
              'upgrade-season')",
    )
    .execute(&owner_pool)
    .await?;

    MIGRATOR
        .run(&owner_pool)
        .await
        .map_err(|_| test_error("schema 0053 queue migration failed"))?;

    let occupied: Vec<(String, i64)> = sqlx::query_as(
        "SELECT queue_name, count(*)::bigint
           FROM ops.job_queue_slots
          WHERE job_id IS NOT NULL
          GROUP BY queue_name
          ORDER BY queue_name",
    )
    .fetch_all(&owner_pool)
    .await?;
    assert_eq!(
        occupied,
        [
            ("image.download".to_owned(), 1),
            ("season.refresh".to_owned(), 1),
            ("title.enrichment".to_owned(), 2),
            ("title.refresh".to_owned(), 2),
        ]
    );
    let preserved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ops.jobs
          WHERE dedup_key LIKE 'upgrade-%' AND status = 'queued'",
    )
    .fetch_one(&owner_pool)
    .await?;
    assert_eq!(preserved, 6);
    let legacy_function: Option<String> =
        sqlx::query_scalar("SELECT to_regprocedure('ops.enforce_image_queue_limit()')::text")
            .fetch_one(&owner_pool)
            .await?;
    assert!(legacy_function.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT schema_revision FROM ops.readiness")
            .fetch_one(&owner_pool)
            .await?,
        "0053"
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn concurrent_actual_migrators_report_exactly_one_application(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    grant_generated_database_create(&owner_pool).await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    let first_pool = role_pool(&database, "migrator", PoolPolicy::Migrator).await?;
    let second_pool = role_pool(&database, "migrator", PoolPolicy::Migrator).await?;
    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);

    let first = async {
        first_barrier.wait().await;
        migrate(&first_pool, TEST_SHARED_DATABASE_OWNER).await
    };
    let second = async {
        second_barrier.wait().await;
        migrate(&second_pool, TEST_SHARED_DATABASE_OWNER).await
    };
    let release = async { barrier.wait().await };
    let (first, second, _) = tokio::join!(first, second, release);
    let mut applied = [
        first.map_err(db_error)?.applied,
        second.map_err(db_error)?.applied,
    ];
    applied.sort_unstable();
    assert_eq!(applied, [0, 53]);

    first_pool.close().await;
    second_pool.close().await;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn migration_runner_rejects_every_role_other_than_migrator(
    owner_pool: PgPool,
) -> sqlx::Result<()> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&owner_pool)
        .await?;
    for role in [
        "api_reader",
        "api_job_submitter",
        "ingest_writer",
        "image_writer",
        "monitor",
    ] {
        let pool = role_pool(&database, role, PoolPolicy::ReadWrite).await?;
        let Err(error) = migrate(&pool, TEST_SHARED_DATABASE_OWNER).await else {
            return Err(test_error("migration accepted a non-migrator role"));
        };
        let debug = format!("{error:?}");
        assert!(!debug.contains("postgres://"));
        let secret = test_database_password()
            .map_err(|_| test_error("test database password was not configured"))?;
        assert!(!debug.contains(&secret));
    }
    Ok(())
}

#[test]
fn database_debug_output_redacts_password() {
    let secret = "test-only-password-that-must-not-appear";
    let config = DatabaseConfig {
        host: "127.0.0.1".to_owned(),
        port: 55432,
        database: "tmdb".to_owned(),
        username: "api_reader".to_owned(),
        password: SecretString::from(secret.to_owned()),
    };
    assert!(!format!("{config:?}").contains(secret));
}

async fn role_pool(database: &str, role: &str, policy: PoolPolicy) -> sqlx::Result<PgPool> {
    let config = DatabaseConfig {
        host: std::env::var("TMDB_TEST_DB_HOST").unwrap_or_else(|_| "host.docker.internal".into()),
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
    connect_direct(&config, policy).await.map_err(db_error)
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

async fn current_user(pool: &PgPool) -> sqlx::Result<String> {
    sqlx::query_scalar("SELECT current_user")
        .fetch_one(pool)
        .await
}

async fn schema_usage(pool: &PgPool, schema: &str) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT has_schema_privilege(current_user, $1, 'USAGE')")
        .bind(schema)
        .fetch_one(pool)
        .await
}

async fn assert_session_policy(
    pool: &PgPool,
    application_name: &str,
    read_only: &str,
) -> sqlx::Result<()> {
    let settings: (String, String, String, String, String) = sqlx::query_as(
        "SELECT current_setting('application_name'), current_setting('TimeZone'),
                current_setting('statement_timeout'), current_setting('lock_timeout'),
                current_setting('default_transaction_read_only')",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(settings.0, application_name);
    assert_eq!(settings.1, "UTC");
    assert_eq!(settings.2, "5s");
    assert_eq!(settings.3, "2s");
    assert_eq!(settings.4, read_only);
    Ok(())
}

async fn denied_then_recovers(pool: &PgPool, statement: &'static str) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    let Err(error) = sqlx::raw_sql(statement).execute(&mut *transaction).await else {
        return Err(test_error("operation expected to be denied was allowed"));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501"),
        "denial must be insufficient_privilege"
    );
    transaction.rollback().await?;
    let one: i32 = sqlx::query_scalar("SELECT 1").fetch_one(pool).await?;
    assert_eq!(one, 1);
    Ok(())
}

async fn assert_api_reader_metadata_insert_is_denied_by_acl(
    reader_acl_probe: &PgPool,
    owner_pool: &PgPool,
) -> sqlx::Result<()> {
    const PROBE_KEY: &str = "api-reader-direct-write-acl-probe";

    let mut transaction = reader_acl_probe.begin().await?;
    let Err(error) = sqlx::query(
        "INSERT INTO ops.service_metadata(key, value)
         VALUES ($1, $2)",
    )
    .bind(PROBE_KEY)
    .bind(serde_json::json!({ "probe": "api_reader_insert_acl" }))
    .execute(&mut *transaction)
    .await
    else {
        return Err(test_error(
            "api_reader metadata INSERT ACL probe was allowed",
        ));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501"),
        "api_reader metadata INSERT must fail on ACLs"
    );
    transaction.rollback().await?;

    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(reader_acl_probe)
        .await?;
    assert_eq!(one, 1);
    let inserted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ops.service_metadata WHERE key = $1")
            .bind(PROBE_KEY)
            .fetch_one(owner_pool)
            .await?;
    assert_eq!(inserted, 0);
    Ok(())
}

async fn assert_readiness_drift(owner_pool: &PgPool) -> sqlx::Result<()> {
    install_required_extensions(owner_pool).await?;
    let readiness_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.readiness")
        .fetch_one(owner_pool)
        .await?;
    assert_eq!(readiness_rows, 0);

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(owner_pool)
        .await?;
    let reader = role_pool(&database, "api_reader", PoolPolicy::ReadOnly).await?;
    let result = readiness(&reader, TEST_SHARED_DATABASE_OWNER).await;
    reader.close().await;
    assert_eq!(result, Err(DbError::Unready));
    Ok(())
}

async fn install_required_extensions(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
         CREATE EXTENSION IF NOT EXISTS pg_trgm;
         CREATE EXTENSION IF NOT EXISTS unaccent;",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn grant_generated_database_create(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::raw_sql(
        "DO $grant$
         BEGIN
             EXECUTE format('GRANT CREATE ON DATABASE %I TO migrator', current_database());
         END
         $grant$;",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn foundation_snapshot(pool: &PgPool) -> sqlx::Result<Vec<String>> {
    let mut snapshot: Vec<String> = sqlx::query_scalar(
        "SELECT 'schema|' || n.nspname || '|' || pg_get_userbyid(n.nspowner)
           FROM pg_namespace n WHERE n.nspname = ANY($1)
         UNION ALL
         SELECT 'object|' || n.nspname || '.' || c.relname || '|' || c.relkind::text
                || '|' || pg_get_userbyid(c.relowner) || '|'
                || coalesce(array_to_string(c.relacl, ','), '<default>')
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = ANY($1) AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
         UNION ALL
         SELECT 'constraint|' || n.nspname || '.' || c.relname || '|' || con.conname
                || '|' || pg_get_constraintdef(con.oid, true)
           FROM pg_constraint con
           JOIN pg_class c ON c.oid = con.conrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = ANY($1)",
    )
    .bind(SCHEMAS)
    .fetch_all(pool)
    .await?;

    let mut default_acl: Vec<String> = sqlx::query_scalar(
        "SELECT 'default_acl|' || coalesce(n.nspname, '*') || '|' || d.defaclobjtype::text || '|'
                || CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                        ELSE coalesce(role.rolname, 'oid:' || acl.grantee::text) END
                || '|' || acl.privilege_type
           FROM pg_default_acl d
           LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace,
                LATERAL aclexplode(d.defaclacl) acl
           LEFT JOIN pg_roles role ON role.oid = acl.grantee
          WHERE d.defaclnamespace = 0 OR n.nspname = ANY($1)",
    )
    .bind(SCHEMAS)
    .fetch_all(pool)
    .await?;
    snapshot.append(&mut default_acl);

    let mut seeds: Vec<String> = sqlx::query_scalar(
        "SELECT 'seed|job|' || job_type || '|' || payload_version || '|' || enabled
           FROM ops.job_type_registry
         UNION ALL
         SELECT 'seed|metadata|' || key || '|' || (value ->> 'revision')
           FROM ops.service_metadata
         UNION ALL
         SELECT 'migration|' || version || '|' || description || '|' || success || '|'
                || encode(checksum, 'hex')
           FROM ops._sqlx_migrations",
    )
    .fetch_all(pool)
    .await?;
    snapshot.append(&mut seeds);
    snapshot.sort();
    Ok(snapshot)
}

async fn assert_sqlstate(
    pool: &PgPool,
    statement: &'static str,
    expected: &str,
) -> sqlx::Result<()> {
    let Err(error) = sqlx::raw_sql(statement).execute(pool).await else {
        return Err(test_error("constraint accepted an invalid row"));
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

fn db_error(error: tmdb_db::DbError) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

fn test_error(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}
