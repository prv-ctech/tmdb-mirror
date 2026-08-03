# TMDB Rust Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and prove the Rust/PostgreSQL 18 foundation: reproducible containers, safe configuration, domain rules, least-privilege roles, durable jobs, health/telemetry, PgBouncer, and read-only legacy auditing.

**Architecture:** A Cargo workspace contains small shared crates and four binary applications. PostgreSQL 18 is the durable source of truth, PgBouncer transaction-pools public reads, workers use direct bounded pools, and every external secret comes from a file. This plan is phase 1 of 4; ingestion, public API/search, and images/production cutover each receive a separate plan after the interfaces they consume have passed this phase.

**Tech Stack:** Rust 1.97.1, edition 2024, Axum 0.8.9, Tokio 1.53.1, SQLx 0.9.0, tower-http 0.7.0, Clap 4.6.4, PostgreSQL 18.4, PgBouncer 1.25.2, Docker Compose v5.

## Global Constraints

- Run Rust compilation and tests inside the verified official Rust image; no host Rust installation is required.
- Pin Rust builder image to rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa.
- Pin PostgreSQL to postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296, verified as PostgreSQL 18.4.
- Build PgBouncer 1.25.2 from the official release tarball and verify SHA-256 924ad35113fd0a71c8e2dbe85b5d03445532e2b7b37a9f8a48983beea238b332.
- Commit Cargo.lock and use cargo commands with --locked after dependency resolution.
- Use PostgreSQL 18 integration tests; never substitute SQLite or an in-memory repository for database behavior.
- Do not write to the live PostgreSQL 16 database. Legacy connections set default_transaction_read_only=on and use fixed SELECT statements.
- Do not commit passwords, TMDB tokens, API keys, cookies, generated secret files, or live connection strings.
- Preserve the legacy Python source unchanged during this phase.
- Treat warnings as errors; require rustfmt, clippy, unit tests, integration tests, Compose health, and restart tests.
- Use test-first steps. A task is not complete until its failure is observed before implementation and its full verification passes afterward.

---

## File and responsibility map

- Cargo.toml: workspace membership, exact framework versions, shared lint policy.
- Cargo.lock: resolved dependency lock.
- rust-toolchain.toml: Rust 1.97.1 with rustfmt and clippy.
- .cargo/config.toml: deterministic terminal/build behavior.
- crates/tmdb-domain: source-independent identifiers, media types, and anime decisions.
- crates/tmdb-config: environment adapters, secret-file loading, path validation, and redacted settings.
- crates/tmdb-db: PostgreSQL options, pools, migrations, readiness, and legacy read-only connection setup.
- crates/tmdb-jobs: durable job types and PostgreSQL repository.
- crates/tmdb-observability: structured tracing and Prometheus registry setup.
- apps/tmdb-api: HTTP listeners and health/readiness routes.
- apps/tmdb-ingest: direct-database worker lifecycle.
- apps/tmdb-images: direct-database worker lifecycle reserved for image jobs.
- apps/tmdb-admin: migrate, doctor, pool-smoke, and legacy-audit commands.
- crates/tmdb-db/migrations: ordered PostgreSQL schema changes.
- deploy/compose.dev.yaml: isolated local PostgreSQL, PgBouncer, and Rust services.
- deploy/env.example: non-secret development settings.
- deploy/secrets/README.md: generated-secret contract; actual secret files are ignored.
- infra/postgres/initdb/10-bootstrap.sh: first-cluster extensions, schemas, and login roles.
- infra/pgbouncer/Dockerfile: verified source build.
- infra/pgbouncer/pgbouncer.ini: transaction-pool policy.
- infra/pgbouncer/entrypoint.sh: tmpfs userlist generation from Docker secrets.
- scripts/bootstrap-dev.sh: cryptographically random local secret generation.
- scripts/verify-toolchain.sh: immutable toolchain/workspace proof.
- scripts/verify-postgres.sh: PostgreSQL version/checksum/extension proof.
- scripts/verify-foundation.sh: clean-volume end-to-end acceptance gate.
- docs/development.md: exact local workflow and troubleshooting.

### Task 1: Reproducible Cargo workspace

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.cargo/config.toml`
- Create: `scripts/verify-toolchain.sh`
- Create minimal manifests and entry points beneath `crates/` and `apps/` from the file map.
- Modify: `.gitignore`
- Modify: `.dockerignore`

**Interfaces:**
- Produces workspace packages named tmdb-domain, tmdb-config, tmdb-db, tmdb-jobs, tmdb-observability, tmdb-api, tmdb-ingest, tmdb-images, and tmdb-admin.
- Produces `scripts/verify-toolchain.sh` as the canonical containerized Cargo command wrapper.

- [ ] **Step 1: Write the failing toolchain/workspace check**

```bash
#!/usr/bin/env bash
set -Eeuo pipefail
image='rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa'
repo_path="$(pwd)"
rust_version="$(docker run --rm "$image" rustc --version)"
[[ "$rust_version" == 'rustc 1.97.1 '* ]] || {
    printf 'Unexpected Rust version: %s\n' "$rust_version" >&2
    exit 1
}
docker run --rm --mount "type=bind,source=$repo_path,target=/workspace" \
    --workdir /workspace "$image" cargo metadata --locked --no-deps --format-version 1 >/dev/null
```

- [ ] **Step 2: Run the check and observe the expected failure**

Run: `bash scripts/verify-toolchain.sh`

Expected: Rust reports 1.97.1, then Cargo fails because the workspace manifest does not exist.

- [ ] **Step 3: Create the workspace manifest and toolchain pin**

```toml
[workspace]
resolver = "3"
members = [
  "crates/tmdb-domain",
  "crates/tmdb-config",
  "crates/tmdb-db",
  "crates/tmdb-jobs",
  "crates/tmdb-observability",
  "apps/tmdb-api",
  "apps/tmdb-ingest",
  "apps/tmdb-images",
  "apps/tmdb-admin",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.97.1"
license = "MIT"

[workspace.dependencies]
anyhow = "1"
async-trait = "0.1"
axum = "=0.8.9"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "=4.6.4", features = ["derive", "env"] }
http = "1"
prometheus-client = "0.24"
secrecy = { version = "0.10", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
sqlx = { version = "=0.9.0", default-features = false, features = ["runtime-tokio", "tls-rustls", "postgres", "migrate", "macros", "chrono", "json", "uuid"] }
tempfile = "3"
thiserror = "2"
tokio = { version = "=1.53.1", features = ["macros", "rt-multi-thread", "signal", "sync", "time", "net"] }
tokio-util = { version = "0.7", features = ["rt"] }
tower = { version = "0.5", features = ["util"] }
tower-http = { version = "=0.7.0", features = ["catch-panic", "compression-zstd", "request-id", "sensitive-headers", "timeout", "trace"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
uuid = { version = "1", features = ["serde", "v7"] }

[workspace.lints.rust]
unsafe_code = "forbid"
missing_debug_implementations = "warn"

[workspace.lints.clippy]
all = "deny"
pedantic = "deny"
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
```

```toml
[toolchain]
channel = "1.97.1"
components = ["clippy", "rustfmt"]
profile = "minimal"
```

- [ ] **Step 4: Add minimal package manifests and entry points**

Each package inherits workspace package metadata and lints. Library entry points contain crate-level documentation only. Binary entry points return `anyhow::Result<()>` and print their package name/version. Do not add runtime behavior in this task.

```rust
fn main() -> anyhow::Result<()> {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    Ok(())
}
```

- [ ] **Step 5: Generate and verify the lockfile in the pinned container**

Run the pinned image with the repository mounted at `/workspace`, execute `cargo generate-lockfile`, then rerun `scripts/verify-toolchain.sh`.

Expected: Cargo metadata succeeds and Cargo.lock is created.

- [ ] **Step 6: Run baseline quality commands**

Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` through the same pinned container wrapper.

Expected: all commands pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .cargo crates apps scripts/verify-toolchain.sh .gitignore .dockerignore
git commit -m "build: create reproducible Rust workspace"
```

### Task 2: Domain identifiers and anime rule

**Files:**
- Modify: `crates/tmdb-domain/Cargo.toml`
- Create: `crates/tmdb-domain/src/media.rs`
- Create: `crates/tmdb-domain/src/anime.rs`
- Modify: `crates/tmdb-domain/src/lib.rs`
- Test: `crates/tmdb-domain/tests/anime_classification.rs`

**Interfaces:**
- Produces: `MediaType::{Movie,Tv}` and `TitleKey::new(MediaType, NonZeroU32)`.
- Produces: `classify_anime(&BTreeSet<u32>, Option<AnimeOverride>) -> AnimeDecision`.
- Produces constants `ANIME_KEYWORD_ID=210024` and `ANIME_RULE_VERSION="anime-keyword-210024-v1"`.

- [ ] **Step 1: Write classification tests**

```rust
use std::collections::BTreeSet;
use tmdb_domain::{
    classify_anime, AnimeOverride, AnimeSource, MediaType, TitleKey,
    ANIME_KEYWORD_ID, ANIME_RULE_VERSION,
};

#[test]
fn keyword_classifies_movie_and_tv_as_anime() -> Result<(), Box<dyn std::error::Error>> {
    let tmdb_id = std::num::NonZeroU32::new(1).ok_or("fixture ID must be non-zero")?;
    for media_type in [MediaType::Movie, MediaType::Tv] {
        let key = TitleKey::new(media_type, tmdb_id);
        let decision = classify_anime(&BTreeSet::from([ANIME_KEYWORD_ID]), None);
        assert!(decision.is_anime);
        assert_eq!(decision.source, AnimeSource::TmdbKeyword);
        assert_eq!(decision.rule_version, ANIME_RULE_VERSION);
        assert_eq!(key.tmdb_id().get(), 1);
    }
    Ok(())
}

#[test]
fn administrator_override_has_precedence_and_keeps_reason() -> Result<(), Box<dyn std::error::Error>> {
    let decision = classify_anime(
        &BTreeSet::from([ANIME_KEYWORD_ID]),
        Some(AnimeOverride::try_new(false, "live action")?),
    );
    assert!(!decision.is_anime);
    assert_eq!(decision.source, AnimeSource::AdministratorOverride);
    assert_eq!(decision.reason.as_deref(), Some("live action"));
    Ok(())
}

#[test]
fn empty_override_reason_is_rejected() {
    assert!(AnimeOverride::try_new(true, "   ").is_err());
}
```

- [ ] **Step 2: Verify tests fail**

Run: `cargo test -p tmdb-domain --test anime_classification --locked`

Expected: compilation fails because the domain types are absent.

- [ ] **Step 3: Implement media identity**

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    Movie,
    Tv,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TitleKey {
    media_type: MediaType,
    tmdb_id: std::num::NonZeroU32,
}
```

Implement `Display` and `FromStr` for MediaType, accepting exactly `movie` and `tv`. Implement immutable TitleKey accessors.

- [ ] **Step 4: Implement the pure anime decision**

Override precedence is absolute. Without an override, keyword 210024 yields true; otherwise false. AnimeDecision records is_anime, source, rule_version, evidence_keyword_ids, and optional non-empty trimmed reason.

- [ ] **Step 5: Run domain tests and lints**

Run domain tests, then workspace clippy. Expected: all pass with no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/tmdb-domain
git commit -m "feat: define media identity and anime classification"
```

### Task 3: Secret-file and path-safe configuration

**Files:**
- Modify: `crates/tmdb-config/Cargo.toml`
- Create: `crates/tmdb-config/src/source.rs`
- Create: `crates/tmdb-config/src/secret.rs`
- Create: `crates/tmdb-config/src/path.rs`
- Create: `crates/tmdb-config/src/settings.rs`
- Modify: `crates/tmdb-config/src/lib.rs`
- Test: `crates/tmdb-config/tests/settings.rs`

**Interfaces:**
- Produces: `ConfigSource`, `EnvSource`, and test-only `MapSource`.
- Produces: `load_secret(source, name) -> Result<SecretString, ConfigError>`.
- Produces: `StorageRoots::try_new(work, images, raw_archive, backups)`.
- Produces: `AppConfig::load(&impl ConfigSource) -> Result<AppConfig, ConfigError>`.

- [ ] **Step 1: Write configuration tests**

```rust
#[test]
fn secret_requires_exactly_one_source() {
    let source = MapSource::from([
        ("TMDB_DB_PASSWORD", "visible"),
        ("TMDB_DB_PASSWORD_FILE", "/run/secrets/db"),
    ]);
    assert!(matches!(
        load_secret(&source, "TMDB_DB_PASSWORD"),
        Err(ConfigError::ConflictingSecretSources(_))
    ));
}

#[test]
fn storage_roots_must_be_absolute_distinct_and_non_root() {
    assert!(StorageRoots::try_new("/", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work", "/work/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work", "/images", "/raw", "/backups").is_ok());
}
```

Add a temp-file test proving one CRLF is removed, empty/NUL secrets fail, and Debug output contains `[REDACTED]` without the secret.

- [ ] **Step 2: Verify tests fail**

Run the tmdb-config tests. Expected: missing types/functions.

- [ ] **Step 3: Implement deterministic configuration sources**

`ConfigSource::get(&self, key: &str) -> Option<OsString>` is the only input boundary. EnvSource delegates to `std::env::var_os`; tests use an immutable BTreeMap and never mutate process-wide environment variables.

- [ ] **Step 4: Implement secret loading and redaction**

If both NAME and NAME_FILE exist, return ConflictingSecretSources. If neither exists, return Missing. Read at most 64 KiB, remove one trailing LF and optional CR, reject empty/NUL values, and return `SecretString`. Production rejects direct-value secrets.

- [ ] **Step 5: Implement lexical storage-root validation**

Require absolute normalized paths. Reject filesystem roots, parent traversal, equality, and ancestor/descendant overlap among work, images, raw archive, and backups. The validator never creates or deletes paths.

- [ ] **Step 6: Implement AppConfig and verify**

AppConfig contains environment, bind addresses, direct/pooled database settings, storage roots, and optional Trawl base URL. Development/test still rejects known example passwords. Run focused tests and full workspace gates.

- [ ] **Step 7: Commit**

```bash
git add crates/tmdb-config
git commit -m "feat: load secrets and validate storage configuration"
```

### Task 4: Verified PostgreSQL 18 development cluster

**Files:**
- Create: `deploy/compose.dev.yaml`
- Create: `deploy/env.example`
- Create: `deploy/secrets/README.md`
- Create: `scripts/bootstrap-dev.sh`
- Create: `scripts/verify-postgres.sh`
- Create: `infra/postgres/initdb/10-bootstrap.sh`
- Modify: `.gitignore`

**Interfaces:**
- Produces Compose service `postgres` on an internal network and loopback port 55432 for development only.
- Produces database `tmdb` and roles migrator, api_reader, api_job_submitter, ingest_writer, image_writer, and monitor.
- Produces generated files under `deploy/secrets/`, never tracked.

- [ ] **Step 1: Write the failing PostgreSQL verification**

```bash
#!/usr/bin/env bash
set -Eeuo pipefail
compose='deploy/compose.dev.yaml'
version="$(docker compose -f "$compose" exec -T postgres psql -U tmdb_owner -d tmdb -Atc 'SHOW server_version')"
[[ "$version" == 18.4* ]] || { printf 'Expected PostgreSQL 18.4, got %s\n' "$version" >&2; exit 1; }
checksums="$(docker compose -f "$compose" exec -T postgres psql -U tmdb_owner -d tmdb -Atc 'SHOW data_checksums')"
[[ "$checksums" == on ]] || { printf '%s\n' 'Data checksums are not enabled' >&2; exit 1; }
extensions="$(docker compose -f "$compose" exec -T postgres psql -U tmdb_owner -d tmdb -Atc "SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension WHERE extname IN ('pg_stat_statements','pg_trgm','unaccent')")"
[[ "$extensions" == pg_stat_statements,pg_trgm,unaccent ]] || { printf 'Missing extensions: %s\n' "$extensions" >&2; exit 1; }
```

- [ ] **Step 2: Observe failure**

Run bootstrap and verification. Expected: failure because Compose and the secret generator do not exist.

- [ ] **Step 3: Implement cryptographic development-secret generation**

Create each missing secret with `RandomNumberGenerator.GetBytes(32)` and base64url encoding. Never print values. Refuse overwrite unless `-Rotate` is passed. Generate owner, migrator, api reader, job submitter, ingest writer, image writer, and monitor passwords.

- [ ] **Step 4: Implement PostgreSQL Compose service**

```yaml
services:
  postgres:
    image: postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296
    environment:
      POSTGRES_DB: tmdb
      POSTGRES_USER: tmdb_owner
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_owner_password
      POSTGRES_INITDB_ARGS: --data-checksums --encoding=UTF8
      PGDATA: /var/lib/postgresql/18/docker
    command: [postgres, -c, shared_preload_libraries=pg_stat_statements, -c, track_io_timing=on, -c, timezone=UTC]
    ports: ["127.0.0.1:55432:5432"]
    volumes:
      - tmdb_pg18_data:/var/lib/postgresql
      - ../infra/postgres/initdb:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: [CMD-SHELL, pg_isready -U tmdb_owner -d tmdb]
      interval: 2s
      timeout: 3s
      retries: 30
    networks: [tmdb-internal]
```

Declare all file secrets, the named volume, and the internal bridge network. Mount each secret only where needed.

- [ ] **Step 5: Implement idempotent first-cluster bootstrap**

Read secret files; create fixed-name NOINHERIT SCRAM login roles; create pg_stat_statements, pg_trgm, and unaccent as owner; revoke CREATE on public from PUBLIC; grant CREATE on database tmdb only to migrator; and grant CONNECT only to named roles. Application schemas are created by the versioned migration in Task 5, not by an unversioned init script. Use psql variable literal quoting, never string-concatenate secrets.

- [ ] **Step 6: Start a new test volume and verify**

Use Compose project `tmdb_rust_foundation_test`. Confirm the exact volume name before removal. Verify PostgreSQL 18.4, checksums on, extensions, UTF8, UTC, SCRAM, and data directory `/var/lib/postgresql/18/docker`.

- [ ] **Step 7: Verify restart persistence and commit**

Create a sentinel row, restart PostgreSQL, verify it remains, then commit.

```bash
git add deploy infra/postgres scripts/bootstrap-dev.sh scripts/verify-postgres.sh .gitignore
git commit -m "feat: add verified PostgreSQL 18 development cluster"
```

### Task 5: Migration runner, readiness metadata, and role isolation

**Files:**
- Modify: `crates/tmdb-db/Cargo.toml`
- Modify: `apps/tmdb-admin/Cargo.toml`
- Create: `crates/tmdb-db/src/options.rs`
- Create: `crates/tmdb-db/src/pool.rs`
- Create: `crates/tmdb-db/src/migrate.rs`
- Create: `crates/tmdb-db/src/readiness.rs`
- Create: `crates/tmdb-db/migrations/0001_foundation.sql`
- Test: `crates/tmdb-db/tests/foundation.rs`
- Modify: `apps/tmdb-admin/src/main.rs`

**Interfaces:**
- Produces: `connect_direct(&DatabaseConfig, PoolPolicy) -> Result<PgPool, DbError>`.
- Produces: `migrate(&PgPool) -> Result<MigrationReport, DbError>`.
- Produces: `readiness(&PgPool) -> Result<ReadinessReport, DbError>`.
- Produces CLI commands `tmdb-admin migrate` and `tmdb-admin doctor --json`.

- [ ] **Step 1: Write failing integration tests**

```rust
#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn foundation_migration_installs_expected_schemas(pool: PgPool) -> sqlx::Result<()> {
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT schema_name FROM information_schema.schemata
         WHERE schema_name = ANY($1) ORDER BY schema_name"
    )
    .bind(vec!["assets", "auth", "catalog", "ops", "search", "source"])
    .fetch_all(&pool).await?;
    assert_eq!(schemas, vec!["assets", "auth", "catalog", "ops", "search", "source"]);
    Ok(())
}
```

Add role tests proving api_reader and api_job_submitter cannot directly insert into ops.service_metadata.

SQLx test database creation uses a generated test-only owner connection through DATABASE_URL because the test macro requires CREATEDB. Service binaries and ordinary migration commands never receive that owner credential.

- [ ] **Step 2: Verify tests fail**

Expected: migration constant, tables, options, and helpers are missing.

- [ ] **Step 3: Implement connection options without URL logging**

Build `PgConnectOptions` field-by-field. Set application_name, statement timeout, lock timeout, UTC, and read-only session options according to PoolPolicy. Debug output must omit passwords.

- [ ] **Step 4: Create foundation migration**

Create:

- catalog, source, ops, search, assets, and auth schemas with migrator ownership.
- ops.service_metadata(key text primary key, value jsonb, updated_at timestamptz).
- ops.job_type_registry(job_type text primary key, payload_version positive integer, enabled boolean, created_at timestamptz).
- source.ingest_runs with UUID, checked run type/status, watermark/count JSON, and timestamps.
- auth.api_keys with UUID, unique identifier, HMAC digest bytea, owner, scopes, expiry/revocation, and timestamps.
- ops.readiness view exposing schema revision and migration time only.
- Default privileges and explicit grants for all service roles.

Seed `system.noop` version 1 and the schema revision. Add check constraints for every status domain.

- [ ] **Step 5: Implement migrations/readiness**

```rust
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, serde::Serialize)]
pub struct ReadinessReport {
    pub postgres_major: u16,
    pub schema_revision: String,
    pub extensions: Vec<String>,
}
```

Readiness verifies PostgreSQL major 18, migrations, required extensions, and one read-only round trip.

- [ ] **Step 6: Implement admin commands and permission matrix**

`migrate` requires migrator. `doctor --json` uses api_reader and emits no secrets. Prove api_reader cannot write; job submitter cannot directly write; ingest/image roles cannot create roles/schemas/extensions or modify the other worker's schema; monitor reads only approved status.

- [ ] **Step 7: Run migrations twice and commit**

Expected: first applies, second performs no work and leaves identical objects.

```bash
git add crates/tmdb-db apps/tmdb-admin
git commit -m "feat: add PostgreSQL migration and readiness foundation"
```

### Task 6: Durable leased job queue

**Files:**
- Modify: `crates/tmdb-jobs/Cargo.toml`
- Create: `crates/tmdb-db/migrations/0002_jobs.sql`
- Create: `crates/tmdb-jobs/src/model.rs`
- Create: `crates/tmdb-jobs/src/repository.rs`
- Create: `crates/tmdb-jobs/src/error.rs`
- Modify: `crates/tmdb-jobs/src/lib.rs`
- Test: `crates/tmdb-jobs/tests/postgres_jobs.rs`

**Interfaces:**
- Produces: `JobRepository::submit, claim, heartbeat, complete, fail, request_cancel, get`.
- Produces: `NewJob`, `ClaimedJob`, `JobId`, `WorkerId`, `JobStatus`, `SubmitOutcome`, and `FailureDisposition`.
- Produces: `NewJob::noop(dedup_key: &str) -> Result<NewJob, ValidationError>`.
- Consumes: `tmdb_db::connect_direct`.

- [ ] **Step 1: Write durable-job integration tests**

Prove active deduplication returns the original ID; eight claimers never duplicate; expired leases reclaim and increment attempts; wrong workers cannot mutate claims; retry scheduling is exact; exhausted attempts dead-letter; cancellation prevents claim; pool recreation loses nothing; and every transition writes an immutable event.

```rust
#[sqlx::test(migrations = "../tmdb-db/migrations")]
async fn duplicate_active_dedup_key_returns_original(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = JobRepository::new(pool);
    let first = repo.submit(NewJob::noop("same-key")?).await?;
    let second = repo.submit(NewJob::noop("same-key")?).await?;
    assert_eq!(first.job_id(), second.job_id());
    assert!(second.was_duplicate());
    Ok(())
}
```

- [ ] **Step 2: Verify tests fail**

Expected: schema and repository absent.

- [ ] **Step 3: Create jobs/events schema**

Create ops.jobs with UUID, registered type/version, JSONB payload, priority, checked status, attempts/max, availability, lease owner/expiry, dedup key, cancellation flag, sanitized result/error, and timestamps. Add active-dedup, claim, and lease-expiry indexes. Create immutable ops.job_events. Add security-definer submit/cancel functions with fixed search_path and registry validation.

- [ ] **Step 4: Implement atomic claim**

```sql
WITH candidate AS (
    SELECT id
    FROM ops.jobs
    WHERE (status IN ('queued', 'retry_wait') AND available_at <= clock_timestamp())
       OR (status = 'running' AND lease_expires_at <= clock_timestamp())
    ORDER BY priority DESC, available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE ops.jobs AS job
SET status = 'running',
    lease_owner = $1,
    lease_expires_at = clock_timestamp() + $2::interval,
    attempts = attempts + 1,
    updated_at = clock_timestamp()
FROM candidate
WHERE job.id = candidate.id
RETURNING job.*;
```

Every mutation checks job ID plus lease owner where applicable and appends its event in the same transaction.

- [ ] **Step 5: Implement typed repository and limits**

Reject empty/oversized worker IDs, dedup keys, job types, and messages. Bound JSON sizes. Distinguish LeaseLost from NotFound.

- [ ] **Step 6: Run race/restart tests**

Run the job integration binary 25 times. Restart PostgreSQL with queued/running fixtures and prove queued persistence plus expired-lease reclamation.

- [ ] **Step 7: Commit**

```bash
git add crates/tmdb-db/migrations/0002_jobs.sql crates/tmdb-jobs
git commit -m "feat: add durable leased PostgreSQL jobs"
```

### Task 7: Structured telemetry and truthful health endpoints

**Files:**
- Modify: `crates/tmdb-observability/Cargo.toml`
- Modify: `apps/tmdb-api/Cargo.toml`
- Create: `crates/tmdb-observability/src/logging.rs`
- Create: `crates/tmdb-observability/src/metrics.rs`
- Modify: `crates/tmdb-observability/src/lib.rs`
- Create: `apps/tmdb-api/src/app.rs`
- Create: `apps/tmdb-api/src/health.rs`
- Create: `apps/tmdb-api/src/problem.rs`
- Modify: `apps/tmdb-api/src/main.rs`
- Test: `apps/tmdb-api/tests/health.rs`

**Interfaces:**
- Produces: `build_router(ApiState) -> Router`.
- Produces: `ReadinessProbe` async trait returning `ReadinessReport`.
- Produces: `init_tracing(service_name, LogFormat)` and `Metrics::new()`.

- [ ] **Step 1: Write router tests**

```rust
#[tokio::test]
async fn liveness_does_not_depend_on_database() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_router(FakeProbe::failing("database unavailable"));
    let request = Request::get("/health/live").body(Body::empty())?;
    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn readiness_is_503_when_database_is_unready() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_router(FakeProbe::failing("database unavailable"));
    let request = Request::get("/health/ready").body(Body::empty())?;
    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("application/problem+json")
    );
    Ok(())
}
```

- [ ] **Step 2: Verify tests fail**

Expected: router, state, problem response, and probe absent.

- [ ] **Step 3: Implement endpoints/middleware**

Liveness returns service/version/status without dependencies. Readiness invokes the probe under 750 ms and returns a sanitized Problem Details response on failure. Add request IDs, sensitive-header marking, tracing, panic conversion, zstd compression, and total timeout.

- [ ] **Step 4: Add structured logs and metrics**

JSON is production default; compact text is development-only. Register HTTP count/duration, readiness failures, pool acquisition duration, and build info. Metrics use a separately configured private listener.

- [ ] **Step 5: Implement graceful shutdown**

Handle Ctrl-C/SIGTERM with CancellationToken and Axum graceful shutdown under a bounded drain deadline.

- [ ] **Step 6: Test actual outage/recovery**

Healthy database gives ready=200. Stop PostgreSQL: live remains 200, ready becomes 503. Restart PostgreSQL: ready returns to 200 without API restart.

- [ ] **Step 7: Commit**

```bash
git add crates/tmdb-observability apps/tmdb-api
git commit -m "feat: add telemetry and truthful health endpoints"
```

### Task 8: Worker lifecycle and administrative doctor

**Files:**
- Modify: `crates/tmdb-jobs/Cargo.toml`
- Modify: `apps/tmdb-ingest/Cargo.toml`
- Modify: `apps/tmdb-images/Cargo.toml`
- Modify: `apps/tmdb-admin/Cargo.toml`
- Create: `crates/tmdb-jobs/src/worker.rs`
- Create: `apps/tmdb-ingest/src/runtime.rs`
- Create: `apps/tmdb-images/src/runtime.rs`
- Modify binary entry points and admin CLI.
- Test: `crates/tmdb-jobs/tests/worker_runtime.rs`
- Test: `apps/tmdb-admin/tests/cli.rs`

**Interfaces:**
- Produces: `JobExecutor` async trait and `Worker::run(CancellationToken)`.
- Produces: `WorkerConfig { worker_id, lease_duration, heartbeat_interval, idle_poll_interval }`.
- Produces admin commands `doctor --json`, `submit-noop`, and `job-status`.

- [ ] **Step 1: Write worker shutdown/recovery tests**

Use NoopExecutor and BlockingExecutor. Prove cancellation stops new claims, drain behavior is bounded, heartbeats retain ownership, and killed-worker jobs reclaim after lease expiry.

- [ ] **Step 2: Verify tests fail**

Expected: worker interfaces absent.

- [ ] **Step 3: Implement bounded worker loop**

Claim one job per worker task. Use a semaphore for concurrency, heartbeat child task, cancellation-aware waits, and deterministic retry disposition. Polling is the correctness path; LISTEN/NOTIFY is optional.

- [ ] **Step 4: Implement process shells and admin commands**

Ingest and image binaries load distinct direct roles, initialize telemetry, and execute system.noop only in this phase. `submit-noop` uses the submission role/function; `job-status` uses the safe view; `doctor` checks identity/version/extensions/timeouts/grants.

- [ ] **Step 5: Run restart proof**

Submit ten noops, force-stop a worker with a lease active, restart, and verify ten successes exactly once with complete event histories.

- [ ] **Step 6: Commit**

```bash
git add crates/tmdb-jobs apps/tmdb-ingest apps/tmdb-images apps/tmdb-admin
git commit -m "feat: add restart-safe Rust worker lifecycle"
```

### Task 9: PgBouncer 1.25.2 transaction pooling

**Files:**
- Modify: `crates/tmdb-db/Cargo.toml`
- Modify: `apps/tmdb-admin/Cargo.toml`
- Create: `infra/pgbouncer/Dockerfile`
- Create: `infra/pgbouncer/pgbouncer.ini`
- Create: `infra/pgbouncer/entrypoint.sh`
- Modify: `deploy/compose.dev.yaml`
- Modify: `apps/tmdb-admin/src/main.rs`
- Test: `crates/tmdb-db/tests/pgbouncer.rs`

**Interfaces:**
- Produces Compose service `pgbouncer` at internal port 6432 and loopback 56432.
- Produces `tmdb-admin pool-smoke --clients 100 --queries-per-client 20`.
- Consumes api_reader and api_job_submitter secret files.

- [ ] **Step 1: Write failing pool smoke test**

Open 100 logical clients through PgBouncer, execute prepared `SELECT 1` and readiness queries 20 times, and record failures. A direct owner query must show no more than 40 backend sessions for tested users.

- [ ] **Step 2: Verify failure**

Expected: service and command absent.

- [ ] **Step 3: Build verified official source**

```dockerfile
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS build
ARG PGBOUNCER_VERSION=1.25.2
ARG PGBOUNCER_SHA256=924ad35113fd0a71c8e2dbe85b5d03445532e2b7b37a9f8a48983beea238b332
```

Install required build packages, download the official tarball, verify sha256sum, build with TLS, and copy into a clean pinned Debian runtime with an unprivileged user.

- [ ] **Step 4: Configure transaction pooling**

Set pool_mode=transaction, max_client_conn=500, default_pool_size=32, reserve_pool_size=8, reserve_pool_timeout=2, max_prepared_statements=100, query_wait_timeout=3, server_connect_timeout=3, server_idle_timeout=60, auth_type=scram-sha-256, and private admin/stats users.

- [ ] **Step 5: Generate tmpfs auth file**

Read only required secrets, reject newline/NUL, write userlist mode 0600 in `/run/pgbouncer`, never log values/lengths, then exec PgBouncer.

- [ ] **Step 6: Prove pooling and permissions**

Expected: 2,000 successful queries, at most 40 backend sessions, prepared statements work across transactions, api_reader writes fail, job submitter calls functions but cannot modify tables. Workers continue direct PostgreSQL connections.

- [ ] **Step 7: Commit**

```bash
git add infra/pgbouncer deploy/compose.dev.yaml apps/tmdb-admin crates/tmdb-db/tests/pgbouncer.rs
git commit -m "feat: add verified PgBouncer transaction pool"
```

### Task 10: Read-only legacy PostgreSQL audit

**Files:**
- Modify: `crates/tmdb-db/Cargo.toml`
- Modify: `apps/tmdb-admin/Cargo.toml`
- Create: `crates/tmdb-db/src/legacy.rs`
- Create: `apps/tmdb-admin/src/legacy_audit.rs`
- Modify: `apps/tmdb-admin/src/main.rs`
- Test: `crates/tmdb-db/tests/legacy_read_only.rs`
- Test: `apps/tmdb-admin/tests/legacy_audit.rs`

**Interfaces:**
- Produces: `connect_legacy_read_only(&LegacyDatabaseConfig) -> Result<PgPool, DbError>`.
- Produces LegacyAuditReport with version, bytes, fixed counts, constraints, indexes, extensions, timestamp, and count checksum.
- Produces `tmdb-admin legacy-audit --json`.

- [ ] **Step 1: Write read-only enforcement tests**

Create a SELECT-only fixture role. Verify default_transaction_read_only=on, application_name `tmdb-rust-legacy-audit`, statement timeout at most 10 seconds, lock timeout at most 1 second, and INSERT/UPDATE/DDL all fail.

- [ ] **Step 2: Verify tests fail**

Expected: module and command absent.

- [ ] **Step 3: Implement fixed queries and redacted report**

No query accepts a user-provided relation/identifier/SQL fragment. Use an internal known-table allowlist plus pg_database_size, pg_class, pg_index, pg_constraint, and pg_extension. Exclude host, port, user, URL, and SQL details from output. Include report schema version and SHA-256 of sorted count payload.

- [ ] **Step 4: Run opt-in live audit safely**

Place the legacy password in a temporary secret file outside the repository. Audit the explicitly supplied legacy database host and port, compare version/size/core counts, delete the temporary secret, and run no migration/write command.

- [ ] **Step 5: Commit**

```bash
git add crates/tmdb-db/src/legacy.rs crates/tmdb-db/tests/legacy_read_only.rs apps/tmdb-admin
git commit -m "feat: add read-only legacy database audit"
```

### Task 11: Clean-volume foundation acceptance and CI

**Files:**
- Create: `scripts/verify-foundation.sh`
- Create: `docs/development.md`
- Create: `.github/workflows/rust-foundation.yml`
- Modify: `README.md`

**Interfaces:**
- Produces one local command recreating phase 1 from an empty, uniquely named test Compose project.
- Produces CI executing the same logical gates without live/TMDB/Trawl credentials.

- [ ] **Step 1: Write acceptance script before wiring commands**

The script must:

1. Confirm Docker server is reachable.
2. Confirm project name equals `tmdb_rust_foundation_test`.
3. Generate isolated test secrets.
4. Start PostgreSQL/PgBouncer from an empty named test volume.
5. Run migrations twice.
6. Run fmt, clippy, unit, PostgreSQL integration, and API contract tests.
7. Run doctor, 100-client pool smoke, and noop restart recovery.
8. Stop PostgreSQL and verify API live=200/ready=503.
9. Restart PostgreSQL and verify ready=200.
10. Print pass/fail without secrets.
11. Remove only resources labeled with the exact test project when `-Clean` is supplied.

- [ ] **Step 2: Run incomplete script and observe failure**

The initial script ends immediately after the Docker/project-name guards with:

```bash
printf '%s\n' 'FOUNDATION_GATE_UNWIRED: bootstrap-dev-secrets' >&2
exit 1
```

Run it once and require that exact failure before replacing the throw in Step 3.

- [ ] **Step 3: Wire verification using condition polling**

Never use fixed sleeps. On failure collect Compose status and the last 200 sanitized log lines; never dump environments or secret mounts.

- [ ] **Step 4: Add CI**

CI uses pinned images, ephemeral secrets, Cargo.lock cache, and:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Also run empty-cluster migrations, role tests, job races, PgBouncer smoke, and API health recovery. Never contact live services.

- [ ] **Step 5: Document workflow and execute twice**

Document bootstrap/start/migrate/doctor/test/log/restart/safe-cleanup. State the Python service is untouched and foundation alone is not production-ready. Run the clean-volume gate twice to prove creation and repeatability.

- [ ] **Step 6: Commit**

```bash
git status --short
git diff --check
git add scripts/verify-foundation.sh docs/development.md .github/workflows/rust-foundation.yml README.md
git commit -m "test: add Rust foundation acceptance gate"
```

## Phase 1 completion evidence

Do not declare this plan complete without:

- Exact rustc/cargo, PostgreSQL, PgBouncer, Docker, and Compose versions.
- Immutable image and PgBouncer source digests.
- Two consecutive clean-volume verification outputs.
- Migration list and role-permission matrix.
- Durable-job concurrency/restart results.
- API liveness/readiness outage/recovery results.
- PgBouncer logical/backend connection counts for 100 clients.
- Read-only legacy audit checksum and confirmation of zero source writes.
- Clean Git status and commit list.

After this evidence passes, write the separate ingestion implementation plan against the verified interfaces above.

## Verified implementation references

- Rust release/toolchain image: https://forge.rust-lang.org/ and https://hub.docker.com/_/rust
- Axum 0.8.9 state/routing: https://docs.rs/axum/0.8.9/axum/
- Tokio 1.53.1 runtime/tasks: https://docs.rs/tokio/1.53.1/tokio/
- SQLx 0.9.0 pools, migrations, and real-database tests: https://docs.rs/sqlx/0.9.0/sqlx/
- tower-http 0.7.0 middleware: https://docs.rs/tower-http/0.7.0/tower_http/
- Clap 4.6.4 derives: https://docs.rs/clap/4.6.4/clap/
- PostgreSQL 18 release and container layout: https://www.postgresql.org/docs/18/release-18.html and https://github.com/docker-library/docs/blob/master/postgres/README.md
- PgBouncer 1.25.2 release and transaction-pooling behavior: https://github.com/pgbouncer/pgbouncer/releases/tag/pgbouncer_1_25_2 and https://www.pgbouncer.org/features.html
