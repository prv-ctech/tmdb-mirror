# TMDB Service Rust Rewrite Design

Status: approved in conversation on 2026-07-31; written specification awaiting final user review.

## 1. Purpose

Replace ghcr.io/jessielw/tmdb-service:1.1.0 with a production-grade Rust system that mirrors TMDB metadata, serves a fast read API, classifies anime separately from ordinary movies and television, and downloads image assets to user-configured storage.

The current Python deployment remains available only during the blue-green migration and rollback window. The final runtime contains no Python service.

## 2. Verified starting point

The source repository is at tag 1.1.0 and commit f700889. The existing service uses Python 3.12, synchronous psycopg2 database access, a PostgreSQL-backed administrative job API, and separate movie and series schemas.

The live database was inspected read-only. At inspection time it contained approximately:

- 1,222,730 movies.
- 227,393 television series.
- 7,635,485 movie-credit associations.
- 2.9 GiB of PostgreSQL relation data.
- 43 indexes, all primary-key B-tree indexes.
- No foreign keys, reverse facet indexes, full-text indexes, trigram indexes, partial indexes, or covering indexes.

Representative title, popularity, recent, anime-keyword, genre, and actor queries planned sequential scans over large relations. These observations establish that schema and query design, rather than programming language alone, are the primary performance constraints.

The target host has a Ryzen 3950X, 128 GiB ECC DDR4, a planned 2 TB SSD for databases, NVMe worker scratch space, and a 129 TB array for images and backups.

## 3. Goals and acceptance criteria

### 3.1 Functional goals

- Mirror movies, television, anime, people, credits, genres, keywords, tags, languages, countries, companies, studios, networks, collections, seasons, episodes, translations, alternative titles, external IDs, and image metadata as available from TMDB.
- Provide versioned read APIs for details, popular, recent, top-rated, discovery, facets, credits, and search.
- Support filters for genre, keyword, tag, media type, language, runtime, person, cast member, crew member, studio/company, network, country, year, date, status, rating, and vote count.
- Provide protected administrative APIs for synchronization, targeted refresh, reclassification, search rebuilds, image backfills, job inspection, retries, and health.
- Download primary image assets eagerly. Download galleries on demand or through controlled backfill jobs.
- Keep anime out of ordinary movie and television collection/search endpoints.

### 3.2 Performance goals

Performance is accepted only after tests on PostgreSQL 18 using production-scale data and the target storage class.

- At 100 concurrent API clients, metadata endpoints have p95 latency at or below 50 ms.
- At 100 concurrent API clients, indexed filtered search has p95 latency at or below 150 ms.
- The 100-client test has no pool exhaustion, connection errors, deadlocks, or lock-timeout responses.
- Additional 250- and 500-client tests establish headroom and graceful degradation; these are characterization tests, not launch capacity promises.
- Key list, facet, actor, anime, recent, popular, top-rated, exact-title, and fuzzy-title queries use appropriate indexes at production cardinality. A sequential scan is acceptable only when measured plans prove it is cheaper for a small or low-selectivity result.
- API queries have bounded execution time, bounded result size, and keyset pagination.

### 3.3 Reliability goals

- A worker or database restart cannot lose accepted jobs.
- Every job is idempotent or protected by a deduplication key.
- Failed jobs retry with bounded exponential backoff and end in an inspectable dead-letter state.
- Image publication is atomic; clients never see partially written files.
- The production database has checksums, tested backups, and point-in-time recovery.
- Blue-green cutover has a documented rollback path.

### 3.4 Security goals

- Users never receive direct database access.
- Read clients and administrators use separately scoped credentials.
- No endpoint accepts arbitrary SQL or unrestricted table/column names.
- Secrets are supplied through secret files or an external secret mechanism and are never committed or logged.
- Database, metrics, Trawl, and administrative surfaces remain private or explicitly access-controlled.

## 4. Delivery decomposition

The rewrite is delivered as four independently testable phases.

1. Foundation: Rust workspace, PostgreSQL 18, migrations, roles, durable jobs, observability, four-container Compose, and migration tooling.
2. Ingestion: TMDB client, exports, changes synchronization, entity normalization, anime classification, repair and validation workflows.
3. Read API and search: public/admin authentication, resource endpoints, filters, search projection, indexes, OpenAPI, and load tests.
4. Images and production hardening: asset metadata, image worker, static serving, Trawl fallback, backup/restore, failure tests, and blue-green cutover tooling.

Each phase must pass its own tests before the next becomes a production dependency. The implementation plan may split phases into smaller reviewable commits, but it must preserve these boundaries.

## 5. Runtime architecture

One Cargo workspace produces:

- tmdb-api: Axum/Tokio HTTP service. It is read-only against catalog tables and submits explicitly allowed administrative jobs when authorized.
- tmdb-ingest: scheduler and ingestion worker for full inventories, change feeds, targeted refreshes, corrections, and derived projections.
- tmdb-images: image metadata discovery, download, verification, publication, and optional variant generation.
- tmdb-admin: non-server commands for migrations, imports, validation, repairs, benchmarks, backup checks, and cutover.

Shared libraries isolate:

- Domain types and classification rules.
- Configuration and secret-file loading.
- PostgreSQL repositories and migrations.
- TMDB transport, models, rate limiting, and retry policy.
- Durable job claiming and execution.
- Search query construction and cursor encoding.
- Image storage and validation.
- Telemetry, error types, and request correlation.

Public clients reach tmdb-api directly or through an operator-selected edge proxy. The API and workers use bounded direct PostgreSQL pools; PostgreSQL MVCC provides independent concurrent reads without a mandatory pooler container.

Static images are served by the media worker's embedded read-only server from `/media`. The Rust API returns metadata and local asset URLs; it does not stream ordinary image traffic.

No separate Redis or external search engine is a launch dependency. Trawl may continue using its own Redis internally, but the TMDB system does not depend on that Redis.

## 6. PostgreSQL design

### 6.1 Cluster

- PostgreSQL 18 runs as a new cluster; the PostgreSQL 16 data directory is never reused by changing the image tag.
- The active data directory and WAL reside on the persistent 2 TB SSD or a persistent redundant NVMe pool.
- Worker scratch files reside on the NVMe cache.
- Images, compressed source archives, and backup repositories reside on the large array.
- Host paths are explicit environment settings. The application rejects empty, root, or unresolved storage paths.
- Data checksums are enabled at cluster creation.
- Required extensions at launch are pg_stat_statements, unaccent, and pg_trgm. The amcheck extension is enabled for maintenance validation when the selected PostgreSQL image includes it.
- An external BM25 or CJK search extension is not installed until a repeatable benchmark proves a material quality or latency benefit and recovery procedures pass.

Baseline PostgreSQL settings are treated as measured starting values, not unquestioned constants. The initial dedicated-host profile uses approximately 32 GiB shared buffers, 80-96 GiB effective cache size, conservative per-query work memory, WAL compression, I/O timing, and SSD-appropriate planner/I/O settings. The implementation must record the exact tested profile and change it only from benchmark and production evidence.

### 6.2 Roles and connections

- migrator owns schema changes and is unavailable to normal services.
- api_reader can select approved catalog objects and execute approved read functions only. Its default transaction mode is read-only.
- api_job_submitter can execute narrowly scoped job-submission/cancellation functions and read administrative job status. It cannot update catalog tables or execute arbitrary SQL.
- ingest_writer can update catalog, source, classification, and job records but cannot manage roles or extensions.
- image_writer can update image/job state and cannot alter unrelated catalog data.
- backup and monitoring roles receive only the permissions required by their tools.

The Rust API uses separate bounded credentials/pools for api_reader and api_job_submitter; public handlers cannot access the submission pool. PostgreSQL sees far fewer sessions than HTTP clients. Initial pool sizes are selected through the 100/250/500-client tests rather than equating one user with one backend.

API connections receive statement, lock, and idle-in-transaction timeouts. Schema migrations use a separate role and explicit short lock timeouts. Destructive or blocking migrations do not run during live traffic.

### 6.3 Core model

The model uses shared entities instead of duplicated movie/series lookup tables.

- titles: internal bigint primary key, media type, TMDB ID, names, synopsis, status, dates, popularity, votes, runtime summaries, adult/video flags, source timestamps, active state, and derived classification flags. The pair of media type and TMDB ID is unique.
- movie_details and tv_details: fields that apply only to one media type.
- seasons and episodes: TMDB identity, numbering, dates, runtime, vote data, summaries, and relationships to a television title.
- people: one row per TMDB person.
- credits: an edge between a title or episode and a person. Character, cast order, department, job, episode count, and credit identifiers belong on this edge, never on the person.
- genres, keywords, tags, companies, networks, collections, languages, and countries: shared canonical dimensions.
- title_genres, title_keywords, title_tags, title_companies, title_networks, title_languages, title_countries, and collection membership: constrained relationship tables.
- translations and alternative_titles: locale-aware discoverability and display metadata.
- external_ids: provider/type/value mappings with uniqueness rules appropriate to each provider.
- source_manifests: checksums, fetch times, entity identity, archive path, response metadata, and parser/schema version for compressed raw-source archives.
- ingest_runs: status, watermarks, counts, validation results, and timestamps for every bulk or incremental run.

Every relationship has foreign keys. Uniqueness and check constraints prevent duplicate or invalid state. Deletion behavior is explicit: source entities are normally marked inactive first and physically pruned only after a successful authoritative run and retention period.

## 7. Anime classification contract

Anime is a first-class category spanning movies and television.

- TMDB keyword ID 210024 is the default positive rule.
- A versioned classification row records the result, rule version, evidence, source, calculation time, and optional administrator override.
- Raw TMDB keywords are never modified to force a classification.
- An administrator override can set anime or non-anime and include a reason. Overrides survive re-ingestion until explicitly removed.
- Classification refresh is idempotent and can be run for one title or the complete catalog.

Public behavior is strict:

- GET /v1/anime returns both movie and television anime when media_type is omitted.
- media_type=movie or media_type=tv restricts the result.
- GET /v1/anime/{media_type}/{tmdb_id} returns anime detail.
- Ordinary movie and television collection/search endpoints always filter is_anime=false.
- Ordinary detail endpoints do not expose anime records; an anime title is retrieved through its anime detail route.
- General GET /v1/search always excludes anime. Anime search is performed through GET /v1/anime?q=... so anime cannot leak into an ordinary search by an omitted or malformed category parameter.

The acceptance fixture for One Piece must demonstrate mixed anime movie/TV results, correct media-type filtering, exclusion from ordinary routes, exclusion of the live-action series unless explicitly overridden, and a documented correction path for TMDB records missing the keyword.

## 8. Index and search design

### 8.1 Relational indexes

Each public query shape has a matching index and an explain-plan test.

- Relationship tables have both title-first uniqueness and reverse indexes such as genre-to-title, keyword-to-title, person-to-title, company-to-title, network-to-title, language-to-title, and tag-to-title.
- Popular, recent, top-rated, date, language, runtime, and status lists use composite indexes whose leading columns match anime/media-type isolation and whose final stable key is the title ID.
- Top-rated lists use a versioned Bayesian weighted-rating projection with separate movie/TV catalog priors and a documented minimum-vote policy, preventing titles with only a few perfect votes from dominating.
- Frequently used active/non-anime/anime subsets use partial indexes only when production statistics show meaningful selectivity.
- External IDs and TMDB identities have unique indexes.
- Covering columns are added only where measured heap access justifies their write and storage cost.
- Extended statistics are added for correlated filters only after planner misestimation is demonstrated.

Main and operational tables are not partitioned in the first production cut. Job, audit, and source-history growth is measured; partitioning is introduced only through a tested migration after table size or vacuum/query evidence demonstrates a need.

### 8.2 Search projection

A denormalized search_documents projection contains one row per title/locale plus a locale-neutral fallback. It includes normalized names, original names, alternative titles, genres, keywords, companies, networks, overview text, and a bounded set of top-billed people/characters. Complete actor/crew filtering uses the indexed credits relationships instead of an unbounded text document.

Search uses three candidate paths:

1. Exact and prefix matches on normalized names and aliases.
2. Weighted PostgreSQL full-text search using a maintained tsvector and GIN.
3. pg_trgm candidates for misspellings and substring fallback.

Candidate sets are combined and deduplicated before final scoring. Exact title matches rank first; then prefix, text relevance, trigram similarity, and a deliberately small popularity/vote prior. User text is parsed with safe PostgreSQL search functions and never interpolated into SQL.

Multilingual title lookup relies on normalized aliases and trigram matching even where language stemming is unavailable. Search quality for CJK overview/body text is benchmarked separately. The repository exposes a search-provider boundary so a later extension can replace candidate generation without changing API contracts.

All result lists use opaque cursor pagination tied to the chosen sort and filters. Deep OFFSET pagination is not supported. Maximum page size is fixed and enforced.

## 9. HTTP API contract

All routes are under /v1. Responses use a stable JSON envelope containing data and request/page metadata. Errors use Problem Details JSON. OpenAPI is generated from the implementation and checked as a versioned artifact.

### 9.1 System routes

- GET /health/live
- GET /health/ready
- GET /metrics on a private listener or protected route

Readiness verifies database access, required migrations, and required projections. Liveness does not claim that dependencies are healthy.

### 9.2 Catalog routes

Movies:

- GET /movies
- GET /movies/{tmdb_id}
- GET /movies/popular
- GET /movies/recent
- GET /movies/top-rated

Television:

- GET /tv
- GET /tv/{tmdb_id}
- GET /tv/popular
- GET /tv/recent
- GET /tv/top-rated
- GET /tv/{tmdb_id}/seasons
- GET /tv/{tmdb_id}/seasons/{season_number}
- GET /tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}

Anime:

- GET /anime
- GET /anime/{media_type}/{tmdb_id}
- GET /anime/popular
- GET /anime/recent
- GET /anime/top-rated

Search and discovery:

- GET /search for ordinary non-anime movie/TV search.
- GET /anime with q for anime search.
- GET /genres
- GET /languages
- GET /keywords
- GET /tags
- GET /people
- GET /companies
- GET /networks
- GET /collections

Resource subpaths expose credits, images, external IDs, translations, and related entities without returning an unbounded graph in one response. An allowlisted include parameter may embed small summary resources.

Filters use stable identifiers where possible. Text aliases can be resolved by facet endpoints. Sort values are allowlisted. Runtime semantics distinguish movie runtime from television episode runtime. Language semantics distinguish original language, spoken language, and response locale.

### 9.3 Authentication and administration

API keys are generated from at least 256 bits of randomness and stored as an HMAC-SHA-256 digest with a separate identifier, owner, scopes, creation/expiry timestamps, and revocation state. The HMAC key is loaded from a secret file and comparisons are constant-time.

- Read keys can search and retrieve catalog data.
- Administrator keys can submit and inspect approved operations.
- Authentication can be disabled only through an explicit development-only setting that refuses to start in production mode.

Administrative routes include:

- Job list, detail, retry, and cancel.
- Full inventory, changes sync, missing-data repair, and prune submission.
- Targeted movie, TV, episode, person, collection, company, and network refresh.
- Anime override creation/removal and classification rebuild.
- Search projection/index rebuild and validation.
- Primary/gallery image backfill and failed-image retry.
- Ingestion, migration, database, and worker status summaries.

Administrative requests are audited. They submit durable work and return a job identifier rather than holding an HTTP request open for a long task.

## 10. Concurrency and caching

Tokio schedules independent HTTP requests concurrently. PostgreSQL MVCC permits reads to proceed concurrently with other reads and ordinary ingestion writes. No user owns a database session for the duration of an application session.

If more requests arrive than pool slots, they wait in a short bounded acquisition queue. The API has explicit pool-acquire, query, and total-request deadlines and returns a controlled overload response instead of hanging.

The first launch uses:

- Indexed PostgreSQL queries and small read models.
- HTTP ETag and Cache-Control headers.
- Compression for eligible JSON responses.
- Optional reverse-proxy caching for explicitly public, non-personalized endpoints.

No application Redis cache is added until metrics show PostgreSQL or serialization is the limiting resource. Cache correctness must never depend on manual invalidation.

## 11. Durable jobs and ingestion

The job table records type, versioned payload, priority, availability, state, attempts, maximum attempts, lease owner, lease expiry, deduplication key, timestamps, result summary, and sanitized error information.

Workers claim jobs in short transactions using FOR UPDATE SKIP LOCKED. A running job has a renewable lease. If a worker dies, the lease expires and another worker can reclaim it. LISTEN/NOTIFY is only a wake-up optimization; polling and persisted state are the source of truth.

The queue is never dropped on startup. Jobs transition to succeeded, cancelled, or dead-letter states. The first production cut performs no automatic job-history deletion; any future retention job must export an audit summary, support a dry run, and never remove unresolved dead letters.

TMDB ingestion uses:

- Daily ID exports for authoritative entity inventories where available.
- Change feeds for incremental synchronization within TMDB's supported window.
- Targeted refresh jobs for corrections and on-demand misses.
- Conditional/batched requests and append-to-response where documented and beneficial.
- A shared token-bucket limiter with a conservative default of 35 requests/second, configurable downward and never raised above TMDB's then-current documented guidance without a verified test.
- Retry-After handling for 429 responses.
- Bounded retries with jitter for transient network and server failures.
- No retry for permanent validation, authentication, or not-found outcomes unless a policy explicitly maps them.

Bulk sweeps upsert into the normalized schema and record a run watermark. Missing records are marked only after the inventory completes and validates. Destructive table-renaming promotions are not used, so indexes and constraints cannot silently disappear.

Production archives compressed raw responses on the array with content checksums and manifest rows. Identical payloads are content-deduplicated; changed payload versions and the current version are retained without automatic deletion in the first production cut. Parser versions allow deterministic reprocessing without another TMDB request.

## 12. Image system

The selected primary paths returned by each entity's detail payload are queued eagerly after ingestion:

- Movie and TV posters and backdrops.
- Season and episode stills/posters.
- Person profiles.
- Collection posters/backdrops.
- Company, studio, network, and other logos.

Gallery assets are queued when requested or through explicitly rate-limited backfill campaigns.

Each source image and local variant has metadata for entity, kind, TMDB path, language, dimensions, format, source revision, checksum, storage state, attempts, and timestamps.

The worker:

1. Claims an idempotent image job.
2. Downloads to a unique file under the configured NVMe scratch root.
3. Enforces status, content-type, byte, redirect, and timeout policies.
4. Computes SHA-256 and validates decoding/dimensions.
5. Deduplicates identical content.
6. Publishes with an atomic rename on the destination filesystem.
7. Commits database state only after publication succeeds.

Files use deterministic sharded paths so no directory contains an excessive number of entries. The original source asset is retained. Optional local sizes/formats are generated from the verified original according to a versioned variant policy; changing that policy creates jobs rather than overwriting existing assets. Other images from each entity's gallery endpoint remain gallery assets and are not part of eager primary ingestion.

The static server uses immutable cache headers for content-addressed paths, safe MIME types, range requests, and no directory listing. Missing local assets may return the upstream URL only when an explicit configuration permits it.

## 13. Trawl fallback

TRAWL_BASE_URL points to the user's existing private service. The verified health endpoint reported a healthy three-browser pool.

Direct TMDB access is always attempted first. Trawl is eligible only for allowlisted TMDB/API/image domains after a response or body matches a tested challenge signature. It is not invoked for:

- 429 rate limiting.
- Authentication or authorization failures.
- Ordinary 403 or 404 responses without a challenge signature.
- Invalid application data.
- General retries after the configured attempt budget.

The native /scrape endpoint is used where its response form supports the requested content and diagnostics. For a binary challenge that requires a connection-bound browser fingerprint, the implementation reports a specific unsupported-fallback state unless the forward-proxy feature is explicitly enabled on the same Trawl instance. No second Trawl deployment is created.

Trawl calls have their own concurrency semaphore, timeout, circuit breaker, metrics, and sanitized logs. Authorization headers are not forwarded unless required for an allowlisted endpoint.

## 14. Deployment and storage

The canonical Compose deployment runs exactly PostgreSQL 18, the Rust API, the consolidated main worker, and the media worker. It connects to the existing Trawl endpoint rather than launching Trawl.

Production deployment uses:

- Explicit image versions and reproducible lockfiles.
- Multi-stage Rust builds and minimal non-root runtime images.
- Read-only root filesystems where possible, dropped Linux capabilities, no-new-privileges, health checks, and resource limits.
- Separate persistent mounts for PostgreSQL, worker scratch, images, raw archives, logs when file logs are enabled, and backups.
- No PostgreSQL host port by default. Temporary migration access is explicitly bound and allowlisted.
- The official PostgreSQL 18 container mounts the host database root at /var/lib/postgresql and sets PGDATA beneath that mount; it does not reuse the PostgreSQL 16 /var/lib/postgresql/data mapping.

The current deployment path contract is fixed inside the containers:

- PostgreSQL data is mounted at `/var/lib/postgresql`.
- Worker scratch, raw exports, checkpoints, and logs use `/config`.
- Permanent media uses `/media`.
- Host directories are selected only by Compose or the Unraid mount template;
  no application environment variable overrides these paths.

The application validates that scratch and permanent roots are distinct and that PostgreSQL data is not nested inside worker, log, image, or backup directories.

## 15. Blue-green migration

1. Create the new PostgreSQL 18 cluster, roles, extensions, and schema.
2. Import the PostgreSQL 16 catalog through a read-only migration connection.
3. Transform duplicated entities into shared dimensions and credits into relationship rows.
4. Backfill indexes and derived search/classification projections.
5. Compare counts, key relationships, checksums, constraints, orphan counts, duplicate counts, and sampled entity responses.
6. Run targeted TMDB repairs for source omissions or legacy-model loss.
7. Run API contract, query-plan, concurrency, restart, and restore tests.
8. Stop only the legacy Python worker, perform a final changes sync, and record the cutover watermark.
9. Switch the reverse proxy to the Rust API and run smoke tests.
10. Keep the old service and database read-only for at least 14 days after successful cutover and backup-restore verification before archival.

No migration writes to or modifies the PostgreSQL 16 source. A failed validation or cutover returns traffic to the old API/service and re-enables the old worker only after checking the synchronization watermark.

The database password and TMDB token exposed in the planning conversation must be rotated before production deployment. Tests and examples use placeholders or generated secrets.

## 16. Observability and operations

- Structured JSON logs include service, version, request/job/run IDs, duration, and outcome.
- Secret values, authorization headers, cookies, and complete upstream URLs containing credentials are redacted.
- Prometheus metrics cover HTTP latency/errors, pool waits, query classes, TMDB rate/retries, Trawl tiers/failures, queue depth/age, worker leases, ingestion progress, image bytes/states, and storage failures.
- pg_stat_statements identifies expensive or unexpectedly frequent SQL.
- pgBackRest performs continuous WAL archiving to the array backup repository, weekly full backups, and daily differential backups. At least two complete backup cycles are retained, and an automated restore into an empty cluster is tested monthly and before legacy deletion.
- Readiness reflects migration and database state.
- Dashboards and alerts focus on user-visible latency, error rate, queue age, database saturation, disk capacity, backup age, and dead letters.

Every release includes a schema version, build revision, and compatibility check. Administrative status endpoints show these without revealing secrets.

## 17. Testing strategy

The user's rule is that assumptions are replaced by tests wherever an executable check is possible.

### 17.1 Automated correctness

- Rust unit tests for domain validation, anime rules/overrides, retry decisions, cursor encoding, filter parsing, ranking components, and path safety.
- Property tests for cursor round trips, idempotency keys, path sharding, and classification invariants.
- PostgreSQL integration tests run against real PostgreSQL 18, never SQLite.
- Migration tests apply every migration from an empty cluster and upgrade from every supported released schema.
- HTTP contract tests validate success, pagination, filters, authorization, errors, ETags, and OpenAPI.
- TMDB and Trawl transports use deterministic mock servers for 200, redirects, 304, 401, 403, 404, 429, 5xx, timeouts, malformed JSON, challenge bodies, and recovery.
- Worker tests kill a worker after claim, during download, and before completion to prove lease recovery and idempotency.
- Image tests cover truncation, wrong MIME type, decompression bombs/limits, invalid dimensions, duplicate content, atomic publication, and filesystem errors.

### 17.2 Data validation

- Production-scale import tests use a snapshot or read-only extraction from the live database.
- Constraint, orphan, duplicate, row-count, relationship-count, and checksum reports are saved as artifacts.
- Known fixtures include One Piece mixed anime results, ordinary-route exclusion, live-action exclusion, multilingual titles, people with multiple characters, companies/networks, episodes, missing images, and local overrides.

### 17.3 Performance

- Explain-analyze-with-buffers tests run for every important query on production-scale cardinalities.
- Cold-cache and warm-cache results are recorded separately where practical.
- HTTP load tests cover 100, 250, and 500 concurrent clients with realistic endpoint mixes.
- A sustained ingestion-plus-read test proves that worker writes do not cause unacceptable read latency.
- Pool size, PostgreSQL memory, autovacuum, and query/index decisions are changed only from these measurements.

### 17.4 Recovery and release gates

- Restart PostgreSQL, API, and workers during staged work.
- Simulate TMDB, Trawl, DNS, and image-storage outages.
- Simulate full scratch storage and unavailable permanent image storage without risking real paths.
- Restore a backup into an empty PostgreSQL 18 cluster and compare validation reports.
- Run formatting, compiler warnings as errors, linting, tests, dependency/security auditing, container health, and smoke tests before release.

No completion claim is made from compilation alone.

## 18. Explicit non-goals for the first production cut

- No arbitrary SQL/query-builder endpoint.
- No Elasticsearch/OpenSearch cluster.
- No Redis application cache without measured need.
- No BM25 extension as the only search path.
- No download of every gallery and every TMDB size before the primary catalog is ready.
- No automatic deletion of legacy data immediately after cutover.
- No in-place upgrade of the PostgreSQL 16 volume.

## 19. Primary references

- PostgreSQL 18 release notes: https://www.postgresql.org/docs/18/release-18.html
- PostgreSQL full-text search: https://www.postgresql.org/docs/18/textsearch.html
- PostgreSQL pg_trgm: https://www.postgresql.org/docs/18/pgtrgm.html
- PostgreSQL unaccent: https://www.postgresql.org/docs/18/unaccent.html
- PostgreSQL indexes: https://www.postgresql.org/docs/18/indexes.html
- PostgreSQL pg_stat_statements: https://www.postgresql.org/docs/current/pgstatstatements.html
- PostgreSQL Docker image: https://github.com/docker-library/docs/blob/master/postgres/README.md
- PgBouncer features: https://www.pgbouncer.org/features.html
- pgBackRest user guide: https://pgbackrest.org/user-guide.html
- Axum: https://docs.rs/axum/latest/axum/
- Tokio graceful shutdown: https://tokio.rs/tokio/topics/shutdown
- SQLx: https://docs.rs/sqlx/latest/sqlx/
- TMDB daily ID exports: https://developer.themoviedb.org/docs/daily-id-exports
- TMDB content changes: https://developer.themoviedb.org/docs/tracking-content-changes
- TMDB rate limiting: https://developer.themoviedb.org/docs/rate-limiting
- TMDB image basics: https://developer.themoviedb.org/docs/image-basics
- Trawl: https://github.com/germondai/trawl
- Unraid shares: https://docs.unraid.net/unraid-os/using-unraid-to/manage-storage/shares/
