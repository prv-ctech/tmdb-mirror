# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Four-container Rust/PostgreSQL 18 deployment: PostgreSQL, API, main worker,
  and media worker.
- Local TMDB v3-compatible document, search/discover/find, session, account,
  list, favorite/watchlist, and rating routes.
- Authenticated admin API for durable catalog scans, worker controls, job
  history, on-demand media requests, statistics maintenance, and pgBackRest
  backups.
- Local-truth on-demand media requests for one to 100 active local titles,
  with bounded expansion, offline persistence, idempotency, and deterministic
  TMDB-ID paths.
- Forward schema revisions `0052` and `0053`: `0052` preserves catalog/source
  documents while replacing legacy media state; `0053` replaces serialized
  queue admission with exact non-blocking queue slots.
- Additive `local_*` media URLs while preserving upstream TMDB image fields.
- Bounded daily-export census, phased enrichment, season processing,
  `daily_sync`, `recovery`, `reconcile`, durable cron slots/watermarks, queue
  backpressure, job retention, and cancellation controls.
- PostgreSQL least-privilege roles, SQLx migrations, trigram/unaccent search,
  pg_stat_statements, WAL archiving, scheduled backups, and PITR verification.
- Linux/Bash Docker stress harness with secret-safe real-TMDB fixtures,
  media checks, worker-state checks, HTTP load, and resilience tests.
- Persistent JSONL logs for PostgreSQL, API, worker, and media under the shared
  `/config/logs` appdata root, with restart and 10 MiB size generations plus
  10-file retention per service.

### Changed

- Standardized host and container listeners on `9000` for the read API,
  `9001` for the authenticated admin API, and `9002` for media.
- Replaced the legacy Python service and custom catalog routes with the Rust
  TMDB v3-compatible surface.
- Replaced global media full/missing/audit scans, original images, derivatives,
  and local re-encoding with exact bounded TMDB CDN renditions requested only
  for locally known titles.
- Removed the anime database/API partition, PgBouncer, PowerShell scripts,
  image variants, `optimized/`, `.masters`, and obsolete compatibility paths.
- Workers now drain eligible durable PostgreSQL work on container startup;
  authenticated controls still start, pause, resume, or cancel each queue.
- Removed the unused `/config/work` storage contract. `/config/raw` remains the
  active bounded export/reconcile store.
- Removed the redundant `/config/media` staging tree. Validated image bytes now
  publish directly through a destination-local temporary file and atomic rename
  under `/media`.
- API health now gates on database/schema readiness, service startup retries
  transient PostgreSQL connections in-process, and Compose grants enough stop
  time for PostgreSQL checkpoints and bounded application shutdown.
- Repeated unchanged pending catalog schedule slots now log at `debug`; new
  submissions and state transitions remain at `info`.
- Normal API SIGTERM shutdown now prioritizes the cancellation signal when a
  listener finishes in the same scheduler turn, avoiding a false listener
  failure and restart log.
- Admin status now includes a bounded, sanitized `activeCatalogWork` projection.
  Competing catalog scans return a machine-readable
  `catalog_maintenance_active` conflict, and the canonical request log records
  the same outcome instead of a generic error.
- Compose now caps Docker `json-file` output at three 10 MiB files per
  container so each container's Docker/Unraid log storage remains bounded.

### Fixed

- Replaced the generic shared-network database hostname `postgres` with the
  product-unique `tmdb-mirror-postgres`, preventing cross-project DNS alias
  collisions from causing intermittent internal-role authentication failures.
- Catalog phase contention, queue-capacity deadlocks and lock timeouts,
  cancellation races, source-path media reconciliation, API schema grants,
  and transient queue failures that previously could terminate a worker
  process.

## [1.1.0] - 2025-11-26

Legacy Python service release retained for repository history; it is not the
current Rust four-container contract.

### Added

- Rest API, this can be used for one offs or full control via automation instead of the CRON schedule.
- CLI arg `test_webhook`.

### Fixed

- Issue where external ids would sometimes cause the ingestion for an id to fail.
- **Changes sync** is handled intelligently, it will go as far back as 14 days if needed but will also only pull the last 24 hours if it's been ran sooner.

### Changed

- Some small logging changes.
- Updated numerous dependencies.
