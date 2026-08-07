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

### Fixed

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
