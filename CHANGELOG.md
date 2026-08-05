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
  history, media scans/audits, statistics maintenance, and pgBackRest backups.
- Dedicated TMDB gallery ingestion for titles, seasons, episodes, people,
  companies, networks, and collections with deterministic local media paths.
- Additive `local_*` media URLs while preserving upstream TMDB image fields.
- Bounded daily-export census, phased enrichment, season processing,
  `daily_sync`, queue backpressure, job retention, and cancellation controls.
- PostgreSQL least-privilege roles, SQLx migrations, trigram/unaccent search,
  pg_stat_statements, WAL archiving, scheduled backups, and PITR verification.
- Linux/Bash Docker stress harness with secret-safe real-TMDB fixtures,
  media checks, worker-state checks, HTTP load, and resilience tests.

### Changed

- Replaced the legacy Python service and custom catalog routes with the Rust
  TMDB v3-compatible surface.
- Removed the anime database/API partition, PgBouncer, automatic catalog
  scheduling, PowerShell scripts, WebP derivatives, `.masters`, and obsolete
  compatibility paths.
- Workers now start in a durable stopped state after restart and require an
  authenticated admin action before claiming work.

### Fixed

- Catalog phase contention, queue-capacity deadlocks, cancellation races,
  source-path media reconciliation, API schema grants, and transient queue
  failures that previously could terminate a worker process.

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
