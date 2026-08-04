# TMDB clone stress-test checklist

## Harness

- [x] Add ignored runtime directory and secret-safe bootstrap.
- [x] Add isolated stress Compose stack and resource limits/tuning for a 16 GB test host.
- [x] Add status, logs, teardown, and artifact collection helpers.

## Smoke checks

- [x] Migrate and verify PostgreSQL 18 extensions, grants, indexes, and PgBouncer.
- [x] Verify API liveness/readiness and all worker containers.
- [x] Submit and complete a deterministic no-op job.

## Load and search

- [x] Add synthetic high-cardinality seed data and `ANALYZE`.
- [x] Verify TMDB v3 document routes and representative filters through the API.
- [x] Add concurrent HTTP load runner with percentile latency and error reporting.
- [x] Capture PostgreSQL activity/statistics and Docker resource samples.

## Real TMDB exercise

- [x] Stream and count complete movie/TV daily exports.
- [x] Add bounded, resumable detail-refresh queueing with rate/concurrency controls.
- [x] Exercise image download and Trawl fallback paths without leaking secrets.

## Resilience and quality

- [x] Test worker restart and dependency recovery.
- [x] Fix discovered defects and add regression tests.
- [x] Run full verification matrix and collect final artifacts.
- [x] Document exact measured results and remaining limits; commit or push only after explicit user authorization.

## Scan controls

- [x] Keep worker startup idle until an authenticated admin action starts it.
- [x] Bound export and missing-catalog fan-out with cursor continuations.
- [x] Add explicit `full_sweep`, `missing_only`, `prune_cleanup`, and
  `daily_sync` catalog modes.
- [x] Run the Docker Desktop stress matrix against the current local image.
- [x] Verify the live catalog queue remains bounded during a real TMDB scan.
