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
- [x] Verify anime exclusion/inclusion and representative filters through the API.
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
- [x] Commit/push verified changes and document exact measured results.
