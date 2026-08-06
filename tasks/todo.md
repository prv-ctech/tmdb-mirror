# TMDB clone stress-test checklist

Status: archived implementation checklist. Checked items describe completed
repository work, not the current health of a running deployment.

## Harness

- [x] Add ignored runtime directory and secret-safe bootstrap.
- [x] Add isolated stress Compose stack and resource limits/tuning for a 16 GB test host.
- [x] Add status, logs, teardown, and artifact collection helpers.

## Smoke checks

- [x] Migrate and verify PostgreSQL 18 extensions, grants, indexes, and bounded
  direct connection pools.
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

- [x] Start both workers in queue-draining state while retaining authenticated
  start, pause, resume, and cancel controls.
- [x] Bound export and missing-catalog fan-out with cursor continuations.
- [x] Add explicit `full_sweep`, `missing_only`, `recovery`, `prune_cleanup`,
  `daily_sync`, and `reconcile` catalog modes with durable schedules for the
  three incremental/repair modes.
- [x] Replace global media scans/audits with durable one-to-100-title
  `/admin/v1/media/requests`, bounded expansion, and exact CDN renditions.
- [x] Run the Docker Desktop stress matrix against the current local image.
- [x] Verify the live catalog queue remains bounded during a real TMDB scan.
