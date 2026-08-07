# Isolated stress testing

Run the Linux/Bash harness with Docker Desktop available. It uses a unique
Compose project, named volumes, loopback-only ports, and ignored runtime files.
It never touches production containers, volumes, networks, or host paths.

## Start a disposable stack

```bash
cp secrets.txt.example secrets.txt
chmod 600 secrets.txt
./scripts/test-stress-secrets.sh
./scripts/stress-tmdb-auth.sh
./scripts/stress-bootstrap.sh \
  --project-name tmdb_stress_test \
  --api-port 18080 \
  --admin-port 18081 \
  --image-port 18090 \
  --postgres-port 55433
```

These loopback-only stress host ports intentionally differ from production;
Compose maps them to the same `9000` read, `9001` admin, and `9002` media
container listeners used by the production images.

The loader reads the real TMDB token only from ignored `secrets.txt`, writes a
mode-600 runtime secret file, and never puts credentials in Compose output,
logs, reports, source, or image layers. The disposable environment disables
catalog cron schedules so tests are deterministic.

Bootstrap builds the local app and PostgreSQL/pgBackRest images, applies all
migrations, waits for four healthy services, verifies the runtime UID, checks
`/config` and `/media` permissions, confirms obsolete `/config/media` is not
created, and validates one JSON object from each of `postgres.log`, `api.log`,
`worker.log`, and `media.log`.

## Exercise the stack

```bash
./scripts/stress-seed.sh --project-name tmdb_stress_test --count 100000
./scripts/stress-artwork.sh --project-name tmdb_stress_test
./scripts/stress-media-assets.sh --project-name tmdb_stress_test
./scripts/stress-media-scans.sh --project-name tmdb_stress_test
./scripts/stress-http.sh --project-name tmdb_stress_test --concurrency 100 --requests-per-worker 100
./scripts/stress-trawl.sh --project-name tmdb_stress_test
./scripts/stress-resilience.sh --project-name tmdb_stress_test
./scripts/stress-scan.sh --project-name tmdb_stress_test --max-active 1000
./scripts/stress-collect.sh --project-name tmdb_stress_test
```

`stress-artwork.sh` enriches a bounded live fixture set and then submits one
bulk `/admin/v1/media/requests` request. It verifies that media work starts only
from active local catalog titles and that catalog writes created no image jobs.
The fixture includes movie `550`, TV `119495`, TV `4586`, season/episode images,
cast/crew, companies, networks, collections, and multiple title galleries.

`stress-media-assets.sh` verifies deterministic TMDB-ID paths, exact JPEG/PNG/
static-WebP rendition bytes, MIME and dimensions, SHA-256 metadata, HTTP/ETag
serving, permissions, and all required owner categories. It requires zero
`optimized/`, `.masters`, original, or variant files.

`stress-media-scans.sh` now tests the on-demand request contract despite its
historical filename: authentication, one- and 100-title submissions, duplicate
normalization, idempotent replay, conflicting replay, atomic unknown-ID `422`,
status counts, offline media-container persistence, startup draining,
pause/resume/cancel, bounded request expansion, and removed legacy scan/audit
routes. It does not perform a global media scan.

`stress-http.sh` measures the public read API with 100 concurrent clients and
records request count, failures, throughput, and p50/p95/p99 latency. It covers
Unicode/accent-folded search, captured TMDB v3 paths, and upstream plus
digest-versioned local media fields.

`stress-scan.sh` submits a bounded `missing_only` catalog run and reports active
queue peak separately from retained history. Full-sweep qualification must
report 500-ID census, 100-title enrichment, and 25-season throughput separately
and prove normal phase waiting consumes no retries. Scheduled-path qualification
must also test hourly `daily_sync`, nightly `missing_only`, twice-monthly
`reconcile`, durable slots/watermarks, overlap prevention, and
`fullSweepRequired` after a changes gap beyond 14 days.

All reports are written below ignored `.stress-runtime/<project>/`. The
collector redacts secrets and fails on PostgreSQL deadlocks or lock-timeout
errors.

## Last full qualification

The 2026-08-06 clean Docker Desktop run applied schema revision `0052`, passed
the 272-test Rust workspace suite, published 1,650 final media assets, and then
reused all 1,650 on a repeat request with zero media failures. Its 100-client
public API sample completed 2,000 requests with no HTTP failures. Full and
differential backup plus offline PITR checks also passed. These dated results
qualify that build and host only; rerun the harness for every release and on
the actual deployment hardware.

The schema `0053` queue-contention qualification then started a fresh isolated
four-container stack and submitted 64 concurrent held title-enrichment jobs
with a 100 ms PostgreSQL lock timeout. All 64 committed, occupied exactly 64
durable queue slots, released all slots on cancellation, and produced no lock
timeout, deadlock, migration, or container-health failure.

The 2026-08-07 runtime-storage qualification built fresh release images,
started a clean four-container schema `0053` stack, passed formatting, strict
Clippy, and all 276 database-backed workspace tests. A five-title live request
published and served 1,636 assets (79,082,815 bytes) with zero failed assets,
dead letters, leftover temporary files, or `/config/media` directory. Temporary
TMDB `429` responses retried successfully; the final request was `succeeded`
and local media returned HTTP `200`.

## Optional k6 profile

```bash
./scripts/k6/run.sh \
  --profile full \
  --base-url http://127.0.0.1:18080 \
  --compose-file deploy/compose.stress.yaml \
  --compose-env-file .stress-runtime/tmdb_stress_test/compose.env \
  --compose-project-name tmdb_stress_test \
  --admin-metrics-url http://127.0.0.1:18081/metrics
```

Start with a smaller endpoint profile before the 100-client run.

## Backup and repository checks

```bash
./scripts/bootstrap-dev.sh
./scripts/verify-toolchain.sh
./scripts/validate-production-compose.sh --env-file .env.example
./scripts/verify-repository-hygiene.sh
./infra/runtime/tests/tmdb-log-run.test.sh
./infra/postgres/tests/pgbackrest-runner.test.sh
./infra/postgres/tests/pgbackrest-pitr.test.sh
```

The backup API accepts `{"type":"full"}` or `{"type":"differential"}`.
Poll the returned durable job, check `/admin/v1/backups`, then verify the
pgBackRest repository. Restore remains an offline procedure.

## Stop

```bash
./scripts/stress-down.sh --project-name tmdb_stress_test
# Add --remove-volumes only for this named disposable project.
```
