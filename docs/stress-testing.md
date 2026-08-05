# Isolated stress testing

Run the harness from Linux/WSL with Docker Desktop available. The scripts use
an isolated Compose project, named volumes, loopback-only ports, and a local
secret env file. They do not touch the production Compose project or host data.

## Prepare and start

Create the ignored secret file and fill in the TMDB v4 read token, optional v3
API key, and optional existing Trawl URL:

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

The loader reads `secrets.txt`. An optional `TMDB_ADMIN_API_KEY` entry is
accepted for shared local secret files but ignored by the stress runtime.
Secret values are never written to the general Compose environment, logs, JSON
results, Docker build context, or Git.

The bootstrap builds the pinned Rust image and the local PostgreSQL/pgBackRest
image, applies migrations, starts the four-container stack, waits for health,
verifies UID 10001, and checks that the fixed `/config` and `/media`
subdirectories are writable by the runtime user. The PostgreSQL stress volume
is initialized with WAL archiving enabled so the explicit backup API and
pgBackRest checks can be exercised. Use a fresh project name when
converting an older stock-PostgreSQL stress volume because `archive_mode` is a
cluster initialization setting. The generated runtime files are under ignored
`.stress-runtime/<project>/`.

## Exercise the stack

Run the bounded checks in this order:

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

The seed creates a large synthetic catalog for indexed list/search/filter
tests. Artwork uses real TMDB requests for a multi-image movie, TV `119495`
(posters, backdrops, logos, seasons, and trailers), TV `4586` (Trailer and
Opening Credits), movie/TV live fixtures, reusable
people, companies, networks, and collections. Run artwork before HTTP so the
gallery and video routes have live rows. The Trawl check is skipped when no
Trawl URL is configured. When configured, `stress-trawl.sh` uses Trawl's
documented JSON `/scrape` endpoint and verifies its status/metadata response;
the native endpoint does not provide a binary image body for this worker.
The catalog-scan check submits a bounded `missing_only` scan through the
authenticated admin API, starts both workers through that API, drains the
resulting catalog/media children, and reports active-queue peak separately
from retained terminal history. It does not submit jobs with a container CLI
or launch a bulk export scan by default. The export parser and daily-sync
paths are covered by focused Rust tests and the explicit admin scan contract.

The HTTP result records request count, failures, throughput, p50/p95/p99
latency, TMDB v3 document routes, season/episode image routes, and
video-type checks. Run it at both 50 and 100 concurrent clients for production
qualification. The configuration route is reported as `not seeded` when
the preceding bounded artwork run used `missing_only`; a `full_sweep` seeds
it. The artwork and media-asset results also report
gallery counts, original and optimized rows, episode optimized-only rows,
variant MIME/path violations, video counts by type/provider, HTTP status,
permissions, worker IDs, and failures. Results and redacted diagnostics remain
under the ignored runtime directory.

For full-sweep performance checks, report title-census throughput separately
from enrichment and season throughput. Verify consecutive 500-title census
batches have no enrichment, season, reusable-gallery, or image-download jobs
between them; 100-title enrichment and 25-season TV phases start only after
the preceding phase drains. A separate `daily_sync` run must prove that a
changed title can add a newly published season or episode without another full
sweep.

`stress-artwork.sh`, `stress-scan.sh`, and `stress-media-assets.sh` start
workers through the authenticated admin API before draining work; this is
required because workers are idle after startup. `stress-media-scans.sh` uses
the disposable admin key from
the generated stress environment and never prints it. The real TMDB
credentials are read from ignored `secrets.txt` for the upstream requests. It
verifies authentication, scan idempotency, audit counters,
pause/resume/start/cancel actions, and that a paused state survives a
media-container restart. It leaves the durable media worker running. Catalog
modes are `full_sweep`, `missing_only`,
`prune_cleanup`, and `daily_sync`; every mode is bounded by durable
continuations. Do not launch a large full sweep against a live catalog until
queue depth and rate limits are being monitored.

Qualification must also cover Unicode and accent-folded title search, all
authenticated admin controls, local TMDB v3 account/list/rating writes,
upstream-plus-local media fields, a 100-session PostgreSQL read burst, backup
creation, and isolated PITR restore. When stopping both workflows under load,
cancel catalog ingest first, let active catalog jobs settle, then cancel media
to clear image jobs committed by work that was already in flight.

The commands above automate Unicode search, worker/media controls, local media
fields, HTTP concurrency, and the gallery filesystem/database checks. Local
account/list/rating writes are covered by the Rust PostgreSQL API tests;
pgBackRest runner/PITR behavior is covered by the two explicit scripts below.
The repository does not claim that one stress script covers every item in this
qualification list.

The private backup API accepts `{"type":"full"}` or
`{"type":"differential"}` and requires an idempotency key. It returns a durable
job ID; poll that job through the admin API, then verify the paired request and
repository with `GET /admin/v1/backups` and `tmdb-pgbackrest info` inside the
PostgreSQL container. The runner and PITR checks below cover the offline
restore path as well.

## k6 load profile

The optional k6 runner is an ephemeral test container, not a production
service. It accepts the same endpoint path overrides as the scenario:

```bash
./scripts/k6/run.sh \
  --profile full \
  --base-url http://127.0.0.1:18080 \
  --compose-file deploy/compose.stress.yaml \
  --compose-env-file .stress-runtime/tmdb_stress_test/compose.env \
  --compose-project-name tmdb_stress_test \
  --admin-metrics-url http://127.0.0.1:18081/metrics
```

Use smaller values first, for example
`--profile endpoints --virtual-users 20 --requests-per-endpoint 100`, then
increase only after the bounded smoke checks pass. A failed run writes
redacted Docker/PostgreSQL diagnostics beside the k6 output.

## Token refresh

Changing an env file does not update an existing container. Recreate the
disposable services with the Linux wrapper:

```bash
./scripts/stress-set-token.sh --project-name tmdb_stress_test
```

If a token has been pasted into a chat, shell history, or log, revoke it and
issue a replacement before using the environment outside this local test.

## PostgreSQL, backup, and repository checks

```bash
./scripts/bootstrap-dev.sh
./scripts/verify-toolchain.sh
./scripts/validate-production-compose.sh --env-file .env.example
./scripts/verify-repository-hygiene.sh
./infra/postgres/tests/pgbackrest-runner.test.sh
./infra/postgres/tests/pgbackrest-pitr.test.sh
```

The PITR test builds only disposable PostgreSQL resources, creates a full and
differential backup, restores to a recorded time, and verifies that a later
record is excluded. Use a unique disposable Docker project for any additional
backup experiments; do not prune shared Docker resources.

## Stop and clean up

```bash
./scripts/stress-down.sh --project-name tmdb_stress_test
# Add --remove-volumes only after the disposable data is no longer needed.
```

The cleanup script targets only the explicit stress project. Runtime files,
downloads, exports, logs, and results are ignored by Git and Docker.
