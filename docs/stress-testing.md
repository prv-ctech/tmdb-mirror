# Isolated stress testing

Run the harness from Linux/WSL with Docker Desktop available. The scripts use
an isolated Compose project, named volumes, loopback-only ports, and a local
secret env file. They do not touch the production Compose project or host data.

## Prepare and start

Create the ignored secret file and fill in the TMDB v4 read token, v3 API key,
and optional existing Trawl URL:

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

The loader reads `secrets.txt`. Secret values are never written to the general
Compose environment, logs, JSON results, Docker build context, or Git.

The bootstrap builds the pinned Rust image and the local PostgreSQL/pgBackRest
image, applies migrations, starts the four-container stack, waits for health,
verifies UID 10001, and checks that the fixed `/config` and `/media`
subdirectories are writable by the runtime user. The PostgreSQL stress volume
is initialized with WAL archiving enabled so the durable backup API can be
exercised by the built-in pgBackRest scheduler. Use a fresh project name when
converting an older stock-PostgreSQL stress volume because `archive_mode` is a
cluster initialization setting. The generated runtime files are under ignored
`.stress-runtime/<project>/`.

## Exercise the stack

Run the bounded checks in this order:

```bash
./scripts/stress-seed.sh --project-name tmdb_stress_test --count 100000
./scripts/stress-artwork.sh --project-name tmdb_stress_test
./scripts/stress-media-assets.sh --project-name tmdb_stress_test
./scripts/stress-http.sh --project-name tmdb_stress_test --concurrency 100 --requests-per-worker 100
./scripts/stress-trawl.sh --project-name tmdb_stress_test
./scripts/stress-resilience.sh --project-name tmdb_stress_test
./scripts/stress-scan.sh --project-name tmdb_stress_test --queue-limit 10
./scripts/stress-collect.sh --project-name tmdb_stress_test
```

The seed creates a large synthetic catalog for indexed list/search/filter
tests. Artwork uses real TMDB requests for a multi-image movie, TV `119495`
(posters, backdrops, logos, seasons, and trailers), TV `4586` (Trailer and
Opening Credits), anime/live-adaptation classification fixtures, reusable
people, companies, networks, and collections. Run artwork before HTTP so the
gallery and video routes have live rows. The Trawl check is skipped when no
Trawl URL is configured. When configured, `stress-trawl.sh` uses Trawl's
documented JSON `/scrape` endpoint and verifies its status/metadata response;
the native endpoint does not provide a binary image body for this worker.
The export scan downloads the latest matching public movie and TV exports,
counts their records, and bounds queued detail work with `--queue-limit`.

The HTTP result records request count, failures, throughput, p50/p95/p99
latency, gallery URL/path redaction checks, season/episode image routes, and
video-type/YouTube URL checks. The artwork and media-asset results also report
gallery counts, original and optimized rows, episode optimized-only rows,
variant MIME/path violations, video counts by type/provider, HTTP status,
permissions, worker IDs, and failures. Results and redacted diagnostics remain
under the ignored runtime directory.

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
