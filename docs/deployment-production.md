# Four-container deployment

`deploy/compose.production.yaml` is the complete canonical Compose file. It
pulls two GitHub-built Linux AMD64 images:

- `ghcr.io/prv-ctech/tmdb-mirror:main` for API, main worker, and media worker.
- `ghcr.io/prv-ctech/tmdb-mirror-postgres:main` for PostgreSQL and its bootstrap.

The stack is deliberately only four services:

1. PostgreSQL 18, including `pg_trgm`, `unaccent`, and `pg_stat_statements`.
2. API, with the TMDB v3 read surface and bounded read connections.
3. Main worker, which migrates and runs explicitly submitted ingest jobs.
4. Media worker, which downloads/verifies images and serves `/media` directly
   through its embedded read-only HTTP server.

There is no PgBouncer, Nginx, migration container, scheduler container, or
storage-init container in the canonical deployment. The worker also has no
in-process catalog scheduler: restarts do not submit changes, trending, or
daily-export jobs. A running worker is reset to stopped on restart, while a
paused worker remains paused. The disposable stress
Compose file uses the same four-service shape with isolated named volumes.
The PostgreSQL service also declares a 2 GiB `/dev/shm`; Docker's 64 MiB
default is too small for parallel query workers during a 100-client burst.

## Fixed paths

The application only knows these container paths:

| Container path | Purpose |
| --- | --- |
| `/media` | Permanent public gallery originals and optimized images |
| `/config` | NVMe worker scratch, raw exports, checkpoints, and logs |
| `/config/backups/pgbackrest` | PostgreSQL-owned same-host pgBackRest repository |
| PostgreSQL `/var/lib/postgresql` | PostgreSQL 18 data/WAL |

The Compose file uses relative bind mounts. Edit the `source:` values when an
existing host data layout must be retained:

```text
./data/postgres18 -> /var/lib/postgresql
./data/config     -> /config
./data/media      -> /media
```

The API and media services publish host ports `9001` and `9002` to container
ports `8080` and `8090`. Host mount paths are Compose deployment settings, not
application environment variables.

All four services use one external Docker network. The tracked Compose files
use `your.network` as a neutral placeholder; replace the root network
`name:` value with an existing network name before starting the stack. No
`tmdb-private` network is created.

The API runs as UID/GID `10001`. The worker and media services begin with a
tiny built-in startup preparer: it creates their fixed child folders, gives
those folders to UID/GID `10001`, verifies an actual write as that user, and
then drops root before starting Rust. No separate storage-init container or
manual `chown` step is needed.

It never recursively changes `/config` or `/media`, so a restart does not walk
millions of images or alter unrelated files in a broad host mount. It changes
only these app-owned paths:

```text
/config/work  /config/raw  /config/logs  /config/media
/media/movies  /media/tv  /media/people  /media/networks
/media/companies  /media/collections
```

PostgreSQL alone creates `/config/backups/pgbackrest`. The worker and media
worker never recursively change that repository or its parent permissions.

Docker must supply the three host-side mount roots themselves. If a network
share or ACL refuses the mount change, startup stops with the fixed path and
operation that failed instead of silently retrying image jobs.

No `TMDB_MEDIA_HOST_ROOT`, `TMDB_WORK_HOST_ROOT`, `TMDB_MEDIA_ROOT`,
`TMDB_WORK_ROOT`, or similar host-path environment variable is read by the
application.

## Environment and startup

Copy the root environment template to a mode-600 runtime file outside the
checkout and replace every angle-bracket value. It contains the PostgreSQL
owner credentials, TMDB read token, admin API key, and local media settings.
Keep that file out of the repository; do not place a real token in `.env`,
Compose YAML, or a generated artifact. The same runtime file is passed to all
four containers through `env_file`. No repository checkout or local Docker
build is needed after GitHub Actions has published the images.

The public, admin, and media routes are listed in [api.md](api.md).

`TZ=America/New_York` controls human-readable terminal timestamps. PostgreSQL
and API timestamps remain UTC. Catalog and media scans are never scheduled by
the application; the authenticated admin API starts them explicitly.

The PostgreSQL service uses `POSTGRES_DB`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD` for the database owner and health check. Application
processes use the fixed `migrator`, `api_reader`, `api_job_submitter`,
`ingest_writer`, `image_writer`, and `monitor` roles with that shared password.
Their database permissions remain separate. The connection is fixed to the
internal Compose service `postgres:5432`; do not add `DATABASE_*`, `TMDB_DB_*`,
role identity, or per-process database settings.
The PostgreSQL service starts as `0:0` so its entrypoint can prepare mounted
data and pgBackRest children, then drops to PostgreSQL's unprivileged user.

The current environment template is not the older Unraid environment. The
values users enter are the database owner credentials, `TMDB_ENVIRONMENT`,
`TZ`, TMDB read token, admin key, API base URL, local-media settings, and the
worker values shown in `.env.example`. The following are fixed by the image or
have safe defaults and should normally be omitted:

```text
TMDB_API_BIND TMDB_ADMIN_BIND TMDB_MEDIA_BIND
PGDATA POSTGRES_INITDB_ARGS
TMDB_MEDIA_HOST_ROOT TMDB_WORK_HOST_ROOT TMDB_MEDIA_ROOT TMDB_WORK_ROOT
TMDB_MIGRATOR_* TMDB_API_READER_* TMDB_API_JOB_SUBMITTER_*
TMDB_INGEST_WRITER_* TMDB_IMAGE_WRITER_* TMDB_MONITOR_*
TMDB_ENABLE_SCHEDULER TMDB_ENABLE_DAILY_EXPORT
```

The first three listener settings default to the container ports `8080`,
`8081`, and `8090`; host ports are defined only in Compose. The database
connection is always `postgres:5432`, and the image supplies PostgreSQL init
defaults. Scheduler toggles are obsolete: catalog and media work are submitted
through the authenticated admin API and do not start automatically after a
restart. Optional retry, timeout, lease, heartbeat, polling, logging, and
image-policy settings remain supported as advanced overrides but are not
required in the minimal template.

The root `docker-compose-example.yaml` and
`deploy/compose.production.yaml` describe the same four-service topology and
security contract. The standalone file expects `.env` beside it and resolves
`./data/*` from its own directory. The production file defaults to `../.env`
and resolves its `./data/*` sources relative to `deploy/`; edit those Compose
`source:` lines for an existing host layout. Do not move host paths or host
ports into `.env`.

Keep `TMDB_RATE_LIMIT` at `40` or lower. The worker rejects a higher value
before it starts upstream requests.

## Terminal logs

`TMDB_LOG_FORMAT=pretty` is the default and produces compact, readable colored
service logs in Docker/Unraid. `TMDB_LOG_LEVEL=info` shows startup, storage
checks, worker lifecycle, retries, dead letters, and media failures without
flooding the terminal with successful health checks. Temporarily set
`TMDB_LOG_LEVEL=debug` to show individual HTTP requests, job claims,
successful jobs, and successful image publication while diagnosing a small
test run. Set
`TMDB_LOG_FORMAT=json` only when a log collector requires JSON; `RUST_LOG`
remains an advanced full filter override. For temporary raw SQLx query timing,
include `sqlx=warn` in that override; it is deliberately off by default.

Operational events contain only bounded fields: fixed container paths, job
IDs/types, retry codes, image entity IDs, safe HTTP status, and safe I/O
classes such as `permission_denied`. They never print database passwords,
tokens, image source URLs, payloads, host bind paths, or raw upstream errors.

```bash
runtime_env=/secure/path/tmdb-mirror.env
cp .env.example "$runtime_env"
chmod 600 "$runtime_env"
# Edit "$runtime_env" and replace every angle-bracket value.
export TMDB_ENV_FILE="$runtime_env"

./scripts/validate-production-compose.sh \
  --env-file "$TMDB_ENV_FILE" \
  --compose-file deploy/compose.production.yaml
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml config --quiet
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml pull
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml up -d
```

The main worker applies migrations under the existing PostgreSQL advisory lock;
restarts are safe and reset a running worker to stopped, so they do not start
catalog or media work. Operators request
`full_sweep`, `missing_only`, `prune_cleanup`, or `daily_sync` through the
private admin API. The same API can start, pause, resume, or cancel either
worker; pausing blocks new claims and does not stop the container. The worker
runs up to eight ingestion loops, bounded by
`TMDB_MAX_CONNECTIONS` and `TMDB_RATE_LIMIT`. The media worker waits for the
durable queue schema before claiming image jobs, so first-boot migrations do
not cause an image-worker crash.

## Media policy

`ALLOW_LOCAL_MEDIA=true` causes the worker to create gallery image jobs in the
same transaction as a committed title/entity and the API returns local URLs
based on `TMDB_MEDIA_BASE_URL`. When false, no new image jobs are created and
image responses have no local URL.

Public paths are deterministic and use TMDB IDs:

```text
/media/tv/{tmdb_id}/posters/poster.jpg
/media/tv/{tmdb_id}/posters/season01-poster.jpg
/media/tv/{tmdb_id}/backdrops/backdrop-01.jpg
/media/tv/{tmdb_id}/logos/logo.png
/media/tv/{tmdb_id}/optimized/posters/poster-w640.jpg
/media/tv/{tmdb_id}/optimized/thumbnails/season01-episode01-thumbnails-w640.jpg
/media/people/{tmdb_person_id}/profile.jpg
/media/companies/{tmdb_company_id}/logos/logo.png
/media/networks/{tmdb_network_id}/logos/logo.png
/media/collections/{tmdb_collection_id}/posters/poster.jpg
```

Original bytes are preserved outside `optimized/`. Optimized posters, seasons,
profiles, and thumbnails are JPEG quality 85 with maximum width 640;
backdrops use 1280 and logos use transparent PNG width 500. Episode thumbnails
are optimized-only at width 640. No WebP derivative, `full` variant, video
file, or `.masters` directory is created. Temporary files are created only
below `/config/media` and are removed after atomic publication.

## Validation

Before exposing the API, validate the deployment definition and run the full
workspace checks:

```bash
./scripts/validate-production-compose.sh --env-file "$TMDB_ENV_FILE"
docker compose --env-file "$TMDB_ENV_FILE" -f deploy/compose.production.yaml ps
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml logs --no-color --tail=200 postgres worker api media
```

Exercise both `ALLOW_LOCAL_MEDIA` modes, a movie, TV, season zero and regular
seasons, episodes, cast, network, company, and
collection galleries. Verify root source digests, optimized dimensions, local
URLs, stable gallery numbering, duplicate source-path handling, and that a
worker restart leaves no orphan job lease.

The database remains MVCC/concurrent: independent API requests use separate
bounded PostgreSQL connections, so one user's metadata read does not hold
another user's request behind it. Measure the 100-client target with the
repository's stress scripts on the actual SSD and host network before calling
capacity production-ready.

The existing Trawl instance is used only as the challenge fallback:
`TMDB_TRAWL_BASE_URL=http://<trawl-host>:8191`.

The API service shares the configured external network with PostgreSQL, the
worker, and media. Port 8081 is never published to the host. A trusted
container on that network can call
`http://tmdb-mirror-api:8081/admin/v1/status` or `/metrics` using the admin
key.

Backup and offline recovery steps are in [backup-recovery.md](backup-recovery.md).
