# Four-container deployment

`deploy/compose.production.yaml` is the complete canonical Compose file. It
pulls two GitHub-built Linux AMD64 images:

- `ghcr.io/prv-ctech/tmdb-mirror:main` for API, main worker, and media worker.
- `ghcr.io/prv-ctech/tmdb-mirror-postgres:main` for PostgreSQL and its bootstrap.

The stack is deliberately only four services:

1. PostgreSQL 18, including `pg_trgm`, `unaccent`, and `pg_stat_statements`.
2. API, with the local TMDB v3-compatible surface and bounded read/write
   connection pools.
3. Main worker, which migrates and runs scheduled or explicitly submitted ingest jobs.
4. Media worker, which expands on-demand requests, downloads/verifies images, and serves `/media` directly
   through its embedded read-only HTTP server.

There is no PgBouncer, Nginx, migration container, scheduler container, or
storage-init container in the canonical deployment. The main worker has a
small in-process cron scheduler backed by durable PostgreSQL slots and
watermarks. Both workers begin draining eligible work when their containers
start. The disposable stress Compose file uses the same four-service shape
with isolated named volumes and disables catalog schedules for deterministic
tests. PostgreSQL runs its independent pgBackRest backup schedule.
The PostgreSQL service also declares a 2 GiB `/dev/shm`; Docker's 64 MiB
default is too small for parallel query workers during a 100-client burst.

## Fixed paths

The application only knows these container paths:

| Container path | Purpose |
| --- | --- |
| `/media` | Permanent on-demand final image renditions |
| `/config` | Raw catalog exports, persistent logs, and backups |
| `/config/backups/pgbackrest` | PostgreSQL-owned same-host pgBackRest repository |
| PostgreSQL `/var/lib/postgresql` | PostgreSQL 18 data/WAL |

The Compose file uses relative bind mounts. Edit the `source:` values when an
existing host data layout must be retained:

```text
./data/postgres18 -> /var/lib/postgresql
./data/config     -> /config
./data/media      -> /media
```

The API and media services publish the fixed public `9000`, admin `9001`, and
media `9002` container listeners on the same host ports. Host mount paths are
Compose deployment settings, not application environment variables.

All four services use one external Docker network. The tracked Compose files
use `your.network` as a neutral placeholder; replace every
`"your.network"` service and top-level network reference with an existing
network name before starting the stack. No `tmdb-private` network is created.

The API, worker, and media processes run as UID/GID `10001`. PostgreSQL prepares
the shared log directory before it becomes healthy. The API validates that
directory; worker and media startup prepare only their fixed writable child
folders, verify an actual write as UID/GID `10001`, and then start Rust. No
separate storage-init container or manual recursive `chown` step is needed.

It never recursively changes `/config` or `/media`, so a restart does not walk
millions of images or alter unrelated files in a broad host mount. It changes
only these app-owned paths:

```text
/config/raw  /config/logs
/media/movies  /media/tv  /media/people  /media/networks
/media/companies  /media/collections
```

`/config/raw` is active worker storage for TMDB daily exports and reconcile ID
files. The obsolete `/config/work` and `/config/media` directories are no
longer created or read and may be removed after the old containers are stopped.
Media publication writes a temporary file beside the final file under `/media`,
syncs it, and atomically renames it into place.

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

`TZ` controls human-readable terminal timestamps, catalog cron evaluation, and
the pgBackRest schedule. `.env.example` uses `America/New_York`; operators may
choose another IANA timezone. PostgreSQL and API timestamps remain UTC.

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

The first three listener settings default to the container ports `9000`,
`9001`, and `9002`; host ports are defined only in Compose. The database
connection is always `postgres:5432`, and the image supplies PostgreSQL init
defaults. Boolean scheduler toggles are obsolete. The three five-field cron
values in `.env.example` schedule `daily_sync`, `missing_only`, and `reconcile`;
set one to an empty value to disable only that schedule. Optional retry,
timeout, lease, heartbeat, and polling settings remain advanced overrides.

The root `docker-compose-example.yaml` and
`deploy/compose.production.yaml` describe the same four-service topology and
security contract. The standalone file expects `.env` beside it and resolves
`./data/*` from its own directory. The production file defaults to `../.env`
and resolves its `./data/*` sources relative to `deploy/`; edit those Compose
`source:` lines for an existing host layout. Do not move host paths or host
ports into `.env`.

Keep `TMDB_RATE_LIMIT` at `40` or lower. The worker rejects a higher value
before it starts upstream requests.

## Persistent logs

All four containers emit JSONL to Docker/Unraid and write the identical stream
to `/config/logs`. The first start creates `api.log`, `worker.log`, `media.log`,
and `postgres.log`. Each later process start creates the next numeric file,
such as `worker-1.log`; only the newest 10 files for each service are retained.
Rotation is automatic and has no environment setting.

`TMDB_LOG_FORMAT` defaults to `json`. `TMDB_LOG_LEVEL=info` shows startup,
storage checks, worker lifecycle, retries, dead letters, and media failures
without logging every successful operation. Temporarily use
`TMDB_LOG_LEVEL=debug` for individual HTTP requests, job claims, successful
jobs, and image publication during a bounded diagnostic run. `RUST_LOG`
remains an advanced full filter override. For temporary raw SQLx query timing,
include `sqlx=warn`; it is deliberately off by default.

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

The main worker applies all embedded SQLx migrations under the PostgreSQL
advisory lock. Both workers begin draining eligible durable work on container
startup. Operators can request `full_sweep`, `missing_only`, `recovery`,
`prune_cleanup`, `daily_sync`, or `reconcile` through the private admin API.
The same API can start, pause, resume, or cancel either worker; pausing blocks
new claims and does not stop the container. The main
worker creates one ingest loop per configured `TMDB_MAX_CONNECTIONS`, clamped
to `1..=64`; the shared upstream request-start limiter remains bounded by
`TMDB_RATE_LIMIT` at `40` requests per second or less. The media worker waits
for the durable queue schema before claiming image jobs, so first-boot
migrations do not cause an image-worker crash.

Schema revision `0052` preserves the catalog and captured TMDB documents while
replacing legacy image state, variants, media scans, and audits with durable
on-demand media requests. Revision `0053` preserves that data and replaces the
shared queue-capacity advisory lock with exact queue-slot admission so parallel
catalog transactions do not serialize or fail on that lock. The migrations do
not delete old filesystem media; remove obsolete files separately after
deploying and verifying the new schema.

The default schedules run `daily_sync` hourly, `missing_only` nightly, and
`reconcile` on days 1 and 15. `daily_sync` refreshes changed titles and
discovers new seasons/episodes. `reconcile` adds IDs from official exports,
repairs new/incomplete/dead-lettered titles, and deactivates absent IDs without
re-enriching every complete title. `full_sweep` remains manual. A changes gap
older than 14 days sets `fullSweepRequired` in admin status.
Busy schedule slots remain pending until incompatible maintenance finishes.
Unresolved child dead letters prevent the corresponding synchronization
watermark from advancing.

## Media policy

`ALLOW_LOCAL_MEDIA=true` allows the media worker to publish files. Catalog
writes never create image jobs. Arrbit submits one to 100 active local title IDs
to `/admin/v1/media/requests`; the media worker expands only the source paths
already stored in PostgreSQL and downloads bytes from TMDB's image CDN. It
never performs metadata discovery. The API preserves each upstream field and
adds a digest-versioned `local_*` URL using `TMDB_MEDIA_BASE_URL`; the local
field is `null` until a verified asset exists.

Title, season, and episode galleries use English plus untagged images captured
with `language=en-US` and `include_image_language=en,null`. People, companies,
networks, and collections contribute only the primary source paths already
normalized from the requested local title.

Public paths are deterministic and use TMDB IDs:

```text
/media/tv/{tmdb_id}/posters/poster.jpg
/media/tv/{tmdb_id}/posters/season01-poster.jpg
/media/tv/{tmdb_id}/backdrops/backdrop-01.jpg
/media/tv/{tmdb_id}/logos/logo.png
/media/tv/{tmdb_id}/thumbnails/season01-episode01-thumbnails.jpg
/media/people/{tmdb_person_id}/profile.jpg
/media/companies/{tmdb_company_id}/logo.png
/media/networks/{tmdb_network_id}/logo.png
/media/collections/{tmdb_collection_id}/poster.jpg
```

The worker stores exact validated CDN rendition bytes: `w500` posters/season
posters, `w1280` backdrops, `w300` episode stills, and `w185` profiles/logos.
It never requests `original`, resizes, recompresses, or generates a derivative.
JPEG, PNG, and static WebP are accepted; SVG-backed logos request PNG. No
`optimized/`, `.masters`, variant, original, or video directory is created.
Publication uses a same-filesystem temporary file and atomic rename.
Publication, verification, and deletion reject symlinked path components.

## Validation

Before exposing the API, validate the deployment definition and run the full
workspace checks:

```bash
./scripts/validate-production-compose.sh --env-file "$TMDB_ENV_FILE"
docker compose --env-file "$TMDB_ENV_FILE" -f deploy/compose.production.yaml ps
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml logs --no-color --tail=200 postgres worker api media
```

Exercise authentication, unknown-ID rejection, idempotency, offline worker
persistence, one- and 100-title media requests, season zero, episodes, cast,
network, company, and collection paths. Verify exact bytes and MIME, digest
URLs, stable numbering, lazy repair/deletion, bounded continuations, and that a
worker restart leaves no orphan lease.

The database remains MVCC/concurrent: independent API requests use separate
bounded PostgreSQL connections, so one user's metadata read does not hold
another user's request behind it. Measure the 100-client target with the
repository's stress scripts on the actual SSD and host network before calling
capacity production-ready. The supplied production and disposable stress
definitions set PostgreSQL `max_connections=200`, leaving room for the four
application pools and a measured 100-client burst; tune memory and connection
limits together for the deployment host.

The existing Trawl instance is used only as the challenge fallback:
`TMDB_TRAWL_BASE_URL=http://<trawl-host>:8191`.

The API service shares the configured external network with PostgreSQL, the
worker, and media. The Compose file publishes host port `9001` for the
authenticated admin listener. It can also be reached by a trusted container
on the network at `http://tmdb-mirror-api:9001/admin/v1/status` or `/metrics`
using the admin key.

Backup and offline recovery steps are in [backup-recovery.md](backup-recovery.md).
