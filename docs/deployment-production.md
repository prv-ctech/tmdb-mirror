# Four-container deployment

`deploy/compose.production.yaml` is the complete canonical Compose file. It
pulls two GitHub-built Linux AMD64 images:

- `ghcr.io/prv-ctech/tmdb-mirror:main` for API, main worker, and media worker.
- `ghcr.io/prv-ctech/tmdb-mirror-postgres:main` for PostgreSQL and its bootstrap.

The stack is deliberately only four services:

1. PostgreSQL 18, including `pg_trgm`, `unaccent`, and `pg_stat_statements`.
2. API, with catalog/anime/search/admin routes and bounded read connections.
3. Main worker, which migrates, initializes, schedules, ingests, retries, and
   maintains durable jobs.
4. Media worker, which downloads/verifies images and serves `/media` directly
   through its embedded read-only HTTP server.

There is no PgBouncer, Nginx, migration container, scheduler container, or
storage-init container in the canonical deployment. The disposable stress
Compose file uses the same four-service shape with isolated named volumes.
The PostgreSQL service also declares a 2 GiB `/dev/shm`; Docker's 64 MiB
default is too small for parallel query workers during a 100-client burst.

## Fixed paths

The application only knows these container paths:

| Container path | Purpose |
| --- | --- |
| `/media` | Permanent public media and private `.masters` originals |
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
/media/.masters  /media/movies  /media/tv  /media/anime/{movie,tv}
/media/casting  /media/networks  /media/companies  /media/collections
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

`TZ=America/New_York` controls schedule interpretation and human-readable
terminal timestamps. PostgreSQL and API timestamps remain UTC.

The PostgreSQL service uses `POSTGRES_DB`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD` for the database owner and health check. Application
processes use the fixed `migrator`, `api_reader`, `api_job_submitter`,
`ingest_writer`, `image_writer`, and `monitor` roles with that shared password.
Their database permissions remain separate. The connection is fixed to the
internal Compose service `postgres:5432`; do not add `DATABASE_*`, `TMDB_DB_*`,
role identity, or per-process database settings.

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
restarts are safe and do not start an implicit full catalog scan. The scheduler
queues only the configured changes, trending, and daily-export jobs. Operators
request bounded `full`, `missing`, or `changes` scans through the private admin
API. The worker runs up to eight ingestion loops, bounded by
`TMDB_MAX_CONNECTIONS` and `TMDB_RATE_LIMIT`. The media worker waits for the
durable queue schema before claiming image jobs, so first-boot migrations do
not cause an image-worker crash.

## Media policy

`ALLOW_LOCAL_MEDIA=true` causes the worker to create image jobs in the same
transaction as a committed title/entity and the API returns local URLs based on
`TMDB_MEDIA_BASE_URL`. When false, no new image jobs are created and the API
returns the original TMDB URL.

Public paths are deterministic:

```text
/media/movies/{tmdb_id}/cover.jpg
/media/tv/{tmdb_id}/season1-episode5.jpg
/media/anime/movie/{tmdb_id}/cover.jpg
/media/anime/tv/{tmdb_id}/specials-episode1.jpg
/media/casting/{local_id}/profile.jpg
/media/networks/{local_id}/logo.jpg
/media/companies/{local_id}/logo.jpg
/media/collections/{local_id}/cover.jpg
```

Original masters are content-addressed below `/media/.masters` and the embedded
server rejects that subtree. Temporary files are created only below
`/config/media`; they are removed after atomic publication.

## Validation

Before exposing the API, validate the deployment definition and run the full
workspace checks:

```bash
./scripts/validate-production-compose.sh --env-file "$TMDB_ENV_FILE"
docker compose --env-file "$TMDB_ENV_FILE" -f deploy/compose.production.yaml ps
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml logs --no-color --tail=200 postgres worker api media
```

Exercise both `ALLOW_LOCAL_MEDIA` modes, a movie, TV, anime movie, anime TV,
season, episode, specials, cast, network, company, and collection asset. Verify
that `/config` contains all scratch/checkpoint/log files, `/media` contains the
final files, `.masters` is not downloadable, duplicate reusable assets point
to one master, and a worker restart leaves no orphan job lease.

The database remains MVCC/concurrent: independent API requests use separate
bounded PostgreSQL connections, so one user's metadata read does not hold
another user's request behind it. Measure the 100-client target with the
repository's stress scripts on the actual SSD and host network before calling
capacity production-ready.

The existing Trawl instance is used only as the challenge fallback:
`TMDB_TRAWL_BASE_URL=http://<trawl-host>:8191`.

The API service is also attached to the existing `prv.network`, but port 8081
is never published to the host. A container on that private network can call
`http://tmdb-mirror-api:8081/admin/v1/status` or `/metrics` using the existing
admin key. PostgreSQL, worker, and media remain only on `tmdb-private`.

Backup and offline recovery steps are in [backup-recovery.md](backup-recovery.md).
