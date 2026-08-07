# TMDB Mirror

Rust + PostgreSQL 18 mirror of TMDB metadata with a local TMDB v3-compatible
API, local image storage, durable jobs, and four runtime services: PostgreSQL,
API, main worker, and media worker.

## Run with Docker Compose

`deploy/compose.production.yaml` is the canonical checkout deployment. It
pulls the published Linux AMD64 images and uses relative `./data` bind mounts:

| Service | Host port | Purpose |
| --- | ---: | --- |
| `postgres` | none | PostgreSQL 18, migrations, WAL archiving, and pgBackRest |
| `api` | `9000`, `9001` | Public catalog and authenticated admin APIs |
| `worker` | none | Migrations, scheduled catalog maintenance, and ingest jobs |
| `media` | `9002` | On-demand image downloads and public image files |

The API container listens on `9000` for the public catalog API and `9001` for
the authenticated admin API. The media container listens on `9002`. The
default Compose files publish those same three ports on the host.

Keep the real runtime environment outside the repository. The template has
placeholders only; never commit a token, password, or private key. For a
Linux deployment:

```bash
runtime_env=/secure/path/tmdb-mirror.env
cp .env.example "$runtime_env"
chmod 600 "$runtime_env"
# Edit "$runtime_env" and replace every angle-bracket placeholder.
export TMDB_ENV_FILE="$runtime_env"

./scripts/validate-production-compose.sh \
  --env-file "$TMDB_ENV_FILE" \
  --compose-file deploy/compose.production.yaml
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml pull
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml up -d
docker compose --env-file "$TMDB_ENV_FILE" \
  -f deploy/compose.production.yaml ps
```

The API health check uses `/health/ready`, so Docker may briefly report it as
`starting` while PostgreSQL recovers or the main worker applies migrations.
`daily_sync` starts only after the worker is ready and does not control
container health. Application database connections retry bounded startup
races instead of exiting, and Compose allows PostgreSQL two minutes to finish
a clean shutdown checkpoint. Do not override that grace period with a shorter
`docker compose stop -t` value.

The external network named in the Compose file must already exist. The Git
examples use `your.network` as a neutral placeholder; replace every
`"your.network"` network reference with your existing Docker network name
before starting the stack. All four services use this one external network.
The application paths inside containers are fixed: `/config` for raw catalog
exports, persistent logs, and backups; `/media` for final public image files.
The media worker publishes through temporary files beside their final
destinations, so it does not use a separate scratch tree.

To reuse existing host directories, edit the `source:` values in the Compose
file. Host mount paths are deployment settings, not application environment.

`docker-compose-example.yaml` is a standalone copy-pasteable Compose file. It
does not use Compose `include`; place it beside the `.env` file.
New checkout deployments can use the canonical file above. See
[production deployment](docs/deployment-production.md) for bind mounts,
permissions, media policy, and validation.
The [documentation map](docs/README.md) indexes the current operator contracts.

These files replace the older deployment contract. They use the `tmdb-runtime`
startup wrapper for API/worker/media file logging and storage preparation,
relative Compose bind sources, and the fixed `9000` read, `9001` admin, and
`9002` media listener contract. The root standalone file reads `.env` beside
it; the production file reads `../.env` by default or the file selected by
`TMDB_ENV_FILE`.

The current `.env.example` is intentionally minimal. Replace its credential
and public media URL placeholders, then adjust only the listed runtime knobs
that differ for your deployment. Do not carry forward listener binds, `PGDATA`,
`POSTGRES_INITDB_ARGS`, host-path variables, or per-role credentials. Host
paths and host ports belong in Compose.

## API

Public catalog routes require no client key and use TMDB's `/3` paths:

```text
GET /health/live
GET /3/configuration
GET /3/movie/{movie_id}
GET /3/movie/{movie_id}/images
GET /3/tv/{tv_id}/season/{season_number}/episode/{episode_number}
GET /3/search/movie?query=matrix
```

Stored metadata reads never call TMDB on demand. Search, discover, find,
authentication/session, list, favorite/watchlist, and rating operations are
implemented locally in PostgreSQL; other supported reads return documents
captured by worker scans.

The private admin API supports status, durable job history, catalog modes,
on-demand media requests, worker controls, cancellation/retry, and
full/differential backups. Admin writes require an `Idempotency-Key`.

A `full_sweep` is phased for throughput. It imports the TMDB daily title-ID
exports in uninterrupted 500-title scheduling batches, enriches titles in
100-title batches after census work drains, then processes TV seasons and
episodes in 25-season batches. Catalog writes never enqueue image downloads.

Use `recovery` after an interrupted or dead-lettered full sweep. It streams the
latest TMDB exports in 500-ID batches, refreshes only missing/incomplete titles
or titles with a newer unresolved dead letter, then processes only unfinished
enrichment in 100-title and 25-season batches. Expected phase waiting schedules
a delayed continuation without using a job retry attempt.

`daily_sync` is the incremental production scan. It reads TMDB's movie and TV
change feeds, refreshes changed titles, and discovers newly added seasons and
episodes through the refreshed TV and season documents.

The main worker schedules `daily_sync` hourly, `missing_only` nightly, and the
lightweight `reconcile` census twice monthly by default. The five-field cron
expressions use `TZ`; an empty value disables that schedule. `full_sweep`
remains manual. Both workers drain eligible durable work when their containers
start, while the admin API can still pause, resume, start, or cancel each queue.

See the complete [API reference](docs/api.md) and the private OpenAPI document
at `/admin/v1/openapi.json`.

## On-demand media

Arrbit discovers titles through the local read API and submits one to 100
active local IDs to `POST /admin/v1/media/requests`. PostgreSQL is the only
metadata source: unknown IDs reject the whole request, and media requests never
perform TMDB metadata discovery. The media worker reads stored title, season,
episode, cast/crew, company, network, and collection paths, then downloads
bounded renditions from TMDB's image CDN.

Title, season, and episode galleries use English plus untagged image metadata
(`language=en-US`, `include_image_language=en,null`). Reusable people, company,
network, and collection assets use only the primary paths already normalized
from the requested titles.

Only validated final JPEG, PNG, or static WebP bytes are stored. Posters use
`w500`, backdrops `w1280`, episode stills `w300`, and profiles/logos `w185`
when those sizes exist in the stored TMDB configuration. There are no original,
`optimized/`, `.masters`, or variant files and no local re-encoding. Public
responses preserve upstream path fields and add versioned `local_*` URLs when
the corresponding file is ready. Videos remain metadata only; no video files
are downloaded.

## Development and stress testing

The development Compose file starts only an isolated PostgreSQL fixture:

```bash
docker compose --env-file deploy/env.example \
  -f deploy/compose.dev.yaml up -d postgres
./scripts/verify-postgres.sh
```

For the full bounded Docker Desktop test, use the Linux/Bash harness in
[stress testing](docs/stress-testing.md). It reads the real TMDB stress token
from the ignored `secrets.txt` file, injects it only into a mode-600 runtime
file, and uses a unique project with
loopback-only ports. Do not put that token in `.env`, Compose YAML, source, or
test artifacts.

## Operations

`TZ` controls catalog cron evaluation, the pgBackRest schedule, and readable
log timestamps; `.env.example` uses `America/New_York`. Persisted API and
database timestamps remain UTC. pgBackRest stores its same-host recovery
repository at `/config/backups/pgbackrest`. See
[backup and recovery](docs/backup-recovery.md).

All four services stream JSONL to Docker and persist the same stream below
`/config/logs`. The first process start uses `api.log`, `worker.log`,
`media.log`, or `postgres.log`; later starts add a numeric suffix. Each service
retains only its newest 10 files.

GitHub publishes rolling `main` images, immutable versioned images, and a
digest-pinned release Compose artifact. See [release notes](docs/release.md).

## TMDB attribution

The consuming application must display TMDB’s required attribution: “This
product uses the TMDB API but is not endorsed or certified by TMDB.” See
[TMDB’s attribution requirements](https://developer.themoviedb.org/docs/faq).
