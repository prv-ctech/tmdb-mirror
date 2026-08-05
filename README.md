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
| `api` | `9001`, `8081` | Public catalog and authenticated admin APIs |
| `worker` | none | Migrations and explicitly submitted ingest jobs |
| `media` | `9002` | Downloaded public image files |

The API container listens on `8080` for the public catalog API and `8081` for
the authenticated admin API. Compose publishes them as host ports `9001` and
`8081` respectively.

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

The external network named in the Compose file must already exist. The Git
examples use `your.network` as a neutral placeholder; replace every
`"your.network"` network reference with your existing Docker network name
before starting the stack. All four services use this one external network.
The application paths inside containers are fixed:
`/config` for scratch, exports, checkpoints, logs, and backups; `/media` for
public image files.

To reuse existing host directories, edit the `source:` values in the Compose
file. Host mount paths are deployment settings, not application environment.

`docker-compose-example.yaml` is a standalone copy-pasteable Compose file. It
does not use Compose `include`; place it beside the `.env` file.
New checkout deployments can use the canonical file above. See
[production deployment](docs/deployment-production.md) for bind mounts,
permissions, media policy, and validation.
The [documentation map](docs/README.md) separates current operator contracts
from historical design records.

These files replace the older deployment contract. The four services and host
ports remain the same, but the current files use the `tmdb-runtime` startup
wrapper for worker/media storage preparation, relative Compose bind sources,
and publish the authenticated admin listener on host port `8081`. The root
standalone file reads `.env` beside it; the production file reads `../.env` by
default or the file selected by `TMDB_ENV_FILE`.

The current `.env.example` is intentionally minimal. Enter database owner
credentials, the TMDB read token, admin key, API base URL, media settings, and
worker tuning shown there. Do not carry forward listener binds, `PGDATA`,
`POSTGRES_INITDB_ARGS`, host-path variables, per-role credentials, or scheduler
toggles from an older environment. Host paths and host ports belong in
Compose.

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

The private admin API supports status, durable job history, explicit
`full_sweep`, `missing_only`, `prune_cleanup`, and `daily_sync` scans,
cancellation/retry, media audits, persistent worker controls, and
full/differential backup requests. Admin writes require an
`Idempotency-Key`; scans, job operations, and backups return durable operation
IDs, while worker controls return the persisted worker state.

A `full_sweep` is phased for throughput. It imports the TMDB daily title-ID
exports in uninterrupted 500-title scheduling batches, enriches titles in
100-title batches after census work drains, then processes TV seasons and
episodes in 25-season batches. Reusable people, company, network, and
collection galleries remain part of explicit media scans.

`daily_sync` is the incremental production scan. It reads TMDB's movie and TV
change feeds, refreshes changed titles, and discovers newly added seasons and
episodes through the refreshed TV and season documents.

Neither worker starts queue processing automatically after a restart. A
previously running worker is reset to stopped; a paused worker remains paused.
Start the main worker before submitting a catalog scan. Start the media worker
only when image downloads should run; a catalog scan may create image jobs when
`ALLOW_LOCAL_MEDIA=true`, but it does not start the media worker.

See the complete [API reference](docs/api.md) and the private OpenAPI document
at `/admin/v1/openapi.json`.

## Media galleries

TMDB posters, backdrops, logos, season images, episode thumbnails, and
reusable-entity galleries are downloaded from the dedicated TMDB image
endpoints. Originals stay in TMDB-ID folders; one JPEG or PNG derivative is
stored below `optimized/`. Episode thumbnails are optimized-only. Videos are
stored as metadata (`site`, `key`, type, name, official/language/publication
fields); no video files are downloaded. The current `/3/.../videos` response
preserves TMDB's document and does not add a synthesized provider URL.

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

`TZ=America/New_York` controls the pgBackRest schedule and readable log
timestamps; persisted API and database timestamps remain UTC. pgBackRest stores
its same-host recovery repository at `/config/backups/pgbackrest`. See
[backup and recovery](docs/backup-recovery.md).

GitHub publishes rolling `main` images, immutable versioned images, and a
digest-pinned release Compose artifact. See [release notes](docs/release.md).

## TMDB attribution

The consuming application must display TMDB’s required attribution: “This
product uses the TMDB API but is not endorsed or certified by TMDB.” See
[TMDB’s attribution requirements](https://developer.themoviedb.org/docs/faq).
