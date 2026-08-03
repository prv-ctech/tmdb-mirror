# TMDB Mirror

Rust + PostgreSQL 18 mirror of TMDB metadata with catalog search, strict anime
isolation, local responsive image storage, durable worker jobs, and four
runtime services: PostgreSQL, API, main worker, and media worker.

## Run with Docker Compose

`deploy/compose.production.yaml` is the canonical checkout deployment. It
pulls the published Linux AMD64 images and uses relative `./data` bind mounts:

| Service | Host port | Purpose |
| --- | ---: | --- |
| `postgres` | none | PostgreSQL 18, migrations, WAL archiving, and pgBackRest |
| `api` | `9001` | Public catalog API |
| `worker` | none | Migrations, schedules, ingest, retries, and durable jobs |
| `media` | `9002` | Downloaded public image files |

The admin listener remains on container port `8081` and is not published by
the production file. A container on the existing `prv.network` can reach it at
`http://tmdb-mirror-api:8081`.

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

The external `prv.network` must already exist. The application paths inside
containers are fixed: `/config` for scratch, exports,
checkpoints, logs, and backups; `/media` for public files and private
`.masters` originals. Masters are never served.

To reuse existing host directories, edit the `source:` values in the Compose
file. Host mount paths are deployment settings, not application environment.

`docker-compose-example.yaml` is a standalone copy-pasteable Compose file. It
does not use Compose `include`; place it beside the `.env` file.
New checkout deployments can use the canonical file above. See
[production deployment](docs/deployment-production.md) for bind mounts,
permissions, media policy, and validation.

## API

Public catalog routes require no client key and have both unversioned and
stable `/v1` forms:

```text
GET /v1/health/live
GET /v1/movies?genreId=28&language=en
GET /v1/tv/{tmdb_id}
GET /v1/anime?q=one%20piece&type=tv
GET /v1/search?q=matrix&limit=20
GET /v1/openapi.json
```

Movie and TV routes never return anime. Anime routes remain isolated and can
search both anime media types unless `type=movie` or `type=tv` is supplied.
The private admin API supports status, durable job history, explicit scans,
cancellation/retry, non-destructive media audits, allowlisted analyze jobs,
and full/differential backup requests. Admin writes require
`Idempotency-Key` and return `202 Accepted` durable jobs.

See the complete [API reference](docs/api.md), or query the public and private
OpenAPI documents at `/v1/openapi.json` and `/admin/v1/openapi.json` from their
respective listeners.

## Development and stress testing

The development Compose file starts only an isolated PostgreSQL fixture:

```bash
docker compose --env-file deploy/env.example \
  -f deploy/compose.dev.yaml up -d postgres
./scripts/verify-postgres.sh
```

For the full bounded Docker Desktop test, use the Linux/Bash harness in
[stress testing](docs/stress-testing.md). It reads the real TMDB stress token
from the ignored `.secrets.txt` file (with `secrets.txt` as a local fallback),
injects it only into a mode-600 runtime file, and uses a unique project with
loopback-only ports. Do not put that token in `.env`, Compose YAML, source, or
test artifacts.

## Operations

`TZ=America/New_York` controls schedule interpretation and readable log
timestamps; persisted API and database timestamps remain UTC. pgBackRest stores
its same-host recovery repository at `/config/backups/pgbackrest`. See
[backup and recovery](docs/backup-recovery.md).

GitHub publishes rolling `main` images, immutable versioned images, and a
digest-pinned release Compose artifact. See [release notes](docs/release.md).

## TMDB attribution

The consuming application must display TMDB’s required attribution: “This
product uses the TMDB API but is not endorsed or certified by TMDB.” See
[TMDB’s attribution requirements](https://developer.themoviedb.org/docs/faq).
