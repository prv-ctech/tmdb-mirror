# TMDB Mirror

Rust + PostgreSQL 18 mirror of TMDB metadata with fast catalog search, strict
anime isolation, local responsive image storage, and four production
containers: PostgreSQL, API, main worker, and media worker.

## Run

```powershell
Copy-Item .env.example .env
docker compose -f docker-compose-example.yaml pull
docker compose -f docker-compose-example.yaml up -d
```

Set the TMDB read token, PostgreSQL password, and admin API key in `.env`.
`POSTGRES_DB` and `POSTGRES_USER` are configurable and used directly by every
service; no duplicate `DATABASE_*` settings exist. Container paths are fixed:
`/config` for cache/exports/logs and `/media` for final media. Choose host
paths only in Compose or Unraid mounts.

The public API is `http://<host>:8080`; local media is normally
`http://<host>:8090/media`. The private admin listener is not host-published:
containers on `prv.network` use `http://tmdb-mirror-api:8081`.

## API

Public paths have compatible unversioned and `/v1` forms:

```text
GET /v1/movies?genreId=28&language=en
GET /v1/tv/{tmdb_id}
GET /v1/anime?q=one%20piece
GET /v1/search?q=matrix
GET /v1/openapi.json
```

Movie/TV routes never return anime. Anime routes search both anime movies and
TV unless `type=movie` or `type=tv` is given. See [API reference](docs/api.md).

Admin uses `X-API-Key` or Bearer authentication and supports status, durable
job history, explicit scans, cancellation/retry, non-destructive media audits,
allowlisted analyze jobs, and full/differential backup requests. Admin writes
require `Idempotency-Key` and return `202 Accepted` jobs.

## Operations

`TZ=America/New_York` controls schedules and readable log timestamps; persisted
timestamps remain UTC. pgBackRest lives inside PostgreSQL and stores its local
repository only at `/config/backups/pgbackrest`. See
[backup and recovery](docs/backup-recovery.md).

The optional k6 runner is an ephemeral test container, not a fifth production
service. Run [stress testing](docs/stress-testing.md) deliberately before a
release. GitHub publishes rolling `main`, immutable `vX.Y.Z` images, and a
digest-pinned release Compose artifact; details are in [release notes](docs/release.md).

## TMDB attribution

The consuming application must display TMDB’s required attribution: “This
product uses the TMDB API but is not endorsed or certified by TMDB.” See
[TMDB’s attribution requirements](https://developer.themoviedb.org/docs/faq).
