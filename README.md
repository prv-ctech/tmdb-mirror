# TMDB Clone

Rust/PostgreSQL 18 TMDB catalog mirror with fast search, anime separation, and
local image storage.

## API

Use the API listener as your app's base URL:

```text
http://<server-host>:8080
```

The complete route and query-parameter reference is in
[docs/api.md](docs/api.md). Catalog routes do not require a client key.

Examples:

```text
GET /movies?limit=20
GET /movies/{tmdb_id}
GET /tv/{tmdb_id}
GET /anime?q=One%20Piece&type=tv
GET /search?q=matrix
GET /anime/movie/{tmdb_id}/images
GET /anime/tv/{tmdb_id}/images
```

`/metrics` is on the private admin listener and requires the application key
through `X-API-Key` or a Bearer header. Do not expose the admin listener
publicly.

Local images are served from port `8090`. Set `TMDB_MEDIA_BASE_URL` to a URL
reachable by your app, for example `http://<server-host>:8090/media`.
The single media container runs four bounded download workers by default;
adjust `TMDB_IMAGE_WORKER_CONCURRENCY` between 1 and 32 if needed.
The main worker runs up to eight ingestion loops, automatically bounded by
`TMDB_MAX_CONNECTIONS` and the TMDB rate limit.

## Deploy

The stack has four containers: PostgreSQL, API, main worker, and media worker.
GitHub Actions publishes the two Linux AMD64 images to GHCR, so a deployment
only needs the Compose file and `.env`—not a repository checkout or Dockerfile.
The same `.env` is passed to every container once through `env_file`; there is
no second copy of each setting in the Compose YAML.

```powershell
Copy-Item .env.example .env
docker compose -f docker-compose-example.yaml config --services
docker compose -f docker-compose-example.yaml pull
docker compose -f docker-compose-example.yaml up -d
```

Replace the angle-bracket values in `.env`, especially the TMDB read token and
the admin API key. Edit the host side of the PostgreSQL, `/config`, and `/media`
bind mounts in `docker-compose-example.yaml`; the paths on the right are fixed
inside the containers.

`POSTGRES_DB` and `POSTGRES_USER` are your names to choose. The stack reads
them directly from `.env`; do not add duplicate `DATABASE_*` settings.

The production template is also a full four-service file (not an include):

```powershell
docker compose -f deploy/compose.production.yaml pull
docker compose -f deploy/compose.production.yaml up -d
```

The application only uses `/media` and `/config` inside containers. Set
`TMDB_TRAWL_BASE_URL` only when an existing Trawl instance is available.

## Stress test

The disposable harness keeps generated data under `.stress-runtime/`. Copy the
ignored local template once; it holds the TMDB v4 read token, v3 API key, and
optional Trawl URL.

```powershell
Copy-Item secrets.txt.example secrets.txt
./scripts/stress-tmdb-auth.ps1
./scripts/stress-bootstrap.ps1
./scripts/stress-http.ps1 -Concurrency 100 -RequestsPerWorker 50
./scripts/stress-artwork.ps1
./scripts/stress-media-assets.ps1
./scripts/stress-trawl.ps1
./scripts/stress-resilience.ps1
./scripts/stress-collect.ps1
```

Run `./scripts/verify-repository-hygiene.ps1` before committing. Keep your
`.env`, `secrets.txt`, and generated runtime artifacts out of Git.
