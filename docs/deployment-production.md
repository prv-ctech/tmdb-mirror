# Four-container deployment

`deploy/compose.production.yaml` includes the canonical
`deploy/compose.compact.yaml`. The stack is deliberately only four services:

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
| PostgreSQL `/var/lib/postgresql` | PostgreSQL 18 data/WAL |

Edit only the left-hand side of the bind mounts in Compose or the Unraid
template. A normal Unraid mapping is:

```text
/media  -> <host-media-directory>
/config -> <host-config-directory>
```

The API is read-only; the worker and media process run as UID/GID `10001`.
Ensure the selected `/config` and `/media` host directories are writable by
that identity before startup. The stack intentionally has no storage-init
container that changes host ownership.

No `TMDB_MEDIA_HOST_ROOT`, `TMDB_WORK_HOST_ROOT`, `TMDB_MEDIA_ROOT`,
`TMDB_WORK_ROOT`, or similar host-path environment variable is read by the
application.

## Secrets and startup

Create the file-backed secrets referenced by `deploy/compose.compact.yaml` in
`deploy/secrets/` (or change those deployment-only file mappings). The root
`docker-compose-example.yaml` uses the same filenames and is the easiest file
to inspect when learning the topology. Database role secrets are 43-byte
base64url values without a trailing newline. Keep the TMDB read token and API
key file-backed and outside source control.

Copy `deploy/env.production.example` to the ignored `deploy/env.production`,
replace its reserved example hostnames, and use that file for Compose:

```powershell
Copy-Item deploy/env.production.example deploy/env.production
./scripts/validate-production-compose.ps1
docker compose --env-file deploy/env.production -f deploy/compose.production.yaml up -d --build
```

The main worker applies migrations under the existing PostgreSQL advisory lock;
restarts are safe. It then expands one idempotent changes-sync slot per media
namespace into the durable job table. The media worker claims only image jobs.

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

```powershell
./scripts/validate-production-compose.ps1
docker compose -f deploy/compose.production.yaml ps
docker compose -f deploy/compose.production.yaml logs --no-color --tail=200 postgres worker api media
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
