 # API reference

The public listener is a local TMDB v3-compatible API. Most metadata reads
return JSON documents captured from TMDB; selected query and user-state routes
are generated from local PostgreSQL data. It does not invent a second catalog
schema or an anime namespace, and it never proxies a request to TMDB on demand.

| Listener | Port | Contract |
| --- | ---: | --- |
| Public API | `9001` | Health and `/3/...` TMDB documents |
| Admin API | `8081` | Authenticated worker, scan, job, and backup control |
| Media | `9002` | Safe regular files below the local `/media` mount |

## Public routes

```text
GET /health/live
GET /health/ready
GET /3/{tmdb_v3_endpoint_path}
```

These routes are generated locally rather than loaded from the document store:

- `search/movie`, `search/tv`, `search/multi`, `search/person`,
  `search/collection`, `search/company`, and `search/keyword`;
- `discover/movie`, `discover/tv`, and `find/{external_id}`;
- authentication tokens, guest sessions, sessions, account lists,
  favorite/watchlist state, local lists, and movie/TV/episode ratings.

Other `/3/{tmdb_v3_endpoint_path}` reads return the matching document captured
by a worker scan. Query strings are canonicalized; a stored default page or
language document can satisfy the equivalent request without those defaults.
A missing generated resource or captured document returns the TMDB not-found
shape:

```json
{
  "status_code": 34,
  "status_message": "The resource you requested could not be found.",
  "success": false
}
```

Worker enrichment currently captures these TMDB v3 document families:

- configuration, certifications, changes, genres, keywords, trending, and
  watch providers;
- movie lists and movie details, including account states, alternative titles,
  changes, credits, external IDs, images, keywords, lists, recommendations,
  release dates, reviews, similar, translations, videos, and watch providers;
- TV lists and TV details, including aggregate credits, content ratings,
  episode groups, and the same title suffixes where TMDB provides them;
- TV seasons and episodes, including details, credits, external IDs, images,
  translations, videos, and watch providers;
- people, person changes, combined/movie/TV credits, external IDs, images,
  tagged images, and translations;
- collections, companies, networks, reviews, and their detail/image/name
  endpoints.

Availability is local-data dependent: an official TMDB route that has not been
generated locally or captured by a scan returns local `404`, not an upstream
request. Use the official TMDB v3 path and query names unchanged. For example:

```bash
curl -sS 'http://127.0.0.1:9001/3/configuration'
curl -sS 'http://127.0.0.1:9001/3/movie/550'
curl -sS 'http://127.0.0.1:9001/3/tv/4586/images?language=en-US&include_image_language=en,null'
curl -sS 'http://127.0.0.1:9001/3/tv/4586/season/1/episode/1/images?language=en-US&include_image_language=en,null'
```

The worker captures title, season, episode, linked credit/review/keyword,
reusable-entity, list/trending, and configuration documents during explicit
scans. An endpoint is not fetched on demand by the public API. TMDB image paths
are preserved and an additive local field contains
the matching full media URL when the asset is ready, or `null` when it is not.
Set `TMDB_MEDIA_BASE_URL` to the public base URL of the media listener.

| TMDB field | Local field |
| --- | --- |
| `file_path` | `local_file_path` |
| `poster_path` | `local_poster_path` |
| `backdrop_path` | `local_backdrop_path` |
| `profile_path` | `local_profile_path` |
| `logo_path` | `local_logo_path` |
| `still_path` | `local_still_path` |

For example:

```json
{
  "poster_path": "/upstream-poster.jpg",
  "local_poster_path": "http://127.0.0.1:9002/media/movies/42/posters/poster.jpg",
  "backdrop_path": "/upstream-backdrop.jpg",
  "local_backdrop_path": null
}
```

The media worker records ready local assets in PostgreSQL. The API resolves
the additive fields from those records; the workers do not communicate
directly and the stored upstream TMDB document is not modified.

## Video metadata

Title video rows are normalized in PostgreSQL by `site`, `video_key`,
`video_type`, name, official flag, language, country, publication time, and
size. `/3/movie/{id}/videos` and `/3/tv/{id}/videos` return the captured TMDB
document, including its provider `site` and `key`. The current API does not add
a provider `url` field. No video files are downloaded or served by the media
listener.

## Media files

The media listener exposes safe regular files below `/media`:

```text
GET /health/live
GET /healthz
GET /media/{relative_path}
```

Hidden paths, traversal, symlink escapes, missing files, and directories return
`404`. Successful files include a content type derived from the extension, an
immutable one-year cache policy, and a weak ETag; matching `If-None-Match`
requests return `304`.

Files are stored under TMDB-ID paths. Originals are outside `optimized/`;
optimized files use JPEG quality 85 at width 640 for posters, seasons,
profiles, and thumbnails, width 1280 for backdrops, and transparent PNG width
500 for logos. Episode stills are optimized-only. No WebP derivative, `full`
variant, `.masters` directory, video file, or `/videos` media folder exists.

## Admin API

The admin listener requires `X-API-Key` or a bearer token containing
`TMDB_ADMIN_API_KEY`. Production publishes it on host port `8081`; protect
that port with the host firewall and keep the key secret.

Every state-changing request requires an `Idempotency-Key`. Scans, job
operations, and backups return durable operation IDs; worker controls return
the persisted worker state. Reusing a key with the same request is idempotent;
reusing it with a different request returns `409`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/v1/openapi.json` | Private OpenAPI document |
| `GET` | `/metrics` | Prometheus metrics for the bounded status projection |
| `GET` | `/admin/v1/status` | Bounded operational status |
| `GET` | `/admin/v1/jobs` | Bounded durable-job page |
| `GET` | `/admin/v1/jobs/{job_id}` | Job and immutable events |
| `POST` | `/admin/v1/scans` | `full_sweep`, `missing_only`, `prune_cleanup`, or `daily_sync` |
| `GET` | `/admin/v1/worker` | Main-worker state |
| `POST` | `/admin/v1/worker` | Main-worker `start`, `pause`, `resume`, or `cancel` |
| `POST` | `/admin/v1/jobs/{job_id}/cancel` | Cancel one eligible job |
| `POST` | `/admin/v1/jobs/{job_id}/retry` | Retry one terminal job |
| `POST` | `/admin/v1/media/scans` | Durable media `full`, `missing`, or `audit` scan |
| `GET` | `/admin/v1/media/scans/{run_id}` | Media-scan status |
| `GET` | `/admin/v1/media/worker` | Media-worker state |
| `POST` | `/admin/v1/media/worker` | Media-worker `start`, `pause`, `resume`, or `cancel` |
| `POST` | `/admin/v1/media/audits` | Non-destructive media audit |
| `POST` | `/admin/v1/maintenance/analyze` | Fixed catalog statistics maintenance |
| `GET` | `/admin/v1/backups` | Backup state |
| `POST` | `/admin/v1/backups` | Full or differential backup |

Queue counts in `/admin/v1/status` are split deliberately. `active` is the
live backlog (`queued`, `running`, and `retry_wait`); `retained` includes
terminal history and is not backlog. Use `active` for queue alarms.
`prune_cleanup` removes old, unreferenced terminal job history in bounded
batches. Once a completed scan is past the retention window, its child-job
links are released; the scan root and its aggregate counters remain available
for audit. Terminal cleanup uses retention indexes and remains an explicit
operator action.

`GET /admin/v1/jobs` accepts `limit` (`1..=100`, default `50`), an opaque
`cursor`, `status`, and `jobType`. Job responses omit raw payloads and
idempotency keys. `/admin/v1/status` reports build/schema identity, database
size and connections, API pool state, movie/TV totals, bounded queue groups,
component heartbeats, and backup state; all API timestamps are UTC.

`full_sweep` imports TMDB's daily movie and TV ID exports in uninterrupted
500-title scheduling batches. Durable 100-title enrichment batches begin only
after census work drains, and 25-season TV season/episode batches begin only
after enrichment drains. Media downloads and reusable-entity galleries require
a separate media scan. `daily_sync` reads TMDB's movie and TV change feeds,
refreshes changed titles, and discovers new seasons and episodes from the
refreshed documents.

Example catalog scan:

```bash
curl -sS -X POST http://127.0.0.1:8081/admin/v1/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: full-sweep-20260803' \
  -d '{"mode":"full_sweep","mediaTypes":["movie","tv"]}'
```

Start the worker before submitting a scan. A scan submission can remain queued
while the worker is stopped:

```bash
curl -sS -X POST http://127.0.0.1:8081/admin/v1/worker \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: worker-start-20260803' \
  -d '{"action":"start"}'

curl -sS -X POST http://127.0.0.1:8081/admin/v1/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: daily-sync-20260803' \
  -d '{"mode":"daily_sync","mediaTypes":["movie","tv"]}'
```

The media worker is independent. Starting it drains eligible image/audit jobs;
submitting a media scan does not bypass its stopped state:

```bash
curl -sS -X POST http://127.0.0.1:8081/admin/v1/media/worker \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: media-start-20260803' \
  -d '{"action":"start"}'

curl -sS -X POST http://127.0.0.1:8081/admin/v1/media/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: media-missing-20260803' \
  -d '{"mode":"missing","repair":false}'
```

`pause` stops new claims and lets the active job finish. `cancel` stops the
worker state, cancels queued work, and requests cancellation for active work.
The container remains running after either action.

Catalog and media controls are independent. For an emergency stop, cancel the
main worker first, wait for its active catalog jobs to settle, then cancel the
media worker. This second step clears any image jobs committed by catalog work
that was already in flight when the first cancellation was requested.

## Official references

- [TMDB API getting started](https://developer.themoviedb.org/docs/getting-started)
- [TMDB API reference](https://developer.themoviedb.org/reference/intro/getting-started)
- [TMDB image languages](https://developer.themoviedb.org/docs/image-languages)
