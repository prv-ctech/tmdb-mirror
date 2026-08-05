# API reference

The public listener is a local, read-only TMDB v3 document mirror. It returns
the JSON document captured from the upstream endpoint; it does not invent a
second catalog schema or an anime namespace.

| Listener | Port | Contract |
| --- | ---: | --- |
| Public API | `9001` | Health and `/3/...` TMDB documents |
| Admin API | `8081` | Authenticated worker, scan, job, and backup control |
| Media | `9002` | Verified local image files |

## Public routes

```text
GET /health/live
GET /health/ready
GET /3/{tmdb_v3_endpoint_path}
```

Implemented search, discovery, account, and write routes query the local
database directly. Other `/3/{tmdb_v3_endpoint_path}` reads return the exact
document captured by a worker scan. The public API never fetches TMDB on
demand. A missing local document returns the TMDB not-found shape:

```json
{
  "status_code": 34,
  "status_message": "The resource you requested could not be found.",
  "success": false
}
```

The route accepts the TMDB v3 read surface, including:

- configuration, certifications, changes, discover, find, genres, keywords,
  search, trending, and watch providers;
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

Use the official TMDB v3 path and query names unchanged. For example:

```bash
curl -sS 'http://127.0.0.1:9001/3/configuration'
curl -sS 'http://127.0.0.1:9001/3/movie/550'
curl -sS 'http://127.0.0.1:9001/3/tv/4586/images?language=en-US&include_image_language=en,null'
curl -sS 'http://127.0.0.1:9001/3/tv/4586/season/1/episode/1/images?language=en-US&include_image_language=en,null'
```

The worker captures title, season, episode, reusable-entity, and configuration
documents during explicit scans. An endpoint is not fetched on demand by the
public API. TMDB image paths are preserved and an additive local field contains
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

## Media files

The media listener exposes only verified local files:

```text
GET /health/live
GET /healthz
GET /media/{relative_path}
```

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
terminal history and is not backlog. Use `active` for queue alarms. `prune_cleanup` removes old,
unreferenced terminal job history in bounded batches. Once a completed scan is
past the retention window, its child-job links are released; the scan root and
its aggregate counters remain available for audit. Terminal cleanup uses
retention indexes and remains an explicit operator action.

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

Example worker control:

```bash
curl -sS -X POST http://127.0.0.1:8081/admin/v1/worker \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: worker-start-20260803' \
  -d '{"action":"start"}'
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
