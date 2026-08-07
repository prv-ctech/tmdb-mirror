# API reference

The public listener is a local TMDB v3-compatible API. Most metadata reads
return JSON documents captured from TMDB; selected query and user-state routes
are generated from local PostgreSQL data. It does not invent a second catalog
schema or an anime namespace, and it never proxies a request to TMDB on demand.

| Listener | Default host port | Contract |
| --- | ---: | --- |
| Public API | `9000` | Health and `/3/...` TMDB documents |
| Admin API | `9001` | Authenticated worker, scan, job, and backup control |
| Media | `9002` | Safe regular files below the local `/media` mount |

The default host mappings and container listeners are `9000` (public), `9001`
(admin), and `9002` (media).

## Public routes

```text
GET /health/live
GET /health/ready
GET /3/{tmdb_v3_endpoint_path}
```

`/health/live` confirms that the API process and listeners are alive.
`/health/ready` additionally verifies the PostgreSQL version, schema revision,
extensions, migrations, and read role. Compose uses the readiness route, so a
brief `starting` state during database recovery or migration is intentional.

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
- global person change/latest/popular and trending-person lists;
- linked credit, review, keyword, and TV episode-group detail documents found
  while enriching titles.

People, companies, networks, and collections are also normalized into catalog
tables from title details and credits. Their primary profile/logo/poster/
backdrop paths are available to on-demand media selection. The worker does not
currently capture dedicated reusable-entity image galleries; those gallery
routes therefore return local `404` unless a future catalog change stores them
explicitly.

Availability is local-data dependent: an official TMDB route that has not been
generated locally or captured by a scan returns local `404`, not an upstream
request. Use the official TMDB v3 path and query names unchanged. For example:

```bash
curl -sS 'http://127.0.0.1:9000/3/configuration'
curl -sS 'http://127.0.0.1:9000/3/movie/550'
curl -sS 'http://127.0.0.1:9000/3/tv/4586/images?language=en-US&include_image_language=en,null'
curl -sS 'http://127.0.0.1:9000/3/tv/4586/season/1/episode/1/images?language=en-US&include_image_language=en,null'
```

The worker captures title, season, episode, linked credit/review/keyword/
episode-group, global list/trending, and configuration documents during catalog
runs. An endpoint is not fetched on demand by the public API. TMDB image paths
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
  "local_poster_path": "http://127.0.0.1:9002/media/movies/42/posters/poster.jpg?v=0123456789abcdef",
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

Files are stored under TMDB-ID paths as exact validated CDN rendition bytes.
Title, season, and episode galleries are limited to English and untagged images
captured with `language=en-US` and `include_image_language=en,null`.
Posters and season posters target `w500`, backdrops `w1280`, episode stills
`w300`, and profiles/logos `w185`. The worker uses the largest configured
rendition at or below the target and never requests `original`. JPEG, PNG, and
static WebP are accepted; malformed, animated, oversized, SVG, GIF, and
MIME-mismatched responses are rejected. No local resize, re-encoding,
`optimized/`, `.masters`, variant, original, or video directory exists.

## Admin API

The admin listener requires `X-API-Key` or a bearer token containing
`TMDB_ADMIN_API_KEY`. Production publishes it on host port `9001`; protect
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
| `POST` | `/admin/v1/scans` | `full_sweep`, `missing_only`, `recovery`, `prune_cleanup`, `daily_sync`, or `reconcile` |
| `GET` | `/admin/v1/worker` | Main-worker state |
| `POST` | `/admin/v1/worker` | Main-worker `start`, `pause`, `resume`, or `cancel` |
| `POST` | `/admin/v1/jobs/{job_id}/cancel` | Cancel one eligible job |
| `POST` | `/admin/v1/jobs/{job_id}/retry` | Retry one terminal job |
| `POST` | `/admin/v1/media/requests` | Queue 1–100 active local movie/TV IDs |
| `GET` | `/admin/v1/media/requests/{request_id}` | Aggregate durable media-request status |
| `GET` | `/admin/v1/media/worker` | Media-worker state |
| `POST` | `/admin/v1/media/worker` | Media-worker `start`, `pause`, `resume`, or `cancel` |
| `POST` | `/admin/v1/maintenance/analyze` | Fixed catalog statistics maintenance |
| `GET` | `/admin/v1/backups` | Backup state |
| `POST` | `/admin/v1/backups` | Full or differential backup |

Queue counts in `/admin/v1/status` are split deliberately. `active` is the
live backlog (`queued`, `running`, and `retry_wait`); `retained` includes
terminal history and is not backlog. Use `active` for queue alarms.
`prune_cleanup` removes old, unreferenced terminal job history in bounded
batches. Completed media requests retain aggregate counters while old terminal
job links are released after retention. Cleanup remains an explicit operator
action.

`GET /admin/v1/jobs` accepts `limit` (`1..=100`, default `50`), an opaque
`cursor`, `status`, and `jobType`. Job responses omit raw payloads and
idempotency keys. `/admin/v1/status` reports build/schema identity, database
size and connections, API pool state, movie/TV totals, bounded queue groups,
component heartbeats, and backup state; all API timestamps are UTC.

`full_sweep` imports TMDB's daily movie and TV ID exports in uninterrupted
500-title scheduling batches. Durable 100-title enrichment batches begin only
after census work drains, and 25-season TV season/episode batches begin only
after enrichment drains. Catalog writes do not submit image work. `daily_sync`
reads TMDB's movie and TV change feeds,
refreshes changed titles, and discovers new seasons and episodes from the
refreshed documents.

`recovery` is the bounded repair path for an interrupted full sweep. It reads
the latest official exports in 500-ID chunks, requeues absent/incomplete title
details and unresolved dead letters newer than stored source data, then queues
only unfinished title enrichment in batches of 100 and TV season enrichment in
batches of 25. Completion found in retained successful job history is preserved
during the schema upgrade. Normal phase waiting creates a delayed, idempotent
continuation and does not consume retry attempts.

`reconcile` compares official movie/TV ID exports with the local catalog,
inserts new IDs, refreshes new/incomplete/dead-lettered titles, and deactivates
IDs absent from the authoritative export. It does not re-enrich complete
titles. By default the main worker schedules `daily_sync` hourly,
`missing_only` nightly, and `reconcile` twice monthly using the five-field cron
values and `TZ`. `full_sweep` is never scheduled. If the last successful change
window is older than TMDB's 14-day range, status exposes
`fullSweepRequired: true`. A slot that collides with active catalog maintenance
remains pending and is retried; a watermark is not advanced while a child job
from that scan remains unresolved in the dead-letter state.

Example catalog scan:

```bash
curl -sS -X POST http://127.0.0.1:9001/admin/v1/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: full-sweep-example-001' \
  -d '{"mode":"full_sweep","mediaTypes":["movie","tv"]}'
```

Recovery scan:

```bash
curl -sS -X POST http://127.0.0.1:9001/admin/v1/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: catalog-recovery-example-001' \
  -d '{"mode":"recovery","mediaTypes":["movie","tv"]}'
```

Both workers begin draining eligible durable work when their containers start.
The controls remain useful for an operational pause or cancellation:

```bash
curl -sS -X POST http://127.0.0.1:9001/admin/v1/worker \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: worker-start-example-001' \
  -d '{"action":"start"}'

curl -sS -X POST http://127.0.0.1:9001/admin/v1/scans \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: daily-sync-example-001' \
  -d '{"mode":"daily_sync","mediaTypes":["movie","tv"]}'
```

The media worker is independent. Submit a single or bulk request with the same
endpoint; the request remains durable if the media container is offline:

```bash
curl -sS -X POST http://127.0.0.1:9001/admin/v1/media/requests \
  -H "X-API-Key: $TMDB_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: dashboard-example-001' \
  -d '{"items":[{"mediaType":"movie","tmdbId":550},{"mediaType":"tv","tmdbId":119495}]}'
```

An accepted submission returns `202`:

```json
{
  "data": {
    "requestId": "7b4df2bc-65c7-4ac0-bfb4-f9a0d6d8f08d",
    "duplicate": false
  }
}
```

The request accepts 1–100 unique items. Duplicate items are removed before
submission. If any item is not an active `catalog.titles` row, the entire
request returns `422` with the invalid items and nothing is persisted. A reused
idempotency key with the same normalized payload returns the original request;
a different payload returns `409`. Admission beyond 1,000 active unique title
items returns `429` with `Retry-After`. Status reports title and source counts,
queued/downloading/ready/reused/deleted/failed totals, and incomplete-catalog
items. An incomplete known title downloads every currently stored source and
finishes `partial`; it never triggers upstream metadata lookup.

`GET /admin/v1/media/requests/{request_id}` returns one of `queued`, `running`,
`succeeded`, `partial`, `failed`, or `cancelled`, plus UTC timestamps and the
aggregate counters described above.

`pause` stops new claims and lets the active job finish. `cancel` stops the
worker state, cancels queued work, and requests cancellation for active work.
The container remains running after either action.

Catalog and media controls are independent. Cancelling catalog work cannot
create or cancel media requests because catalog writes never enqueue images.

## Official references

- [TMDB API getting started](https://developer.themoviedb.org/docs/getting-started)
- [TMDB API reference](https://developer.themoviedb.org/reference/intro/getting-started)
- [TMDB image languages](https://developer.themoviedb.org/docs/image-languages)
