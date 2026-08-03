# API reference

The API has three listeners in the four-container deployment:

| Listener | Published address | Contract |
| --- | --- | --- |
| Public catalog | `http://<host>:9001` | Unauthenticated catalog, search, health, and the public OpenAPI document |
| Private admin | `http://<private-host>:8081` | API-key protected operations, jobs, backups, and metrics |
| Media | `http://<host>:9002` | Public verified image files and media health |

The production Compose file publishes host ports `9001` and `9002` to the API
and media container listeners `8080` and `8090`. The admin listener is available
to containers on `prv.network` as
`http://tmdb-mirror-api:8081`; the disposable stress Compose file publishes it
to a loopback-only test port.

Public catalog routes are available with both their existing unversioned path
and their stable `/v1` alias. New clients should use `/v1`. The generated
public contract is `GET /v1/openapi.json`; the private contract is
`GET /admin/v1/openapi.json` on the admin listener.

`{tmdb_id}` is a positive TMDB ID. `{media_type}` is `movie` or `tv`. JSON
properties use camelCase. Public list responses use `{ "data": [...],
"nextCursor": ... }`; non-paged resources use `{ "data": ... }`.

## Health and OpenAPI

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health/live` or `/v1/health/live` | Process liveness; does not require PostgreSQL |
| `GET` | `/health/ready` or `/v1/health/ready` | PostgreSQL, schema, extensions, and role readiness |
| `GET` | `/v1/openapi.json` | Machine-readable public catalog route document |

Example:

```bash
curl -i http://127.0.0.1:9001/v1/health/live
curl -sS http://127.0.0.1:9001/v1/openapi.json
```

## Common catalog query parameters

`limit` defaults to 20 and accepts 1–100. `cursor` is returned by a paged
movie, TV, or anime list and must be sent back unchanged. Cursor formats are
internal contract values; do not construct or modify them.

`q` is the search term. It is required by `/search`, optional on the popular
anime route, limited to 256 characters, and is accent-insensitive. `query` is
not an alias for `q`.

`type` and `mediaType` are accepted aliases for `movie` or `tv` where a route
allows media selection. The following filter aliases are accepted on search,
movie/TV lists, and anime lists:

| Filter | Accepted names | Example |
| --- | --- | --- |
| Genre | `genreId` or `genre` | `genreId=28` |
| Keyword | `keywordId` or `keyword` | `keywordId=210024` |
| Tag | `tagId` or `tag` | `tagId=7` |
| Language | `language` or `lang` | `language=en` |
| Runtime | `runtimeMin`/`lengthMin`, `runtimeMax`/`lengthMax` | `runtimeMin=90` |
| Person | `personId`, `person`, `actorId`, or `actor` | `actorId=500` |
| Company | `companyId`, `company`, `studioId`, or `studio` | `studioId=420` |
| Network | `networkId` or `network` | `networkId=213` |
| Year | `year` | `year=2024` |
| Release status | `status` | `status=Released` |

Unknown, duplicate, malformed, or route-incompatible parameters return `400`.

## Movies and TV

These routes always exclude anime. The list routes accept `limit`, `cursor`, and
the filters above, but not `q`, `type`, or `anime`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/movies` or `/v1/movies/popular` | Popular non-anime movies |
| `GET` | `/v1/movies/recent` | Recently released non-anime movies |
| `GET` | `/v1/movies/top-rated` | Highest-rated non-anime movies |
| `GET` | `/v1/movies/{tmdb_id}` | Full movie metadata and facets |
| `GET` | `/v1/movies/{tmdb_id}/credits` | Movie cast and crew |
| `GET` | `/v1/movies/{tmdb_id}/images` | Movie image metadata and URLs |
| `GET` | `/v1/tv` or `/v1/tv/popular` | Popular non-anime TV series |
| `GET` | `/v1/tv/recent` | Recently aired non-anime TV series |
| `GET` | `/v1/tv/top-rated` | Highest-rated non-anime TV series |
| `GET` | `/v1/tv/{tmdb_id}` | Full TV metadata and facets |
| `GET` | `/v1/tv/{tmdb_id}/credits` | TV cast and crew |
| `GET` | `/v1/tv/{tmdb_id}/images` | TV image metadata and URLs |
| `GET` | `/v1/tv/{tmdb_id}/seasons` | All seasons |
| `GET` | `/v1/tv/{tmdb_id}/seasons/{season_number}` | One season |
| `GET` | `/v1/tv/{tmdb_id}/seasons/{season_number}/images` | Season gallery |
| `GET` | `/v1/tv/{tmdb_id}/seasons/{season_number}/episodes` | All episodes in one season |
| `GET` | `/v1/tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}` | One episode |
| `GET` | `/v1/tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}/images` | Episode thumbnail metadata |

The following title facets are available on the matching detail routes below;
movies use `/release-dates`, while TV uses `/certifications`:

| Suffix | Data |
| --- | --- |
| `/translations` | Localized title, overview, tagline, and homepage |
| `/alternate-titles` | Regional and typed alternate names |
| `/external-ids` | Known IMDB, TVDB, Wikidata, and social identifiers |
| `/videos` | TMDB video metadata; video files are not proxied |
| `/release-dates` | Movie regional dates, release types, and certifications |
| `/certifications` | TV regional certifications |

Movie and TV detail routes return `404` when the ID belongs to the opposite
anime partition. This prevents an anime title from leaking into the normal
catalog.

## Anime

Anime routes return only titles with both the TMDB `anime` keyword and the
`Animation` genre. The popular anime route becomes anime-only search when `q`
is supplied. `type=movie` or `type=tv` narrows a list or search; omitting it
searches both namespaces.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/anime` or `/v1/anime/popular` | Popular anime movies and TV; optional `q` searches |
| `GET` | `/v1/anime/recent` | Recently released/aired anime |
| `GET` | `/v1/anime/top-rated` | Highest-rated anime |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}` | Anime movie or TV metadata |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/credits` | Anime cast and crew |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/images` | Anime image metadata and URLs |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/seasons/{season_number}/images` | Anime season gallery |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}/images` | Anime episode thumbnail metadata |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/translations` | Anime translations |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/alternate-titles` | Anime alternate titles |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/external-ids` | Anime external IDs |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/videos` | Anime video metadata |
| `GET` | `/v1/anime/{media_type}/{tmdb_id}/release-dates` | Anime regional dates/certifications |

The anime `recent` and `top-rated` routes are list routes; they do not accept
`q`. All anime list routes accept the common filters and `type`/`mediaType`.

## Search, discovery, trends, and calendars

### Search

`GET /v1/search` requires `q`, accepts `type`/`mediaType`, `limit`, and the
catalog filters, and searches only non-anime titles.

```text
GET /v1/search?q=one%20piece&type=tv&limit=20
GET /v1/movies?genreId=28&language=en&runtimeMax=140&limit=20
GET /v1/tv/top-rated?networkId=213&year=2024
GET /v1/anime?q=one%20piece
```

### Discovery dimensions

The following routes return `{ "data": [...] }`. They accept `q`, `limit`,
and `anime=true|false`; they do not accept `cursor`, `type`, or catalog
filters.

| Path | Data |
| --- | --- |
| `/v1/genres` | Genres |
| `/v1/languages` | Languages |
| `/v1/keywords` | TMDB keywords |
| `/v1/tags` | Local tags |
| `/v1/people` | Cast and crew people |
| `/v1/companies` | Production companies/studios |
| `/v1/networks` | TV networks |
| `/v1/collections` | Movie collections |

### Trending

| Path | Query | Purpose |
| --- | --- | --- |
| `/v1/trending/day` or `/v1/trending/week` | optional `type`/`mediaType`, `limit` | Non-anime trend rankings |
| `/v1/anime/trending/day` or `/v1/anime/trending/week` | optional `type`/`mediaType`, `limit` | Anime-only trend rankings |

Trending routes reject `q`, `cursor`, `anime`, and catalog filters. Rankings
are refreshed by durable worker jobs; a missing or stale ranking returns a
problem response rather than performing an unbounded upstream request.

### Calendar

`/v1/calendar/movies` and `/v1/calendar/tv` require `start=YYYY-MM-DD` and
`end=YYYY-MM-DD`, accept optional `limit`, and allow a maximum 366-day range.
The range must not run backwards.

```text
GET /v1/calendar/movies?start=2026-08-01&end=2026-08-31&limit=50
GET /v1/calendar/tv?start=2026-08-01&end=2026-08-31
```

## Images, galleries, and media files

Image routes return one row per unique TMDB source path. Each row includes
`imageKind`, `galleryIndex`, source dimensions, source MIME type, source size,
SHA-256 metadata, and local `url` values for the original and optimized files.
Internal TMDB source keys and filesystem paths are not public response fields.

The first detail image is gallery index 1. Additional posters are
`poster-02`, `poster-03`, and so on; backdrops start at `backdrop-01`; season
zero uses `season-specials-poster`. Episode stills are optimized-only
thumbnails. Originals are stored outside `optimized/`; optimized files use
JPEG quality 85 with maximum widths 640 for posters/seasons/thumbnails, 1280
for backdrops, 320 for profiles, and transparent PNG width 500 for logos.
No WebP derivative, `full` variant, video file, or `.masters` directory exists.

With `ALLOW_LOCAL_MEDIA=true`, `url` uses `TMDB_MEDIA_BASE_URL` and points to a
verified file on the media listener. With it disabled, local URLs are null and
no new image jobs are created.

The media listener exposes:

| Method | Path | Response |
| --- | --- | --- |
| `GET` | `/health/live` or `/healthz` | `204 No Content` |
| `GET` | `/media/{path}` | Verified public file, with `ETag` and immutable cache headers |

Send `If-None-Match` with the returned `ETag` to receive `304 Not Modified`.
Path traversal and missing files return `404`.

Title videos remain metadata references. The API returns every TMDB video type,
including Trailer, Teaser, Clip, Featurette, Opening Credits, and Bloopers.
YouTube URLs are derived from `site=YouTube` and `key` as
`https://www.youtube.com/watch?v=<key>`. Unknown providers keep their `site`
and `key` but return `url: null`; no URL column or `/videos` media folder is
used.

## Private admin API

The admin listener requires either of these headers:

```text
X-API-Key: <TMDB_ADMIN_API_KEY>
Authorization: Bearer <TMDB_ADMIN_API_KEY>
```

Production does not publish port `8081`; call these routes from a trusted
container on `prv.network`. Every write is durable and asynchronous. Include a
unique `Idempotency-Key` on every write, up to 128 ASCII characters. Reusing a
key with the same operation and payload returns the original submission;
changing the payload returns `409 Conflict`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/v1/openapi.json` | Private OpenAPI document |
| `GET` | `/admin/v1/status` | Build/schema, database, pools, catalog, queues, component, and backup state |
| `GET` | `/admin/v1/jobs?limit=50&cursor=...&status=...&jobType=...` | Bounded durable job list; status is `queued`, `running`, `retry_wait`, `succeeded`, `dead_letter`, or `cancelled` |
| `GET` | `/admin/v1/jobs/{job_id}` | One job and immutable audit events |
| `POST` | `/admin/v1/scans` | Queue `full`, `missing`, or `changes` scan for one or both media types |
| `POST` | `/admin/v1/jobs/{job_id}/cancel` | Request cancellation of an eligible job |
| `POST` | `/admin/v1/jobs/{job_id}/retry` | Queue an auditable retry without rewriting history |
| `POST` | `/admin/v1/media/audits` | Verify media metadata/files; `repair` only queues replacements |
| `POST` | `/admin/v1/maintenance/analyze` | Queue fixed, allowlisted catalog statistics maintenance |
| `GET` | `/admin/v1/backups` | Read backup state |
| `POST` | `/admin/v1/backups` | Queue a `full` or `differential` pgBackRest backup |
| `GET` | `/metrics` | Prometheus/OpenMetrics metrics |

State-changing requests return `202 Accepted` with:

```json
{
  "data": {
    "jobId": "<uuid>",
    "duplicate": false
  }
}
```

Exact request examples:

```bash
admin_base=http://tmdb-mirror-api:8081
admin_key='<TMDB_ADMIN_API_KEY>'

curl -sS "$admin_base/admin/v1/status" \
  -H "X-API-Key: $admin_key"

curl -sS -X POST "$admin_base/admin/v1/scans" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: scan-movies-tv-20260803' \
  -d '{"mode":"missing","mediaTypes":["movie","tv"]}'

curl -sS -X POST "$admin_base/admin/v1/media/audits" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: media-audit-20260803' \
  -d '{"repair":true}'

curl -sS -X POST "$admin_base/admin/v1/backups" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: backup-full-20260803' \
  -d '{"type":"full"}'

curl -sS -X POST "$admin_base/admin/v1/backups" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: backup-differential-20260803' \
  -d '{"type":"differential"}'
```

`POST /admin/v1/maintenance/analyze` and job cancel/retry requests have no
JSON body, but still require `Idempotency-Key`. Restore, raw SQL, shell
execution, arbitrary reindexing, and direct media deletion are intentionally
not API operations. Restore is the offline procedure in
[backup-recovery.md](backup-recovery.md).

## Errors and headers

Problem responses use `Content-Type: application/problem+json` and contain
`type`, `title`, `status`, `detail`, and `requestId`. The API also returns the
same value in `X-Request-Id` for tracing.

Common statuses are:

| Status | Meaning |
| ---: | --- |
| `400` | Invalid path, query, JSON, or required header |
| `401` | Missing or invalid admin key |
| `404` | Missing item, wrong anime partition, or private media path |
| `405` | Method is not allowed |
| `409` | Idempotency key conflicts with a previous payload |
| `413` | Admin JSON body exceeds 16 KiB |
| `422` | Valid admin operation is not currently allowed |
| `501` | Requested catalog ordering is not implemented |
| `503` | Database, schema, queue, or ranking dependency is unavailable |

The API never returns credentials, raw SQL, filesystem paths, or raw upstream
error bodies in a problem response.
