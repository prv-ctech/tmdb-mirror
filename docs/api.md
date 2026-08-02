# API reference

All catalog endpoints are `GET` and use the public API base URL, normally
`http://<server-host>:8080`. They do not require a client key. Catalog responses
contain `data`; paged title lists also return `nextCursor`.

Every public path below also has a `/v1` alias, for example `/v1/movies` and
`/v1/anime?q=one%20piece`. Existing unversioned paths remain supported. The
machine-readable document is `GET /v1/openapi.json`.

`{id}` is a positive TMDB ID. `{type}` is `movie` or `tv`. Use the `nextCursor`
value unchanged in the next request's `cursor` parameter.

## Health

| Path | Purpose |
| --- | --- |
| `/health/live` | Process liveness check; does not require PostgreSQL. |
| `/health/ready` | Readiness check for PostgreSQL, schema, extensions, and roles. |

## Movies and TV

These paths deliberately exclude anime. They accept `limit` (1-100), `cursor`,
and the catalog filters listed below.

| Path | Order or resource |
| --- | --- |
| `/movies` or `/movies/popular` | Popular non-anime movies. |
| `/movies/recent` | Recently released non-anime movies. |
| `/movies/top-rated` | Highest-rated non-anime movies. |
| `/movies/{id}` | Full movie metadata and facets. |
| `/movies/{id}/credits` | Movie cast and crew. |
| `/movies/{id}/images` | Movie image metadata and URLs. |
| `/tv` or `/tv/popular` | Popular non-anime TV series. |
| `/tv/recent` | Recently aired non-anime TV series. |
| `/tv/top-rated` | Highest-rated non-anime TV series. |
| `/tv/{id}` | Full TV metadata and facets. |
| `/tv/{id}/credits` | TV cast and crew. |
| `/tv/{id}/images` | TV image metadata and URLs. |
| `/tv/{id}/seasons` | All seasons for a non-anime TV series. |
| `/tv/{id}/seasons/{season}` | One season. |
| `/tv/{id}/seasons/{season}/episodes` | All episodes in one season. |
| `/tv/{id}/seasons/{season}/episodes/{episode}` | One episode. |

For a movie or TV title, these TMDB-parity facets are available on the matching
path. They preserve the same anime isolation as the title route.

| Suffix | Data returned |
| --- | --- |
| `/translations` | Localized title, overview, tagline, and homepage data. |
| `/alternate-titles` | Regional and type-specific alternate names. |
| `/external-ids` | Available IMDB, TVDB, Wikidata, and social identifiers. |
| `/videos` | TMDB video metadata; it does not proxy video files. |
| `/release-dates` (movies) | Regional movie dates, release types, and certifications. |
| `/certifications` (TV) | Regional TV content ratings/certifications. |

## Anime

Anime paths return only titles classified by the `anime` keyword. Normal movie
and TV paths return `404` for those same titles, so the two catalogs never mix.

| Path | Order or resource |
| --- | --- |
| `/anime` or `/anime/popular` | Popular anime movies and TV; `q` turns this into anime-only search. |
| `/anime/recent` | Recently released/aired anime. |
| `/anime/top-rated` | Highest-rated anime. |
| `/anime/{type}/{id}` | Anime movie or TV metadata. |
| `/anime/{type}/{id}/images` | Anime image metadata and URLs. |

Anime detail routes also support `/translations`, `/alternate-titles`,
`/external-ids`, `/videos`, `/release-dates`, and `/credits`. They always
require `{type}` to be `movie` or `tv`; list/search routes omit it to search
both anime namespaces together.

`/anime` and `/anime/popular` accept `type=movie` or `type=tv` to narrow the
result. Do not add a type when you want both anime movies and anime TV results,
for example `GET /anime?q=one%20piece`.

## Search and filters

| Path | Required and optional query parameters |
| --- | --- |
| `/search` | Required `q`; optional `type=movie\|tv`, `limit`, and catalog filters. Searches only non-anime titles. |
| `/anime` or `/anime/popular` | Optional `q`, `type=movie\|tv`, `limit`, and catalog filters. Search results remain anime-only. |
| Movie/TV list paths | Optional `limit`, `cursor`, and catalog filters. |

Catalog filter parameters can be combined on `/search`, movie/TV lists, and
anime lists:

| Filter | Accepted names | Example |
| --- | --- | --- |
| Genre | `genreId` or `genre` | `genreId=28` |
| Keyword | `keywordId` or `keyword` | `keywordId=210024` |
| Tag | `tagId` or `tag` | `tagId=7` |
| Language | `language` or `lang` | `language=en` |
| Runtime | `runtimeMin`/`lengthMin`, `runtimeMax`/`lengthMax` | `runtimeMin=90&runtimeMax=180` |
| Cast or crew person | `personId`, `person`, `actorId`, or `actor` | `actorId=500` |
| Company or studio | `companyId`, `company`, `studioId`, or `studio` | `studioId=420` |
| Network | `networkId` or `network` | `networkId=213` |
| Year | `year` | `year=2024` |
| Release status | `status` | `status=Released` |

`q` is the search parameter; `query` is not a valid alias. Search is accent
insensitive, so `q=cafe` can find a title stored with an accented spelling.

Examples:

```text
GET /search?q=one%20piece&type=tv&limit=20
GET /anime?q=one%20piece
GET /movies?genreId=28&language=en&runtimeMax=140&limit=20
GET /tv/top-rated?networkId=213&year=2024
```

## Discovery endpoints

These paths return `{ "data": [...] }`. They accept `q` for a name search,
`limit` (1-100), and optional `anime=true` to list values used by anime only.
They do not accept catalog filters, `cursor`, or `type`.

| Path | Data returned |
| --- | --- |
| `/genres` | Genres. |
| `/languages` | Languages. |
| `/keywords` | TMDB keywords. |
| `/tags` | Local tags. |
| `/people` | Cast and crew people. |
| `/companies` | Production companies/studios. |
| `/networks` | TV networks. |
| `/collections` | Movie collections. |

## Trending and calendar

| Path | Purpose |
| --- | --- |
| `/trending/day` or `/trending/week` | Current non-anime TMDB trend rankings for movie and TV. |
| `/anime/trending/day` or `/anime/trending/week` | Same trend rankings restricted to anime. |
| `/calendar/movies` | Movie release calendar entries. |
| `/calendar/tv` | TV air-date calendar entries. |

Trending refreshes are durable worker jobs. A stale/missing ranking returns a
normal unavailable/problem response instead of inventing a live upstream
result in the API request.

## Images and media files

Image metadata endpoints above return `url` for each image. With
`ALLOW_LOCAL_MEDIA=true`, that URL is based on `TMDB_MEDIA_BASE_URL` and points
to the media server, for example `http://<server-host>:8090/media/...`. With it
disabled, the API returns the original TMDB image URL instead.

Verified local assets include `variants` with deterministic JPEG/WebP paths,
dimensions, MIME types, byte sizes, and checksums. The media server provides
an `ETag`; send `If-None-Match` to receive `304 Not Modified` when unchanged.
Original `.masters` files are private and never returned.

The media server exposes:

| Listener | Path | Purpose |
| --- | --- | --- |
| `TMDB_MEDIA_BIND` | `/health/live` or `/healthz` | Media-worker health check. |
| `TMDB_MEDIA_BIND` | `/media/{path}` | Public downloaded image. |

`/media/.masters` is intentionally unavailable; original deduplicated masters
are never public.

## Private admin API

The admin listener is `TMDB_ADMIN_BIND` and is not host-published by the
supplied Compose file. It is reachable only to containers on `prv.network` at
`http://tmdb-mirror-api:8081`. Authenticate with either
`X-API-Key: <TMDB_ADMIN_API_KEY>` or
`Authorization: Bearer <TMDB_ADMIN_API_KEY>`.

| Path | Authentication | Purpose |
| --- | --- | --- |
| `/metrics` | `X-API-Key: <TMDB_ADMIN_API_KEY>` or `Authorization: Bearer <TMDB_ADMIN_API_KEY>` | Prometheus metrics. |
| `/admin/v1/openapi.json` | Same | Private admin OpenAPI document. |
| `GET /admin/v1/status` | Same | Build/schema, database, pools, catalog counts, queues, component health, and backups. |
| `GET /admin/v1/jobs` and `/admin/v1/jobs/{id}` | Same | Bounded filters, opaque cursor, immutable job history/events. |
| `POST /admin/v1/scans` | Same + `Idempotency-Key` | Queue explicit `full`, `missing`, or `changes` movie/TV scans. Restart never starts a full scan. |
| `POST /admin/v1/jobs/{id}/cancel` or `/retry` | Same + `Idempotency-Key` | Request cancellation or create a new auditable retry job. |
| `POST /admin/v1/media/audits` | Same + `Idempotency-Key` | Verify metadata/files; `repair=true` only queues replacements and never deletes media. |
| `POST /admin/v1/maintenance/analyze` | Same + `Idempotency-Key` | Queue allowlisted catalog statistics maintenance. |
| `GET`/`POST /admin/v1/backups` | Same; POST also uses `Idempotency-Key` | Read backup state or queue one full/differential backup. Restore is offline only. |

All admin writes return `202 Accepted` and a durable job. Reusing a key with
the same operation/payload returns the original job; a changed payload is
rejected. There is no admin route for raw SQL, a shell command, arbitrary
reindexing, direct media deletion, or restore.

## Errors

The API returns RFC 9457-style JSON problem responses. Common status codes are
`400` for invalid paths or parameters, `404` for a missing item or wrong anime
partition, `401` for unauthenticated private requests, `409` for an idempotency
payload mismatch, `422` for an operation that is not allowed, and `503` when a
dependency is unavailable. Responses include an `X-Request-Id` header for tracing.
