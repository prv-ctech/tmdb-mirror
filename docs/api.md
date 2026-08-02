# API reference

All catalog endpoints are `GET` and use the public API base URL, normally
`http://<server-host>:8080`. They do not require a client key. Catalog responses
contain `data`; paged title lists also return `nextCursor`.

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

## Images and media files

Image metadata endpoints above return `url` for each image. With
`ALLOW_LOCAL_MEDIA=true`, that URL is based on `TMDB_MEDIA_BASE_URL` and points
to the media server, for example `http://<server-host>:8090/media/...`. With it
disabled, the API returns the original TMDB image URL instead.

The media server exposes:

| Listener | Path | Purpose |
| --- | --- | --- |
| `TMDB_MEDIA_BIND` | `/health/live` or `/healthz` | Media-worker health check. |
| `TMDB_MEDIA_BIND` | `/media/{path}` | Public downloaded image. |

`/media/.masters` is intentionally unavailable; original deduplicated masters
are never public.

## Private admin endpoint

The admin listener is `TMDB_ADMIN_BIND` and is not published by the supplied
Compose file. Publish it only on a private network if required.

| Path | Authentication | Purpose |
| --- | --- | --- |
| `/metrics` | `X-API-Key: <TMDB_ADMIN_API_KEY>` or `Authorization: Bearer <TMDB_ADMIN_API_KEY>` | Prometheus metrics. |

## Errors

The API returns RFC 9457-style JSON problem responses. Common status codes are
`400` for invalid paths or parameters, `404` for a missing item or wrong anime
partition, `503` when the catalog is unavailable, and `401` for unauthenticated
admin metrics requests. Responses include an `X-Request-Id` header for tracing.
