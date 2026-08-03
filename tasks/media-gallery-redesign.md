# TMDB Gallery, Media, Video, and Controlled Scan Redesign

Status: implemented and verified in unit, database, API, and bounded Docker
stress tests. The development database and media directory may be recreated.

## Goal

Store complete English/untagged TMDB image galleries with stable local names,
keep original source files at the public media root, generate one bounded
optimized derivative, expose title-level video references, and provide durable
admin control over media scans and the media queue.

## Research findings

The current client has dedicated methods for title, season, episode, person,
company, network, and collection image galleries. Reusable people and network
assets are currently enqueued only as a side effect of title or season refresh.
There is no independent backfill for reusable entities.

Official TMDB endpoints:

- Person profiles: `/person/{person_id}/images` ([reference](https://developer.themoviedb.org/reference/person-images)).
- Network logos: `/network/{network_id}/images` ([reference](https://developer.themoviedb.org/reference/network-images)).
- Collection details: `/collection/{collection_id}` ([reference](https://developer.themoviedb.org/reference/collection-details)).
- Collection posters/backdrops: `/collection/{collection_id}/images` ([reference](https://developer.themoviedb.org/reference/collection-images)).

Live checks confirmed:

- Person `1373074` returns 4 profiles.
- Network `614` returns 2 logos.
- TV `302051` links to network `614` (`Tokyo MX`).
- Scary Movie Collection is collection `4246`, with 6 movie parts, 7 posters,
  and 10 backdrops.
- Movie `1739259` is a valid title even though its filtered image endpoint
  returns 404; that optional gallery failure must not abort title persistence.

TMDB collection galleries provide posters and backdrops, not collection logos.
Collection members retain their own movie IDs; collection artwork uses only
the real `collection_id`.

## Upstream contract

Use dedicated image endpoints for movies, TV titles, seasons, episodes,
people, companies, networks, and collections. Every image request uses:

```text
language=en-US
include_image_language=en,null
```

Title video requests use dedicated movie/TV video endpoints and retain every
returned TMDB video type.

Gallery records contain `file_path`, dimensions, aspect ratio, language, vote
metadata, and optional `file_type`. YouTube URLs are derived from `site` and
`key`; no redundant URL column is stored.

Optional gallery 404 responses are nonfatal after a successful detail response.
The detail poster/backdrop remains eligible for download. Other upstream
failures continue to retry normally.

## Media layout

Title assets use one of `movies/<movie_id>`, `tv/<tv_id>`,
`anime/movie/<movie_id>`, or `anime/tv/<tv_id>`:

```text
<scope>/<title_id>/
  posters/poster.jpg
  posters/poster-02.jpg
  posters/season01-poster.jpg
  posters/season-specials-poster.jpg
  backdrops/backdrop-01.jpg
  backdrops/backdrop-02.jpg
  logos/logo.png
  optimized/posters/*-w640.jpg
  optimized/backdrops/*-w1280.jpg
  optimized/logos/*-w500.png
  optimized/thumbnails/*-w640.jpg
```

Reusable entities use their actual TMDB entity IDs:

```text
people/<person_id>/
  profile.jpg
  profile-02.jpg
  optimized/profile-w640.jpg

companies/<company_id>/
  logos/logo.png
  optimized/logos/logo-w500.png

networks/<network_id>/
  logos/logo.png
  logos/logo-02.png
  optimized/logos/logo-w500.png

collections/<collection_id>/
  posters/poster.jpg
  posters/poster-02.jpg
  backdrops/backdrop-01.jpg
  optimized/posters/poster-w640.jpg
  optimized/backdrops/backdrop-01-w1280.jpg
```

Cast and crew profiles use `person_id`. Network logos use `network_id`.
Companies use `company_id`; collections use `collection_id`. Never invent,
generate, or substitute local sequential IDs.

The primary detail image is index 1 and keeps its unsuffixed name. Remaining
unique TMDB paths are sorted lexicographically and numbered from `02`.
Backdrops always start at `01`. Season zero is `season-specials`.

Rename `/media/casting` to `/media/people`. Remove all old `casting` paths,
constants, runtime directories, tests, and documentation. Do not keep a
compatibility alias.

## Format policy

Original files preserve downloaded source bytes and use the validated HTTP MIME
type for their extension. JPEG remains JPEG; PNG remains PNG. Source WebP/GIF
may be retained at the root, but no WebP derivative is generated.

Posters, seasons, thumbnails, and profiles get one JPEG derivative at quality
85. Logos get one PNG derivative. Width limits are:

- Posters, seasons, thumbnails, and profiles: `640`.
- Backdrops: `1280`.
- Logos: `500`.

Never upscale. Episode stills are optimized-only. Do not create `.masters`,
WebP derivatives, `full` variants, responsive variants, or a `/videos`
folder. For SVG-backed logos, request/store the safe PNG representation; TMDB
documents the SVG/PNG behavior in its [image basics](https://developer.themoviedb.org/docs/image-basics).

## Ingest and reusable-entity backfill

- Keep the existing title/season enqueue path for immediate downloads.
- Add one bounded reusable-gallery refresh job supporting `person`,
  `company`, `network`, and `collection` entities.
- A media full scan enumerates every local title, season, episode, person,
  company, network, and collection and refreshes their dedicated galleries.
- Include every `catalog.people` record, covering cast and crew.
- Use TMDB IDs as the source key and existing deduplication to prevent duplicate
  download jobs.
- Preserve primary detail paths even when an optional gallery is unavailable.

## Database and API

Continue storing source TMDB paths, original MIME type, dimensions, file size,
SHA-256, and original root storage paths in `assets.image_assets`.

Enforce:

- `gallery_index` from 1 through 99.
- Unique owner, image kind, and gallery index.
- Optimized-only variants under `optimized/`.
- Thumbnail variants under `optimized/thumbnails/` and no wider than 640.
- No duplicate primary, `jpeg_full`, or WebP variants.

Public image responses return local media URLs, not raw TMDB paths. This applies
to titles, seasons, episodes, people, companies, networks, and collections.

Add durable media scan state containing the run ID, mode, phase, status,
timestamps, counts, and linked jobs. Add persistent media queue control state;
do not use the Docker socket or container lifecycle as the control mechanism.

## Media workflow and admin API

Add one durable media-scan workflow:

- `full`: refresh the catalog first, then refresh all title, season, episode,
  person, company, network, and collection galleries.
- `missing`: discover and download only missing or invalid assets.
- `audit`: report local file/database problems; `repair: true` explicitly
  queues verified repairs.

Add these private admin endpoints:

```text
POST /admin/v1/media/scans
GET  /admin/v1/media/scans/{run_id}
POST /admin/v1/media/worker
GET  /admin/v1/media/worker
```

Worker actions are `start`, `pause`, `resume`, and `cancel`.

- `pause` stops new media claims and lets active downloads finish.
- `resume` re-enables claims from the paused state.
- `start` re-enables claims after a stopped/cancelled state.
- `cancel` immediately aborts active media work, cancels queued media jobs,
  leaves catalog-ingest jobs untouched, and puts the media queue in stopped
  state.
- Cancelled work remains stopped until explicitly started or resumed.
- Compose continues starting the container; the API controls only durable
  media queue behavior.
- Scan status reports phase, timestamps, queued/completed/failed counts, audit
  counts, and linked job summaries.
- Full scans wait for both catalog and media phases before completing.

## Videos

Keep all title-level TMDB video records, including Trailer, Teaser, Clip,
Featurette, Opening Credits, Bloopers, and future TMDB types. Store normalized
metadata:

```text
site
video_key
video_type
name
official
language_code
country_code
published_at
size
```

Build YouTube URLs as:

```text
https://www.youtube.com/watch?v=<key>
```

Unknown providers retain `site` and `key` but return `url: null`. Videos
are metadata references and are never downloaded.

## Tests and stress verification

Add coverage for:

- Person `1373074` and network `614` gallery parsing.
- Network `file_type=.svg` with PNG download handling.
- Collection `4246` detail, member IDs, primary artwork, and multiple gallery
  posters/backdrops.
- Cast and crew assets stored under `/media/people`.
- Profile derivatives capped at `w640`.
- No `/media/casting`, `.masters`, WebP derivatives, or duplicate jobs.
- Independent person/network backfill without a title refresh.
- Movie `1739259` detail success with gallery 404.
- Pause, resume, start, cancel, restart persistence, authorization, and
  idempotency.
- Full, missing, and audit scan counts.
- Exact filesystem paths, permissions, database ownership, optimized variants,
  HTTP serving, and failed-job reporting.
- Bounded live stress tests using ignored `secrets.txt` without printing
  credentials.

Stress fixtures include TV `119495`, TV `4586`, a multi-image movie, a
company/network rasterized SVG logo, person `1373074`, network `614`, and
collection `4246`.

## Documentation and agent rules

Update `README.md`, `docs/api.md`, `docs/deployment-production.md`,
`docs/stress-testing.md`, `AGENTS.md`, runtime directory preparation, media
layout code, and stress scripts.

Agent rules must state:

- Dedicated TMDB gallery endpoints are mandatory.
- People and reusable entities use real TMDB entity IDs.
- `/media/people` replaces `/media/casting`.
- Original files are root assets; optimized files use the defined subfolders
  and width limits.
- Episode thumbnails are optimized-only.
- Videos are metadata references, not downloaded files.
- `.masters`, compatibility paths, and old media layouts are not retained.
