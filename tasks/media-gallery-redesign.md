# TMDB Gallery, Media, and Video Redesign

Status: implemented (development database may be recreated).

## Goal

Store the complete English/untagged TMDB image galleries with stable local
names, keep source files at the public media root, generate one bounded
optimized derivative, and expose title-level video references without writing
video files.

## Upstream contract

Use TMDB's dedicated image endpoints for movies, TV titles, seasons, episodes,
people, companies, networks, and collections. Every image request uses
`language=en-US` and `include_image_language=en,null`. Title video requests use
the dedicated movie/TV video endpoint and retain every returned TMDB video type.

Gallery records contain the TMDB file path, dimensions, aspect ratio, language,
vote metadata, and optional `file_type`. A TMDB YouTube URL is derived from
`site=YouTube` and `key`; it is not stored as a database column.

## Media layout

Title assets use one of `movies/<id>`, `tv/<id>`, `anime/movie/<id>`, or
`anime/tv/<id>`:

```text
<scope>/<id>/
  posters/poster.jpg, poster-02.jpg, season01-poster.jpg,
    season01-poster-02.jpg, season-specials-poster.jpg
  backdrops/backdrop-01.jpg, backdrop-02.jpg
  logos/logo.png, logo-02.png
  optimized/posters/*-w640.jpg
  optimized/backdrops/*-w1280.jpg
  optimized/logos/*-w500.png
  optimized/thumbnails/*-w640.jpg
```

Reusable entities use TMDB IDs under `casting`, `companies`, `networks`, and
`collections`. The primary detail image keeps its unsuffixed name; remaining
unique gallery paths are sorted lexicographically and numbered from `02`.
Backdrops always start at `01`. Season zero is `season-specials`.

Original files preserve source bytes and validated MIME-derived extensions.
Posters, seasons, backdrops, profiles, and thumbnails get one JPEG derivative
at quality 85; logos get one PNG derivative. Widths are capped at 640, 1280,
320, and 500 respectively, and images are never upscaled. Episode stills are
optimized-only. There is no `.masters` directory, no WebP derivative, no
`full` variant, and no `/videos` media folder.

## Database and API

`assets.image_assets` stores `gallery_index` (1..99), source TMDB path, source
MIME/dimensions/size/digest, and the original root path where one exists.
`owner + image_kind + gallery_index` is unique. Variants are optimized-only;
thumbnail variants must be under `optimized/thumbnails/` and be no wider than
640.

Title image routes return all gallery assets with `imageKind`, `galleryIndex`,
local original/optimized URLs, dimensions, MIME, and digest metadata. Season
and episode image routes are available for TV and anime TV titles. Public
season, episode, person, company, network, and collection image fields use
local media URLs rather than raw TMDB paths.

Title videos retain `site`, `video_key`, `video_type`, `name`, `official`,
language, country, publication time, and size. YouTube returns
`https://www.youtube.com/watch?v=<key>`; unknown providers return `url: null`.
Season and episode video ownership is out of scope.

## Acceptance checks

- Dedicated gallery and video client methods parse multiple records and
  language/file-type/provider metadata.
- Primary paths are deduplicated, stable, and assigned gallery indexes.
- Season zero and episode thumbnail paths are correct.
- Source bytes/digests are preserved; PNG logos retain transparency.
- No `.masters`, generated WebP, `full`, banner, or compatibility paths exist.
- Database uniqueness, variant restrictions, video deduplication, and source
  metadata are enforced.
- API returns local media URLs, all image groups, all video types, and derived
  provider URLs.
- Bounded stress tests cover TV `119495`, TV `4586`, a multi-image movie, a
  company/network rasterized SVG logo, and a person gallery without printing
  credentials.
