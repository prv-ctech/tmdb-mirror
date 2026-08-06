# Local-Truth On-Demand Media Redesign

Status: implemented in schema revision `0052` and qualified on 2026-08-06.

## Contract

- PostgreSQL is the only metadata source exposed to Arrbit and the media
  worker. Only the main worker calls TMDB metadata APIs.
- `POST /admin/v1/media/requests` accepts one to 100 active local movie/TV IDs.
  Unknown IDs reject the whole request. One endpoint handles single and bulk
  submissions with durable idempotency.
- Media requests persist in PostgreSQL while the media container is offline.
  Both workers drain eligible durable work when their containers start.
- Catalog writes never create image jobs. Global media full/missing/audit/
  repair scans and their routes, tables, job types, and coordinators are gone.
- Request expansion reads title/season/episode galleries from captured TMDB
  documents and primary relational paths for titles, cast/crew, companies,
  networks, and collections. It performs no metadata lookup.
- Gallery selection includes English and untagged title/season/episode images;
  reusable entities contribute only their primary locally stored paths.
- Expansion is limited to 250 sources per continuation, one continuation per
  request, 1,000 active unique title items, and 10,000 active image jobs.

## Renditions and files

The worker downloads configured TMDB CDN renditions and preserves those exact
validated bytes:

- posters and season posters: `w500`;
- backdrops: `w1280`;
- episode stills: `w300`;
- profiles and logos: `w185`;
- SVG-backed logos: PNG rendition.

If a preferred size is absent, use the largest configured size at or below the
target. Never request `original`, upscale, resize, recompress, or re-encode.
Accept static JPEG, PNG, and WebP. Reject SVG, GIF, animation, malformed,
oversized, and MIME-mismatched responses.

Only final files exist:

```text
movies/<tmdb_id>/posters/poster.jpg
movies/<tmdb_id>/backdrops/backdrop-01.jpg
tv/<tmdb_id>/posters/season01-poster.jpg
tv/<tmdb_id>/thumbnails/season01-episode01-thumbnails.jpg
people/<person_id>/profile.jpg
companies/<company_id>/logo.png
networks/<network_id>/logo.png
collections/<collection_id>/poster.jpg
```

No `optimized/`, `.masters`, original, variant, compatibility, or video
directory exists. Files publish through same-filesystem temporary files and
atomic rename. PostgreSQL stores final path, MIME, dimensions, size, SHA-256,
status, and verification time. Public local URLs use a digest query parameter.
Lazy verification repairs requested files and deletes stale files only inside
the exact validated entity directory.

## Catalog scheduling

- Manual modes remain `full_sweep`, `missing_only`, `recovery`,
  `prune_cleanup`, `daily_sync`, and `reconcile`.
- Five-field cron defaults are hourly `daily_sync`, nightly `missing_only`, and
  twice-monthly `reconcile`; empty values disable one schedule.
- Durable schedule slots and watermarks prevent duplicates and serialize
  incompatible maintenance.
- Busy slots remain pending for retry, and unresolved scan child dead letters
  prevent watermark advancement.
- `daily_sync` uses TMDB changes and discovers seasons/episodes.
- `reconcile` imports authoritative IDs, adds new titles, repairs incomplete or
  eligible dead-lettered titles, and deactivates absent IDs without refreshing
  every complete title.
- A changes gap beyond 14 days sets `fullSweepRequired`; it does not claim a
  successful incremental synchronization.

## Migration and verification

Migration `0052` preserves catalog rows and `source.tmdb_documents`, clears old
database image state and pending legacy image jobs, drops image variants and
media scan/audit objects, and creates least-privilege request/selector state.
It intentionally does not delete old filesystem media.

Acceptance covered local-truth API and role tests, offline persistence, startup
draining, backpressure, bounded continuation, pause/resume/cancel, exact-byte
image tests, atomic publication, lazy repair/deletion, schedule/watermark tests,
forward migration, Docker media requests, concurrent reads, backups/PITR, and
secret-free logs. The clean-stack run produced 1,650 ready final assets; a
repeat request reused all 1,650 with zero failures. The 100-client public API
sample completed 2,000 requests without failures. These figures describe that
bounded acceptance run, not a permanent production capacity guarantee.
