# TMDB Mirror implementation plan

## Current contract

- PostgreSQL stores TMDB response documents and normalized catalog data.
- The public read surface uses the `/3/...` TMDB v3 path and response shape.
- Ingest and media workers start idle. Authenticated admin actions control
  `start`, `pause`, `resume`, and `cancel`.
- Catalog scans are explicit: `full_sweep`, `missing_only`, `prune_cleanup`,
  and `daily_sync`.
- Media scans remain `full`, `missing`, and `audit`; repairs require an
  explicit `repair: true` audit request.
- There is one movie namespace and one TV namespace. Anime is not a storage,
  database, or public API partition.

## Queue safety

- A daily export is read in 500-ID batches.
- A continuation is scheduled only after the current batch finishes.
- Active title/season/reusable refresh work is capped at 1,000 jobs.
- Active image-download work is capped at 10,000 jobs.
- Missing-catalog scans use 500-row keyset batches and cursor continuations.
- Idempotency keys prevent duplicate refresh jobs.
- A restart does not enqueue catalog or media work.
- Main-worker concurrency follows `TMDB_MAX_CONNECTIONS` and is clamped to 64;
  request starts remain capped by `TMDB_RATE_LIMIT` at 40 per second.

## Media contract

- Download title, season, episode, person, company, network, and collection
  galleries through dedicated TMDB endpoints.
- Keep original source bytes at the canonical root path and one optimized
  derivative in `optimized/`; episode stills are optimized-only thumbnails.
- Use TMDB IDs for reusable entities. Do not create `.masters`, WebP
  derivatives, video files, or compatibility paths.
- Store video metadata only. The current public response preserves `site` and
  `key` but does not synthesize a provider URL.

## Verification

- Run Rust checks and tests in the Linux Docker builder.
- Run the isolated Compose smoke, worker-control, queue-bound, real-artwork,
  media-serving, API, backup, and recovery checks with `secrets.txt` loaded
  only at runtime.
- Record queue maxima, downloaded/optimized counts, video types, HTTP results,
  failures, and container health without recording credentials.

## Publication

Do not commit or push unless the user explicitly requests it. Before any
publication, inspect the staged diff and verify that secrets, runtime files,
databases, media, backups, logs, and stress artifacts are ignored.
