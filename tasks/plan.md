# TMDB Mirror implementation plan

Status: implemented through schema revision `0053`; current operational
details live in `README.md` and `docs/`.

## Current contract

- PostgreSQL stores TMDB response documents and normalized catalog data.
- The public read surface uses the `/3/...` TMDB v3 path and response shape.
- Ingest and media workers drain durable work on container startup.
  Authenticated admin actions control `start`, `pause`, `resume`, and `cancel`.
- Catalog modes are `full_sweep`, `missing_only`, `recovery`, `prune_cleanup`,
  `daily_sync`, and `reconcile`; three cron schedules are durable and bounded.
- Media is requested only through `POST /admin/v1/media/requests` for active
  local catalog titles. PostgreSQL is the sole metadata source.
- `/config/raw`, `/config/logs`, and `/config/backups` are active persistent
  state. `/config/work` and `/config/media` are obsolete.
- API, worker, media, and PostgreSQL write JSONL below `/config/logs`, retaining
  the newest 10 restart generations per service.
- There is one movie namespace and one TV namespace. Anime is not a storage,
  database, or public API partition.

## Queue safety

- A daily export is read in 500-ID batches.
- A continuation is scheduled only after the current batch finishes.
- Active title/season refresh work is capped at 1,000 jobs.
- Active image-download work is capped at 10,000 jobs.
- Media requests admit at most 1,000 unique active title items and expand 250
  sources per continuation.
- Missing-catalog scans use 500-row keyset batches and cursor continuations.
- Idempotency keys prevent duplicate refresh jobs.
- A restart resumes eligible durable work without duplicating submissions.
- Main-worker concurrency follows `TMDB_MAX_CONNECTIONS` and is clamped to 64;
  request starts remain capped by `TMDB_RATE_LIMIT` at 40 per second.

## Media contract

- Read image source paths only from local PostgreSQL documents and relations;
  the media worker never calls TMDB metadata APIs.
- Select English and untagged title/season/episode galleries; reusable entities
  contribute only their primary locally stored path.
- Download exact bounded CDN renditions without original files, local resizing,
  re-encoding, derivatives, or variants.
- Publish validated bytes through a destination-local temporary file and atomic
  rename directly under `/media`; no separate scratch tree exists.
- Use TMDB IDs for reusable entities. Do not create `optimized/`, `.masters`,
  video files, or compatibility paths.
- Store video metadata only. The current public response preserves `site` and
  `key` but does not synthesize a provider URL.

## Verification

- Run Rust checks and tests in the Linux Docker builder.
- Run the isolated Compose smoke, worker-control, queue-bound, real-artwork,
  media-serving, API, backup, and recovery checks with `secrets.txt` loaded
  only at runtime.
- Record queue maxima, final rendition counts, video types, HTTP results,
  failures, and container health without recording credentials.
- The 2026-08-06 clean-stack acceptance run completed the Rust test suite,
  on-demand media reuse/repair checks, 100-client HTTP load, and pgBackRest
  backup/PITR checks.
- The 2026-08-07 runtime-storage run passed formatting, strict Clippy, and 276
  database-backed tests, then published and served 1,636 assets (79,082,815
  bytes) with no failed assets, dead letters, temporary files, or obsolete
  `/config/media` directory. These are repository qualification results, not a
  claim about the current health of an arbitrary production deployment.

## Publication

Do not commit or push unless the user explicitly requests it. Before any
publication, inspect the staged diff and verify that secrets, runtime files,
databases, media, backups, logs, and stress artifacts are ignored.
