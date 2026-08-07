# TMDB Mirror Agent Rules

Read this file and the relevant `.agents/skills/*/SKILL.md` files before
changing code. This repository is Linux-first; use Bash, Docker Desktop, and
`docker compose`. Never use PowerShell or `.ps1` scripts.

## Product contract

- The main worker copies TMDB metadata into PostgreSQL.
- The media worker downloads TMDB images and serves local media.
- Both workers begin draining eligible durable PostgreSQL work when their
  containers start. Admin `pause` and `cancel` remain operational controls,
  but a container restart returns that worker to `running`.
- PostgreSQL's pgBackRest schedule and the main worker's catalog schedules are
  independent of worker-control requests.
- Both workers are controlled by the authenticated admin API: `start`,
  `pause`, `resume`, and `cancel`.
- Catalog and media controls are independent. Catalog writes never create
  image jobs; only durable on-demand media requests do.
- Production job submission and scan control use the authenticated admin API;
  do not ship or invoke a direct database job-submission CLI.
- Catalog modes are `full_sweep`, `missing_only`, `recovery`, `prune_cleanup`,
  `daily_sync`, and `reconcile`. Full sweeps are manual. Five-field cron
  schedules submit hourly `daily_sync`, nightly `missing_only`, and twice-
  monthly `reconcile` work by default; empty schedule values disable a mode.
- Scan fan-out is bounded. A scan may submit only bounded batches and one
  cursor continuation. Never enqueue an entire TMDB export at once.
- Exact active-job ceilings use durable queue-slot rows claimed with
  `FOR UPDATE SKIP LOCKED`. Never restore a shared transaction-level capacity
  lock or a count-before-insert admission check. Multi-job title-refresh
  producers use only the short non-blocking batch admission turn.
- Expected phase waiting must succeed and schedule one delayed idempotent
  continuation. Never consume retry attempts while healthy child work drains.
- A busy cron slot remains durable and pending until incompatible catalog work
  finishes. Never discard a nightly or twice-monthly slot after one collision,
  and never advance a synchronization watermark past an unresolved child dead
  letter.
- `recovery` streams official exports in 500-ID chunks, refreshes only
  missing/incomplete title details and unresolved dead letters newer than the
  stored source, then queues only unfinished title and season enrichment in
  100-title and 25-season batches. Source refreshes clear completion markers;
  set them only after catalog and exact TMDB documents are durably stored.
- Queue status must distinguish live work from retained history: `active` is
  `queued + running + retry_wait`; `retained` also includes terminal rows and
  is not backlog. Use `active` for backlog alarms and prune old terminal
  history explicitly.
  Completed-scan child links are released after the retention window so they
  do not pin terminal job history forever; scan root records remain auditable.
  Terminal cleanup is index-backed and remains an explicit operator action.
- The public API mirrors the TMDB v3 path and JSON shape. Do not create anime
  partitions or custom public catalog routes.
- `daily_sync` is the incremental production path: consume TMDB movie/TV
  changes, refresh changed titles, and discover new seasons and episodes from
  refreshed TV and season documents.
- PostgreSQL is the only catalog source visible to media requests. The media
  worker may download image bytes from TMDB's CDN but must never call TMDB
  metadata endpoints, discover unknown titles, or create waiting-catalog work.
- `POST /admin/v1/media/requests` is the only media submission endpoint. It
  accepts one to 100 unique active local movie/TV IDs, rejects the whole
  request when any ID is unknown, and persists while the media container is
  offline. Do not restore global media scans or audits.
- Preserve TMDB media fields (`file_path`, `poster_path`, `backdrop_path`,
  `profile_path`, `logo_path`, and `still_path`). Add the corresponding
  `local_*` field as a full local URL when a ready asset exists, otherwise
  `null`. Never replace the upstream field or mutate the stored TMDB document.
- A transient PostgreSQL queue failure must not terminate a worker process.
  Retry queue access at a bounded interval while preserving immediate
  cancellation; validation and rejected-state errors remain fatal.

## Throughput objective

- Full sweeps must move title metadata as close as safely possible to TMDB's
  documented request ceiling without bypassing `429` responses.
- TMDB has no arbitrary multi-ID detail endpoint. Use one bounded concurrent
  request per ID, `append_to_response` for same-title data, and bounded local
  batches for scheduling and database writes.
- Keep full sweeps phased: run the title census first in uninterrupted
  500-title batches, then title enrichment in batches of 100, then TV seasons
  and episodes in batches of 25. Census writes must not enqueue child jobs.
- Reuse appended title documents from the detail response. On-demand media
  selection reads those stored documents plus relational primary paths; it
  never fetches reusable-entity galleries.
- Use measured concurrency, bulk persistence where it removes round trips, and
  structured timing/count events. Do not add speculative caching, queues,
  abstractions, or a fake TMDB batch API.
- In-flight request concurrency may exceed the requests-per-second setting to
  hide measured upstream latency; the shared request-start limiter must remain
  capped at 40 requests per second and continue honoring `429` responses.
- Season and episode detail appends include their image galleries; do not issue
  a second image request when that appended document is present. Report census,
  enrichment, season, and media throughput separately.

## Simplicity rule

Make the smallest direct change that proves the behavior. Do not add dead
code, duplicate implementations, compatibility aliases, speculative layers,
or abstractions used by one call site. Remove obsolete paths completely after
checking their callers and tests. Add a regression test for every behavior
change.

## Data and media

- Store TMDB response documents as the source for the local v3 read surface.
- The main worker captures image metadata in PostgreSQL. On-demand media
  selection uses title/season/episode documents plus primary person, company,
  network, and collection paths already related to the requested titles.
- Capture and select English plus untagged title/season/episode gallery images
  with `language=en-US` and `include_image_language=en,null`. Reusable entities
  use only their primary locally stored paths; do not fetch their galleries.
- Use real TMDB movie, TV, person, company, network, collection, season, and
  episode IDs. Never substitute local title IDs in public paths or job payloads.
- Download exact configured TMDB CDN renditions: `w500` posters/season posters,
  `w1280` backdrops, `w300` episode stills, and `w185` profiles/logos. Select
  the largest configured size at or below the target and never use `original`.
- Accept validated static JPEG, PNG, and WebP bytes. Reject SVG, GIF, animated,
  malformed, oversized, or MIME-mismatched responses. SVG-backed logos use a
  PNG rendition. Never resize, recompress, re-encode, or create derivatives.
- Store only final files in deterministic entity directories. Never create
  `optimized/`, `.masters`, variant, original, or compatibility directories.
- Publish through a destination-local temporary file and atomic rename, store
  size/SHA-256/verification time, lazily revalidate requested assets, and
  restrict stale-file deletion to the exact validated TMDB entity directory.
  Local URLs include a digest query parameter.
- Reject symlinks in every publication/deletion path component. Serialize one
  active image job per owner/kind/index slot, and serialize media cancellation
  with request expansion so files and database metadata cannot diverge.
- Videos are metadata only. Do not download video files or create a videos
  folder. The current public `/3/.../videos` response preserves TMDB's `site`
  and `key` and does not synthesize a provider URL. If URL derivation is added,
  it requires an API contract change and regression tests.

## Secrets and local state

- Never print, commit, push, log, or store credentials in fixtures or reports.
- Real stress credentials come only from ignored `secrets.txt` and are loaded
  into a mode-600 runtime environment at execution time. Ignore both
  `secrets.txt` and `.secrets.txt` defensively.
- Never place real credentials in `.env`, Compose YAML, source, image layers,
  build arguments, generated reports, or shell history.
- Before commit or push, inspect the staged diff and confirm secrets,
  environment files, databases, media, backups, logs, and runtime artifacts
  are ignored by Git and Docker.
- `/config/raw` is active catalog export/reconcile storage. `/config/logs` and
  `/config/backups` are active persistent state. `/config/work` and
  `/config/media` are obsolete and must not be recreated.
- API, worker, media, and PostgreSQL must emit JSONL to Docker and persist the
  identical stream under `/config/logs`. Use `api.log`, `worker.log`,
  `media.log`, and `postgres.log` for the first process start, increment a
  numeric suffix on each restart, and retain only the newest 10 per service.

## Compose contract

- Keep `docker-compose-example.yaml` a complete standalone document.
- Put host bind sources and published ports in Compose, not `.env`.
- Portable sources are `./data/postgres18`, `./data/config`, and
  `./data/media`; operators may edit those Compose lines for their host.
- All four services mount the same `/config` appdata root so logs are durable.
- Keep the default mappings `9000:9000` for the public API, `9001:9001` for
  the authenticated admin API, and `9002:9002` for media. Treat host port
  `9001` as private operational access and protect it with the host firewall.
- Use explicit unique project names and loopback ports for disposable stress
  stacks. Never touch unrelated containers, volumes, networks, or databases.
- Never use `down -v` except for a named disposable stress project.
- Validate interpolation, health, read-only roots, capability drops, mounts,
  runtime-created folders, and port bindings before declaring a stack healthy.
- Gate the API Compose health check on `/health/ready`; liveness alone does not
  prove migrations and PostgreSQL are ready. A brief startup state is valid
  during recovery or migration.
- Keep `stop_grace_period: 2m` for PostgreSQL and `45s` for application
  services. Do not override PostgreSQL with a shorter stop timeout that can
  force crash recovery on the next start.
- Direct service startup connections retry transient PostgreSQL races within a
  fixed bound. Do not restore crash/restart as the normal retry mechanism.

## Testing

- Establish a read-only baseline and preserve unrelated user changes.
- Use unit tests for pure logic, PostgreSQL/filesystem integration tests for
  boundaries, and bounded Docker end-to-end tests for worker/API/media flows.
- Verify migrations, roles, queue bounds, deduplication, pause/resume/cancel,
  retries, restart persistence, API authorization, image paths and MIME types,
  local HTTP serving, permissions, backups, and restore/PITR.
- Verify media requests make zero TMDB metadata calls, atomically reject
  unknown IDs, persist while the media worker is offline, expand no more than
  250 sources per continuation, honor the 1,000-title and 10,000-image-job
  ceilings, and produce no legacy media tables, routes, jobs, or directories.
- Restricted API write tests must verify both table privileges and schema
  `USAGE`; table grants alone are not sufficient for production roles.
- Stress tests must report bounded request counts, failures, latency, queue
  depth, downloaded files, database rows, and container errors without
  revealing credentials. TMDB/Trawl rate limits must be reported, never
  bypassed with an unbounded scan.
- When a test fails, reproduce it, identify the failing layer, fix the root
  cause, add a guard, and rerun the affected matrix.

## Git and handoff

- Run `git status --short` before and after work. Preserve unrelated edits;
  never reset or clean the worktree to hide them.
- Treat `README.md`, `docs/*.md`, `.env.example`, the Compose examples, and
  `CHANGELOG.md` as one public contract. When routes, migrations, schedules,
  startup behavior, ports, paths, or media policy change, update every affected
  document in the same change and remove contradictory historical claims.
- Derive current documentation from code, SQLx migrations, Compose files, and
  executable scripts. Keep historical design records clearly labeled and do
  not present an unverified acceptance checklist as a current test result.
- Keep behavior, refactors, tests, and docs reviewable and separable.
- Commit or push only when the user explicitly requests publication.
- Handoff must state changed files, commands and measured results, fixed
  failures, untouched user files, and remaining limits.
