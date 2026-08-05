# TMDB Mirror Agent Rules

Read this file and the relevant `.agents/skills/*/SKILL.md` files before
changing code. This repository is Linux-first; use Bash, Docker Desktop, and
`docker compose`. Never use PowerShell or `.ps1` scripts.

## Product contract

- The main worker copies TMDB metadata into PostgreSQL.
- The media worker downloads TMDB images and serves local media.
- Neither worker starts work automatically after restart. Database migration
  and health checks may run; catalog and media work require the admin API.
  A previously `running` state is reset to `stopped` during startup; a
  `paused` state remains paused for emergency persistence.
- PostgreSQL's built-in pgBackRest schedule is independent of both worker
  controls and remains the stack's only automatic scheduled work.
- Both workers are controlled by the authenticated admin API: `start`,
  `pause`, `resume`, and `cancel`.
- Catalog and media controls are independent. For an emergency stop, cancel
  catalog ingest first, wait for active catalog jobs to settle, then cancel
  media so already in-flight catalog work cannot leave image jobs queued.
- Production job submission and scan control use the authenticated admin API;
  do not ship or invoke a direct database job-submission CLI.
- Catalog scans are explicit and durable: `full_sweep`, `missing_only`,
  `prune_cleanup`, and `daily_sync`.
- Scan fan-out is bounded. A scan may submit only bounded batches and one
  cursor continuation. Never enqueue an entire TMDB export at once.
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
- Reuse appended title documents from the detail response. Reusable-entity
  galleries belong to an explicit media scan and must not run per title during
  a catalog full sweep.
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
- Use dedicated TMDB image endpoints for titles, seasons, episodes, people,
  companies, networks, and collections.
- Request `language=en-US` and `include_image_language=en,null` for image
  galleries.
- Use TMDB IDs for reusable entities. Keep original bytes outside `optimized/`.
- Optimized derivatives are JPEG quality 85, never upscaled: width 640 for
  posters, seasons, profiles, and thumbnails; 1280 for backdrops; PNG width
  500 for logos. Never create WebP derivatives, `full` variants, or `.masters`.
- Episode stills are optimized-only under `optimized/thumbnails/`.
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

## Compose contract

- Keep `docker-compose-example.yaml` a complete standalone document.
- Put host bind sources and published ports in Compose, not `.env`.
- Portable sources are `./data/postgres18`, `./data/config`, and
  `./data/media`; operators may edit those Compose lines for their host.
- Keep the default mappings `9001:8080` for the public API, `8081:8081` for
  the authenticated admin API, and `9002:8090` for media. Treat host port
  `8081` as private operational access and protect it with the host firewall.
- Use explicit unique project names and loopback ports for disposable stress
  stacks. Never touch unrelated containers, volumes, networks, or databases.
- Never use `down -v` except for a named disposable stress project.
- Validate interpolation, health, read-only roots, capability drops, mounts,
  runtime-created folders, and port bindings before declaring a stack healthy.

## Testing

- Establish a read-only baseline and preserve unrelated user changes.
- Use unit tests for pure logic, PostgreSQL/filesystem integration tests for
  boundaries, and bounded Docker end-to-end tests for worker/API/media flows.
- Verify migrations, roles, queue bounds, deduplication, pause/resume/cancel,
  retries, restart persistence, API authorization, image paths and MIME types,
  local HTTP serving, permissions, backups, and restore/PITR.
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
- Keep behavior, refactors, tests, and docs reviewable and separable.
- Commit or push only when the user explicitly requests publication.
- Handoff must state changed files, commands and measured results, fixed
  failures, untouched user files, and remaining limits.
