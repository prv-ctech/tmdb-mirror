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
- Both workers are controlled by the authenticated admin API: `start`,
  `pause`, `resume`, and `cancel`.
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
  folder. Build recognized provider URLs from provider metadata; unknown
  providers return `null`.

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
- Keep the default mappings `9001:8080` for the public API and `9002:8090`
  for media. The admin listener stays container-only on `8081`.
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
