# TMDB Mirror Agent Rules

These rules apply to every agent working in this repository. Keep the
repository understandable, reproducible, and safe to publish.

## Scope and working tree

- Read this file, `README.md`, the relevant `docs/` pages, and the current
  `tasks/` plan before changing behavior.
- The worktree may already contain user changes. Run `git status --short`
  before and after work, preserve unrelated changes, and do not reset, clean,
  or overwrite files merely to get a clean baseline.
- Make the smallest change that proves the requested behavior. Do not add
  speculative features, compatibility layers, duplicate abstractions, or
  drive-by formatting.
- Keep behavior changes, refactors, tests, and documentation separable. Use a
  regression test for every fixed bug or changed behavior.

## Secrets and private data

- Never commit, push, print, log, screenshot, or place in test artifacts a
  token, password, private key, cookie, or other private value.
- The local stress secret source is `secrets.txt`. Keep both `secrets.txt` and
  `.secrets.txt` ignored by Git and Docker as a defensive guard. Read secret
  values only into a mode-600 ignored runtime env file or process environment
  at execution time.
- Never put a real TMDB token in `.env`, Compose YAML, source, fixtures,
  generated reports, Docker build args, or shell history. Do not run commands
  that echo the token or expose it through `docker compose config` output.
- Before any commit or push, inspect the staged diff and confirm that
  `.gitignore` and `.dockerignore` cover secrets, environment files, local
  databases, downloaded media, backups, logs, and runtime artifacts. A secret
  that reaches a remote is compromised: stop and rotate it.

## Docker Desktop and Compose

- Use Docker Desktop through `docker`/`docker compose`. Prefer the isolated
  stress Compose file and an explicit unique project name and loopback-only
  host ports.
- This repository is exercised from Linux/WSL. Use Bash and standard Linux
  tools; never invoke PowerShell, `pwsh`, `powershell.exe`, or `.ps1` scripts
  as part of the active workflow.
- Never stop, remove, prune, or recreate an unrelated container, volume,
  network, image, or database. Never use `down -v` outside the named disposable
  stress project. Keep production bind mounts out of stress tests.
- Validate Compose interpolation before startup. Verify container health,
  process identity, read-only roots, dropped capabilities, fixed `/config` and
  `/media` mounts, runtime-created folders, and published ports.
- Keep credentials out of image layers and build context. Inspect the build
  context ignore rules before every local image build.

### Compose and environment contract

- Compose files define deployment topology. Keep host bind sources under
  `volumes.source` and published host ports under `ports`; do not move those
  values into `.env` or pass them to containers through `env_file`.
- The portable production and standalone examples use relative bind sources:
  `./data/postgres18`, `./data/config`, and `./data/media`. Operators may edit
  those `source:` lines for an existing host layout. Never commit Unraid,
  workstation, LAN-IP, or other host-specific paths to a tracked Compose file.
- Keep host port mappings explicit in Compose: `9001:8080` for the API and
  `9002:8090` for media. The admin listener remains container-only on `8081`.
  A host address or alternate port is a Compose edit, not an application env
  setting.
- `.env` and service `env_file` contain application configuration and runtime
  secrets only. `env_file` injects variables into containers; Compose
  interpolation is a separate mechanism. The standalone example expects a
  `.env` beside the file. The checkout production file may use
  `TMDB_ENV_FILE` only to select an external, ignored runtime env file; never
  use it for host paths or ports.
- Database role names are fixed in the PostgreSQL bootstrap. All six internal
  roles use `POSTGRES_PASSWORD`; never add per-role `*_USER` or `*_PASSWORD`
  entries to the runtime template.
- Listener addresses, `PGDATA`, and PostgreSQL init arguments have fixed image
  defaults. Do not make users enter them unless the application contract
  changes.
- `docker-compose-example.yaml` must remain a complete standalone Compose
  document. Do not replace it with `include`, `extends`, or a wrapper that
  requires another Compose file to run.
- Before changing this contract, verify behavior in the official Docker docs:
  [Compose environment variables](https://docs.docker.com/compose/how-tos/environment-variables/),
  [Compose variable interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/),
  and [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/).

### TMDB gallery and media contract

- Fetch title, season, episode, person, company, network, and collection
  galleries through their dedicated TMDB image endpoints. Request only
  `language=en-US` with `include_image_language=en,null`.
- Keep the primary detail image at gallery index 1. Number additional unique
  source paths deterministically. Use `backdrop`, never the old `banner` kind.
- Store original source bytes in the title/entity root folders. Store one
  optimized derivative under the matching `optimized/` folder: JPEG quality
  85 at max widths 640 for posters/seasons/thumbnails, 1280 for backdrops, 320
  for profiles, and PNG width 500 for logos. Never upscale or generate WebP,
  `full`, or responsive variants.
- Episode thumbnails are optimized-only under `optimized/thumbnails/`; no
  original episode still is published. No `.masters` directory or old media
  layout is created or retained.
- Use TMDB IDs in reusable entity paths. Do not introduce local IDs for people,
  companies, networks, or collections.
- Videos are normalized metadata only. Do not download video files or create a
  `/videos` folder. Build a YouTube watch URL from `site` and `key`; unknown
  providers return a null URL.
- Public API image fields must use local media URLs. Do not expose TMDB source
  paths or filesystem paths; retain source paths only as internal sync keys.
- The development schema may be recreated for this redesign. Do not add
  compatibility paths or fallback media layouts.

## Testing and debugging

- Establish a read-only baseline first. When something fails, preserve the
  exact command and output, reproduce it, localize the failing layer, fix the
  root cause, add a guard, and rerun the full affected matrix.
- Prefer unit tests for pure logic, integration tests for database/filesystem
  boundaries, and bounded end-to-end tests for critical flows.
- Exercise the API contract, health/readiness transitions, authorization and
  idempotency, database migrations/roles/indexes, image downloads and HTTP
  serving, path traversal and permissions, folder creation, worker retries
  and restarts, backups, restore/PITR, and duplicate prevention.
- Stress tests must be bounded and report request count, errors, throughput,
  latency percentiles, resource usage, and container/log failures. External
  TMDB and Trawl limits must be reported, never hidden or worked around by an
  unbounded scan.
- A test result is not valid if credentials appear in output or artifacts.
  Redact first, then collect diagnostics.

## Simplicity and maintenance

- Do not keep dead code, commented-out implementations, duplicate paths, or a
  wrapper/strategy/factory used by only one case. Confirm call sites, tests,
  and history before removal.
- Prefer direct, explicit code over clever generic infrastructure. Refactor
  only after behavior is covered and keep each simplification reviewable.
- Do not weaken validation, authorization, error handling, isolation, or
  least-privilege settings in the name of simplicity.

## Git and handoff

- Use descriptive, atomic changes and verify before committing. Do not commit
  or push unless the user explicitly asks for publication.
- Final handoff must list changed files, commands run, measured results,
  failures fixed, intentionally untouched user files, and remaining limits.
