# TMDB clone stress-test plan

## Objective

Build and run a reproducible, isolated stress-testing environment for the Rust TMDB clone. The test must exercise PostgreSQL 18, PgBouncer, migrations, API, ingest workers, image storage/download handling, job execution, restart behavior, and concurrent API access. A real TMDB export scan must be observable and rate-limited; the supplied read token may be used only at runtime.

## Acceptance criteria

1. A dedicated Compose project can be started and stopped without touching any existing Docker project, container, volume, network, or database.
2. Runtime secrets are generated outside git and are absent from source, Compose output, logs, metrics, and test artifacts.
3. PostgreSQL 18 migrations, extensions, role grants, indexes, PgBouncer connectivity, API liveness/readiness, ingest, and image worker startup all pass automated smoke checks.
4. A set-based synthetic dataset large enough to exercise catalog/search indexes is loaded, analyzed, and queried through the public API.
5. A bounded concurrent HTTP test reports request count, throughput, error count, and p50/p95/p99 latency; failed requests and container errors are surfaced rather than hidden.
6. At least one worker restart and one database/API dependency failure-recovery scenario are tested with bounded timeouts and captured logs.
7. TMDB movie and TV daily exports are downloaded and fully parsed/countable. Detail refresh work is queued with explicit limits and TMDB rate/concurrency controls; an unbounded multi-day scan is never started silently.
8. Any defects found by the tests are fixed, regression-tested, and re-tested in the isolated stack.
9. The final report records exact commands, image/container versions, timings, resource observations, errors, fixes, and remaining limits. No claim of full TMDB parity is made unless evidence supports it.

## Execution phases

### Phase 1 — isolated harness

- Add a stress-only Compose definition with unique project-scoped names, loopback-only host ports, PostgreSQL 18, PgBouncer, migrator, API, ingest, and image services.
- Add a bootstrap script that creates runtime-only secrets and environment files under an ignored directory, validates Docker availability, and starts the stack.
- Add teardown/status/log collection helpers that target only the explicit stress project.

### Phase 2 — deterministic smoke and database checks

- Run migrations and the existing doctor/verification checks.
- Verify extensions, role grants, indexes, pool connectivity, health endpoints, worker registration, and job execution.
- Capture container logs and resource snapshots with secrets redacted.

### Phase 3 — load dataset and API stress

- Add a set-based seed script for synthetic movies, TV, anime classification, people, companies, genres, keywords, tags, and images/jobs.
- Run `ANALYZE`, verify representative query plans, and check API filtering/search semantics.
- Add a bounded concurrent HTTP runner and produce machine-readable plus human-readable results.

### Phase 4 — real TMDB export/refresh exercise

- Add a streaming export scanner that downloads and counts the complete current movie and TV ID exports.
- Add an explicit bounded queue mode for detail refresh jobs, with configurable maximum IDs, rate limit, worker concurrency, and resume/checkpoint state.
- Exercise image enqueue/download paths using the configured Trawl fallback without putting the token in files or logs.

### Phase 5 — resilience and defect fixing

- Restart each worker during load and verify jobs remain claimable and idempotent.
- Stop/start the API dependency and verify readiness transitions and recovery.
- Inspect errors, slow queries, queue lag, retries, dead letters, and resource ceilings; patch defects and add regression tests.

### Phase 6 — final verification and handoff

- Repeat formatting, unit/integration tests, strict clippy, image builds, Compose config validation, and the stress smoke/load suite.
- Hand off intentional changes and report the measured results and any work that remains outside the tested scope. Commit or push only after explicit user authorization.

## Follow-up plan: anime/live-action classification

### Research findings (2026-08-03)

- Before this implementation, ingest set `is_anime` from TMDB keyword `210024` only. `original_language` is stored but unused by classification.
- TMDB movie `1132850` (Skye Hoshi) is `en`, has the `anime` keyword, and has no `Animation` genre (`16`).
- TMDB TV `63648` (Death Note) is `ja`, has no `anime` keyword, and is already non-anime under the current rule.
- Live TMDB discovery counts for keyword `210024`: 2,428 movies and 4,380 TV titles. Adding genre `16` returns 2,351 movies and 4,335 TV titles; 77 movies and 45 TV titles do not have that genre.
- English titles are not safe to exclude: 66 movies and 46 TV titles have both the `anime` keyword and `Animation` genre. The explicit TMDB live-action keyword family had no direct overlap with `anime` in the sampled movie/TV discovery queries.

### Applied rule

Use `anime keyword AND Animation genre` as the strict positive rule. The
keyword is TMDB ID `210024`; the genre is TMDB ID `16`. Ignore
`original_language`, live-action keywords, and manual overrides.

Titles missing either signal remain ordinary media. Raw TMDB keywords and
genres remain unchanged.

### Implementation slices

1. Centralize the rule in `tmdb-domain` and remove the ingest-only duplicate predicate.
2. Update movie and TV ingest fixtures so both metadata signals are required.
3. Reset the disposable development database/media state instead of reclassifying old files.
4. Add regression fixtures for Skye Hoshi, Death Note, One Piece, English-language anime, and keyword-only/genre-only cases.
5. Re-run database/API/image/download tests and the bounded real-TMDB stress checks.

### Acceptance criteria

- Skye Hoshi is absent from anime routes and appears in ordinary movies.
- Death Note remains ordinary TV.
- English-language anime with genre `16` remains anime.
- Keyword-only and genre-only titles remain ordinary media.
- A fresh development reset has no stale anime files, duplicate assets, or broken public URLs.
