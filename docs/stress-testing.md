# Isolated stress testing

The stress harness is disposable and isolated from the production Compose
project. It uses exactly four containers: PostgreSQL 18, the Rust API, the
consolidated migration/scheduler/ingest worker, and the media worker with its
embedded static server. Project-scoped named volumes/networks keep it isolated.
PostgreSQL receives a 1 GiB `/dev/shm` so parallel workers are exercised under
the same shared-memory contract as production instead of Docker's 64 MiB
default. The stress database uses 32 MiB `work_mem` to keep broad unaccented
search sorts in memory; the production compact Compose template uses the same
setting. The disposable `/media` volume is disk-backed and prepared for UID
10001 by the bootstrap script, so image downloads do not consume Docker's RAM
quota; production `/media` is the persistent host bind mount selected by
Unraid/Compose.
Host ports default to `55433` (PostgreSQL), `18080` (API), `18081` (admin), and
`18090` (media), all bound to loopback.

The disposable harness deliberately uses `tmdb_stress_catalog` and
`tmdb_stress_owner`, proving that the stack does not require fixed database or
owner names.

## Start

Run from the repository root in PowerShell. Copy `secrets.txt.example` to the
ignored `secrets.txt`, fill in the TMDB v4 read token and v3 API key without
quotation marks, and optionally set the Trawl URL. The bootstrap script writes
only the v4 read token to `.stress-runtime/<project>/compose.env`, the single
Compose environment file for the disposable project.

```powershell
Copy-Item secrets.txt.example secrets.txt
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-tmdb-auth.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-bootstrap.ps1
```

An existing Trawl instance can be used as the image challenge fallback through
`TMDB_TRAWL_BASE_URL=http://<trawl-host>:8191`. The harness does not start a
second Trawl container.

## Exercise the stack

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-seed.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-http.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-artwork.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-media-assets.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-trawl.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-resilience.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-scan.ps1 -QueueLimit 500
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-collect.ps1
```

`stress-http.ps1` reports every request, throughput, status counts, and p50,
p95, and p99 latency. `stress-scan.ps1` streams and counts the complete daily
movie and TV ID exports, while its queue limit deliberately bounds detail
refresh work. When no explicit `-Date` is supplied, it uses the latest matching
movie/TV export published by TMDB (up to seven days back). Increase the queue
limit only after validating the token, rate limit, worker concurrency, and
database capacity.

The resilience check restarts the media worker and stops PostgreSQL long enough
to require an API readiness failure, then verifies recovery. Its JSON and log
artifacts are written below the ignored runtime directory.

`stress-media-assets.ps1` checks one live file for every media owner class,
four internal media-worker lease IDs, shared source reuse, and zero dead-letter
image jobs. `stress-trawl.ps1` verifies the configured existing Trawl instance
without writing its URL or any credential to an artifact.

## Refresh a token without rebuilding

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-set-token.ps1
docker compose --env-file .stress-runtime\tmdb_stress_test\compose.env --project-name tmdb_stress_test --file deploy\compose.stress.yaml up -d --force-recreate --no-deps worker
```

Docker restart alone does not reload an `env_file`; recreate the worker after
changing the file. The
supplied token must be valid before a detail-refresh run can populate
metadata. If a token has been pasted into a chat, shell history, or logs, revoke
it and issue a replacement before production use.

## Stop and clean up

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-down.ps1
# Add -RemoveVolumes only when the disposable database and test data are no
# longer needed.
```

The cleanup script targets only the explicit stress project name. It does not
touch unrelated Docker containers, volumes, networks, or the production data
directory.
