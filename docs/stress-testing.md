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

## Start

Run from the repository root in PowerShell. Supply the TMDB read token through
an environment variable or a process argument; the bootstrap script writes it
to the ignored `.stress-runtime/<project>/compose.env` file, which is the single
Compose environment file for the disposable project.

```powershell
$env:TMDB_STRESS_READ_TOKEN = '<paste-tmdb-read-token>'
$env:TMDB_STRESS_TRAWL_BASE_URL = 'http://<trawl-host>:8191'
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-bootstrap.ps1
$env:TMDB_STRESS_READ_TOKEN = $null
$env:TMDB_STRESS_TRAWL_BASE_URL = $null
```

An existing Trawl instance can be used as the image challenge fallback through
`TMDB_TRAWL_BASE_URL=http://<trawl-host>:8191`. The harness does not start a
second Trawl container.

## Exercise the stack

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-seed.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-http.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-resilience.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-scan.ps1 -QueueLimit 500
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-collect.ps1
```

`stress-http.ps1` reports every request, throughput, status counts, and p50,
p95, and p99 latency. `stress-scan.ps1` streams and counts the complete daily
movie and TV ID exports, while its queue limit deliberately bounds detail
refresh work. Increase that limit only after validating the token, rate limit,
worker concurrency, and database capacity.

The resilience check restarts the media worker and stops PostgreSQL long enough
to require an API readiness failure, then verifies recovery. Its JSON and log
artifacts are written below the ignored runtime directory.

## Refresh a token without rebuilding

```powershell
$env:TMDB_STRESS_READ_TOKEN = '<paste-new-tmdb-read-token>'
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stress-set-token.ps1
$env:TMDB_STRESS_READ_TOKEN = $null
docker compose --env-file .stress-runtime\tmdb_stress_test\compose.env --project-name tmdb_stress_test --file deploy\compose.stress.yaml restart worker
```

Restart the ingest worker after changing the file so it reloads the token. The
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
