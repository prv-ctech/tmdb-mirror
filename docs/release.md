# Releases

The publish workflow builds and publishes images; it does not run the local
integration, stress, backup, or restore matrix. A push to `main` publishes the
rolling `main` and commit-SHA tags. Create an immutable semantic tag such as
`v1.2.3` to publish matching immutable image and SHA tags plus a GitHub release
asset named `tmdb-mirror-v1.2.3.compose.yaml`.

That release Compose file has no `build:` section and pins both images by
digest. It starts the same four production containers with named volumes by
default:

```text
tmdb-postgres  -> /var/lib/postgresql
tmdb-config    -> /config
tmdb-media     -> /media
```

Keep the runtime environment in a mode-600 file outside the release directory.
Start from the current `.env.example`; it contains the PostgreSQL owner
credentials, TMDB read token, admin API key, logging, local media settings,
worker identities, and catalog schedules used by the current images. Do not
put real credentials in a repository `.env`, the release artifact, or shell
history. Point the release Compose file at that runtime file:

```bash
runtime_env=/secure/path/tmdb-mirror-release.env
cp .env.example "$runtime_env"
chmod 600 "$runtime_env"
# Edit "$runtime_env" and replace every angle-bracket value.
export TMDB_ENV_FILE="$runtime_env"
docker compose --env-file "$TMDB_ENV_FILE" \
  -f <release-compose-file> up -d
```

The release template defaults to `./.env` when `TMDB_ENV_FILE` is not set. It
and the checkout production example both publish the fixed container listeners
as host ports `9000` (read API), `9001` (admin API), and `9002` (media). To use
different host ports, edit only the left side of the Compose `ports:` mappings.

The release artifact uses the same neutral external network key
`"your.network"`. Replace every occurrence with an existing Docker network
name before startup; Compose does not create that external network.

For Unraid, replace the named-volume entries once with dedicated bind mounts.
The right-hand container paths stay fixed:

```text
<host-postgres-directory> -> /var/lib/postgresql
<host-config-directory>   -> /config
<host-media-directory>    -> /media
```

Do not add host paths to `.env`. All four containers share `/config`.
PostgreSQL stores backups below `/config/backups/pgbackrest`; API, worker,
media, and PostgreSQL persist restart-rotated JSONL files below `/config/logs`.
Worker and media use the remaining fixed children below `/config` and `/media`.

Both workers drain eligible durable work on startup. The main worker also
evaluates the three configured catalog cron schedules; `full_sweep` remains a
manual admin operation. Media is submitted only through
`POST /admin/v1/media/requests`; removed global media scan/audit routes are not
part of a release.

Run the deliberate integration, recovery, and k6 commands in
[stress-testing.md](stress-testing.md) before creating a version tag. They are
operator verification, not GitHub Actions quality gates.
