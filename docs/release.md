# Releases

GitHub Actions only builds and publishes images. A push to `main` publishes the
rolling `main` tags. Create an immutable semantic tag such as `v1.2.3` to
publish matching immutable image tags and a GitHub release asset named
`tmdb-mirror-v1.2.3.compose.yaml`.

That release Compose file has no `build:` section and pins both images by
digest. It starts the same four production containers with named volumes by
default:

```text
tmdb-postgres  -> /var/lib/postgresql
tmdb-config    -> /config
tmdb-media     -> /media
```

Keep the runtime environment in a mode-600 file outside the release directory.
It must contain the PostgreSQL owner credentials, all least-privilege role
credentials, TMDB read token, and admin API key from `.env.example`. Do not put
real credentials in a repository `.env`, the release artifact, or shell
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

The release template defaults to `./.env` for compatibility when
`TMDB_ENV_FILE` is not set. The production checkout Compose file and the
release artifact use the same four-service contract.

For Unraid, replace the named-volume entries once with dedicated bind mounts.
The right-hand container paths stay fixed:

```text
<host-postgres-directory> -> /var/lib/postgresql
<host-config-directory>   -> /config
<host-media-directory>    -> /media
```

Do not add host paths to `.env`. The PostgreSQL container creates its backup
repository below `/config/backups/pgbackrest`; the worker and media containers
use the other fixed children below `/config` and `/media`.

Run the deliberate integration, recovery, and k6 commands in
[stress-testing.md](stress-testing.md) before creating a version tag. They are
operator verification, not GitHub Actions quality gates.
