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

Copy `.env.example` beside the downloaded release Compose file, fill in the
three secrets, then run `docker compose -f <release-compose-file> up -d`.

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
