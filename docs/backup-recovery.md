# Backup and recovery

PostgreSQL contains pgBackRest 2.59. Its only repository is
`/config/backups/pgbackrest`; it is same-host recovery storage, not an off-host
disaster-recovery copy. Do not expose it through the API or mount it in an
untrusted container.

## Normal operation

WAL archiving is enabled with `wal_level=replica`, `archive_mode=on`, and
pgBackRest's archive command. The built-in PostgreSQL runner uses the configured
`TZ`; `.env.example` defaults it to `America/New_York`:

| Configured local time | Backup |
| --- | --- |
| Sunday 05:00 | Full |
| Monday–Friday 05:00 | Differential |
| Saturday | None |

The date-keyed durable submission prevents a duplicate on the fall DST hour.
Manual full or differential backups are queued through `POST /admin/v1/backups`
with an `Idempotency-Key`; they remain jobs rather than blocking an API call.
The external API uses `type: "full"` or `type: "differential"` exactly (the
internal durable job type for the latter is `database.backup_diff`). Transient
failures return both the job and its paired backup request to the retry queue;
terminal failures retain the durable failure record.

From a trusted container on the external network configured in Compose:

```bash
admin_base=http://tmdb-mirror-api:9001
admin_key='<TMDB_ADMIN_API_KEY>'

curl -sS -X POST "$admin_base/admin/v1/backups" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: backup-full-example-001' \
  -d '{"type":"full"}'

curl -sS -X POST "$admin_base/admin/v1/backups" \
  -H "X-API-Key: $admin_key" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: backup-differential-example-001' \
  -d '{"type":"differential"}'

curl -sS "$admin_base/admin/v1/backups" \
  -H "X-API-Key: $admin_key"
```

Each write returns `202 Accepted` with a durable `jobId`. Poll
`GET /admin/v1/jobs/{job_id}` until the job reaches a terminal state, then
verify the paired request through `GET /admin/v1/backups` and inspect the
repository with `tmdb-pgbackrest info` inside PostgreSQL.

A backup runs `check` after every backup and `verify` after a new full backup.
It creates the backup with expiration deferred, then expires the old recovery
chain only after those checks succeed. The generated pgBackRest configuration
sets `repo1-retention-full=1`, `repo1-retention-diff=6`, and archive retention
by differential chain. A failed backup or verification leaves the prior chain
intact.

Read backup state with `GET /admin/v1/backups` or the bounded backup metrics on
the private `/metrics` listener. For a shell-only inspection from the running
PostgreSQL container:

```text
tmdb-pgbackrest info
tmdb-pgbackrest check
tmdb-pgbackrest verify
```

## Offline PITR procedure

Restore is intentionally not an HTTP operation. Stop all four services first,
preserve the current PostgreSQL volume, and restore into a new empty data
volume. Do not overwrite the only live data volume before the restore has been
checked.

Use the same PostgreSQL image and mount the existing `/config` repository plus
the empty replacement data volume. Its `PGDATA` must remain
`/var/lib/postgresql/18/docker`. Run the restore before PostgreSQL starts:

```text
pgbackrest --stanza=tmdb \
  --type=time --target='YYYY-MM-DD HH:MM:SS+00' \
  --target-action=promote restore
```

Start PostgreSQL against that replacement volume, validate the selected data
point with application queries, then switch the Compose volume mapping only
after validation. `--type=name` with a previously created restore point is
also valid when an operator needs a named boundary. All target timestamps are
UTC even though schedules and terminal logs use the configured `TZ`.

The repository has no encryption key because it is a local recovery copy. Use
storage access controls and add an independently managed off-host backup if
the host itself must be recoverable.
