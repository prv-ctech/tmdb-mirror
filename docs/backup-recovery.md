# Backup and recovery

PostgreSQL contains pgBackRest 2.59. Its only repository is
`/config/backups/pgbackrest`; it is same-host recovery storage, not an off-host
disaster-recovery copy. Do not expose it through the API or mount it in an
untrusted container.

## Normal operation

WAL archiving is enabled with `wal_level=replica`, `archive_mode=on`, and
pgBackRest's archive command. The built-in PostgreSQL runner uses
`TZ=America/New_York`:

| Local time | Backup |
| --- | --- |
| Sunday 05:00 | Full |
| Monday–Friday 05:00 | Differential |
| Saturday | None |

The date-keyed durable submission prevents a duplicate on the fall DST hour.
Manual full or differential backups are queued through `POST /admin/v1/backups`
with an `Idempotency-Key`; they remain jobs rather than blocking an API call.
Transient failures return both the job and its paired backup request to the
retry queue; terminal failures retain the durable failure record.

A backup runs `check` after every backup and `verify` after a new full backup.
It creates the backup with expiration deferred, then expires the old recovery
chain only after those checks succeed. Retention is one full and its five
weekday differentials plus their required WAL. A failed backup or verification
leaves the prior chain intact.

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
UTC even though schedules and terminal logs use America/New_York.

The repository has no encryption key because it is a local recovery copy. Use
storage access controls and add an independently managed off-host backup if
the host itself must be recoverable.
