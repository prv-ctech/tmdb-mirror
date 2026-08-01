# Compose secret files

Run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap-dev.ps1`
from the repository root. The script creates seven 32-byte base64url passwords in
this directory without printing their values. Existing files are validated and
preserved. Pass `-Rotate` only before a cluster exists to replace them.

Generated files are ignored by Git. Do not copy production or legacy credentials
here, and do not commit any generated password.

Both Compose examples expect these nine files under `deploy/secrets/`:

```text
postgres_owner_password
migrator_password
api_reader_password
api_job_submitter_password
ingest_writer_password
image_writer_password
monitor_password
tmdb_read_access_token
tmdb_api_key
```

The bootstrap creates the first seven. Create the final two from your TMDB
credentials using the same protected-file policy. Never put credentials in a
Compose file, `.env`, an environment example, a command line, or logs.

The tracked `.gitattributes` rule is intentionally limited to
`infra/postgres/initdb/*.sh` so Git always checks container-executed shell scripts
out with LF line endings on Windows.

The database joins both the internal application network and a separate standard
bridge used only to make the explicitly loopback-bound development port work on
Docker Desktop. The internal network remains isolated; the loopback bridge is the
minimum host-facing path required for `127.0.0.1:55432`. Attaching PostgreSQL to
that standard bridge also permits outbound traffic from the development container;
the loopback host binding still prevents remote inbound database connections.
