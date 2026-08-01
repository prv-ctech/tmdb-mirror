# Development PostgreSQL secrets

Run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap-dev.ps1`
from the repository root. The script creates seven 32-byte base64url passwords in
this directory without printing their values. Existing files are validated and
preserved. Pass `-Rotate` only before a cluster exists to replace them.

Generated files are ignored by Git. Do not copy production or legacy credentials
here, and do not commit any generated password.

The production Compose template additionally expects files named
`tmdb_read_access_token` and `tmdb_api_key` under the configured `TMDB_SECRET_ROOT`.
Populate them
from the TMDB account's read-only token using the same protected-file policy;
never put the token in a Compose file, environment example, command line, or
logs. The development bootstrap intentionally does not generate this upstream
credential.

The tracked `.gitattributes` rule is intentionally limited to
`infra/postgres/initdb/*.sh` so Git always checks container-executed shell scripts
out with LF line endings on Windows.

The database joins both the internal application network and a separate standard
bridge used only to make the explicitly loopback-bound development port work on
Docker Desktop. The internal network remains isolated; the loopback bridge is the
minimum host-facing path required for `127.0.0.1:55432`. Attaching PostgreSQL to
that standard bridge also permits outbound traffic from the development container;
the loopback host binding still prevents remote inbound database connections.
