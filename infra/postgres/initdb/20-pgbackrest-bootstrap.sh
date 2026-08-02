#!/usr/bin/env bash
set -Eeuo pipefail

# The official PostgreSQL entrypoint executes this only while initializing a
# new cluster. Existing clusters are initialized idempotently by the runtime
# entrypoint after PostgreSQL becomes ready.
pgbackrest --stanza=tmdb --log-level-console=info stanza-create
pgbackrest --stanza=tmdb --log-level-console=info check
