#!/usr/bin/env bash
set -Eeuo pipefail

readonly PGBACKREST_CONFIG_DIRECTORY=/etc/pgbackrest
readonly PGBACKREST_CONFIG_FILE=/etc/pgbackrest/pgbackrest.conf
readonly PGBACKREST_REPOSITORY=/config/backups/pgbackrest
readonly PGBACKREST_STANZA=tmdb

postgres_pid=""
scheduler_pid=""

log() {
    printf '%s [tmdb-postgres] %s\n' "$(date --iso-8601=seconds)" "$*" >&2
}

fail() {
    log "event=backup_prepare_failed reason=$1"
    exit 78
}

is_postgres_server_command() {
    if [[ "${1:-}" == -* ]]; then
        return 0
    fi
    [[ "${1:-}" == postgres ]]
}

is_postgres_help_command() {
    local argument
    for argument in "$@"; do
        case "$argument" in
            -'?'|--help|--describe-config|-V|--version)
                return 0
                ;;
        esac
    done
    return 1
}

prepare_pgbackrest() {
    local pgdata="${PGDATA:-/var/lib/postgresql/18/docker}"
    local pgdata_parent
    local postgres_user="${POSTGRES_USER:-postgres}"
    local temporary_config

    export TZ="${TZ:-America/New_York}"
    case "$pgdata" in
        /var/lib/postgresql/*)
            ;;
        *)
            fail invalid_pgdata
            ;;
    esac
    if [[ "$pgdata" == *$'\n'* || "$pgdata" == *$'\r'* \
        || "$postgres_user" == *$'\n'* || "$postgres_user" == *$'\r'* ]]; then
        fail invalid_pgdata
    fi
    # The official entrypoint normally creates PGDATA during first bootstrap,
    # but a pgBackRest restore writes into a fresh mounted parent first. Make
    # the fixed child usable on both paths without changing ownership of the
    # deployment-owned volume root or recursively touching existing data.
    # pgBackRest may recreate the immediate ancestor as root-only during an
    # offline restore; PostgreSQL must be able to traverse it after `gosu`.
    pgdata_parent="$(dirname "$pgdata")"
    install -d -o postgres -g postgres -m 0750 "$pgdata_parent" \
        || fail database_parent_directory_unavailable
    install -d -o postgres -g postgres -m 0700 "$pgdata" \
        || fail database_directory_unavailable
    if [[ -e "$PGBACKREST_REPOSITORY" && ! -d "$PGBACKREST_REPOSITORY" ]]; then
        fail repository_not_directory
    fi

    # Only the repository child is owned by PostgreSQL. Shared /config parents
    # remain deployment-owned and are never recursively changed.
    install -d -m 0755 /config/backups || fail repository_parent_unavailable
    install -d -o postgres -g postgres -m 0700 "$PGBACKREST_REPOSITORY" \
        || fail repository_unavailable
    install -d -o postgres -g postgres -m 0750 "$PGBACKREST_CONFIG_DIRECTORY" \
        || fail configuration_directory_unavailable

    temporary_config="$(mktemp)"
    trap 'rm -f "$temporary_config"' RETURN
    (
        umask 077
        cat >"$temporary_config" <<EOF
[global]
repo1-path=$PGBACKREST_REPOSITORY
repo1-retention-full=1
# pgBackRest counts the full backup as one differential retention set, so six
# retains one full plus the five weekday differentials required by TMDB Mirror.
repo1-retention-diff=6
repo1-retention-archive-type=diff
repo1-retention-history=0
compress-type=zst
process-max=4
start-fast=y
log-level-console=info
log-level-file=off

[$PGBACKREST_STANZA]
pg1-path=$pgdata
pg1-user=$postgres_user
EOF
    )
    install -o postgres -g postgres -m 0640 "$temporary_config" "$PGBACKREST_CONFIG_FILE" \
        || fail configuration_unavailable
}

wait_for_postgres() {
    local attempt
    for attempt in $(seq 1 180); do
        if pg_isready -U "${POSTGRES_USER:?POSTGRES_USER is required}" \
            -d "${POSTGRES_DB:?POSTGRES_DB is required}" -h 127.0.0.1 -t 1 >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$postgres_pid" >/dev/null 2>&1; then
            return 1
        fi
        sleep 1
    done
    return 1
}

stop_children() {
    local signal="$1"
    if [[ -n "$scheduler_pid" ]]; then
        kill -"$signal" "$scheduler_pid" >/dev/null 2>&1 || true
        wait "$scheduler_pid" >/dev/null 2>&1 || true
    fi
    if [[ -n "$postgres_pid" ]]; then
        kill -"$signal" "$postgres_pid" >/dev/null 2>&1 || true
        wait "$postgres_pid" >/dev/null 2>&1 || true
    fi
}

forward_signal() {
    log "event=shutdown signal=$1"
    stop_children "$1"
    exit 0
}

main() {
    export POSTGRES_INITDB_ARGS="${POSTGRES_INITDB_ARGS:---data-checksums --encoding=UTF8 --auth-local=scram-sha-256 --auth-host=scram-sha-256}"
    if [[ "${1:-}" == -* ]]; then
        set -- postgres "$@"
    fi
    if ! is_postgres_server_command "$@" || is_postgres_help_command "$@"; then
        exec /usr/local/bin/docker-entrypoint.sh "$@"
    fi

    prepare_pgbackrest
    /usr/local/bin/docker-entrypoint.sh "$@" &
    postgres_pid=$!
    trap 'forward_signal TERM' TERM
    trap 'forward_signal INT' INT

    if ! wait_for_postgres; then
        log "event=startup_failed reason=postgres_not_ready"
        stop_children TERM
        exit 78
    fi
    if ! gosu postgres /usr/local/bin/tmdb-pgbackrest ensure; then
        log "event=startup_failed reason=pgbackrest_not_ready"
        stop_children TERM
        exit 78
    fi

    gosu postgres /usr/local/bin/tmdb-pgbackrest scheduler &
    scheduler_pid=$!
    log "event=backup_scheduler_started timezone=$TZ"

    set +e
    wait "$postgres_pid"
    local status=$?
    set -e
    if [[ -n "$scheduler_pid" ]]; then
        kill -TERM "$scheduler_pid" >/dev/null 2>&1 || true
        wait "$scheduler_pid" >/dev/null 2>&1 || true
    fi
    exit "$status"
}

main "$@"
