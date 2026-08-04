#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

env_file="$REPO_ROOT/.env"
compose_file="$REPO_ROOT/deploy/compose.production.yaml"
while (($#)); do
    case "$1" in
        --env-file) env_file="$2"; shift 2 ;;
        --compose-file) compose_file="$2"; shift 2 ;;
        -h|--help)
            printf '%s\n' 'Usage: validate-production-compose.sh [--env-file PATH] [--compose-file PATH]'
            exit 0
            ;;
        *) die "unknown option: $1" ;;
    esac
done

env_file="$(realpath -e "$env_file")" || die 'production env file cannot be resolved'
compose_file="$(realpath -e "$compose_file")" || die 'production Compose file cannot be resolved'
[[ -f "$env_file" ]] || die "production env file is missing: $env_file"
[[ -f "$compose_file" ]] || die "production Compose file is missing: $compose_file"

declare -A entries=()
while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "$line" == \#* ]] && continue
    [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.*)$ ]] \
        || die "invalid production env entry in $env_file"
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "$value" == \"*\" || "$value" == \'*\' ]]; then
        value="${value:1:${#value}-2}"
    fi
    [[ -z "${entries[$key]+present}" ]] || die "duplicate production env key: $key"
    entries["$key"]="$value"
done <"$env_file"

required_keys=(
    TMDB_ENVIRONMENT POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
    TMDB_API_BASE_URL TMDB_READ_ACCESS_TOKEN TMDB_ADMIN_API_KEY ALLOW_LOCAL_MEDIA
    TMDB_MEDIA_BASE_URL TZ
)
for key in "${required_keys[@]}"; do
    [[ -n "${entries[$key]:-}" ]] || die "required deployment setting is missing: $key"
done

unsupported_database_keys=(
    DATABASE_HOST DATABASE_PORT DATABASE_NAME DATABASE_USER DATABASE_PASSWORD
    TMDB_DB_HOST TMDB_DB_PORT TMDB_DB_NAME TMDB_DB_USER TMDB_DB_PASSWORD
    TMDB_DIRECT_DB_HOST TMDB_DIRECT_DB_PORT TMDB_DIRECT_DB_NAME TMDB_DIRECT_DB_USER TMDB_DIRECT_DB_PASSWORD
    TMDB_POOLED_DB_HOST TMDB_POOLED_DB_PORT TMDB_POOLED_DB_NAME TMDB_POOLED_DB_USER TMDB_POOLED_DB_PASSWORD
)
for key in "${unsupported_database_keys[@]}"; do
    [[ -z "${entries[$key]+present}" && -z "${entries[${key}_FILE]+present}" ]] \
        || die "unsupported database setting: $key"
done
unsupported_role_keys=(
    TMDB_MIGRATOR_USER TMDB_MIGRATOR_PASSWORD
    TMDB_API_READER_USER TMDB_API_READER_PASSWORD
    TMDB_API_JOB_SUBMITTER_USER TMDB_API_JOB_SUBMITTER_PASSWORD
    TMDB_INGEST_WRITER_USER TMDB_INGEST_WRITER_PASSWORD
    TMDB_IMAGE_WRITER_USER TMDB_IMAGE_WRITER_PASSWORD
    TMDB_MONITOR_USER TMDB_MONITOR_PASSWORD
)
for key in "${unsupported_role_keys[@]}"; do
    [[ -z "${entries[$key]+present}" && -z "${entries[${key}_FILE]+present}" ]] \
        || die "unsupported role setting: $key"
done
unsupported_storage_keys=(TMDB_MEDIA_HOST_ROOT TMDB_WORK_HOST_ROOT TMDB_MEDIA_ROOT TMDB_WORK_ROOT)
for key in "${unsupported_storage_keys[@]}"; do
    [[ -z "${entries[$key]+present}" && -z "${entries[${key}_FILE]+present}" ]] \
        || die "unsupported filesystem-root setting: $key"
done

grep -Eq 'target:[[:space:]]*/media([[:space:]]|$)' "$compose_file" || die 'Compose is missing the fixed /media mount'
grep -Eq 'target:[[:space:]]*/config([[:space:]]|$)' "$compose_file" || die 'Compose is missing the fixed /config mount'
for service in postgres api worker media; do
    grep -Eq "^[[:space:]]{2}${service}:" "$compose_file" || die "Compose is missing service: $service"
done
for legacy in pgbouncer image-server admin-migrate storage-init; do
    ! grep -Eq "^[[:space:]]{2}${legacy}:" "$compose_file" || die "legacy service remains: $legacy"
done
! grep -Eq 'pg_isready[^\n]*(tmdb_owner|-d[[:space:]]+tmdb([[:space:]]|\$))' "$compose_file" \
    || die 'PostgreSQL health checks use an obsolete fixed identity'
grep -Fq '$$POSTGRES_USER' "$compose_file" || die 'health check must use POSTGRES_USER'
grep -Fq '$$POSTGRES_DB' "$compose_file" || die 'health check must use POSTGRES_DB'
for setting in wal_level=replica archive_mode=on 'archive_command=pgbackrest --stanza=tmdb archive-push %p'; do
    grep -Fq -- "$setting" "$compose_file" || die "PITR setting is missing: $setting"
done
! grep -Eq '^[[:space:]]*-[[:space:]]*[^[:space:]]*:8081' "$compose_file" \
    || die 'private admin listener must not be published'
grep -Fq 'app-network:' "$compose_file" || die 'external application network is missing'
grep -Eq '^[[:space:]]{4}external:[[:space:]]+true[[:space:]]*$' "$compose_file" \
    || die 'application network must be external'
grep -Eq '^[[:space:]]{4}name:[[:space:]]+[A-Za-z0-9_.-]+[[:space:]]*$' "$compose_file" \
    || die 'external application network name is missing'
grep -Fq 'tmdb-mirror-api' "$compose_file" || die 'private API alias is missing'
for role in worker media; do
    grep -Eq "entrypoint:[[:space:]]*\[/usr/local/bin/tmdb-runtime,[[:space:]]*${role}\]" "$compose_file" \
        || die "$role must start through tmdb-runtime"
done
! grep -Eq 'entrypoint:[[:space:]]*\[/usr/local/bin/tmdb-(worker|images)' "$compose_file" \
    || die 'worker services bypass tmdb-runtime'
grep -Eq 'cap_add:[[:space:]]*\[[[:space:]]*CHOWN,[[:space:]]*DAC_OVERRIDE,[[:space:]]*FOWNER,[[:space:]]*SETGID,[[:space:]]*SETUID,[[:space:]]*SETPCAP[[:space:]]*\]' "$compose_file" \
    || die 'runtime storage capability contract is missing'

# Docker Desktop on Windows does not inherit WSL process variables. Put only
# the interpolation path in a disposable ignored file so the selected env
# file is tested without exposing its values in command output.
mkdir -p "$REPO_ROOT/.stress-runtime"
interpolation_file="$REPO_ROOT/.stress-runtime/compose-validator.$$.env"
trap 'rm -f "$interpolation_file"' EXIT
printf 'TMDB_ENV_FILE=%s\n' "$(docker_path "$env_file")" >"$interpolation_file"
chmod 600 "$interpolation_file"
docker_command compose \
    --env-file "$(docker_path "$interpolation_file")" \
    --file "$(docker_path "$compose_file")" config --quiet >/dev/null \
    || die 'Docker Compose rejected the production template or interpolation'

printf '%s\n' 'Production Compose interpolation and fixed four-container contracts passed.'
