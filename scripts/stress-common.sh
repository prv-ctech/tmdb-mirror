#!/usr/bin/env bash
set -Eeuo pipefail

# Shared Linux-only helpers for the disposable TMDB stress project.
# Secret values are read without sourcing the file and are written only to a
# mode-600 ignored Compose env file; the general runtime env contains no
# upstream credential.

COMMON_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$COMMON_SCRIPT_DIR/.." && pwd)"
PROJECT_NAME="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
API_PORT="${TMDB_STRESS_API_PORT:-18080}"
ADMIN_PORT="${TMDB_STRESS_ADMIN_PORT:-18081}"
IMAGE_PORT="${TMDB_STRESS_IMAGE_PORT:-18090}"
POSTGRES_PORT="${TMDB_STRESS_PG_PORT:-55433}"
POSTGRES_SERVICE=tmdb-mirror-postgres
COMPOSE_FILE="$REPO_ROOT/deploy/compose.stress.yaml"
RUNTIME_ROOT="$REPO_ROOT/.stress-runtime/$PROJECT_NAME"
SECRET_RUNTIME_ROOT="${TMDB_STRESS_SECRET_RUNTIME_ROOT:-/tmp/tmdb-stress-secrets}"
ENV_FILE="$RUNTIME_ROOT/compose.env"
OVERRIDE_FILE="$RUNTIME_ROOT/compose.override.yaml"
SECRET_ENV_FILE="$SECRET_RUNTIME_ROOT/$PROJECT_NAME/compose.secret.env"
METADATA_FILE="$RUNTIME_ROOT/metadata.json"
RESULT_ROOT="$RUNTIME_ROOT/results"
EXPORT_ROOT="$RUNTIME_ROOT/exports"

if [[ -n "${TMDB_DOCKER_BIN:-}" ]]; then
    DOCKER_BIN="$TMDB_DOCKER_BIN"
elif command -v docker >/dev/null 2>&1; then
    DOCKER_BIN="$(command -v docker)"
else
    DOCKER_BIN=''
fi

docker_command() {
    [[ -n "$DOCKER_BIN" ]] || die 'Docker CLI is unavailable'
    "$DOCKER_BIN" "$@"
}

docker_path() {
    printf '%s\n' "$1"
}

refresh_paths() {
    COMPOSE_FILE="$REPO_ROOT/deploy/compose.stress.yaml"
    RUNTIME_ROOT="$REPO_ROOT/.stress-runtime/$PROJECT_NAME"
    ENV_FILE="$RUNTIME_ROOT/compose.env"
    OVERRIDE_FILE="$RUNTIME_ROOT/compose.override.yaml"
    METADATA_FILE="$RUNTIME_ROOT/metadata.json"
    RESULT_ROOT="$RUNTIME_ROOT/results"
    EXPORT_ROOT="$RUNTIME_ROOT/exports"
    SECRET_ENV_FILE="$SECRET_RUNTIME_ROOT/$PROJECT_NAME/compose.secret.env"
}

configure_runtime() {
    PROJECT_NAME="$1"
    API_PORT="$2"
    ADMIN_PORT="$3"
    IMAGE_PORT="$4"
    POSTGRES_PORT="$5"
    export TMDB_STRESS_PROJECT="$PROJECT_NAME"
    export TMDB_STRESS_API_PORT="$API_PORT"
    export TMDB_STRESS_ADMIN_PORT="$ADMIN_PORT"
    export TMDB_STRESS_IMAGE_PORT="$IMAGE_PORT"
    export TMDB_STRESS_PG_PORT="$POSTGRES_PORT"
    refresh_paths
}

configure_existing_runtime() {
    local key value
    configure_runtime "$@"
    [[ -f "$ENV_FILE" ]] || return 0
    for key in TMDB_STRESS_API_PORT TMDB_STRESS_ADMIN_PORT TMDB_STRESS_IMAGE_PORT TMDB_STRESS_PG_PORT; do
        value="$(awk -F= -v key="$key" '$1 == key { print substr($0, index($0, "=") + 1); exit }' "$ENV_FILE")"
        [[ "$value" =~ ^[0-9]+$ ]] || die "invalid $key in existing stress runtime"
        case "$key" in
            TMDB_STRESS_API_PORT) API_PORT="$value" ;;
            TMDB_STRESS_ADMIN_PORT) ADMIN_PORT="$value" ;;
            TMDB_STRESS_IMAGE_PORT) IMAGE_PORT="$value" ;;
            TMDB_STRESS_PG_PORT) POSTGRES_PORT="$value" ;;
        esac
        export "$key=$value"
    done
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    if [[ "$1" == docker ]]; then
        [[ -n "$DOCKER_BIN" ]] || die 'required command is missing: docker'
    else
        command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
    fi
}

require_runtime_files() {
    [[ -f "$ENV_FILE" ]] || die "runtime environment is missing: $ENV_FILE; run stress-bootstrap.sh"
    [[ -f "$OVERRIDE_FILE" ]] || die "runtime Compose override is missing: $OVERRIDE_FILE; run stress-bootstrap.sh"
    [[ -f "$SECRET_ENV_FILE" ]] || die "runtime secret env is missing: $SECRET_ENV_FILE; run stress-bootstrap.sh"
    [[ -f "$COMPOSE_FILE" ]] || die "stress Compose file is missing: $COMPOSE_FILE"
}

select_secrets_file() {
    if [[ -n "${TMDB_STRESS_SECRETS_FILE:-}" ]]; then
        printf '%s\n' "$TMDB_STRESS_SECRETS_FILE"
    elif [[ -f "$REPO_ROOT/secrets.txt" ]]; then
        printf '%s\n' "$REPO_ROOT/secrets.txt"
    else
        die "no local secrets file found; expected secrets.txt"
    fi
}

read_stress_secrets() {
    local path="${1:-$(select_secrets_file)}" line key value
    [[ -f "$path" ]] || die "local secrets file is missing: $path"
    [[ "$(wc -c <"$path")" -le 65536 ]] || die 'local secrets file exceeds 64 KiB'

    STRESS_READ_TOKEN=''
    STRESS_API_KEY=''
    STRESS_TRAWL_URL=''
    declare -gA STRESS_SECRETS=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ -z "${line//[[:space:]]/}" || "$line" == \#* ]] && continue
        [[ "$line" =~ ^(TMDB_STRESS_READ_TOKEN|TMDB_STRESS_API_KEY|TMDB_STRESS_TRAWL_BASE_URL|TMDB_ADMIN_API_KEY)=(.+)$ ]] \
            || die "invalid local stress secret entry in $path"
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
        [[ -z "${STRESS_SECRETS[$key]+present}" ]] || die "duplicate local stress secret: $key"
        case "$value" in
            *[[:space:]]*|*\"*|*\'*) die "invalid local stress secret value for $key" ;;
        esac
        [[ "$key" != 'TMDB_ADMIN_API_KEY' ]] || continue
        STRESS_SECRETS["$key"]="$value"
    done < "$path"

    STRESS_READ_TOKEN="${STRESS_SECRETS[TMDB_STRESS_READ_TOKEN]:-}"
    STRESS_API_KEY="${STRESS_SECRETS[TMDB_STRESS_API_KEY]:-}"
    STRESS_TRAWL_URL="${STRESS_SECRETS[TMDB_STRESS_TRAWL_BASE_URL]:-}"
}

load_runtime() {
    require_command docker
    require_runtime_files
    read_stress_secrets "${TMDB_STRESS_SECRETS_FILE:-$(select_secrets_file)}"
    [[ -n "$STRESS_READ_TOKEN" ]] || die 'TMDB_STRESS_READ_TOKEN is missing from the local secrets file'
    export TMDB_READ_ACCESS_TOKEN="$STRESS_READ_TOKEN"
    if [[ -n "$STRESS_TRAWL_URL" ]]; then
        export TMDB_TRAWL_BASE_URL="$STRESS_TRAWL_URL"
    else
        unset TMDB_TRAWL_BASE_URL
    fi
    export TMDB_STRESS_ENV_FILE="$ENV_FILE"
    export TMDB_STRESS_PROJECT="$PROJECT_NAME"
}

redact() {
    local text="${1:-}"
    if [[ -n "${TMDB_READ_ACCESS_TOKEN:-}" ]]; then
        text="${text//${TMDB_READ_ACCESS_TOKEN}/<redacted>}"
    fi
    if [[ -n "${STRESS_API_KEY:-}" ]]; then
        text="${text//"$STRESS_API_KEY"/<redacted>}"
    fi
    if [[ -n "${STRESS_ADMIN_KEY:-}" ]]; then
        text="${text//"$STRESS_ADMIN_KEY"/<redacted>}"
    fi
    printf '%s' "$text"
}

redact_file() {
    local source_file="$1" destination_file="$2" line
    : >"$destination_file"
    while IFS= read -r line || [[ -n "$line" ]]; do
        redact "$line" >>"$destination_file"
        printf '\n' >>"$destination_file"
    done <"$source_file"
}

docker_checked() {
    local output
    if ! output="$(docker_command "$@" 2>&1)"; then
        redact "$output" >&2
        return 1
    fi
    printf '%s' "$output"
}

compose() {
    docker_command compose \
        --env-file "$(docker_path "$ENV_FILE")" \
        --project-name "$PROJECT_NAME" \
        --file "$(docker_path "$COMPOSE_FILE")" \
        --file "$(docker_path "$OVERRIDE_FILE")" "$@"
}

compose_checked() {
    local output
    if ! output="$(compose "$@" 2>&1)"; then
        redact "$output" >&2
        return 1
    fi
    printf '%s' "$output"
}

env_value() {
    local name="$1" line key value
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ "$line" == "$name="* ]] || continue
        value="${line#*=}"
        printf '%s\n' "$value"
        return 0
    done < "$ENV_FILE"
    return 1
}

database_name() { env_value POSTGRES_DB; }
database_user() { env_value POSTGRES_USER; }

database_password() {
    compose exec -T "$POSTGRES_SERVICE" printenv POSTGRES_PASSWORD
}

psql_query() {
    local password="$1" sql="$2"
    compose exec -T -e "PGPASSWORD=$password" "$POSTGRES_SERVICE" psql -X -v ON_ERROR_STOP=1 \
        --username "$(database_user)" --dbname "$(database_name)" -c "$sql"
}

psql_at() {
    local password="$1" sql="$2"
    compose exec -T -e "PGPASSWORD=$password" "$POSTGRES_SERVICE" psql -X -v ON_ERROR_STOP=1 -At \
        --username "$(database_user)" --dbname "$(database_name)" -c "$sql"
}

wait_for_health() {
    local service="$1" timeout="${2:-180}" container state deadline
    deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        container="$(compose ps -q "$service" 2>/dev/null || true)"
        if [[ -n "$container" ]]; then
            state="$(docker_command inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container" 2>/dev/null || true)"
            [[ "$state" == 'running|healthy' ]] && return 0
            if [[ "$state" == exited\|* || "$state" == dead\|* ]]; then
                compose logs --no-color --timestamps "$service" | redact >&2 || true
                die "stress service stopped before becoming healthy: $service"
            fi
        fi
        sleep 2
    done
    compose logs --no-color --timestamps "$service" | redact >&2 || true
    die "timed out waiting for stress service: $service"
}

wait_for_migrations() {
    local password table version deadline
    password="$(database_password)"
    deadline=$((SECONDS + ${1:-180}))
    while (( SECONDS < deadline )); do
        table="$(psql_at "$password" "SELECT COALESCE(to_regclass('ops._sqlx_migrations')::text, '')" 2>/dev/null || true)"
        if [[ "$table" != 'ops._sqlx_migrations' ]]; then
            sleep 2
            continue
        fi
        version="$(psql_at "$password" "SELECT COALESCE(max(version), 0) FROM ops._sqlx_migrations WHERE success" 2>/dev/null || true)"
        if [[ "$version" =~ ^[0-9]+$ ]] && (( version >= 50 )); then
            return 0
        fi
        sleep 2
    done
    compose logs --no-color --timestamps worker | redact >&2 || true
    die 'timed out waiting for worker migrations'
}

assert_port_free() {
    local port="$1"
    if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$port" | tail -n +2 | grep -q .; then
        die "stress-test port 127.0.0.1:$port is already in use"
    fi
}

write_runtime_files() {
    local tmp_env tmp_override tmp_secret tmp_metadata trawl_line=''
    mkdir -p "$RUNTIME_ROOT" "$RESULT_ROOT" "$EXPORT_ROOT" "$(dirname "$SECRET_ENV_FILE")"
    chmod 700 "$SECRET_RUNTIME_ROOT" "$(dirname "$SECRET_ENV_FILE")"
    umask 077
    tmp_env="$(mktemp "$RUNTIME_ROOT/compose.env.XXXXXX")"
    tmp_override="$(mktemp "$RUNTIME_ROOT/compose.override.yaml.XXXXXX")"
    tmp_secret="$(mktemp "$(dirname "$SECRET_ENV_FILE")/compose.secret.env.XXXXXX")"
    tmp_metadata="$(mktemp "$RUNTIME_ROOT/metadata.json.XXXXXX")"
    if [[ -n "$STRESS_TRAWL_URL" ]]; then
        [[ "$STRESS_TRAWL_URL" =~ ^https?://[^[:space:]/]+(:[0-9]+)?(/[^[:space:]]*)?$ ]] \
            || die 'TMDB_STRESS_TRAWL_BASE_URL must be an http(s) URL without credentials'
        trawl_line="TMDB_TRAWL_BASE_URL=$STRESS_TRAWL_URL"
    fi
    printf 'TMDB_READ_ACCESS_TOKEN=%s\n' "$STRESS_READ_TOKEN" >"$tmp_secret"
    if [[ -n "$STRESS_TRAWL_URL" ]]; then
        printf 'TMDB_TRAWL_BASE_URL=%s\n' "$STRESS_TRAWL_URL" >>"$tmp_secret"
    fi
    cat >"$tmp_env" <<EOF
TMDB_STRESS_PROJECT=$PROJECT_NAME
TMDB_STRESS_ENV_FILE=$(docker_path "$ENV_FILE")
TMDB_STRESS_APP_IMAGE=tmdb-stress-app:local
TMDB_STRESS_POSTGRES_IMAGE=tmdb-stress-postgres:local
TMDB_STRESS_API_PORT=$API_PORT
TMDB_STRESS_ADMIN_PORT=$ADMIN_PORT
TMDB_STRESS_IMAGE_PORT=$IMAGE_PORT
TMDB_STRESS_PG_PORT=$POSTGRES_PORT
TMDB_ENVIRONMENT=test
POSTGRES_DB=tmdb_stress_catalog
POSTGRES_USER=tmdb_stress_owner
POSTGRES_PASSWORD=tmdb-stress
TZ=America/New_York
TMDB_LOG_FORMAT=json
TMDB_LOG_LEVEL=info
TMDB_ADMIN_API_KEY=test-admin-key-placeholder-0123456789
TMDB_API_BASE_URL=https://api.themoviedb.org/3
TMDB_MEDIA_BASE_URL=http://127.0.0.1:$IMAGE_PORT/media
TMDB_RATE_LIMIT=40
TMDB_MAX_CONNECTIONS=64
TMDB_MAX_ATTEMPTS=4
TMDB_REQUEST_TIMEOUT_SECONDS=30
TMDB_DAILY_EXPORT_MAX_BYTES=536870912
TMDB_WORKER_ID=tmdb-stress-worker
TMDB_IMAGE_WORKER_ID=tmdb-stress-media
TMDB_IMAGE_WORKER_CONCURRENCY=4
TMDB_WORKER_LEASE_SECONDS=60
TMDB_WORKER_HEARTBEAT_SECONDS=15
TMDB_WORKER_IDLE_POLL_MS=100
TMDB_DAILY_SYNC_CRON=
TMDB_MISSING_ONLY_CRON=
TMDB_RECONCILE_CRON=
TMDB_STRESS_PG_MAX_CONNECTIONS=200
TMDB_STRESS_PG_SHARED_BUFFERS=2GB
TMDB_STRESS_PG_EFFECTIVE_CACHE_SIZE=8GB
TMDB_STRESS_PG_WORK_MEM=32MB
TMDB_STRESS_PG_MAINTENANCE_WORK_MEM=512MB
ALLOW_LOCAL_MEDIA=true
$trawl_line
EOF
    cat >"$tmp_override" <<'EOF'
services:
  api:
    env_file:
      - PLACEHOLDER_SECRET_ENV_FILE
  worker:
    env_file:
      - PLACEHOLDER_SECRET_ENV_FILE
  media:
    env_file:
      - PLACEHOLDER_SECRET_ENV_FILE
EOF
    sed -i "s#PLACEHOLDER_SECRET_ENV_FILE#$(docker_path "$SECRET_ENV_FILE")#g" "$tmp_override"
    cat >"$tmp_metadata" <<EOF
{
  "project": "$PROJECT_NAME",
  "compose_file": "$COMPOSE_FILE",
  "runtime_root": "$RUNTIME_ROOT",
  "api_url": "http://127.0.0.1:$API_PORT",
  "admin_url": "http://127.0.0.1:$ADMIN_PORT",
  "image_url": "http://127.0.0.1:$IMAGE_PORT",
  "postgres_host": "127.0.0.1",
  "postgres_port": $POSTGRES_PORT,
  "started_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    mv -f "$tmp_env" "$ENV_FILE"
    mv -f "$tmp_override" "$OVERRIDE_FILE"
    mv -f "$tmp_secret" "$SECRET_ENV_FILE"
    mv -f "$tmp_metadata" "$METADATA_FILE"
    chmod 600 "$ENV_FILE" "$OVERRIDE_FILE" "$SECRET_ENV_FILE" "$METADATA_FILE"
}

json_field() {
    local field="$1" json="$2"
    if command -v jq >/dev/null 2>&1; then
        jq -r ".$field // empty" <<<"$json"
    else
        printf '%s\n' "$json" | sed -nE "s/.*\"$field\"[[:space:]]*:[[:space:]]*\"?([^,\"}]*)\"?.*/\1/p" | head -n 1
    fi
}

ensure_owner_paths() {
    local service="$1" check="$2"
    compose exec -T --user 10001:10001 "$service" sh -ec "$check" >/dev/null
}
