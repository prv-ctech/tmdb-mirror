#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=stress-common.sh
source "$SCRIPT_DIR/stress-common.sh"

usage() {
    cat <<'USAGE'
Usage: scripts/stress-bootstrap.sh [options]

Options:
  --project-name NAME       isolated Compose project (default: tmdb_stress_test)
  --api-port PORT           loopback API port (default: 18080)
  --admin-port PORT         loopback admin port (default: 18081)
  --image-port PORT         loopback media port (default: 18090)
  --postgres-port PORT      loopback PostgreSQL port (default: 55433)
  --secrets-file PATH       local ignored secrets file
  --skip-build              reuse tmdb-stress-app:local
USAGE
}

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
postgres_port="${TMDB_STRESS_PG_PORT:-55433}"
secrets_file="${TMDB_STRESS_SECRETS_FILE:-}"
skip_build=false

while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        --postgres-port) postgres_port="$2"; shift 2 ;;
        --secrets-file) secrets_file="$2"; shift 2 ;;
        --skip-build) skip_build=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown option: $1" ;;
    esac
done

[[ "$project" =~ ^[a-z0-9][a-z0-9_-]{2,48}$ ]] || die 'project name must be 3-49 lowercase characters'
for port in "$api_port" "$admin_port" "$image_port" "$postgres_port"; do
    [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1024 && port <= 65535 )) \
        || die "invalid port: $port"
done

configure_runtime "$project" "$api_port" "$admin_port" "$image_port" "$postgres_port"
if [[ -z "$secrets_file" ]]; then
    secrets_file="$(select_secrets_file)"
fi
export TMDB_STRESS_SECRETS_FILE="$secrets_file"
read_stress_secrets "$secrets_file"
[[ -n "$STRESS_READ_TOKEN" ]] || die 'TMDB_STRESS_READ_TOKEN is missing from the local secrets file'
export TMDB_READ_ACCESS_TOKEN="$STRESS_READ_TOKEN"
if [[ -n "$STRESS_TRAWL_URL" ]]; then
    export TMDB_TRAWL_BASE_URL="$STRESS_TRAWL_URL"
else
    unset TMDB_TRAWL_BASE_URL
fi
trap 'unset TMDB_READ_ACCESS_TOKEN TMDB_TRAWL_BASE_URL STRESS_READ_TOKEN STRESS_API_KEY' EXIT

require_command docker
docker_command version --format '{{.Server.Version}}' >/dev/null || die 'Docker Desktop daemon is unavailable'

existing="$(docker_command ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME")"
if [[ -z "$existing" ]]; then
    assert_port_free "$api_port"
    assert_port_free "$admin_port"
    assert_port_free "$image_port"
    assert_port_free "$postgres_port"
fi

write_runtime_files
if grep -q '^TMDB_READ_ACCESS_TOKEN=' "$ENV_FILE"; then
    die 'runtime Compose env must not persist the TMDB token'
fi

if [[ "$skip_build" != true ]]; then
    printf '%s\n' 'Building the pinned PostgreSQL/pgBackRest image...'
    docker_checked build --pull=false \
        --file "$(docker_path "$REPO_ROOT/infra/postgres/Dockerfile")" \
        --tag tmdb-stress-postgres:local \
        "$(docker_path "$REPO_ROOT")"
    printf '%s\n' 'Building the pinned Rust application image...'
    docker_checked build --pull=false --file "$(docker_path "$REPO_ROOT/Dockerfile")" --tag tmdb-stress-app:local "$(docker_path "$REPO_ROOT")"
else
    docker_command image inspect tmdb-stress-postgres:local >/dev/null \
        || die '--skip-build requested but tmdb-stress-postgres:local does not exist'
    docker_command image inspect tmdb-stress-app:local >/dev/null \
        || die '--skip-build requested but tmdb-stress-app:local does not exist'
fi

compose_checked config --quiet >/dev/null

printf 'Starting isolated PostgreSQL project %s...\n' "$PROJECT_NAME"
compose_checked up -d --remove-orphans "$POSTGRES_SERVICE"
wait_for_health "$POSTGRES_SERVICE"
compose exec -T "$POSTGRES_SERVICE" sh -ec \
    'test -x /usr/local/bin/tmdb-pgbackrest && pgbackrest version >/dev/null && test -d /config/backups/pgbackrest && test "$(stat -c %U /config/backups/pgbackrest)" = postgres' \
    || die 'PostgreSQL pgBackRest runtime contract is not ready'

printf '%s\n' 'Starting the consolidated worker so it applies migrations...'
compose_checked up -d --remove-orphans worker
wait_for_migrations

printf '%s\n' 'Starting API and media worker...'
compose_checked up -d --remove-orphans api media
wait_for_health api
wait_for_health media

assert_process_identity() {
    local service="$1" process="$2"
    compose exec -T --user 0:0 "$service" sh -ec '
        for proc in /proc/[0-9]*; do
            read -r comm < "$proc/comm" || continue
            if [ "$comm" = "$1" ]; then
                [ "$(stat -c %u "$proc")" = 10001 ]
                exit $?
            fi
        done
        exit 1
    ' sh "$process"
}

assert_process_identity worker tmdb-worker
assert_process_identity media tmdb-images
assert_process_identity api tmdb-api
ensure_owner_paths api 'test -w /config/logs'
ensure_owner_paths worker 'test -w /config/raw && test -w /config/logs'
ensure_owner_paths media 'test ! -e /config/media && test -w /media/movies && test -w /media/tv && test -w /media/people && test -w /media/networks && test -w /media/companies && test -w /media/collections'

for service in api worker media; do
    compose exec -T "$service" sh -ec \
        'for path in "/config/logs/$1.log" /config/logs/"$1"-*.log; do
            if test -s "$path" && head -n 1 "$path" | jq -e '\''type == "object"'\'' >/dev/null; then
                exit 0
            fi
         done
         exit 1' \
        sh "$service" \
        || die "$service JSONL file log is not ready"
done
compose exec -T "$POSTGRES_SERVICE" sh -ec \
    'for path in /config/logs/postgres.log /config/logs/postgres-*.log; do
        if test -s "$path" && head -n 1 "$path" | jq -e '\''type == "object"'\'' >/dev/null; then
            exit 0
        fi
     done
     exit 1' \
    || die 'postgres JSONL file log is not ready'

printf 'Stress stack is ready: http://127.0.0.1:%s\n' "$api_port"
printf 'Runtime metadata: %s\n' "$METADATA_FILE"
