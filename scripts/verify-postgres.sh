#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project='tmdb_rust_foundation_test'
port=55432
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --port) port="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: verify-postgres.sh [--project-name NAME] [--port 55432]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$project" =~ ^[a-z0-9][a-z0-9_.-]{2,63}$ ]] || die 'invalid Compose project name'
[[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1024 && port <= 65535 )) || die 'invalid PostgreSQL port'

compose_file="$REPO_ROOT/deploy/compose.dev.yaml"
env_file="$REPO_ROOT/deploy/env.example"
[[ -f "$compose_file" && -f "$env_file" ]] || die 'development PostgreSQL Compose files are missing'
dev_env_file="$(mktemp /tmp/tmdb-verify-postgres-env.XXXXXX)"
chmod 600 "$dev_env_file"
cp "$env_file" "$dev_env_file"
printf '\nTMDB_DEV_PG_PORT=%s\n' "$port" >>"$dev_env_file"
trap 'unlink "$dev_env_file"' EXIT
dev_args=(compose --env-file "$(docker_path "$dev_env_file")" --project-name "$project" --file "$(docker_path "$compose_file")")
dev_compose() { docker_command "${dev_args[@]}" "$@"; }
dev_compose_checked() {
    local output
    if ! output="$(dev_compose "$@" 2>&1)"; then
        redact "$output" >&2
        return 1
    fi
    printf '%s' "$output"
}

postgres_service=tmdb-mirror-postgres
container_name="${project}-${postgres_service}-1"
volume_name="${project}_tmdb_pg18_data"
internal_network="${project}_tmdb-internal"
loopback_network="${project}_tmdb-loopback"
dev_env_value() {
    local name="$1" line
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ "$line" == "$name="* ]] || continue
        printf '%s\n' "${line#*=}"
        return 0
    done <"$env_file"
    return 1
}

dev_compose_checked config --quiet >/dev/null
existing="$(docker_command ps -aq --filter "label=com.docker.compose.project=$project" --filter "label=com.docker.compose.service=$postgres_service")"
if [[ -z "$existing" ]] && command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$port" | tail -n +2 | grep -q .; then
    die "127.0.0.1:$port is occupied by an unrelated process"
fi
dev_compose_checked up -d "$postgres_service" >/dev/null

container_id=''
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
    container_id="$(dev_compose ps -q "$postgres_service" 2>/dev/null || true)"
    if [[ -n "$container_id" ]]; then
        health="$(docker_command inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
        [[ "$health" == 'running|healthy' ]] && break
    fi
    sleep 2
done
[[ -n "$container_id" ]] || die 'development PostgreSQL container was not created'
health="$(docker_command inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id")"
[[ "$health" == 'running|healthy' ]] || die 'development PostgreSQL container is not healthy'

published="$(docker_command inspect --format '{{json .NetworkSettings.Ports}}' "$container_id")"
grep -Fq '127.0.0.1' <<<"$published" || die 'PostgreSQL is not loopback-bound'
grep -Fq "$port" <<<"$published" || die "development PostgreSQL port drifted from $port"

actual_image="$(docker_command inspect --format '{{.Config.Image}}' "$container_id")"
grep -Fq 'postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296' <<<"$actual_image" \
    || die 'development PostgreSQL image is not the pinned PostgreSQL 18 image'

mounts="$(docker_command inspect --format '{{range .Mounts}}{{println .Type "|" .Destination "|" .Name "|" .RW}}{{end}}' "$container_id")"
grep -Fq "volume | /var/lib/postgresql | $volume_name | true" <<<"$mounts" || die 'PostgreSQL data volume contract drifted'
grep -Fq 'bind | /docker-entrypoint-initdb.d/10-bootstrap.sh |  | false' <<<"$mounts" || die 'PostgreSQL init mount must be read-only'

network_info() {
    docker_command network inspect --format '{{.Internal}}|{{.Driver}}|{{index .Labels "com.docker.compose.project"}}|{{index .Labels "com.docker.compose.network"}}' "$1"
}
[[ "$(network_info "$internal_network")" == "true|bridge|$project|tmdb-internal" ]] \
    || die 'internal development network contract drifted'
[[ "$(network_info "$loopback_network")" == "false|bridge|$project|tmdb-loopback" ]] \
    || die 'loopback development network contract drifted'
[[ "$(docker_command volume inspect --format '{{.Driver}}|{{index .Labels "com.docker.compose.project"}}|{{index .Labels "com.docker.compose.volume"}}' "$volume_name")" == "local|$project|tmdb_pg18_data" ]] \
    || die 'PostgreSQL volume metadata contract drifted'

database_name="$(dev_env_value POSTGRES_DB)"
database_user="$(dev_env_value POSTGRES_USER)"
database_password="$(dev_env_value POSTGRES_PASSWORD)"
psql_scalar() {
    dev_compose exec -T -e "PGPASSWORD=$database_password" "$postgres_service" psql -X -v ON_ERROR_STOP=1 -At \
        --username "$database_user" --dbname "$database_name" -c "$1"
}
[[ "$(psql_scalar 'SHOW server_version')" =~ ^18\. ]] || die 'PostgreSQL server major version is not 18'
[[ "$(psql_scalar 'SHOW data_checksums')" == on ]] || die 'data checksums are not enabled'
[[ "$(psql_scalar 'SHOW shared_preload_libraries')" == pg_stat_statements ]] || die 'pg_stat_statements preload drifted'
extensions="$(psql_scalar "SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension WHERE extname IN ('pg_stat_statements', 'pg_trgm', 'unaccent')")"
[[ "$extensions" == 'pg_stat_statements,pg_trgm,unaccent' ]] || die 'required PostgreSQL extensions are missing'

printf 'PostgreSQL development cluster is healthy under project %s.\n' "$project"
printf '%s\n' 'Checks passed: pinned image, loopback port, checksums, preload, extensions, mounts, networks, and volume labels.'
