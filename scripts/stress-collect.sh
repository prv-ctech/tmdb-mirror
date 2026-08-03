#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-collect.sh [--project-name NAME]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
configure_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" \
    "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
compose_file="$RESULT_ROOT/compose-$stamp.txt"
logs_file="$RESULT_ROOT/logs-$stamp.txt"
stats_file="$RESULT_ROOT/docker-stats-$stamp.jsonl"
postgres_logs="$RESULT_ROOT/postgres-logs-$stamp.txt"
database_file="$RESULT_ROOT/database-$stamp.json"

compose ps --all >"$compose_file" 2>&1
raw_logs="$(mktemp)"
raw_postgres="$(mktemp)"
trap 'rm -f "$raw_logs" "$raw_postgres"' EXIT
compose logs --no-color --timestamps >"$raw_logs" 2>&1 || true
compose logs --no-color --timestamps postgres >"$raw_postgres" 2>&1 || true
redact_file "$raw_logs" "$logs_file"
redact_file "$raw_postgres" "$postgres_logs"
container_ids=()
while IFS= read -r container_id; do
    [[ -n "$container_id" ]] && container_ids+=("$container_id")
done < <(docker_command ps -q --filter "label=com.docker.compose.project=$PROJECT_NAME" 2>/dev/null || true)
if ((${#container_ids[@]} > 0)); then
    docker_command stats --no-stream --format '{{json .}}' "${container_ids[@]}" >"$stats_file" 2>/dev/null || true
else
    : >"$stats_file"
fi

password="$(database_password)"
db_stats="$(psql_at "$password" "SELECT json_build_object(
  'database_size_bytes', pg_database_size(current_database()),
  'database_size', pg_size_pretty(pg_database_size(current_database())),
  'titles', (SELECT count(*) FROM catalog.titles),
  'anime_titles', (SELECT count(*) FROM catalog.titles WHERE is_anime),
  'people', (SELECT count(*) FROM catalog.people),
  'search_documents', (SELECT count(*) FROM search.search_documents),
  'jobs_queued', (SELECT count(*) FROM ops.jobs WHERE status IN ('queued','retry_wait','running')),
  'jobs_succeeded', (SELECT count(*) FROM ops.jobs WHERE status = 'succeeded'),
  'jobs_failed', (SELECT count(*) FROM ops.jobs WHERE status = 'dead_letter'),
  'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
  'cache_hit_ratio', (SELECT round(100.0 * sum(blks_hit) / nullif(sum(blks_hit + blks_read), 0), 3) FROM pg_stat_database WHERE datname = current_database())
)")"
printf '%s\n' "$db_stats" >"$database_file"

contention=false
grep -Eiq 'ERROR:[[:space:]]+(deadlock detected|canceling statement due to lock timeout)' "$postgres_logs" && contention=true || true
cat <<EOF
Collected stress artifacts under $RESULT_ROOT
Compose status: $compose_file
Database statistics: $database_file
PostgreSQL contention detected: $contention
EOF
if [[ "$contention" == true ]]; then
    die "catalog write contention was detected; see $postgres_logs"
fi
