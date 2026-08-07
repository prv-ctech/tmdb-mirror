#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../stress-common.sh"

result_dir=''
compose_file=''
compose_env_file=''
compose_project=''
admin_metrics_url=''
run_started=''
while (($#)); do
    case "$1" in
        --result-directory) result_dir="$2"; shift 2 ;;
        --compose-file) compose_file="$2"; shift 2 ;;
        --compose-env-file) compose_env_file="$2"; shift 2 ;;
        --compose-project-name) compose_project="$2"; shift 2 ;;
        --admin-metrics-url) admin_metrics_url="$2"; shift 2 ;;
        --run-started-at-utc) run_started="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: collect-diagnostics.sh --result-directory PATH [options]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ -n "$result_dir" ]] || die '--result-directory is required'
[[ -d "$result_dir" ]] || die "result directory is missing: $result_dir"

if secret_path="$(select_secrets_file 2>/dev/null)"; then
    read_stress_secrets "$secret_path"
    export TMDB_READ_ACCESS_TOKEN="$STRESS_READ_TOKEN"
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
compose_output="$result_dir/diagnostics-compose-$stamp.txt"
stats_output="$result_dir/diagnostics-stats-$stamp.jsonl"
logs_output="$result_dir/diagnostics-logs-$stamp.txt"
postgres_output="$result_dir/diagnostics-postgres-$stamp.txt"
metrics_output="$result_dir/diagnostics-admin-metrics-$stamp.txt"
manifest="$result_dir/diagnostics-$stamp.json"

compose_status='not_requested'
compose_args=()
if [[ -n "$compose_file" ]]; then
    [[ -f "$compose_file" ]] || die "Compose file is missing: $compose_file"
    compose_args=(compose)
    [[ -n "$compose_env_file" ]] && compose_args+=(--env-file "$(docker_path "$compose_env_file")")
    [[ -n "$compose_project" ]] && compose_args+=(--project-name "$compose_project")
    compose_args+=(--file "$(docker_path "$compose_file")")
    if docker_command "${compose_args[@]}" ps --all >"$compose_output" 2>&1; then
        compose_status='captured'
    else
        compose_status='error'
    fi
    redact_file "$compose_output" "$compose_output.redacted"
    mv -f "$compose_output.redacted" "$compose_output"
fi

container_ids=()
if [[ -n "$compose_project" ]]; then
    while IFS= read -r id; do
        [[ -n "$id" ]] && container_ids+=("$id")
    done < <(docker_command ps -q --filter "label=com.docker.compose.project=$compose_project" 2>/dev/null || true)
fi
docker_status='not_requested'
if ((${#container_ids[@]} > 0)); then
    docker_status='captured'
    docker_command stats --no-stream --format '{{json .}}' "${container_ids[@]}" >"$stats_output" 2>&1 || true
    raw_logs="$(mktemp)"
    trap 'rm -f "$raw_logs" "$raw_logs.tail"' EXIT
    docker_command logs --timestamps --since "${run_started:-1h}" "${container_ids[0]}" >"$raw_logs" 2>&1 || true
    for id in "${container_ids[@]:1}"; do
        docker_command logs --timestamps --since "${run_started:-1h}" "$id" >>"$raw_logs" 2>&1 || true
    done
    tail -n 2000 "$raw_logs" >"$raw_logs.tail"
    redact_file "$raw_logs.tail" "$logs_output"
    rm -f "$raw_logs" "$raw_logs.tail"
fi

postgres_status='not_requested'
postgres_id=''
if [[ -n "$compose_project" ]]; then
    postgres_id="$(docker_command ps -q --filter "label=com.docker.compose.project=$compose_project" --filter "label=com.docker.compose.service=$POSTGRES_SERVICE" | head -n 1 || true)"
fi
if [[ -n "$postgres_id" ]]; then
    postgres_status='captured'
    docker_command exec -i "$postgres_id" sh -ec '
        PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 -At --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<"SQL"
SELECT json_build_object(
  'waiting_backends', (SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL),
  'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
  'cache_hit_ratio', (SELECT round(100.0 * sum(blks_hit) / nullif(sum(blks_hit + blks_read), 0), 3) FROM pg_stat_database WHERE datname = current_database())
);
SELECT calls, round(total_exec_time::numeric, 3), round(mean_exec_time::numeric, 3), left(query, 300)
FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 25;
SQL
    ' >"$postgres_output" 2>&1 || true
    redact_file "$postgres_output" "$postgres_output.redacted"
    mv -f "$postgres_output.redacted" "$postgres_output"
fi

admin_status='not_requested'
if [[ -n "$admin_metrics_url" ]]; then
    [[ "$admin_metrics_url" =~ ^https?://[^[:space:]/?#@]+([:/][^[:space:]?#@]+)?$ ]] \
        || die 'admin metrics URL must be an http(s) URL without credentials or query data'
    admin_key="${TMDB_K6_ADMIN_API_KEY:-}"
    if [[ -n "$admin_key" ]]; then
        curl --silent --show-error --fail --connect-timeout 5 --max-time 20 \
            -H "X-API-Key: $admin_key" "$admin_metrics_url" >"$metrics_output" 2>&1 || true
        redact_file "$metrics_output" "$metrics_output.redacted"
        mv -f "$metrics_output.redacted" "$metrics_output"
        admin_status='captured'
    else
        admin_status='skipped_missing_key'
    fi
fi

compose_json='null'
[[ -n "$compose_project" ]] && compose_json="\"$compose_project\""
cat >"$manifest" <<EOF
{
  "schema_version": 1,
  "captured_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_started_at_utc": "${run_started:-}",
  "compose_project": $compose_json,
  "compose_status": "$compose_status",
  "docker_status": "$docker_status",
  "container_count": ${#container_ids[@]},
  "postgres_status": "$postgres_status",
  "admin_metrics_status": "$admin_status"
}
EOF
printf 'Diagnostics collected under %s\n' "$result_dir"
printf 'Manifest: %s\n' "$manifest"
