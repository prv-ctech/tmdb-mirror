#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-resilience.sh [--project-name NAME] [--admin-port PORT] [--api-port PORT] [--image-port PORT]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_existing_runtime "$project" "$api_port" "$admin_port" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
api_port="$API_PORT"
admin_port="$ADMIN_PORT"
image_port="$IMAGE_PORT"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/resilience-$stamp.json"
log_file="$RESULT_ROOT/resilience-logs-$stamp.txt"
base_url="http://127.0.0.1:$api_port"
ready_url="$base_url/health/ready"
admin_key="$(env_value TMDB_ADMIN_API_KEY)"

start_worker() {
    local path="$1" key="$2" output
    if ! output="$(curl --silent --show-error --fail-with-body -X POST \
        -H "X-API-Key: $admin_key" \
        -H "Idempotency-Key: $key" \
        -H 'Content-Type: application/json' \
        --data '{"action":"start"}' \
        "http://127.0.0.1:$admin_port$path" 2>&1)"; then
        redact "$output" >&2
        die "could not start worker for resilience test: $path"
    fi
    if ! grep -q '"state":"running"' <<<"$output"; then
        redact "$output" >&2
        die "worker did not enter running state for resilience test: $path"
    fi
}

start_worker /admin/v1/worker "resilience-start-ingest-$stamp"
start_worker /admin/v1/media/worker "resilience-start-media-$stamp"

admin_worker_state() {
    local path="$1"
    curl --silent --show-error --fail \
        -H "X-API-Key: $admin_key" \
        "http://127.0.0.1:$admin_port$path" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["data"]["state"])'
}

before_ingest_control="$(admin_worker_state /admin/v1/worker)"
before_media_control="$(admin_worker_state /admin/v1/media/worker)"

compose_checked restart -t 10 worker >/dev/null
sleep 3
after_worker_restart_control="$(admin_worker_state /admin/v1/worker)"
start_worker /admin/v1/worker "resilience-restart-start-ingest-$stamp"

http_status() {
    curl --silent --show-error --output /dev/null --write-out '%{http_code}' --connect-timeout 5 --max-time 10 "$1" || printf '000'
}

container_state() {
    local service="$1" id
    id="$(compose ps -q "$service")"
    [[ -n "$id" ]] || { printf 'missing'; return; }
    docker_command inspect --format '{{.State.Status}}' "$id"
}

before_media="$(container_state media)"
compose_checked restart -t 10 media >/dev/null
wait_for_health media 90
after_media="$(container_state media)"
after_media_restart_control="$(admin_worker_state /admin/v1/media/worker)"
start_worker /admin/v1/media/worker "resilience-restart-start-media-$stamp"

compose_checked stop postgres >/dev/null
sleep 5
during_ready="$(http_status "$ready_url")"
compose_checked start postgres >/dev/null
wait_for_health postgres 90

after_ready=000
deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
    after_ready="$(http_status "$ready_url")"
    [[ "$after_ready" == 200 ]] && break
    sleep 2
done
worker_state="$(container_state worker)"
media_state="$(container_state media)"
after_dependency_ingest_control="$(admin_worker_state /admin/v1/worker)"
after_dependency_media_control="$(admin_worker_state /admin/v1/media/worker)"
start_worker /admin/v1/worker "resilience-recovery-start-ingest-$stamp"
start_worker /admin/v1/media/worker "resilience-recovery-start-media-$stamp"
final_ingest_control="$(admin_worker_state /admin/v1/worker)"
final_media_control="$(admin_worker_state /admin/v1/media/worker)"

compose logs --no-color --timestamps api postgres worker media >"$log_file" 2>&1 || true
redact "$(<"$log_file")" >"$log_file.redacted"
mv -f "$log_file.redacted" "$log_file"

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "worker_restart": {"control_before": "$before_ingest_control", "control_after": "$after_worker_restart_control", "reset_to_stopped": $([[ "$before_ingest_control" == running && "$after_worker_restart_control" == stopped ]] && echo true || echo false)},
  "media_restart": {"before": "$before_media", "after": "$after_media", "control_before": "$before_media_control", "control_after": "$after_media_restart_control", "reset_to_stopped": $([[ "$before_media" == running && "$after_media" == running && "$before_media_control" == running && "$after_media_restart_control" == stopped ]] && echo true || echo false)},
  "dependency_recovery": {"readiness_during_postgres_stop": $during_ready, "readiness_after_recovery": $after_ready, "failure_observed": $([[ "$during_ready" != 200 ]] && echo true || echo false), "recovered": $([[ "$after_ready" == 200 ]] && echo true || echo false), "worker": "$worker_state", "media": "$media_state", "control_after_outage": {"ingest": "$after_dependency_ingest_control", "media": "$after_dependency_media_control"}, "remained_running": $([[ "$after_dependency_ingest_control" == running && "$after_dependency_media_control" == running ]] && echo true || echo false)},
  "final_control": {"ingest": "$final_ingest_control", "media": "$final_media_control"},
  "log_artifact": "$log_file"
}
EOF
cat "$result_file"
if [[ "$before_ingest_control" != running || "$after_worker_restart_control" != stopped || "$before_media" != running || "$after_media" != running || "$before_media_control" != running || "$after_media_restart_control" != stopped || "$during_ready" == 200 || "$after_ready" != 200 || "$worker_state" != running || "$media_state" != running || "$after_dependency_ingest_control" != running || "$after_dependency_media_control" != running || "$final_ingest_control" != running || "$final_media_control" != running ]]; then
    die "resilience checks failed; see $result_file and $log_file"
fi
printf '%s\n' 'Resilience checks passed.'
