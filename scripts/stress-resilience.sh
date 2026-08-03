#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-resilience.sh [--project-name NAME] [--api-port PORT] [--image-port PORT]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_runtime "$project" "$api_port" "${TMDB_STRESS_ADMIN_PORT:-18081}" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/resilience-$stamp.json"
log_file="$RESULT_ROOT/resilience-logs-$stamp.txt"
base_url="http://127.0.0.1:$api_port"
ready_url="$base_url/health/ready"

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

compose logs --no-color --timestamps api postgres worker media >"$log_file" 2>&1 || true
redact "$(<"$log_file")" >"$log_file.redacted"
mv -f "$log_file.redacted" "$log_file"

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "media_restart": {"before": "$before_media", "after": "$after_media", "passed": $([[ "$before_media" == running && "$after_media" == running ]] && echo true || echo false)},
  "dependency_recovery": {"readiness_during_postgres_stop": $during_ready, "readiness_after_recovery": $after_ready, "failure_observed": $([[ "$during_ready" != 200 ]] && echo true || echo false), "recovered": $([[ "$after_ready" == 200 ]] && echo true || echo false), "worker": "$worker_state", "media": "$media_state"},
  "log_artifact": "$log_file"
}
EOF
cat "$result_file"
if [[ "$before_media" != running || "$after_media" != running || "$during_ready" == 200 || "$after_ready" != 200 || "$worker_state" != running || "$media_state" != running ]]; then
    die "resilience checks failed; see $result_file and $log_file"
fi
printf '%s\n' 'Resilience checks passed.'
