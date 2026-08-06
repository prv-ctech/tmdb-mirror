#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
media_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=300
max_active=1000
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --media-port) media_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        --max-active) max_active="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-scan.sh [--project-name NAME] [--admin-port PORT] [--media-port PORT] [--timeout SECONDS] [--max-active N]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$timeout" =~ ^[0-9]+$ ]] && (( timeout >= 30 && timeout <= 1800 )) || die 'invalid timeout'
[[ "$max_active" =~ ^[0-9]+$ ]] && (( max_active > 0 && max_active <= 10000 )) || die 'invalid max-active'

configure_existing_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "$admin_port" "$media_port" "${TMDB_STRESS_PG_PORT:-55433}"
admin_port="$ADMIN_PORT"
media_port="$IMAGE_PORT"
load_runtime
require_command curl
require_command python3
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/catalog-scan-$stamp.json"
admin_key="$(env_value TMDB_ADMIN_API_KEY)"
[[ -n "$admin_key" ]] || die 'TMDB_ADMIN_API_KEY is missing from the stress runtime'
base_url="http://127.0.0.1:$admin_port"
password="$(database_password)"
trap 'unset admin_key' EXIT

admin_post() {
    local path="$1" key="$2" body="$3" response_file error_file http_status response
    response_file="$(mktemp)"
    error_file="$(mktemp)"
    if ! http_status="$(curl --silent --show-error --connect-timeout 10 --max-time 30 \
        -X POST \
        -H "X-API-Key: $admin_key" \
        -H "Idempotency-Key: $key" \
        -H 'Content-Type: application/json' \
        --data "$body" \
        --output "$response_file" --write-out '%{http_code}' \
        "$base_url$path" 2>"$error_file")"; then
        redact "$(<"$error_file")" >&2
        rm -f "$response_file" "$error_file"
        die "admin request failed: $path"
    fi
    response="$(<"$response_file")"
    if [[ "$http_status" != 200 && "$http_status" != 202 ]]; then
        redact "$response$(<"$error_file")" >&2
        rm -f "$response_file" "$error_file"
        die "admin request returned HTTP $http_status: $path"
    fi
    rm -f "$response_file" "$error_file"
    printf '%s\n' "$response"
}

start_worker() {
    local path="$1" key="$2" response
    response="$(admin_post "$path" "$key" '{"action":"start"}')"
    grep -q '"state":"running"' <<<"$response" || die "worker did not enter running state: $path"
}

start_worker /admin/v1/worker "catalog-scan-start-ingest-$stamp"

active_work() {
    psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE status IN ('queued', 'running', 'retry_wait')"
}

active_before="$(active_work)"
dead_letters_before="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE status = 'dead_letter'")"
scan_response="$(admin_post /admin/v1/scans "catalog-scan-$stamp" '{"mode":"missing_only","mediaTypes":["movie","tv"]}')"
scan_job_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["jobId"])' <<<"$scan_response")"
[[ "$scan_job_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'catalog scan response returned no valid job ID'

scan_status='pending'
scan_deadline=$((SECONDS + timeout))
while (( SECONDS < scan_deadline )); do
    job_response="$(curl --silent --show-error --fail \
        -H "X-API-Key: $admin_key" \
        "$base_url/admin/v1/jobs/$scan_job_id" 2>/dev/null || true)"
    scan_status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["job"]["status"])' <<<"$job_response" 2>/dev/null || printf 'pending')"
    case "$scan_status" in
        succeeded|dead_letter|cancelled|failed) break ;;
    esac
    sleep 2
done

active_peak="$active_before"
pending_child_jobs=-1
drain_deadline=$((SECONDS + timeout))
while (( SECONDS < drain_deadline )); do
    pending_child_jobs="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv', 'ingest.refresh_season', 'ingest.enrich_movie', 'ingest.enrich_tv') AND status IN ('queued', 'running', 'retry_wait')" 2>/dev/null || printf '%s' '-1')"
    active_now="$(active_work 2>/dev/null || printf '%s' '-1')"
    if [[ "$active_now" =~ ^[0-9]+$ ]] && (( active_now > active_peak )); then
        active_peak="$active_now"
    fi
    if [[ "$pending_child_jobs" =~ ^[0-9]+$ ]] && (( pending_child_jobs == 0 )); then
        break
    fi
    sleep 3
done

active_after="$(active_work 2>/dev/null || printf '%s' '-1')"
dead_letters_after="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE status = 'dead_letter'")"
new_dead_letters=$((dead_letters_after - dead_letters_before))

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scan_mode": "missing_only",
  "scan_job_id": "$scan_job_id",
  "scan_status": "$scan_status",
  "active_before": $active_before,
  "active_peak": $active_peak,
  "active_after": $active_after,
  "pending_catalog_children": $pending_child_jobs,
  "max_active": $max_active,
  "dead_letters_before": $dead_letters_before,
  "dead_letters_after": $dead_letters_after,
  "new_dead_letters": $new_dead_letters
}
EOF
cat "$result_file"
printf 'Catalog scan artifact: %s\n' "$result_file"

if [[ "$scan_status" != succeeded ]] || (( pending_child_jobs != 0 || active_peak > max_active || new_dead_letters != 0 )); then
    die 'API-controlled catalog scan verification failed'
fi
printf '%s\n' 'API-controlled catalog scan verification passed.'
