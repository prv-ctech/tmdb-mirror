#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
media_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=180
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --media-port) media_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-media-scans.sh [--project-name NAME] [--admin-port PORT] [--media-port PORT] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$timeout" =~ ^[0-9]+$ ]] && (( timeout >= 30 && timeout <= 1800 )) || die 'invalid timeout'

configure_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "$admin_port" "$media_port" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
require_command curl
require_command python3
STRESS_ADMIN_KEY="$(env_value TMDB_ADMIN_API_KEY)"
[[ -n "$STRESS_ADMIN_KEY" ]] || die 'TMDB_ADMIN_API_KEY is missing from the stress runtime'
trap 'unset STRESS_ADMIN_KEY' EXIT

mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/media-scans-$stamp.json"
base_url="http://127.0.0.1:$admin_port"
failures=0
last_http_status=000
last_body=''

api_call() {
    local method="$1" path="$2" idempotency_key="${3:-}" body="${4:-}"
    local response_file error_file
    response_file="$(mktemp)"
    error_file="$(mktemp)"
    if [[ "$method" == GET ]]; then
        if ! last_http_status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
            -H "X-API-Key: $STRESS_ADMIN_KEY" \
            --output "$response_file" --write-out '%{http_code}' \
            "$base_url$path" 2>"$error_file")"; then
            last_http_status=000
            redact "$(<"$error_file")" >&2
        fi
    else
        if ! last_http_status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
            -X "$method" \
            -H "X-API-Key: $STRESS_ADMIN_KEY" \
            -H "Idempotency-Key: $idempotency_key" \
            -H 'Content-Type: application/json' \
            --data "$body" \
            --output "$response_file" --write-out '%{http_code}' \
            "$base_url$path" 2>"$error_file")"; then
            last_http_status=000
            redact "$(<"$error_file")" >&2
        fi
    fi
    last_body="$(<"$response_file")"
    rm -f "$response_file" "$error_file"
}

api_call_without_auth() {
    local path="$1" response_file error_file
    response_file="$(mktemp)"
    error_file="$(mktemp)"
    if ! last_http_status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
        --output "$response_file" --write-out '%{http_code}' \
        "$base_url$path" 2>"$error_file")"; then
        last_http_status=000
        redact "$(<"$error_file")" >&2
    fi
    last_body="$(<"$response_file")"
    rm -f "$response_file" "$error_file"
}

json_value() {
    local field="$1"
    printf '%s' "$last_body" | python3 -c '
import json
import sys

value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    if not isinstance(value, dict):
        value = None
        break
    value = value.get(part)
if isinstance(value, bool):
    print(str(value).lower())
elif value is not None:
    print(value)
' "$field"
}

expect_status() {
    local name="$1" expected="$2"
    if [[ "$last_http_status" != "$expected" ]]; then
        printf 'FAIL %s: expected HTTP %s, got %s\n' "$name" "$expected" "$last_http_status" >&2
        redact "$last_body" >&2
        failures=$((failures + 1))
    fi
}

expect_value() {
    local name="$1" field="$2" expected="$3" actual
    actual="$(json_value "$field" 2>/dev/null || true)"
    if [[ "$actual" != "$expected" ]]; then
        printf 'FAIL %s: expected %s=%s, got %s\n' "$name" "$field" "$expected" "$actual" >&2
        failures=$((failures + 1))
    fi
}

api_call_without_auth /admin/v1/media/worker
auth_status="$last_http_status"
if [[ "$auth_status" != 401 ]]; then
    printf 'FAIL unauthenticated media-worker request: expected 401, got %s\n' "$auth_status" >&2
    failures=$((failures + 1))
fi

api_call POST /admin/v1/media/worker "media-worker-start-$stamp" '{"action":"start"}'
expect_status start_worker 200
initial_state="$(json_value data.state 2>/dev/null || true)"
expect_value start_worker data.state running

api_call POST /admin/v1/media/scans "media-scan-invalid-$stamp" '{"mode":"full","repair":true}'
expect_status full_repair_rejected 422

audit_key="media-scan-audit-$stamp"
api_call POST /admin/v1/media/scans "$audit_key" '{"mode":"audit","repair":true}'
expect_status audit_scan_submitted 202
audit_run_id="$(json_value data.runId 2>/dev/null || true)"
[[ "$audit_run_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'audit scan response did not contain a valid run ID'
expect_value audit_scan_submitted data.duplicate false

api_call POST /admin/v1/media/scans "$audit_key" '{"mode":"audit","repair":true}'
expect_status audit_scan_duplicate 202
expect_value audit_scan_duplicate data.duplicate true
expect_value audit_scan_duplicate data.runId "$audit_run_id"

pause_key="media-worker-pause-$stamp"
api_call POST /admin/v1/media/worker "$pause_key" '{"action":"pause"}'
expect_status pause_worker 200
expect_value pause_worker data.state paused
api_call POST /admin/v1/media/worker "$pause_key" '{"action":"pause"}'
expect_status pause_worker_duplicate 200
expect_value pause_worker_duplicate data.state paused

compose_checked restart -t 10 media >/dev/null
wait_for_health media 90
api_call GET /admin/v1/media/worker
expect_status paused_state_after_restart 200
expect_value paused_state_after_restart data.state paused

api_call POST /admin/v1/media/worker "media-worker-resume-$stamp" '{"action":"resume"}'
expect_status resume_worker 200
expect_value resume_worker data.state running

poll_scan() {
    local run_id="$1" max_seconds="$2" status=''
    local deadline=$((SECONDS + max_seconds))
    while (( SECONDS < deadline )); do
        api_call GET "/admin/v1/media/scans/$run_id"
        if [[ "$last_http_status" != 200 ]]; then
            sleep 2
            continue
        fi
        status="$(json_value data.status 2>/dev/null || true)"
        case "$status" in
            succeeded|failed|cancelled)
                poll_scan_status="$status"
                poll_scan_body="$last_body"
                return 0
                ;;
        esac
        sleep 3
    done
    poll_scan_status="timeout"
    poll_scan_body="$last_body"
    return 1
}

if ! poll_scan "$audit_run_id" "$timeout"; then
    printf 'FAIL audit scan did not reach a terminal state\n' >&2
    failures=$((failures + 1))
fi
first_audit_status="$poll_scan_status"
first_audit_body="$poll_scan_body"

cancel_pause_key="media-worker-cancel-pause-$stamp"
api_call POST /admin/v1/media/worker "$cancel_pause_key" '{"action":"pause"}'
expect_status cancel_setup_pause 200
expect_value cancel_setup_pause data.state paused

cancel_scan_key="media-scan-cancel-$stamp"
api_call POST /admin/v1/media/scans "$cancel_scan_key" '{"mode":"audit","repair":false}'
expect_status cancel_scan_submitted 202
cancel_run_id="$(json_value data.runId 2>/dev/null || true)"
[[ "$cancel_run_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'cancel scan response did not contain a valid run ID'

cancel_phase=''
cancel_deadline=$((SECONDS + 60))
while (( SECONDS < cancel_deadline )); do
    api_call GET "/admin/v1/media/scans/$cancel_run_id"
    cancel_phase="$(json_value data.phase 2>/dev/null || true)"
    [[ "$cancel_phase" == audit ]] && break
    sleep 2
done
if [[ "$cancel_phase" != audit ]]; then
    printf 'FAIL cancel scan did not reach the audit phase before cancellation\n' >&2
    failures=$((failures + 1))
fi

cancel_key="media-worker-cancel-$stamp"
api_call POST /admin/v1/media/worker "$cancel_key" '{"action":"cancel"}'
expect_status cancel_worker 200
expect_value cancel_worker data.state stopped
api_call GET /admin/v1/media/worker
expect_status stopped_state 200
expect_value stopped_state data.state stopped

if ! poll_scan "$cancel_run_id" 90; then
    printf 'FAIL cancelled media scan did not reach a terminal state\n' >&2
    failures=$((failures + 1))
fi
cancel_scan_status="$poll_scan_status"
if [[ "$cancel_scan_status" != cancelled ]]; then
    printf 'FAIL cancelled media scan ended with %s\n' "$cancel_scan_status" >&2
    failures=$((failures + 1))
fi

api_call POST /admin/v1/media/worker "media-worker-final-start-$stamp" '{"action":"start"}'
expect_status final_start_worker 200
expect_value final_start_worker data.state running

first_audited="$(printf '%s' "$first_audit_body" | python3 -c '
import json, sys
value = json.load(sys.stdin).get("data", {}).get("auditedCount", 0)
print(value)
' 2>/dev/null || printf '0')"
first_invalid="$(printf '%s' "$first_audit_body" | python3 -c '
import json, sys
value = json.load(sys.stdin).get("data", {}).get("invalidCount", 0)
print(value)
' 2>/dev/null || printf '0')"
first_repairs="$(printf '%s' "$first_audit_body" | python3 -c '
import json, sys
value = json.load(sys.stdin).get("data", {}).get("repairQueuedCount", 0)
print(value)
' 2>/dev/null || printf '0')"

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "unauthenticated_status": $auth_status,
  "initial_worker_state": "$initial_state",
  "worker_restart_preserved_pause": true,
  "audit_scan": {
    "run_id": "$audit_run_id",
    "status": "$first_audit_status",
    "audited_count": $first_audited,
    "invalid_count": $first_invalid,
    "repair_queued_count": $first_repairs
  },
  "cancelled_scan": {
    "run_id": "$cancel_run_id",
    "status": "$cancel_scan_status"
  },
  "final_worker_state": "$(json_value data.state 2>/dev/null || true)",
  "failures": $failures
}
EOF
cat "$result_file"
printf 'Media-scan control artifact: %s\n' "$result_file"
(( failures == 0 )) || die 'media-scan control verification failed'
printf '%s\n' 'Media-scan control verification passed.'
