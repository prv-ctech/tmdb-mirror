#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
timeout=300
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-media-scans.sh [--project-name NAME] [--admin-port PORT] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_existing_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "$admin_port" "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
admin_port="$ADMIN_PORT"
load_runtime
require_command curl
require_command python3
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/media-requests-$stamp.json"
base_url="http://127.0.0.1:$admin_port"
admin_key="$(env_value TMDB_ADMIN_API_KEY)"
password="$(database_password)"
[[ -n "$admin_key" ]] || die 'TMDB_ADMIN_API_KEY is missing from the stress runtime'
trap 'unset admin_key' EXIT

# Reserve a disposable, non-TMDB range so the 100-item contract and
# zero-asset incomplete-title behavior are deterministic in every stress run.
psql_at "$password" "INSERT INTO catalog.titles (
    media_type, tmdb_id, display_title, poster_path, backdrop_path, active, enriched_at
) SELECT CASE WHEN id % 2 = 0 THEN 'tv' ELSE 'movie' END,
         id, 'Incomplete media-request fixture ' || id, NULL, NULL, true, NULL
    FROM generate_series(899000001, 899000100) AS id
ON CONFLICT (media_type, tmdb_id) DO UPDATE SET
    poster_path = NULL, backdrop_path = NULL, active = true, enriched_at = NULL" >/dev/null

last_status=000
last_body=''
api_call() {
    local method="$1" path="$2" key="${3:-}" body="${4:-}" response_file error_file
    response_file="$(mktemp)"
    error_file="$(mktemp)"
    if [[ "$method" == GET ]]; then
        last_status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
            -H "X-API-Key: $admin_key" --output "$response_file" --write-out '%{http_code}' \
            "$base_url$path" 2>"$error_file" || true)"
    else
        last_status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
            -X "$method" -H "X-API-Key: $admin_key" -H "Idempotency-Key: $key" \
            -H 'Content-Type: application/json' --data "$body" \
            --output "$response_file" --write-out '%{http_code}' \
            "$base_url$path" 2>"$error_file" || true)"
    fi
    last_body="$(<"$response_file")"
    [[ "$last_status" != 000 ]] || redact "$(<"$error_file")" >&2
    rm -f "$response_file" "$error_file"
}

json_value() {
    local field="$1"
    printf '%s' "$last_body" | python3 -c '
import json, sys
value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    value = value.get(part) if isinstance(value, dict) else None
if isinstance(value, bool): print(str(value).lower())
elif value is not None: print(value)
' "$field"
}

expect_status() {
    local name="$1" expected="$2"
    if [[ "$last_status" != "$expected" ]]; then
        printf 'FAIL %s: expected HTTP %s, got %s\n' "$name" "$expected" "$last_status" >&2
        redact "$last_body" >&2
        failures=$((failures + 1))
    fi
}

poll_request() {
    local request_id="$1" deadline=$((SECONDS + timeout))
    poll_status='timeout'
    poll_body=''
    while (( SECONDS < deadline )); do
        api_call GET "/admin/v1/media/requests/$request_id"
        if [[ "$last_status" == 200 ]]; then
            poll_body="$last_body"
            poll_status="$(json_value data.status 2>/dev/null || printf unknown)"
            case "$poll_status" in succeeded|partial|failed|cancelled) return 0 ;; esac
        fi
        sleep 2
    done
    return 1
}

failures=0
unauthenticated="$(curl --silent --output /dev/null --write-out '%{http_code}' "$base_url/admin/v1/media/worker" || true)"
[[ "$unauthenticated" == 401 ]] || failures=$((failures + 1))

api_call POST /admin/v1/media/scans "removed-scan-$stamp" '{"mode":"full"}'
legacy_scan_status="$last_status"
expect_status removed_media_scan 404
api_call POST /admin/v1/media/audits "removed-audit-$stamp" '{}'
legacy_audit_status="$last_status"
expect_status removed_media_audit 404

# A durable request is accepted while the media container is offline and is
# claimed directly from PostgreSQL after container startup.
compose_checked stop media >/dev/null
offline_key="offline-media-$stamp"
api_call POST /admin/v1/media/requests "$offline_key" '{"items":[{"mediaType":"movie","tmdbId":550},{"mediaType":"movie","tmdbId":550}]}'
expect_status offline_submission 202
offline_request_id="$(json_value data.requestId 2>/dev/null || true)"
offline_duplicate="$(json_value data.duplicate 2>/dev/null || true)"
[[ "$offline_request_id" =~ ^[0-9a-fA-F-]{36}$ ]] || failures=$((failures + 1))
persisted_while_offline="$(psql_at "$password" "SELECT count(*) FROM ops.media_requests WHERE id = '$offline_request_id'::uuid AND status = 'queued'")"

api_call POST /admin/v1/media/requests "$offline_key" '{"items":[{"mediaType":"movie","tmdbId":550}]}'
expect_status idempotent_replay 202
replay_request_id="$(json_value data.requestId 2>/dev/null || true)"
replay_duplicate="$(json_value data.duplicate 2>/dev/null || true)"
[[ "$replay_request_id" == "$offline_request_id" && "$replay_duplicate" == true ]] || failures=$((failures + 1))

api_call POST /admin/v1/media/requests "$offline_key" '{"items":[{"mediaType":"tv","tmdbId":119495}]}'
conflict_status="$last_status"
expect_status idempotency_conflict 409

api_call POST /admin/v1/media/requests "unknown-media-$stamp" '{"items":[{"mediaType":"movie","tmdbId":550},{"mediaType":"tv","tmdbId":9223372036854775807}]}'
unknown_status="$last_status"
expect_status atomic_unknown_rejection 422

compose_checked start media >/dev/null
wait_for_health media 90
poll_request "$offline_request_id" || failures=$((failures + 1))
offline_terminal="$poll_status"
[[ "$offline_terminal" == succeeded || "$offline_terminal" == partial ]] || failures=$((failures + 1))

# Pausing blocks new request claims; resuming drains them.
api_call POST /admin/v1/media/worker "pause-media-$stamp" '{"action":"pause"}'
expect_status pause_worker 200
api_call POST /admin/v1/media/requests "paused-request-$stamp" '{"items":[{"mediaType":"tv","tmdbId":4586}]}'
expect_status paused_submission 202
paused_request_id="$(json_value data.requestId 2>/dev/null || true)"
sleep 2
paused_state="$(psql_at "$password" "SELECT status FROM ops.media_requests WHERE id = '$paused_request_id'::uuid")"
[[ "$paused_state" == queued ]] || failures=$((failures + 1))
api_call POST /admin/v1/media/worker "resume-media-$stamp" '{"action":"resume"}'
expect_status resume_worker 200
poll_request "$paused_request_id" || failures=$((failures + 1))
paused_terminal="$poll_status"

# A 100-title payload uses the same endpoint and implementation.
bulk_payload="$(psql_at "$password" "SELECT json_build_object('items', json_agg(json_build_object('mediaType', media_type, 'tmdbId', tmdb_id) ORDER BY tmdb_id)) FROM (SELECT media_type, tmdb_id FROM catalog.titles WHERE active AND tmdb_id BETWEEN 899000001 AND 899000100 ORDER BY tmdb_id) titles")"
bulk_count="$(printf '%s' "$bulk_payload" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))')"
if (( bulk_count == 100 )); then
    api_call POST /admin/v1/media/requests "bulk-media-$stamp" "$bulk_payload"
    expect_status bulk_submission 202
    bulk_request_id="$(json_value data.requestId 2>/dev/null || true)"
    poll_request "$bulk_request_id" || failures=$((failures + 1))
    bulk_terminal="$poll_status"
    bulk_source_cursor="$(psql_at "$password" "SELECT source_cursor FROM ops.media_requests WHERE id = '$bulk_request_id'::uuid")"
    bulk_catalog_incomplete="$(psql_at "$password" "SELECT count(*) FROM ops.media_request_items WHERE request_id = '$bulk_request_id'::uuid AND catalog_incomplete")"
    [[ "$bulk_terminal" == partial && "$bulk_catalog_incomplete" == 100 ]] || failures=$((failures + 1))
else
    bulk_request_id=''
    bulk_terminal='not_run'
    bulk_source_cursor=0
    bulk_catalog_incomplete=0
    failures=$((failures + 1))
fi

# Cancelling the media worker cancels queued durable media requests.
api_call POST /admin/v1/media/worker "cancel-pause-$stamp" '{"action":"pause"}'
expect_status cancel_setup_pause 200
api_call POST /admin/v1/media/requests "cancel-request-$stamp" '{"items":[{"mediaType":"tv","tmdbId":119495}]}'
expect_status cancel_submission 202
cancel_request_id="$(json_value data.requestId 2>/dev/null || true)"
api_call POST /admin/v1/media/worker "cancel-media-$stamp" '{"action":"cancel"}'
expect_status cancel_worker 200
cancelled_status="$(psql_at "$password" "SELECT status FROM ops.media_requests WHERE id = '$cancel_request_id'::uuid")"
[[ "$cancelled_status" == cancelled ]] || failures=$((failures + 1))
api_call POST /admin/v1/media/worker "final-start-$stamp" '{"action":"start"}'
expect_status final_start 200

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "unauthenticated_status": $unauthenticated,
  "legacy_scan_status": $legacy_scan_status,
  "legacy_audit_status": $legacy_audit_status,
  "offline_request": {"id": "$offline_request_id", "initial_duplicate": $offline_duplicate, "persisted_queued": $persisted_while_offline, "terminal_status": "$offline_terminal"},
  "idempotent_replay": {"same_request": $([[ "$replay_request_id" == "$offline_request_id" ]] && echo true || echo false), "duplicate": $replay_duplicate, "conflict_status": $conflict_status},
  "unknown_item_status": $unknown_status,
  "paused_request": {"queued_while_paused": $([[ "$paused_state" == queued ]] && echo true || echo false), "terminal_status": "$paused_terminal"},
  "bulk_request": {"title_count": $bulk_count, "request_id": "$bulk_request_id", "terminal_status": "$bulk_terminal", "source_cursor": $bulk_source_cursor, "catalog_incomplete_count": $bulk_catalog_incomplete},
  "cancelled_request_status": "$cancelled_status",
  "failures": $failures
}
EOF
cat "$result_file"
printf 'Media-request stress artifact: %s\n' "$result_file"
(( failures == 0 )) || die 'media-request API checks failed'
printf '%s\n' 'Media-request API checks passed.'
