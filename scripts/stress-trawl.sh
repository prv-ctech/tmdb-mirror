#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-trawl.sh [--project-name NAME]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
configure_existing_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" \
    "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/trawl-$stamp.json"

if [[ -z "$STRESS_TRAWL_URL" ]]; then
    printf '{"skipped":true,"reason":"no Trawl URL configured"}\n' | tee "$result_file"
    exit 0
fi

password="$(database_password)"
source_key="$(psql_at "$password" "SELECT source_key FROM assets.image_assets WHERE status = 'ready' AND source = 'tmdb' ORDER BY id LIMIT 1")"
[[ "$source_key" =~ ^/[A-Za-z0-9._/-]+$ && "$source_key" != *..* ]] || die 'no safe TMDB image source key is available for the Trawl probe'
response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT
status="$(curl --silent --show-error --output "$response_file" --write-out '%{http_code}' \
    --connect-timeout 10 --max-time 30 -X POST -H 'Content-Type: application/json' \
    --data "{\"url\":\"https://image.tmdb.org/t/p/w185$source_key\",\"maxTimeout\":20000}" \
    "$STRESS_TRAWL_URL/scrape")"
upstream=0
grep -Eq '"statusCode"[[:space:]]*:[[:space:]]*200|"status"[[:space:]]*:[[:space:]]*200' "$response_file" && upstream=200 || true
response_metadata=false
grep -Eq '"url"[[:space:]]*:|"html"[[:space:]]*:|"tier"[[:space:]]*:' "$response_file" && response_metadata=true || true
cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "configured": true,
  "trawl_http_status": $status,
  "upstream_http_status": $upstream,
  "response_metadata_present": $response_metadata
}
EOF
cat "$result_file"
if [[ "$status" != 200 || "$upstream" != 200 || "$response_metadata" != true ]]; then
    die 'Trawl probe failed'
fi
printf '%s\n' 'Trawl probe passed.'
