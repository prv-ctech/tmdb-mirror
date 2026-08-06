#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=600
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-artwork.sh [--project-name NAME] [--admin-port PORT] [--api-port PORT] [--image-port PORT] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_existing_runtime "$project" "$api_port" "$admin_port" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
admin_port="$ADMIN_PORT"
image_port="$IMAGE_PORT"
load_runtime
require_command curl
require_command python3
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/artwork-$stamp.json"
password="$(database_password)"
admin_key="$(env_value TMDB_ADMIN_API_KEY)"
[[ -n "$admin_key" ]] || die 'TMDB_ADMIN_API_KEY is missing from the stress runtime'
base_url="http://127.0.0.1:$admin_port"
trap 'unset admin_key' EXIT

admin_post() {
    local path="$1" key="$2" body="$3" response_file error_file status response
    response_file="$(mktemp)"
    error_file="$(mktemp)"
    status="$(curl --silent --show-error --connect-timeout 10 --max-time 30 \
        -X POST -H "X-API-Key: $admin_key" -H "Idempotency-Key: $key" \
        -H 'Content-Type: application/json' --data "$body" \
        --output "$response_file" --write-out '%{http_code}' \
        "$base_url$path" 2>"$error_file" || true)"
    response="$(<"$response_file")"
    if [[ "$status" != 200 && "$status" != 202 ]]; then
        redact "$response$(<"$error_file")" >&2
        rm -f "$response_file" "$error_file"
        die "admin request returned HTTP $status: $path"
    fi
    rm -f "$response_file" "$error_file"
    printf '%s\n' "$response"
}

targets=("movie 550" "movie 900667" "movie 1132850" "tv 119495" "tv 4586")
target_values=''
media_items=''
for target in "${targets[@]}"; do
    read -r media_type tmdb_id <<<"$target"
    target_values+="('$media_type', $tmdb_id, true, NULL, NULL),"
    media_items+="{\"mediaType\":\"$media_type\",\"tmdbId\":$tmdb_id},"
done
target_values="${target_values%,}"
media_items="${media_items%,}"
psql_at "$password" "INSERT INTO catalog.titles (media_type, tmdb_id, active, display_title, source_updated_at)
    VALUES $target_values
    ON CONFLICT (media_type, tmdb_id) DO UPDATE
        SET active = true, display_title = NULL, source_updated_at = NULL" >/dev/null

image_jobs_before="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")"
scan_response="$(admin_post /admin/v1/scans "artwork-catalog-$stamp" '{"mode":"missing_only","mediaTypes":["movie","tv"]}')"
scan_job_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["jobId"])' <<<"$scan_response")"
[[ "$scan_job_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'catalog scan returned no valid job ID'

scan_status='pending'
deadline=$((SECONDS + timeout))
while (( SECONDS < deadline )); do
    response="$(curl --silent --show-error --fail -H "X-API-Key: $admin_key" \
        "$base_url/admin/v1/jobs/$scan_job_id" 2>/dev/null || true)"
    scan_status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["job"]["status"])' <<<"$response" 2>/dev/null || printf pending)"
    case "$scan_status" in succeeded|dead_letter|cancelled) break ;; esac
    sleep 2
done
[[ "$scan_status" == succeeded ]] || die "catalog scan ended with status $scan_status"

pending_catalog=-1
deadline=$((SECONDS + timeout))
while (( SECONDS < deadline )); do
    pending_catalog="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type LIKE 'ingest.%' AND status IN ('queued','running','retry_wait')" 2>/dev/null || printf '%s' -1)"
    (( pending_catalog == 0 )) && break
    sleep 2
done
image_jobs_after_catalog="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")"

media_response="$(admin_post /admin/v1/media/requests "artwork-media-$stamp" "{\"items\":[$media_items]}")"
media_request_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["requestId"])' <<<"$media_response")"
[[ "$media_request_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'media request returned no valid request ID'

media_status='queued'
media_body=''
deadline=$((SECONDS + timeout))
while (( SECONDS < deadline )); do
    media_body="$(curl --silent --show-error --fail -H "X-API-Key: $admin_key" \
        "$base_url/admin/v1/media/requests/$media_request_id" 2>/dev/null || true)"
    media_status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["status"])' <<<"$media_body" 2>/dev/null || printf queued)"
    case "$media_status" in succeeded|partial|failed|cancelled) break ;; esac
    sleep 2
done

ready_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE source = 'tmdb' AND status = 'ready'")"
failed_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE source = 'tmdb' AND status = 'failed'")"
optimized_files="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE storage_path ~ '(^|/)optimized/'")"
variant_table_removed="$(psql_at "$password" "SELECT to_regclass('assets.image_variants') IS NULL")"
asset_path="$(psql_at "$password" "SELECT storage_path FROM assets.image_assets WHERE status = 'ready' ORDER BY id LIMIT 1")"
movie_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'movie' AND title.tmdb_id IN (550,900667,1132850) AND asset.status = 'ready' AND asset.storage_path LIKE 'movies/%'")"
tv_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'tv' AND title.tmdb_id IN (119495,4586) AND asset.status = 'ready' AND asset.storage_path LIKE 'tv/%'")"
primary_path_mismatches="$(psql_at "$password" "SELECT count(*)
  FROM catalog.titles AS title
 WHERE (title.media_type = 'movie' AND title.tmdb_id IN (550,900667,1132850)
        OR title.media_type = 'tv' AND title.tmdb_id IN (119495,4586))
   AND ((title.poster_path IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM assets.image_assets AS asset
            WHERE asset.title_id = title.id AND asset.image_kind = 'poster'
              AND asset.gallery_index = 1 AND asset.source_key = title.poster_path
              AND asset.status = 'ready'
       )) OR (title.backdrop_path IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM assets.image_assets AS asset
            WHERE asset.title_id = title.id AND asset.image_kind = 'backdrop'
              AND asset.gallery_index = 1 AND asset.source_key = title.backdrop_path
              AND asset.status = 'ready'
       )))")"
gallery_counts="$(psql_at "$password" "SELECT COALESCE(json_object_agg(image_kind, asset_count ORDER BY image_kind), '{}'::json)::text FROM (SELECT image_kind, count(*) AS asset_count FROM assets.image_assets WHERE status = 'ready' GROUP BY image_kind) counts")"

http_status=0
if [[ -n "$asset_path" && "$asset_path" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ && "$asset_path" != *..* ]]; then
    http_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
        --connect-timeout 10 --max-time 30 "http://127.0.0.1:$image_port/media/$asset_path")"
fi

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "targets": ${#targets[@]},
  "catalog_scan_status": "$scan_status",
  "pending_catalog_jobs": $pending_catalog,
  "image_jobs_created_by_catalog": $((image_jobs_after_catalog - image_jobs_before)),
  "media_request_id": "$media_request_id",
  "media_request_status": "$media_status",
  "media_request": $(printf '%s' "$media_body" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin).get("data", {})))'),
  "ready_assets": $ready_assets,
  "failed_assets": $failed_assets,
  "gallery_counts_by_kind": $gallery_counts,
  "movie_asset_paths": $movie_paths,
  "tv_asset_paths": $tv_paths,
  "primary_path_mismatches": $primary_path_mismatches,
  "optimized_paths": $optimized_files,
  "variant_table_removed": $variant_table_removed,
  "media_http_status": $http_status
}
EOF
cat "$result_file"
printf 'Artwork stress artifact: %s\n' "$result_file"
if [[ "$media_status" != succeeded && "$media_status" != partial ]] || \
   (( pending_catalog != 0 || image_jobs_after_catalog != image_jobs_before || ready_assets == 0 || failed_assets != 0 || movie_paths == 0 || tv_paths == 0 || primary_path_mismatches != 0 || optimized_files != 0 || http_status != 200 )) || \
   [[ "$variant_table_removed" != t ]]; then
    die 'on-demand artwork checks failed'
fi
printf '%s\n' 'On-demand artwork and image-serving checks passed.'
