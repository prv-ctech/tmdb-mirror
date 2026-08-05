#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=300
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
api_port="$API_PORT"
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

start_worker() {
    local path="$1" key="$2" output
    if ! output="$(curl --silent --show-error --fail-with-body -X POST \
        -H "X-API-Key: $admin_key" \
        -H "Idempotency-Key: $key" \
        -H 'Content-Type: application/json' \
        --data '{"action":"start"}' \
        "http://127.0.0.1:$admin_port$path" 2>&1)"; then
        redact "$output" >&2
        die "could not start worker for artwork stress: $path"
    fi
    if ! grep -q '"state":"running"' <<<"$output"; then
        redact "$output" >&2
        die "worker did not enter running state for artwork stress: $path"
    fi
}

start_worker /admin/v1/worker "artwork-start-ingest-$stamp"
start_worker /admin/v1/media/worker "artwork-start-media-$stamp"

targets=("movie 550" "movie 900667" "movie 1132850" "tv 119495" "tv 4586")
target_file="$RESULT_ROOT/artwork-targets-$stamp.tsv"
: >"$target_file"
target_values=''
for target in "${targets[@]}"; do
    read -r media_type tmdb_id <<<"$target"
    target_values+="('$media_type', $tmdb_id, true, NULL, NULL),"
    printf '%s\t%s\n' "$media_type" "$tmdb_id" >>"$target_file"
done
target_values="${target_values%,}"
psql_at "$password" "INSERT INTO catalog.titles (media_type, tmdb_id, active, display_title, source_updated_at)
    VALUES $target_values
    ON CONFLICT (media_type, tmdb_id) DO UPDATE
        SET active = true, display_title = NULL, source_updated_at = NULL" >/dev/null

scan_response_file="$(mktemp)"
scan_error_file="$(mktemp)"
if ! scan_http_status="$(curl --silent --show-error --connect-timeout 10 --max-time 30 \
    -X POST \
    -H "X-API-Key: $admin_key" \
    -H "Idempotency-Key: artwork-scan-$stamp" \
    -H 'Content-Type: application/json' \
    --data '{"mode":"missing_only","mediaTypes":["movie","tv"]}' \
    --output "$scan_response_file" --write-out '%{http_code}' \
    "$base_url/admin/v1/scans" 2>"$scan_error_file")"; then
    redact "$(<"$scan_error_file")" >&2
    rm -f "$scan_response_file" "$scan_error_file"
    die 'catalog scan submission failed'
fi
scan_response="$(<"$scan_response_file")"
scan_error="$(<"$scan_error_file")"
rm -f "$scan_response_file" "$scan_error_file"
if [[ "$scan_http_status" != 202 ]]; then
    redact "$scan_response$scan_error" >&2
    die "catalog scan submission returned HTTP $scan_http_status"
fi
scan_job_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["jobId"])' <<<"$scan_response")"
[[ "$scan_job_id" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'catalog scan response returned no valid job ID'

scan_status='pending'
scan_deadline=$((SECONDS + timeout))
while (( SECONDS < scan_deadline )); do
    job_response="$(curl --silent --show-error --fail \
        -H "X-API-Key: $admin_key" \
        "$base_url/admin/v1/jobs/$scan_job_id")" || true
    scan_status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["job"]["status"])' <<<"$job_response" 2>/dev/null || printf 'pending')"
    case "$scan_status" in
        succeeded|dead_letter|cancelled|failed) break ;;
    esac
    sleep 2
done
[[ "$scan_status" == succeeded ]] || die "catalog scan ended with status $scan_status"

child_deadline=$((SECONDS + timeout))
pending_child_jobs=-1
quiet_since=0
while (( SECONDS < child_deadline )); do
    pending_child_jobs="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type IN ('ingest.refresh_season', 'ingest.refresh_reusable_gallery', 'image.download') AND status IN ('queued', 'running', 'retry_wait')" 2>/dev/null || printf '%s' '-1')"
    if [[ "$pending_child_jobs" =~ ^[0-9]+$ ]] && (( pending_child_jobs == 0 )); then
        if (( quiet_since == 0 )); then
            quiet_since=$SECONDS
        elif (( SECONDS - quiet_since >= 10 )); then
            break
        fi
    else
        quiet_since=0
    fi
    sleep 3
done
child_failed_jobs="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type IN ('ingest.refresh_season', 'ingest.refresh_reusable_gallery', 'image.download') AND status = 'dead_letter'")"

ready_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE source = 'tmdb' AND status = 'ready'")"
asset_path="$(psql_at "$password" "SELECT storage_path FROM assets.image_assets WHERE source = 'tmdb' AND status = 'ready' ORDER BY id LIMIT 1")"
movie_asset_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'movie' AND title.tmdb_id IN (550, 900667, 1132850) AND asset.status = 'ready' AND asset.storage_path LIKE 'movies/%'")"
tv_asset_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'tv' AND title.tmdb_id IN (119495, 4586) AND asset.status = 'ready' AND asset.storage_path LIKE 'tv/%'")"
gallery_counts="$(psql_at "$password" "SELECT COALESCE(json_object_agg(image_kind, asset_count ORDER BY image_kind), '{}'::json)::text FROM (SELECT image_kind, count(*) AS asset_count FROM assets.image_assets WHERE status = 'ready' GROUP BY image_kind) counts")"
optimized_files="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND storage_path ~ '(^|/)optimized/'")"
optimized_variants="$(psql_at "$password" "SELECT count(*) FROM assets.image_variants")"
episode_optimized_only="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND episode_id IS NOT NULL AND storage_path ~ '(^|/)optimized/' AND source_storage_path IS NULL")"
webp_derivatives="$(psql_at "$password" "SELECT count(*) FROM assets.image_variants WHERE mime_type = 'image/webp' OR storage_path ~* '\\.webp$'")"
video_counts="$(psql_at "$password" "SELECT COALESCE(json_object_agg(video_type || '/' || site, video_count ORDER BY video_type, site), '{}'::json)::text FROM (SELECT COALESCE(video_type, 'unknown') AS video_type, site, count(*) AS video_count FROM catalog.title_videos GROUP BY COALESCE(video_type, 'unknown'), site) counts")"
http_status=0
conditional_status=0
if [[ -n "$asset_path" && "$asset_path" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ && "$asset_path" != *..* ]]; then
    body_file="$(mktemp)"
    headers_file="$(mktemp)"
    trap 'rm -f "$body_file" "$headers_file"' EXIT
    http_status="$(curl --silent --show-error --output "$body_file" --dump-header "$headers_file" --write-out '%{http_code}' \
        --connect-timeout 10 --max-time 30 "http://127.0.0.1:$image_port/media/$asset_path")"
    etag="$(sed -n 's/^[Ee][Tt][Aa][Gg]:[[:space:]]*//p' "$headers_file" | tr -d '\r' | head -n 1)"
    if [[ -n "$etag" ]]; then
        conditional_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
            --connect-timeout 10 --max-time 30 -H "If-None-Match: $etag" "http://127.0.0.1:$image_port/media/$asset_path")"
    fi
fi

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "targets": $(wc -l <"$target_file"),
  "catalog_scan_job_id": "$scan_job_id",
  "catalog_scan_status": "$scan_status",
  "pending_child_jobs": $pending_child_jobs,
  "failed_child_jobs": $child_failed_jobs,
  "ready_tmdb_assets": $ready_assets,
  "gallery_counts_by_kind": $gallery_counts,
  "optimized_asset_rows": $optimized_files,
  "optimized_variant_rows": $optimized_variants,
  "episode_optimized_only_rows": $episode_optimized_only,
  "webp_derivative_rows": $webp_derivatives,
  "video_counts_by_type_and_site": $video_counts,
  "movie_asset_paths": $movie_asset_paths,
  "tv_asset_paths": $tv_asset_paths,
  "representative_asset_path_present": $([[ -n "$asset_path" ]] && echo true || echo false),
  "media_http_status": $http_status,
  "media_conditional_status": $conditional_status
}
EOF
cat "$result_file"
printf 'Artwork stress artifact: %s\n' "$result_file"
if (( pending_child_jobs != 0 || child_failed_jobs > 0 || ready_assets == 0 || optimized_variants == 0 || episode_optimized_only == 0 || webp_derivatives != 0 || http_status != 200 || conditional_status != 304 || movie_asset_paths == 0 || tv_asset_paths == 0 )); then
    die 'real artwork/image download checks failed'
fi
printf '%s\n' 'Real artwork and image serving checks passed.'
