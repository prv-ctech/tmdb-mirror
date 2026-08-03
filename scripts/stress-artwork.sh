#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=300
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-artwork.sh [--project-name NAME] [--api-port PORT] [--image-port PORT] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_runtime "$project" "$api_port" "${TMDB_STRESS_ADMIN_PORT:-18081}" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/artwork-$stamp.json"
password="$(database_password)"

submit_refresh() {
    local media_type="$1" tmdb_id="$2" output job_id
    if ! output="$(compose run --rm --no-deps --entrypoint /usr/local/bin/tmdb-admin worker \
        submit-refresh --media-type "$media_type" --tmdb-id "$tmdb_id" 2>&1)"; then
        redact "$output" >&2
        die "refresh submission failed for $media_type/$tmdb_id"
    fi
    job_id="$(grep -Eo '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' <<<"$output" | tail -n 1 || true)"
    [[ -n "$job_id" ]] || { redact "$output" >&2; die 'refresh submission returned no job ID'; }
    printf '%s\t%s\t%s\n' "$media_type" "$tmdb_id" "$job_id"
}

targets=("movie 550" "tv 1399" "tv 37854" "movie 900667" "movie 1132850" "tv 63648")
target_file="$RESULT_ROOT/artwork-targets-$stamp.tsv"
: >"$target_file"
for target in "${targets[@]}"; do
    read -r media_type tmdb_id <<<"$target"
    submit_refresh "$media_type" "$tmdb_id" >>"$target_file"
done

deadline=$((SECONDS + timeout))
failed_jobs=0
while (( SECONDS < deadline )); do
    pending=0
    while IFS=$'\t' read -r media_type tmdb_id job_id; do
        status="$(psql_at "$password" "SELECT status FROM ops.jobs WHERE id = '$job_id'::uuid" 2>/dev/null || true)"
        case "$status" in
            succeeded) ;;
            dead_letter|failed) failed_jobs=$((failed_jobs + 1)) ;;
            *) pending=$((pending + 1)) ;;
        esac
    done <"$target_file"
    (( pending == 0 )) && break
    sleep 3
done

ready_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE source = 'tmdb' AND status = 'ready'")"
asset_path="$(psql_at "$password" "SELECT storage_path FROM assets.image_assets WHERE source = 'tmdb' AND status = 'ready' ORDER BY id LIMIT 1")"
skye_anime="$(psql_at "$password" "SELECT CASE WHEN COALESCE(is_anime, false) THEN 'true' ELSE 'false' END FROM catalog.titles WHERE media_type = 'movie' AND tmdb_id = 1132850")"
death_note_anime="$(psql_at "$password" "SELECT CASE WHEN COALESCE(is_anime, false) THEN 'true' ELSE 'false' END FROM catalog.titles WHERE media_type = 'tv' AND tmdb_id = 63648")"
one_piece_anime="$(psql_at "$password" "SELECT CASE WHEN COALESCE(is_anime, false) THEN 'true' ELSE 'false' END FROM catalog.titles WHERE media_type = 'tv' AND tmdb_id = 37854")"
skye_anime_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'movie' AND title.tmdb_id = 1132850 AND asset.status = 'ready' AND asset.storage_path LIKE 'anime/%'")"
death_note_anime_paths="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets asset JOIN catalog.titles title ON title.id = asset.title_id WHERE title.media_type = 'tv' AND title.tmdb_id = 63648 AND asset.status = 'ready' AND asset.storage_path LIKE 'anime/%'")"
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
  "failed_jobs": $failed_jobs,
  "ready_tmdb_assets": $ready_assets,
  "skye_hoshi_is_anime": $([[ "$skye_anime" == true ]] && echo true || echo false),
  "death_note_is_anime": $([[ "$death_note_anime" == true ]] && echo true || echo false),
  "one_piece_is_anime": $([[ "$one_piece_anime" == true ]] && echo true || echo false),
  "skye_hoshi_anime_asset_paths": $skye_anime_paths,
  "death_note_anime_asset_paths": $death_note_anime_paths,
  "representative_asset_path_present": $([[ -n "$asset_path" ]] && echo true || echo false),
  "media_http_status": $http_status,
  "media_conditional_status": $conditional_status
}
EOF
cat "$result_file"
printf 'Artwork stress artifact: %s\n' "$result_file"
if (( failed_jobs > 0 || ready_assets == 0 || http_status != 200 || conditional_status != 304 )) \
    || [[ "$skye_anime" != false || "$death_note_anime" != false || "$one_piece_anime" != true ]] \
    || (( skye_anime_paths != 0 || death_note_anime_paths != 0 )); then
    die 'real artwork/image download checks failed'
fi
printf '%s\n' 'Real artwork and image serving checks passed.'
