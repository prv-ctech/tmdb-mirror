#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
admin_port="${TMDB_STRESS_ADMIN_PORT:-18081}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
expected_workers=4
timeout=300
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --admin-port) admin_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        --expected-workers) expected_workers="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-media-assets.sh [--project-name NAME] [--admin-port PORT] [--image-port PORT] [--expected-workers N] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$timeout" =~ ^[0-9]+$ ]] && (( timeout >= 1 && timeout <= 3600 )) || die 'invalid timeout'

configure_existing_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "$admin_port" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
admin_port="$ADMIN_PORT"
image_port="$IMAGE_PORT"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/media-assets-$stamp.json"
admin_key="$(env_value TMDB_ADMIN_API_KEY)"
if ! start_output="$(curl --silent --show-error --fail-with-body -X POST \
    -H "X-API-Key: $admin_key" \
    -H "Idempotency-Key: media-assets-start-$stamp" \
    -H 'Content-Type: application/json' \
    --data '{"action":"start"}' \
    "http://127.0.0.1:$admin_port/admin/v1/media/worker" 2>&1)"; then
    redact "$start_output" >&2
    die 'could not start the media worker for asset verification'
fi
if ! grep -q '"state":"running"' <<<"$start_output"; then
    redact "$start_output" >&2
    die 'media worker did not enter the running state'
fi
password="$(database_password)"
dead_letters_before="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status = 'dead_letter'")"
pending_image_jobs_before="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status IN ('queued', 'running', 'retry_wait')")"

deadline=$((SECONDS + timeout))
drain_started=$SECONDS
pending_image_jobs=-1
while (( SECONDS < deadline )); do
    pending_image_jobs="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status IN ('queued', 'running', 'retry_wait')" 2>/dev/null || printf '%s' '-1')"
    if [[ "$pending_image_jobs" =~ ^[0-9]+$ ]] && (( pending_image_jobs == 0 )); then
        break
    fi
    sleep 3
done
drain_seconds=$((SECONDS - drain_started))

asset_rows="$(psql_at "$password" "WITH categorized AS (
  SELECT CASE
    WHEN title_id IS NOT NULL THEN 'title'
    WHEN season_id IS NOT NULL THEN 'season'
    WHEN episode_id IS NOT NULL THEN 'episode'
    WHEN person_id IS NOT NULL THEN 'person'
    WHEN company_id IS NOT NULL THEN 'company'
    WHEN network_id IS NOT NULL THEN 'network'
    WHEN collection_id IS NOT NULL THEN 'collection'
    ELSE 'other' END AS category, storage_path
  FROM assets.image_assets WHERE status = 'ready'
), ranked AS (
  SELECT category, storage_path, row_number() OVER (PARTITION BY category ORDER BY storage_path) AS n
  FROM categorized
)
SELECT category || E'\t' || storage_path FROM ranked WHERE n = 1 ORDER BY category")"

required=(title season episode person company network collection)
checked=0
failed=0
while IFS=$'\t' read -r category storage_path; do
    [[ -n "$category" ]] || continue
    if [[ ! "$storage_path" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ || "$storage_path" == *..* ]]; then
        failed=$((failed + 1)); continue
    fi
    http="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
        --connect-timeout 10 --max-time 30 "http://127.0.0.1:$image_port/media/$storage_path" || printf '000')"
    if ! compose exec -T media sh -ec "test -f '/media/$storage_path'" </dev/null >/dev/null 2>&1; then
        failed=$((failed + 1))
    elif [[ "$http" != 200 ]]; then
        failed=$((failed + 1))
    fi
    checked=$((checked + 1))
done <<<"$asset_rows"

missing=0
for category in "${required[@]}"; do
    grep -q "^${category}"$'\t' <<<"$asset_rows" || missing=$((missing + 1))
done
dead_letters="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status = 'dead_letter'")"
new_dead_letters=$((dead_letters - dead_letters_before))
worker_count="$(psql_at "$password" "SELECT count(DISTINCT event.worker_id) FROM ops.job_events event JOIN ops.jobs job ON job.id = event.job_id WHERE job.job_type = 'image.download' AND event.worker_id LIKE 'tmdb-stress-media-%'")"
shared_groups="$(psql_at "$password" "SELECT count(*) FROM (SELECT source, source_key FROM assets.image_assets GROUP BY source, source_key HAVING count(DISTINCT owner_type || ':' || owner_id) > 1) groups")"
gallery_counts="$(psql_at "$password" "SELECT COALESCE(json_object_agg(image_kind, asset_count ORDER BY image_kind), '{}'::json)::text FROM (SELECT image_kind, count(*) AS asset_count FROM assets.image_assets WHERE status = 'ready' GROUP BY image_kind) counts")"
downloaded_originals="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND source_storage_path IS NOT NULL")"
optimized_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND storage_path ~ '(^|/)optimized/'")"
optimized_variants="$(psql_at "$password" "SELECT count(*) FROM assets.image_variants")"
episode_optimized_only="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND episode_id IS NOT NULL AND storage_path ~ '(^|/)optimized/' AND source_storage_path IS NULL")"
invalid_variants="$(psql_at "$password" "SELECT count(*) FROM assets.image_variants WHERE mime_type NOT IN ('image/jpeg', 'image/png') OR storage_path !~ '(^|/)optimized/' OR (storage_path ~ '(^|/)optimized/thumbnails/' AND width > 640)")"
video_counts="$(psql_at "$password" "SELECT COALESCE(json_object_agg(video_type || '/' || site, video_count ORDER BY video_type, site), '{}'::json)::text FROM (SELECT COALESCE(video_type, 'unknown') AS video_type, site, count(*) AS video_count FROM catalog.title_videos GROUP BY COALESCE(video_type, 'unknown'), site) counts")"
media_permissions=true
if ! compose exec -T media sh -ec 'test -d /media && test -d /media/movies && test -d /media/tv && test -d /media/people && test -d /media/companies && test -d /media/networks && test -d /media/collections && test ! -e /media/.masters' </dev/null >/dev/null 2>&1; then
    media_permissions=false
fi

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "checked_assets": $checked,
  "pending_image_jobs_before": $pending_image_jobs_before,
  "pending_image_jobs": $pending_image_jobs,
  "drain_seconds": $drain_seconds,
  "missing_owner_categories": $missing,
  "invalid_or_unserved_assets": $failed,
  "observed_worker_ids": $worker_count,
  "expected_worker_ids": $expected_workers,
  "shared_source_owner_groups": $shared_groups,
  "gallery_counts_by_kind": $gallery_counts,
  "downloaded_original_rows": $downloaded_originals,
  "optimized_asset_rows": $optimized_assets,
  "optimized_variant_rows": $optimized_variants,
  "episode_optimized_only_rows": $episode_optimized_only,
  "invalid_variant_rows": $invalid_variants,
  "video_counts_by_type_and_site": $video_counts,
  "media_permission_contract": $media_permissions,
  "dead_letter_image_jobs_before": $dead_letters_before,
  "dead_letter_image_jobs": $dead_letters,
  "new_dead_letter_image_jobs": $new_dead_letters
}
EOF
cat "$result_file"
printf 'Media-asset verification artifact: %s\n' "$result_file"
if (( pending_image_jobs != 0 || missing > 0 || failed > 0 || worker_count != expected_workers || shared_groups == 0 || downloaded_originals == 0 || optimized_assets == 0 || optimized_variants == 0 || episode_optimized_only == 0 || invalid_variants != 0 || new_dead_letters != 0 )) || [[ "$media_permissions" != true ]]; then
    die 'media-asset verification failed'
fi
printf '%s\n' 'Media-asset verification passed.'
