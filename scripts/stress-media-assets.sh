#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
timeout=300
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        --timeout) timeout="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-media-assets.sh [--project-name NAME] [--image-port PORT] [--timeout SECONDS]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_existing_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
image_port="$IMAGE_PORT"
load_runtime
require_command curl
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/media-assets-$stamp.json"
password="$(database_password)"

deadline=$((SECONDS + timeout))
pending=-1
while (( SECONDS < deadline )); do
    pending="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status IN ('queued','running','retry_wait')" 2>/dev/null || printf '%s' -1)"
    (( pending == 0 )) && break
    sleep 2
done

asset_rows="$(psql_at "$password" "WITH categorized AS (
  SELECT CASE
    WHEN title_id IS NOT NULL THEN 'title'
    WHEN season_id IS NOT NULL THEN 'season'
    WHEN episode_id IS NOT NULL THEN 'episode'
    WHEN person_id IS NOT NULL THEN 'person'
    WHEN company_id IS NOT NULL THEN 'company'
    WHEN network_id IS NOT NULL THEN 'network'
    WHEN collection_id IS NOT NULL THEN 'collection'
    ELSE 'other' END AS category,
    storage_path, sha256, file_size_bytes
  FROM assets.image_assets WHERE status = 'ready'
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY category ORDER BY storage_path) AS n
  FROM categorized
)
SELECT category || E'\\t' || storage_path || E'\\t' || sha256 || E'\\t' || file_size_bytes
FROM ranked WHERE n = 1 ORDER BY category")"

required=(title season episode person company network collection)
missing=0
for category in "${required[@]}"; do
    grep -q "^${category}"$'\t' <<<"$asset_rows" || missing=$((missing + 1))
done

checked=0
invalid_files=0
while IFS=$'\t' read -r category storage_path expected_sha expected_size; do
    [[ -n "$category" ]] || continue
    if [[ ! "$storage_path" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ || "$storage_path" == *..* ]]; then
        invalid_files=$((invalid_files + 1))
        continue
    fi
    actual="$(compose exec -T media sh -ec "test -f '/media/$storage_path' && wc -c < '/media/$storage_path' && sha256sum '/media/$storage_path' | cut -d' ' -f1" </dev/null 2>/dev/null || true)"
    actual_size="$(sed -n '1p' <<<"$actual" | tr -d '[:space:]')"
    actual_sha="$(sed -n '2p' <<<"$actual" | tr -d '[:space:]')"
    http="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
        --connect-timeout 10 --max-time 30 "http://127.0.0.1:$image_port/media/$storage_path" || printf 000)"
    if [[ "$actual_size" != "$expected_size" || "$actual_sha" != "$expected_sha" || "$http" != 200 ]]; then
        invalid_files=$((invalid_files + 1))
    fi
    checked=$((checked + 1))
done <<<"$asset_rows"

invalid_metadata="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready' AND (
  mime_type NOT IN ('image/jpeg','image/png','image/webp') OR storage_path IS NULL OR
  storage_path ~ '(^|/)(optimized|\\.masters)(/|$)' OR verified_at IS NULL OR
  file_size_bytes IS NULL OR file_size_bytes <= 0 OR sha256 !~ '^[0-9a-f]{64}$' OR
  (image_kind IN ('poster') AND width > 500) OR
  (image_kind = 'backdrop' AND width > 1280) OR
  (image_kind = 'still' AND width > 300) OR
  (image_kind IN ('profile','logo') AND width > 185)
)")"
legacy_tables="$(psql_at "$password" "SELECT count(*) FROM (VALUES
  (to_regclass('assets.image_variants')),
  (to_regclass('ops.media_scan_runs')),
  (to_regclass('ops.media_audit_runs'))
) AS legacy(object_name) WHERE object_name IS NOT NULL")"
dead_letters="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status = 'dead_letter'")"
ready_assets="$(psql_at "$password" "SELECT count(*) FROM assets.image_assets WHERE status = 'ready'")"
worker_count="$(psql_at "$password" "SELECT count(DISTINCT event.worker_id) FROM ops.job_events event JOIN ops.jobs job ON job.id = event.job_id WHERE job.job_type = 'image.download' AND event.worker_id LIKE 'tmdb-stress-media-%'")"
media_permissions=true
if ! compose exec -T media sh -ec 'test -d /media/movies && test -d /media/tv && test -d /media/people && test -d /media/companies && test -d /media/networks && test -d /media/collections && test ! -e /media/optimized && test ! -e /media/.masters' </dev/null >/dev/null 2>&1; then
    media_permissions=false
fi

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "pending_image_jobs": $pending,
  "ready_assets": $ready_assets,
  "checked_representative_files": $checked,
  "missing_owner_categories": $missing,
  "invalid_files_or_digests": $invalid_files,
  "invalid_rendition_metadata": $invalid_metadata,
  "legacy_media_tables": $legacy_tables,
  "dead_letter_image_jobs": $dead_letters,
  "observed_worker_ids": $worker_count,
  "media_permission_contract": $media_permissions
}
EOF
cat "$result_file"
printf 'Media-asset verification artifact: %s\n' "$result_file"
if (( pending != 0 || ready_assets == 0 || checked == 0 || missing != 0 || invalid_files != 0 || invalid_metadata != 0 || legacy_tables != 0 || dead_letters != 0 || worker_count == 0 )) || [[ "$media_permissions" != true ]]; then
    die 'media-asset verification failed'
fi
printf '%s\n' 'Media-asset verification passed.'
