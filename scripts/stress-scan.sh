#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
# A detail scan can fan out to many image downloads. Keep the default small
# enough for the companion media verifier to drain within its bounded window;
# callers can raise it deliberately when they also extend that window.
queue_limit=10
max_image_job_fanout=50000
max_lookback=7
requested_date="$(date -u +%F)"
explicit_date=false
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --date) requested_date="$2"; explicit_date=true; shift 2 ;;
        --queue-limit) queue_limit="$2"; shift 2 ;;
        --max-image-job-fanout) max_image_job_fanout="$2"; shift 2 ;;
        --max-lookback-days) max_lookback="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-scan.sh [--project-name NAME] [--date YYYY-MM-DD] [--queue-limit N] [--max-image-job-fanout N] [--max-lookback-days N] (default queue limit: 10)'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$requested_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die 'date must be YYYY-MM-DD'
[[ "$queue_limit" =~ ^[0-9]+$ ]] && (( queue_limit <= 100000 )) || die 'invalid queue limit'
[[ "$max_image_job_fanout" =~ ^[0-9]+$ ]] && (( max_image_job_fanout <= 1000000 )) || die 'invalid max image job fanout'
[[ "$max_lookback" =~ ^[0-9]+$ ]] && (( max_lookback <= 14 )) || die 'invalid max lookback'

configure_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" \
    "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$EXPORT_ROOT" "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$RESULT_ROOT/tmdb-scan-$stamp.json"
password="$(database_password)"
image_jobs_before="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")"
[[ "$image_jobs_before" =~ ^[0-9]+$ ]] || die 'could not read the image-job baseline'

date_for_offset() { date -u -d "$requested_date - $1 day" +%m_%d_%Y; }
selected_date=''
movie_file=''
tv_file=''
for ((offset=0; offset<=max_lookback; offset++)); do
    date_text="$(date_for_offset "$offset")"
    candidate_movie="$EXPORT_ROOT/movie_ids_$date_text.json.gz"
    candidate_tv="$EXPORT_ROOT/tv_series_ids_$date_text.json.gz"
    movie_url="https://files.tmdb.org/p/exports/movie_ids_$date_text.json.gz"
    tv_url="https://files.tmdb.org/p/exports/tv_series_ids_$date_text.json.gz"
    movie_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --connect-timeout 10 --max-time 30 "$movie_url" || printf '000')"
    tv_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --connect-timeout 10 --max-time 30 "$tv_url" || printf '000')"
    if [[ "$movie_status" == 200 && "$tv_status" == 200 ]]; then
        curl --silent --show-error --fail --connect-timeout 10 --max-time 300 "$movie_url" -o "$candidate_movie"
        curl --silent --show-error --fail --connect-timeout 10 --max-time 300 "$tv_url" -o "$candidate_tv"
        selected_date="$(date -u -d "$requested_date - $offset day" +%F)"
        movie_file="$candidate_movie"
        tv_file="$candidate_tv"
        break
    fi
    if [[ "$explicit_date" == true ]]; then
        die "TMDB exports for the requested date are unavailable"
    fi
done
[[ -n "$selected_date" ]] || die "TMDB did not publish matching exports within $max_lookback day(s)"

scan_one() {
    local media_type="$1" host_file="$2" file_name container output json_line target_path
    file_name="$(basename "$host_file")"
    container="$(compose ps -q worker)"
    [[ -n "$container" ]] || die 'worker container is unavailable'
    target_path="/config/raw/$file_name"
    docker_command cp "$(docker_path "$host_file")" "$container:$target_path"
    if ! output="$(compose run --rm --no-deps --entrypoint /usr/local/bin/tmdb-admin worker \
        scan-export --path "$target_path" --media-type "$media_type" --queue-limit "$queue_limit" 2>&1)"; then
        redact "$output" >&2
        die "TMDB export scan failed for $media_type"
    fi
    json_line="$(grep -E '^[[:space:]]*\{' <<<"$output" | tail -n 1 || true)"
    [[ -n "$json_line" ]] || { redact "$output" >&2; die "TMDB export scan returned no JSON for $media_type"; }
    printf '%s\t%s\t%s\n' "$media_type" "$(gzip -dc "$host_file" | wc -l)" "$json_line"
}

movie_result="$(scan_one movie "$movie_file")"
tv_result="$(scan_one tv "$tv_file")"
movie_records="$(cut -f2 <<<"$movie_result")"
tv_records="$(cut -f2 <<<"$tv_result")"
movie_json="$(cut -f3- <<<"$movie_result")"
tv_json="$(cut -f3- <<<"$tv_result")"
image_jobs_after="$(psql_at "$password" "SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download'")"
[[ "$image_jobs_after" =~ ^[0-9]+$ ]] || die 'could not read the image-job total'
image_job_fanout=$((image_jobs_after - image_jobs_before))
(( image_job_fanout >= 0 )) || die 'image-job count moved backwards during scan'
fanout_exceeded=false
(( image_job_fanout <= max_image_job_fanout )) || fanout_exceeded=true

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "selected_date_utc": "$selected_date",
  "movie_export_records": $movie_records,
  "tv_export_records": $tv_records,
  "queue_limit": $queue_limit,
  "image_jobs_before": $image_jobs_before,
  "image_jobs_after": $image_jobs_after,
  "image_job_fanout": $image_job_fanout,
  "max_image_job_fanout": $max_image_job_fanout,
  "image_job_fanout_exceeded": $fanout_exceeded,
  "movie_scan": $movie_json,
  "tv_scan": $tv_json
}
EOF
cat "$result_file"
printf 'TMDB scan artifact: %s\n' "$result_file"
if [[ "$fanout_exceeded" == true ]]; then
    die "downstream image-job fanout exceeded the bounded scan limit: $image_job_fanout > $max_image_job_fanout"
fi
