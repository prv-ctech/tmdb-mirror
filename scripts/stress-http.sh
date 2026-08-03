#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
concurrency=20
requests=50
api_port="${TMDB_STRESS_API_PORT:-18080}"
image_port="${TMDB_STRESS_IMAGE_PORT:-18090}"
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --concurrency) concurrency="$2"; shift 2 ;;
        --requests-per-worker) requests="$2"; shift 2 ;;
        --api-port) api_port="$2"; shift 2 ;;
        --image-port) image_port="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-http.sh [--project-name NAME] [--api-port PORT] [--image-port PORT] [--concurrency N] [--requests-per-worker N]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$concurrency" =~ ^[0-9]+$ ]] && (( concurrency >= 1 && concurrency <= 1000 )) || die 'invalid concurrency'
[[ "$requests" =~ ^[0-9]+$ ]] && (( requests >= 1 && requests <= 10000 )) || die 'invalid requests-per-worker'

configure_runtime "$project" "$api_port" "${TMDB_STRESS_ADMIN_PORT:-18081}" "$image_port" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
mkdir -p "$RESULT_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
work_root="${TMDB_STRESS_HTTP_WORK_ROOT:-/tmp/tmdb-stress-http/$project}"
mkdir -p "$work_root"
work_dir="$(mktemp -d "$work_root/http.XXXXXX")"
result_file="$RESULT_ROOT/http-$stamp.json"
cleanup() { rm -rf "$work_dir"; }
trap cleanup EXIT

base_url="http://127.0.0.1:$api_port"
image_url="http://127.0.0.1:$image_port"
urls=(
    "$base_url/health/live"
    "$base_url/v1/movies?limit=20"
    "$base_url/v1/tv?limit=20"
    "$base_url/v1/search?q=Cafe&limit=20"
    "$base_url/v1/anime?q=One%20Piece&limit=20"
    "$base_url/v1/movies?genreId=900000002&language=en&runtimeMin=40&runtimeMax=120&personId=900000002&companyId=900000002&limit=20"
)

request_one() {
    local index="$1" slot=$(( (index - 1) % ${#urls[@]} )) out="$work_dir/$index" err="$work_dir/$index.err"
    if ! curl --silent --show-error --connect-timeout 5 --max-time 30 \
        --output /dev/null --write-out '%{http_code} %{time_total}\n' \
        "${urls[$slot]}" >"$out" 2>"$err"; then
        printf '%s\n' '000 30.000000' >"$out"
    fi
}

running=()
total=$((concurrency * requests))
started_ns="$(date +%s%N)"
for index in $(seq 1 "$total"); do
    request_one "$index" &
    running+=("$!")
    if ((${#running[@]} >= concurrency)); then
        wait "${running[0]}" || true
        running=("${running[@]:1}")
    fi
done
for pid in "${running[@]}"; do wait "$pid" || true; done
finished_ns="$(date +%s%N)"

metrics="$work_dir/metrics.tsv"
: >"$metrics"
request_count=0
failed_requests=0
success_count=0
for file in "$work_dir"/[0-9]*; do
    [[ -f "$file" ]] || continue
    file_name="${file##*/}"
    [[ "$file_name" =~ ^[0-9]+$ ]] || continue
    read -r status seconds <"$file" || { status=000; seconds=30; }
    printf '%s\t%s\n' "$status" "$seconds" >>"$metrics"
    request_count=$((request_count + 1))
    if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
        success_count=$((success_count + 1))
    else
        failed_requests=$((failed_requests + 1))
    fi
done

percentile() {
    local fraction="$1"
    (( success_count > 0 )) || { printf '0.000000\n'; return; }
    sort -n -k2 "$metrics" | awk -v n="$success_count" -v p="$fraction" '
        BEGIN { target_index = int(n * p + 0.999999); if (target_index < 1) target_index = 1 }
        $1 ~ /^2[0-9][0-9]$/ { success_index++; if (success_index == target_index) { printf "%.6f\n", $2; exit } }
    '
}

p50="$(percentile 0.50)"
p95="$(percentile 0.95)"
p99="$(percentile 0.99)"
elapsed="$(awk -v start="$started_ns" -v finish="$finished_ns" 'BEGIN { seconds = (finish - start) / 1000000000; if (seconds > 0) printf "%.3f", seconds; else print "0.000" }')"
throughput="$(awk -v n="$request_count" -v seconds="$elapsed" 'BEGIN { if (seconds > 0) printf "%.3f", n / seconds; else print "0.000" }')"

semantic_failures=0
semantic_file="$work_dir/semantic.tsv"
check_body() {
    local name="$1" url="$2" pattern="$3" body
    body="$work_dir/body-$name.json"
    if ! curl --silent --show-error --fail --connect-timeout 5 --max-time 30 "$url" -o "$body"; then
        printf '%s\tFAIL\n' "$name" >>"$semantic_file"
        semantic_failures=$((semantic_failures + 1))
    elif [[ "$pattern" == absent-anime && $(grep -ci '"isAnime"[[:space:]]*:[[:space:]]*true' "$body" || true) -ne 0 ]]; then
        printf '%s\tFAIL\n' "$name" >>"$semantic_file"
        semantic_failures=$((semantic_failures + 1))
    elif [[ "$pattern" == present && ! -s "$body" ]]; then
        printf '%s\tFAIL\n' "$name" >>"$semantic_file"
        semantic_failures=$((semantic_failures + 1))
    else
        printf '%s\tPASS\n' "$name" >>"$semantic_file"
    fi
}

: >"$semantic_file"
check_body ordinary_routes_exclude_anime "$base_url/v1/movies?limit=100" absent-anime
check_body anime_route_returns_data "$base_url/v1/anime?q=One%20Piece&limit=20" present
check_body accent_search "$base_url/v1/search?q=Caf%C3%A9&limit=20" present
check_body multi_facet_filter "$base_url/v1/movies?genreId=900000002&language=en&runtimeMin=40&runtimeMax=120&personId=900000002&companyId=900000002&limit=20" present
if curl --silent --show-error --fail --connect-timeout 5 --max-time 30 "$image_url/healthz" >/dev/null; then
    printf '%s\tPASS\n' media_health >>"$semantic_file"
else
    printf '%s\tFAIL\n' media_health >>"$semantic_file"
    semantic_failures=$((semantic_failures + 1))
fi

cat >"$result_file" <<EOF
{
  "checked_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "requests": $request_count,
  "successful_requests": $success_count,
  "failed_requests": $failed_requests,
  "concurrency": $concurrency,
  "requests_per_worker": $requests,
  "elapsed_seconds": $elapsed,
  "throughput_requests_per_second": $throughput,
  "latency_seconds": {"p50": $p50, "p95": $p95, "p99": $p99},
  "semantic_failures": $semantic_failures
}
EOF
cat "$result_file"
printf 'HTTP stress artifact: %s\n' "$result_file"
(( failed_requests == 0 && semantic_failures == 0 )) || exit 2
