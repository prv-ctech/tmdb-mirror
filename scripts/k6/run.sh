#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../stress-common.sh"

profile=full
base_url='http://127.0.0.1:18080'
virtual_users=100
requests_per_endpoint=10000
burn_requests=100000
max_duration=30m
request_timeout=30
metadata_path=''
list_path=''
search_path=''
filter_path=''
k6_image='grafana/k6:1.0.0@sha256:f21270290d702cbf0a7d6ba5d7ed100b63bcb233b558b885ed787547b3488279'
results_dir=''
network=''
compose_file=''
compose_env_file=''
compose_project=''
admin_metrics_url=''
while (($#)); do
    case "$1" in
        --profile) profile="$2"; shift 2 ;;
        --base-url) base_url="$2"; shift 2 ;;
        --virtual-users) virtual_users="$2"; shift 2 ;;
        --requests-per-endpoint) requests_per_endpoint="$2"; shift 2 ;;
        --burn-requests) burn_requests="$2"; shift 2 ;;
        --max-duration) max_duration="$2"; shift 2 ;;
        --request-timeout-seconds) request_timeout="$2"; shift 2 ;;
        --metadata-path) metadata_path="$2"; shift 2 ;;
        --list-path) list_path="$2"; shift 2 ;;
        --search-path) search_path="$2"; shift 2 ;;
        --filter-path) filter_path="$2"; shift 2 ;;
        --k6-image) k6_image="$2"; shift 2 ;;
        --results-directory) results_dir="$2"; shift 2 ;;
        --network) network="$2"; shift 2 ;;
        --compose-file) compose_file="$2"; shift 2 ;;
        --compose-env-file) compose_env_file="$2"; shift 2 ;;
        --compose-project-name) compose_project="$2"; shift 2 ;;
        --admin-metrics-url) admin_metrics_url="$2"; shift 2 ;;
        -h|--help)
            printf '%s\n' 'Usage: k6/run.sh [--profile endpoints|burn|full] [--base-url URL] [load options]'
            exit 0
            ;;
        *) die "unknown option: $1" ;;
    esac
done

[[ "$profile" == endpoints || "$profile" == burn || "$profile" == full ]] || die 'invalid k6 profile'
for value_name in virtual_users requests_per_endpoint burn_requests request_timeout; do
    value="${!value_name}"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "invalid $value_name"
done
[[ "$max_duration" =~ ^[1-9][0-9]*[smh]$ ]] || die 'invalid max duration'
[[ "$k6_image" =~ ^[a-z0-9][a-z0-9._/-]*(:[A-Za-z0-9._-]+)?@sha256:[a-f0-9]{64}$ ]] || die 'k6 image must be digest pinned'
[[ "$base_url" =~ ^https?://[^[:space:]/?#@]+(:[0-9]+)?$ ]] || die 'base URL must be a credential-free HTTP origin'
for path_name in metadata_path list_path search_path filter_path; do
    path_value="${!path_name}"
    if [[ -n "$path_value" ]]; then
        [[ "$path_value" == /* && "$path_value" != //* && "$path_value" != *$'\n'* && "$path_value" != *$'\r'* && ${#path_value} -le 2048 ]] \
            || die "$path_name must be a relative API path"
    fi
done
[[ -z "$network" || "$network" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] || die 'invalid Docker network name'
[[ -z "$compose_file" || -f "$compose_file" ]] || die 'configured Compose file is missing'
[[ -z "$compose_env_file" || -f "$compose_env_file" ]] || die 'configured Compose env file is missing'

scenario="$SCRIPT_DIR/tmdb-api.js"
[[ -f "$scenario" ]] || die "k6 scenario is missing: $scenario"
if secret_path="$(select_secrets_file 2>/dev/null)"; then
    read_stress_secrets "$secret_path"
    export TMDB_READ_ACCESS_TOKEN="$STRESS_READ_TOKEN"
fi
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "$results_dir" ]]; then
    results_dir="$REPO_ROOT/.stress-runtime/k6/$timestamp"
fi
mkdir -p "$results_dir"
results_dir="$(realpath -e "$results_dir")"

cat >"$results_dir/run.json" <<EOF
{
  "schema_version": 1,
  "started_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "profile": "$profile",
  "virtual_users": $virtual_users,
  "requests_per_endpoint": $requests_per_endpoint,
  "burn_requests": $burn_requests,
  "max_duration": "$max_duration",
  "request_timeout_seconds": $request_timeout,
  "k6_image": "$k6_image"
}
EOF

container_origin="$base_url"
case "$container_origin" in
    http://127.0.0.1*) container_origin="http://host.docker.internal${container_origin#http://127.0.0.1}" ;;
    https://127.0.0.1*) container_origin="https://host.docker.internal${container_origin#https://127.0.0.1}" ;;
    http://localhost*) container_origin="http://host.docker.internal${container_origin#http://localhost}" ;;
    https://localhost*) container_origin="https://host.docker.internal${container_origin#https://localhost}" ;;
esac

run_k6() {
    local mode="$1" endpoint_class="$2" iterations="$3" run_name="$4"
    local summary="k6-$run_name-$timestamp.summary.json" console="k6-$run_name-$timestamp.console.txt"
    local -a args=(run --rm --init --add-host host.docker.internal:host-gateway)
    [[ -n "$network" ]] && args+=(--network "$network")
    args+=(--user "$(id -u):$(id -g)"
        --volume "$(docker_path "$scenario"):/scripts/tmdb-api.js:ro"
        --volume "$(docker_path "$results_dir"):/results"
        "$k6_image" run
        --summary-export="/results/$summary"
        --env "TMDB_K6_BASE_URL=$container_origin"
        --env "TMDB_K6_RUN_MODE=$mode"
        --env "TMDB_K6_VUS=$virtual_users"
        --env "TMDB_K6_ITERATIONS=$iterations"
        --env "TMDB_K6_MAX_DURATION=$max_duration"
        --env "TMDB_K6_REQUEST_TIMEOUT=${request_timeout}s")
    [[ -n "$endpoint_class" ]] && args+=(--env "TMDB_K6_ENDPOINT_CLASS=$endpoint_class")
    [[ -n "$metadata_path" ]] && args+=(--env "TMDB_K6_METADATA_PATH=$metadata_path")
    [[ -n "$list_path" ]] && args+=(--env "TMDB_K6_LIST_PATH=$list_path")
    [[ -n "$search_path" ]] && args+=(--env "TMDB_K6_SEARCH_PATH=$search_path")
    [[ -n "$filter_path" ]] && args+=(--env "TMDB_K6_FILTER_PATH=$filter_path")
    args+=(/scripts/tmdb-api.js)
    if docker_command "${args[@]}" >"$results_dir/$console" 2>&1; then
        status=0
    else
        status=$?
    fi
    redact_file "$results_dir/$console" "$results_dir/$console.redacted"
    mv -f "$results_dir/$console.redacted" "$results_dir/$console"
    [[ -f "$results_dir/$summary" ]] || printf '%s\n' "{\"run_name\":\"$run_name\",\"exit_code\":$status}" >"$results_dir/$summary"
    return "$status"
}

failed=0
if [[ "$profile" == endpoints || "$profile" == full ]]; then
    for endpoint in metadata list search filter; do
        if ! run_k6 endpoint "$endpoint" "$requests_per_endpoint" "endpoint-$endpoint"; then
            failed=1
            break
        fi
    done
fi
if (( failed == 0 )) && [[ "$profile" == burn || "$profile" == full ]]; then
    run_k6 burn '' "$burn_requests" burn || failed=1
fi

if (( failed != 0 )) && [[ -n "$compose_project" && -n "$compose_file" ]]; then
    "$SCRIPT_DIR/collect-diagnostics.sh" --result-directory "$results_dir" \
        --compose-file "$compose_file" --compose-env-file "$compose_env_file" \
        --compose-project-name "$compose_project" --admin-metrics-url "$admin_metrics_url" \
        --run-started-at-utc "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" >/dev/null || true
fi
printf 'k6 results: %s\n' "$results_dir"
exit "$failed"
