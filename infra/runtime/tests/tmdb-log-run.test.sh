#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
runner="$repo_root/infra/runtime/tmdb-log-run"
sandbox="$(mktemp -d)"

cleanup() {
    rm -rf "$sandbox"
}
trap cleanup EXIT

export TMDB_LOG_RUN_SOURCE_ONLY=1
# shellcheck source=/dev/null
source "$runner"
unset TMDB_LOG_RUN_SOURCE_ONLY

logs="$sandbox/logs"
mkdir -p "$logs"

for _ in $(seq 1 12); do
    allocate_log_file "$logs" worker >/dev/null
done

mapfile -t retained < <(find "$logs" -maxdepth 1 -type f -name 'worker*.log' -printf '%f\n' | sort -V)
expected=(
    worker-2.log
    worker-3.log
    worker-4.log
    worker-5.log
    worker-6.log
    worker-7.log
    worker-8.log
    worker-9.log
    worker-10.log
    worker-11.log
)

if [[ "${retained[*]}" != "${expected[*]}" ]]; then
    printf 'unexpected retained logs: %s\n' "${retained[*]}" >&2
    exit 1
fi

if allocate_log_file "$logs" unknown >/dev/null 2>&1; then
    printf '%s\n' 'unknown service name was accepted' >&2
    exit 1
fi

real_logs="$sandbox/real-logs"
mkdir "$real_logs"
ln -s "$real_logs" "$sandbox/symlink-logs"
if allocate_log_file "$sandbox/symlink-logs" api >/dev/null 2>&1; then
    printf '%s\n' 'symlink log directory was accepted' >&2
    exit 1
fi

printf '%s\n' 'runtime log rotation test passed'
