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

size_logs="$sandbox/size-logs"
size_input="$sandbox/size-input.jsonl"
size_output="$sandbox/size-output.jsonl"
max_bytes=50
mkdir -p "$size_logs"
initial_log="$(allocate_log_file "$size_logs" worker)"
for index in $(seq -w 0 11); do
    printf '{"record":"%s-日本語-日本語"}\n' "$index"
done >"$size_input"

persist_log_stream "$size_logs" worker "$max_bytes" "$initial_log" \
    <"$size_input" >"$size_output"

if ! cmp -s "$size_input" "$size_output"; then
    printf '%s\n' 'persistent log stream did not preserve stdout exactly' >&2
    exit 1
fi

mapfile -t size_retained < <(
    find "$size_logs" -maxdepth 1 -type f -name 'worker*.log' -printf '%f\n' | sort -V
)
if [[ "${size_retained[*]}" != "${expected[*]}" ]]; then
    printf 'unexpected size-rotated logs: %s\n' "${size_retained[*]}" >&2
    exit 1
fi
tail -n 10 "$size_input" >"$sandbox/expected-retained.jsonl"
for filename in "${size_retained[@]}"; do
    cat "$size_logs/$filename"
done >"$sandbox/actual-retained.jsonl"
if ! cmp -s "$sandbox/expected-retained.jsonl" "$sandbox/actual-retained.jsonl"; then
    printf '%s\n' 'size rollover did not retain the newest complete JSONL records' >&2
    exit 1
fi
while IFS= read -r path; do
    if (("$(stat -c %s "$path")" > max_bytes)); then
        printf 'size-rotated log exceeded limit: %s\n' "$path" >&2
        exit 1
    fi
done < <(find "$size_logs" -maxdepth 1 -type f -name 'worker*.log' -print)

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
