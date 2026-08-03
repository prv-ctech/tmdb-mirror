#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

tracked_files=()
while IFS= read -r -d '' file; do
    tracked_files+=("$file")
done < <(git -C "$REPO_ROOT" ls-files -z)
(( ${#tracked_files[@]} > 0 )) || die 'could not enumerate tracked files'

rules=(
    'TMDB/JWT credential|(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}(?![A-Za-z0-9_-])'
    'URI credentials|(?i)\bhttps?://[^/\s:@]+:[^@\s/]{16,}@'
    'URL credential query|(?i)[?&](?:token|api[_-]?key|access[_-]?token|password)=(?!test[-_]?|example|placeholder|your[-_])[A-Za-z0-9._~+/=-]{16,}'
    'private IPv4 address|\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.(?:\d{1,3}\.)\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.(?:\d{1,3}\.)\d{1,3})\b'
    'host-specific filesystem path|(?i)(?:[A-Z]:\\Users\\|/Users/[^/\s]+/|/mnt/(?:user|cache|disk|pool)/)'
    'hardcoded API credential assignment|(?i)\b(?:TMDB_API_KEY|TMDB_READ_ACCESS_TOKEN|API_KEY|ACCESS_TOKEN)\s*[:=]\s*(?!<|your[-_]|example|placeholder|changeme|test[-_]|unit[-_])[A-Za-z0-9._~+/=-]{16,}'
)

violation_count=0
for rule in "${rules[@]}"; do
    rule_name="${rule%%|*}"
    pattern="${rule#*|}"
    while IFS= read -r relative_path; do
        [[ -n "$relative_path" && "$relative_path" != scripts/verify-repository-hygiene.sh ]] || continue
        full_path="$REPO_ROOT/$relative_path"
        if [[ "$relative_path" == crates/tmdb-config/tests/settings.rs ]]; then
            if ! sed '/must-not-appear/d' "$full_path" | rg --pcre2 --quiet -- "$pattern"; then
                continue
            fi
        fi
        printf 'Hygiene violation: %s in %s\n' "$rule_name" "$relative_path" >&2
        violation_count=$((violation_count + 1))
    done < <(git -C "$REPO_ROOT" grep -IlP -- "$pattern" -- "${tracked_files[@]}" 2>/dev/null || true)
done
(( violation_count == 0 )) || die 'repository hygiene found prohibited private data patterns'

ignored_probes=(
    .env .secrets.txt secrets.txt
    .stress-runtime/example/token target/debug/example example.log
    data/media/example.jpg data/config/example.json
)
for probe in "${ignored_probes[@]}"; do
    git -C "$REPO_ROOT" check-ignore --no-index --quiet -- "$probe" \
        || die "Git ignore policy does not cover generated path: $probe"
done

printf 'Repository hygiene passed for %s tracked files.\n' "${#tracked_files[@]}"
