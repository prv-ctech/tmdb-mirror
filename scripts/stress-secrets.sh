#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=stress-common.sh
source "$SCRIPT_DIR/stress-common.sh"

if [[ "${1:-}" == '--self-test' ]]; then
    require_command mktemp
    test_file="$(mktemp)"
    trap 'rm -f "$test_file"' EXIT
    printf '%s\n' \
        '# local only' \
        'TMDB_STRESS_READ_TOKEN=unit-read-token' \
        'TMDB_STRESS_API_KEY=unit-v3-api-key' \
        'TMDB_STRESS_TRAWL_BASE_URL=http://trawl.example:8191' >"$test_file"
    read_stress_secrets "$test_file"
    [[ "$STRESS_READ_TOKEN" == 'unit-read-token' ]] || die 'read token parser test failed'
    [[ "$STRESS_API_KEY" == 'unit-v3-api-key' ]] || die 'API key parser test failed'
    [[ "$STRESS_TRAWL_URL" == 'http://trawl.example:8191' ]] || die 'Trawl parser test failed'

    printf '%s' 'TMDB_STRESS_READ_TOKEN="quoted-value"' >"$test_file"
    if (read_stress_secrets "$test_file") 2>/dev/null; then
        die 'quoted secret values must be rejected'
    fi
    printf '%s\n' 'Stress secret loader tests passed.'
    exit 0
fi

cat <<'USAGE'
Usage: scripts/stress-secrets.sh --self-test

The reusable parser is sourced by the other Linux stress scripts. It reads the
ignored secrets.txt file without sourcing values.
USAGE
