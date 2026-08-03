#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
shifted=()
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        *) shifted+=("$1"); shift ;;
    esac
done
exec "$SCRIPT_DIR/stress-bootstrap.sh" --project-name "$project" --skip-build "${shifted[@]}"
