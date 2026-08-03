#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
remove_volumes=false
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --remove-volumes) remove_volumes=true; shift ;;
        -h|--help) printf '%s\n' 'Usage: stress-down.sh [--project-name NAME] [--remove-volumes]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

configure_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" \
    "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
args=(down --remove-orphans)
[[ "$remove_volumes" == true ]] && args+=(--volumes)
compose_checked "${args[@]}"
