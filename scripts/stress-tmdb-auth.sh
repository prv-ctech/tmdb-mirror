#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

secrets_file="${TMDB_STRESS_SECRETS_FILE:-$(select_secrets_file)}"
read_stress_secrets "$secrets_file"
[[ -n "$STRESS_READ_TOKEN" && -n "$STRESS_API_KEY" ]] || die 'both TMDB stress credentials are required'
trap 'unset STRESS_READ_TOKEN STRESS_API_KEY' EXIT

status_for() {
    local path="$1" mode="$2" status
    if [[ "$mode" == bearer ]]; then
        status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
            --connect-timeout 10 --max-time 30 \
            -H "Authorization: Bearer $STRESS_READ_TOKEN" "https://api.themoviedb.org/3/$path")"
    else
        status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
            --connect-timeout 10 --max-time 30 \
            "https://api.themoviedb.org/3/$path?api_key=$STRESS_API_KEY")"
    fi
    printf '%s\n' "$status"
}

bearer_configuration="$(status_for configuration bearer)"
bearer_movie="$(status_for movie/550 bearer)"
v3_configuration="$(status_for configuration v3)"
v3_movie="$(status_for movie/550 v3)"
printf 'bearer configuration=%s movie=550:%s\n' "$bearer_configuration" "$bearer_movie"
printf 'v3 configuration=%s movie=550:%s\n' "$v3_configuration" "$v3_movie"

if [[ "$bearer_configuration" != 200 || "$bearer_movie" != 200 || "$v3_configuration" != 200 || "$v3_movie" != 200 ]]; then
    die 'TMDB authentication checks failed'
fi
printf '%s\n' 'TMDB authentication checks passed.'
