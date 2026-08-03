#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

project="${TMDB_STRESS_PROJECT:-tmdb_stress_test}"
count=100000
while (($#)); do
    case "$1" in
        --project-name) project="$2"; shift 2 ;;
        --count) count="$2"; shift 2 ;;
        -h|--help) printf '%s\n' 'Usage: stress-seed.sh [--project-name NAME] [--count 100000]'; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
[[ "$count" =~ ^[0-9]+$ ]] && (( count >= 1000 && count <= 2000000 )) || die 'count must be between 1000 and 2000000'

configure_runtime "$project" "${TMDB_STRESS_API_PORT:-18080}" "${TMDB_STRESS_ADMIN_PORT:-18081}" \
    "${TMDB_STRESS_IMAGE_PORT:-18090}" "${TMDB_STRESS_PG_PORT:-55433}"
load_runtime
seed_file="$REPO_ROOT/scripts/stress-seed.sql"
[[ -f "$seed_file" ]] || die "seed SQL is missing: $seed_file"

password="$(database_password)"
[[ -n "$password" ]] || die 'disposable database password is empty'
container="$(compose ps -q postgres)"
[[ -n "$container" ]] || die 'postgres container is unavailable'
container_seed_path=/tmp/tmdb-stress-seed.sql
docker_command cp "$(docker_path "$seed_file")" "$container:$container_seed_path"
cleanup() { compose exec -T postgres rm -f "$container_seed_path" >/dev/null 2>&1 || true; }
trap cleanup EXIT

compose exec -T -e "PGPASSWORD=$password" postgres psql -X -v ON_ERROR_STOP=1 \
    --username "$(database_user)" --dbname "$(database_name)" \
    --set="seed_count=$count" --set='seed_base=900000000' --file "$container_seed_path"

verification="$(psql_at "$password" "SELECT json_build_object(
  'titles', (SELECT count(*) FROM catalog.titles WHERE tmdb_id >= 900000001 AND tmdb_id < 900000001 + $count),
  'anime', (SELECT count(*) FROM catalog.titles WHERE is_anime AND tmdb_id >= 900000001 AND tmdb_id < 900000001 + $count),
  'search_documents', (SELECT count(*) FROM search.search_documents WHERE title_id IN (
    SELECT id FROM catalog.titles WHERE tmdb_id >= 900000001 AND tmdb_id < 900000001 + $count
  ))
)" )"
printf '%s\n' "$verification"
