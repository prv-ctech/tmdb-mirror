#!/usr/bin/env bash
set -Eeuo pipefail

# Exercises the PostgreSQL image as an operator would: the image must provide
# pgBackRest 2.59, archive WAL to the fixed /config repository, and preserve a
# successful backup that can be inspected. This test creates only disposable
# Docker resources and always removes them.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
if [[ -n "${TMDB_DOCKER_BIN:-}" ]]; then
    docker_bin="$TMDB_DOCKER_BIN"
elif command -v docker.exe >/dev/null 2>&1; then
    docker_bin="$(command -v docker.exe)"
else
    docker_bin="$(command -v docker)"
fi

docker() {
    "$docker_bin" "$@"
}

docker_path() {
    if [[ "$docker_bin" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
        wslpath -w "$1" | tr '\\' '/'
    else
        printf '%s\n' "$1"
    fi
}

image_tag="tmdb-mirror-postgres-pitr-test:${RANDOM}${RANDOM}"
container_name="tmdb-mirror-pitr-test-${RANDOM}${RANDOM}"
restore_container_name="${container_name}-restore"
data_volume="${container_name}-data"
restore_data_volume="${container_name}-restore-data"
config_volume="${container_name}-config"

cleanup() {
    docker rm -f "$restore_container_name" >/dev/null 2>&1 || true
    docker rm -f "$container_name" >/dev/null 2>&1 || true
    docker volume rm -f "$data_volume" "$restore_data_volume" "$config_volume" >/dev/null 2>&1 || true
}

fail_with_container_logs() {
    printf '%s\n' 'pgBackRest/PITR test container did not become ready' >&2
    docker logs "$container_name" >&2 || true
    exit 1
}

trap cleanup EXIT

docker build \
    --file "$(docker_path "$repo_root/infra/postgres/Dockerfile")" \
    --tag "$image_tag" \
    "$(docker_path "$repo_root/infra/postgres")"

docker volume create "$data_volume" >/dev/null
docker volume create "$restore_data_volume" >/dev/null
docker volume create "$config_volume" >/dev/null

docker run -d \
    --name "$container_name" \
    --env POSTGRES_DB=tmdb_test \
    --env POSTGRES_USER=tmdb_test \
    --env POSTGRES_PASSWORD=test-only-password \
    --env PGDATA=/var/lib/postgresql/18/docker \
    --env 'POSTGRES_INITDB_ARGS=--data-checksums --encoding=UTF8 --auth-local=scram-sha-256 --auth-host=scram-sha-256' \
    --env TZ=America/New_York \
    --volume "$data_volume:/var/lib/postgresql" \
    --volume "$config_volume:/config" \
    "$image_tag" \
    postgres \
    -c wal_level=replica \
    -c archive_mode=on \
    -c 'archive_command=pgbackrest --stanza=tmdb archive-push %p' \
    -c timezone=UTC >/dev/null

ready=false
for _ in $(seq 1 20); do
    if docker exec "$container_name" pg_isready -U tmdb_test -d tmdb_test -h 127.0.0.1 -t 1 >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done

if [[ "$ready" != true ]]; then
    fail_with_container_logs
fi
docker exec "$container_name" sh -ec 'pgbackrest version | grep -F "2.59.0" >/dev/null'
docker exec "$container_name" sh -ec 'test -d /config/backups/pgbackrest && test "$(stat -c %U /config/backups/pgbackrest)" = postgres'
docker exec "$container_name" sh -ec 'PGPASSWORD=test-only-password psql -U tmdb_test -d tmdb_test -Atqc "SHOW wal_level"' | grep -Fx replica
docker exec "$container_name" sh -ec 'PGPASSWORD=test-only-password psql -U tmdb_test -d tmdb_test -Atqc "SHOW archive_mode"' | grep -Fx on
docker exec "$container_name" sh -ec 'PGPASSWORD=test-only-password psql -U tmdb_test -d tmdb_test -Atqc "SHOW archive_command"' | grep -F "pgbackrest --stanza=tmdb archive-push %p" >/dev/null
docker exec "$container_name" sh -ec 'PGPASSWORD=test-only-password psql -U tmdb_test -d tmdb_test -Atqc "SHOW timezone"' | grep -Fx UTC
docker exec "$container_name" sh -ec 'grep -Fx "repo1-retention-full=1" /etc/pgbackrest/pgbackrest.conf && grep -Fx "repo1-retention-diff=6" /etc/pgbackrest/pgbackrest.conf'
docker exec "$container_name" tmdb-pgbackrest schedule-type 2026-08-02 | grep -Fx full
docker exec "$container_name" tmdb-pgbackrest schedule-type 2026-08-03 | grep -Fx diff
if docker exec "$container_name" tmdb-pgbackrest schedule-type 2026-08-01 >/dev/null 2>&1; then
    printf '%s\n' 'Saturday must not schedule a backup' >&2
    exit 1
fi

psql_source() {
    docker exec \
        --env PGPASSWORD=test-only-password \
        "$container_name" \
        psql -U tmdb_test -d tmdb_test -Atqc "$1"
}

psql_source 'CREATE TABLE pitr_fixture (name text PRIMARY KEY, phase integer NOT NULL)'
psql_source "INSERT INTO pitr_fixture(name, phase) VALUES ('full-record', 1)"
docker exec "$container_name" tmdb-pgbackrest backup full
psql_source "INSERT INTO pitr_fixture(name, phase) VALUES ('included-record', 2)"
docker exec "$container_name" tmdb-pgbackrest backup diff

# The target is recorded after the differential is safely complete and before
# the excluded transaction. Switch WAL before the exclusion so the target is
# already in an archived segment; this removes timing ambiguity from a
# disposable recovery test while leaving the later transaction beyond it.
pitr_target="$(psql_source "SELECT to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.USOF')")"
printf '%s\n' "PITR recovery target: $pitr_target"
psql_source 'SELECT pg_switch_wal()' >/dev/null
docker exec "$container_name" tmdb-pgbackrest check
sleep 2
psql_source "INSERT INTO pitr_fixture(name, phase) VALUES ('excluded-record', 3)"
psql_source 'SELECT pg_switch_wal()' >/dev/null
docker exec "$container_name" tmdb-pgbackrest check

docker stop "$container_name" >/dev/null
docker run --rm \
    --entrypoint bash \
    --env PGDATA=/var/lib/postgresql/18/docker \
    --env PITR_TARGET="$pitr_target" \
    --volume "$restore_data_volume:/var/lib/postgresql" \
    --volume "$config_volume:/config" \
    "$image_tag" \
    -ec '
        install -d -m 0750 /etc/pgbackrest
        printf "%s\\n" \
            "[global]" \
            "repo1-path=/config/backups/pgbackrest" \
            "compress-type=zst" \
            "[tmdb]" \
            "pg1-path=/var/lib/postgresql/18/docker" \
            "pg1-user=tmdb_test" \
            > /etc/pgbackrest/pgbackrest.conf
        pgbackrest --stanza=tmdb --type=time --target="$PITR_TARGET" --target-exclusive --target-action=promote restore
    '

docker run -d \
    --name "$restore_container_name" \
    --env POSTGRES_DB=tmdb_test \
    --env POSTGRES_USER=tmdb_test \
    --env POSTGRES_PASSWORD=test-only-password \
    --env PGDATA=/var/lib/postgresql/18/docker \
    --env 'POSTGRES_INITDB_ARGS=--data-checksums --encoding=UTF8 --auth-local=scram-sha-256 --auth-host=scram-sha-256' \
    --env TZ=America/New_York \
    --volume "$restore_data_volume:/var/lib/postgresql" \
    --volume "$config_volume:/config" \
    "$image_tag" \
    postgres \
    -c wal_level=replica \
    -c archive_mode=on \
    -c 'archive_command=pgbackrest --stanza=tmdb archive-push %p' \
    -c timezone=UTC >/dev/null

restore_ready=false
for _ in $(seq 1 90); do
    if docker exec "$restore_container_name" pg_isready -U tmdb_test -d tmdb_test -h 127.0.0.1 -t 1 >/dev/null 2>&1; then
        restore_ready=true
        break
    fi
    sleep 1
done
if [[ "$restore_ready" != true ]]; then
    printf '%s\n' 'PITR restore container did not become ready' >&2
    docker logs "$restore_container_name" >&2 || true
    exit 1
fi

restored_records="$(docker exec \
    --env PGPASSWORD=test-only-password \
    "$restore_container_name" \
    psql -U tmdb_test -d tmdb_test -Atqc "SELECT string_agg(name, ',' ORDER BY phase) FROM pitr_fixture")"
if [[ "$restored_records" != "full-record,included-record" ]]; then
    printf '%s\n' "unexpected PITR records: ${restored_records:-none}" >&2
    docker logs "$restore_container_name" >&2 || true
    exit 1
fi
excluded_present="$(docker exec \
    --env PGPASSWORD=test-only-password \
    "$restore_container_name" \
    psql -U tmdb_test -d tmdb_test -Atqc "SELECT CASE WHEN EXISTS (SELECT 1 FROM pitr_fixture WHERE name = 'excluded-record') THEN 'true' ELSE 'false' END")"
if [[ "$excluded_present" != false ]]; then
    printf '%s\n' 'PITR included the post-target record' >&2
    docker logs "$restore_container_name" >&2 || true
    exit 1
fi

printf '%s\n' 'pgBackRest PITR test passed: selected time restored included records and excluded later records'
