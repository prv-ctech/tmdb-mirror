#!/usr/bin/env bash
set -Eeuo pipefail

# The backup runner relies on psql meta-variable substitution. PostgreSQL only
# receives the substitution when the SQL is fed through standard input, not
# through psql's -c argument. Capture every invocation without a database and
# assert that the SQL and variables are passed by the supported mechanism.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
runner="$repo_root/infra/postgres/pgbackrest-runner.sh"
capture_dir="$(mktemp -d)"

cleanup() {
    rm -rf "$capture_dir"
}
trap cleanup EXIT

fake_bin="$capture_dir/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/psql" <<'PSQL'
#!/usr/bin/env bash
set -Eeuo pipefail

: "${TMDB_PSQL_CAPTURE_DIR:?}"
counter_file="$TMDB_PSQL_CAPTURE_DIR/counter"
counter=0
if [[ -f "$counter_file" ]]; then
    counter="$(<"$counter_file")"
fi
counter=$((counter + 1))
printf '%s\n' "$counter" >"$counter_file"
prefix="$(printf '%02d' "$counter")"
printf '%s\n' "$@" >"$TMDB_PSQL_CAPTURE_DIR/$prefix.args"
cat >"$TMDB_PSQL_CAPTURE_DIR/$prefix.sql"
printf '%s\n' job-id
PSQL
chmod +x "$fake_bin/psql"

cat >"$fake_bin/pgbackrest" <<'PGBACKREST'
#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${TMDB_FAKE_PGBACKREST_HAS_FULL:-false}" == true ]]; then
    printf '%s\n' '[{"backup":[{"type":"full"}]}]'
else
    printf '%s\n' '[{"backup":[]}]'
fi
PGBACKREST
chmod +x "$fake_bin/pgbackrest"

export PATH="$fake_bin:$PATH"
export TMDB_PSQL_CAPTURE_DIR="$capture_dir"
export POSTGRES_DB=test_db
export POSTGRES_USER=test_user
export POSTGRES_PASSWORD=test_only_value

# shellcheck source=/dev/null
source "$runner"

assert_file_contains() {
    local path="$1"
    local expected="$2"
    if ! grep -Fqx -- "$expected" "$path"; then
        printf 'expected %s in %s\n' "$expected" "$path" >&2
        cat "$path" >&2
        exit 1
    fi
}

assert_no_command_argument() {
    local path="$1"
    if grep -Fqx -- '-c' "$path"; then
        printf 'psql -c bypasses variable substitution: %s\n' "$path" >&2
        cat "$path" >&2
        exit 1
    fi
}

record_backup_heartbeat ready
refresh_job_heartbeat 11111111-1111-1111-1111-111111111111
fail_unpaired_job 22222222-2222-2222-2222-222222222222
fail_backup_request_and_job 33333333-3333-3333-3333-333333333333 backup
scheduled_job_id="$(submit_scheduled_backup 2026-08-03 diff)"

if [[ "$scheduled_job_id" != job-id ]]; then
    printf 'expected fake scheduled job ID, got %s\n' "$scheduled_job_id" >&2
    exit 1
fi
if record_backup_heartbeat invalid; then
    printf '%s\n' 'invalid heartbeat state was accepted' >&2
    exit 1
fi

if has_full_backup; then
    printf '%s\n' 'empty pgBackRest info reported a full backup' >&2
    exit 1
fi
export TMDB_FAKE_PGBACKREST_HAS_FULL=true
has_full_backup
unset TMDB_FAKE_PGBACKREST_HAS_FULL

assert_file_contains "$capture_dir/01.args" '--set=state=ready'
assert_file_contains "$capture_dir/01.sql" "SELECT ops.record_component_heartbeat('backup', :'state');"
assert_file_contains "$capture_dir/02.args" '--set=job_id=11111111-1111-1111-1111-111111111111'
assert_file_contains "$capture_dir/02.sql" "SELECT ops.heartbeat_job(:'job_id'::uuid, :'worker_id', :'lease_microseconds'::bigint);"
assert_file_contains "$capture_dir/03.sql" "    :'retry_microseconds'::bigint"
assert_file_contains "$capture_dir/04.sql" "    :'failure_step',"
assert_file_contains "$capture_dir/05.args" '--set=scheduled_for=2026-08-03'
assert_file_contains "$capture_dir/05.sql" "SELECT ops.submit_scheduled_backup(:'backup_type', :'scheduled_for'::date)::text;"

for args in "$capture_dir"/*.args; do
    assert_no_command_argument "$args"
done

printf '%s\n' 'pgBackRest runner SQL invocation test passed'
