#!/usr/bin/env bash
set -Eeuo pipefail

readonly BACKUP_WORKER_ID=tmdb-pgbackrest
readonly PGBACKREST_STANZA=tmdb
readonly PGBACKREST_REPOSITORY=/config/backups/pgbackrest
readonly BACKUP_LOCK_FILE=/config/backups/pgbackrest/.tmdb-pgbackrest.lock
readonly SCHEDULE_STATE_FILE=/config/backups/pgbackrest/.tmdb-pgbackrest-schedule
readonly JOB_LEASE_MICROSECONDS=3600000000
readonly JOB_RETRY_MICROSECONDS=300000000
readonly POLL_SECONDS=15

last_failure_step=""
queue_unavailable_logged=false

if [[ "${BASH_SOURCE[0]}" == "$0" && "$(id -u)" == 0 ]]; then
    exec gosu postgres "$0" "$@"
fi

export TZ="${TZ:-America/New_York}"
# pgBackRest uses libpq for local database checks. Reuse PostgreSQL's existing
# runtime password in-process; it is deliberately never written to the
# pgBackRest configuration or emitted in a command line/log.
export PGPASSWORD="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

log() {
    printf '%s [tmdb-pgbackrest] %s\n' "$(date --iso-8601=seconds)" "$*" >&2
}

usage() {
    printf '%s\n' \
        'usage: tmdb-pgbackrest {ensure|backup full|backup diff|check|verify|info|scheduler|schedule-type YYYY-MM-DD}' \
        >&2
}

require_database_environment() {
    : "${POSTGRES_DB:?POSTGRES_DB is required}"
    : "${POSTGRES_USER:?POSTGRES_USER is required}"
    : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
}

psql_tmdb() {
    require_database_environment
    PGPASSWORD="$POSTGRES_PASSWORD" \
        psql -X --no-psqlrc --set=ON_ERROR_STOP=1 \
            --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

validate_backup_type() {
    case "${1:-}" in
        full|diff)
            ;;
        *)
            log "event=backup_rejected reason=invalid_type"
            return 64
            ;;
    esac
}

run_backup_unlocked() {
    local backup_type="$1"
    validate_backup_type "$backup_type"

    last_failure_step=backup
    if ! pgbackrest --stanza="$PGBACKREST_STANZA" --type="$backup_type" --no-expire \
        --log-level-console=info backup; then
        return 1
    fi
    last_failure_step=archive_check
    if ! pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info check; then
        return 1
    fi
    if [[ "$backup_type" == full ]]; then
        last_failure_step=verify
        if ! pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info verify; then
            return 1
        fi
    fi
    # Backups are intentionally created with --no-expire. Only after the
    # archive check (and full-backup verification) succeeds can pgBackRest
    # retire an older recovery chain and its unneeded WAL.
    last_failure_step=expire
    if ! pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info expire; then
        return 1
    fi
    last_failure_step=""
}

run_backup() {
    local backup_type="$1"
    local result
    local lock_fd

    exec {lock_fd}>"$BACKUP_LOCK_FILE"
    if ! flock -n "$lock_fd"; then
        last_failure_step=already_running
        eval "exec ${lock_fd}>&-"
        log "event=backup_skipped reason=already_running"
        return 75
    fi
    if run_backup_unlocked "$backup_type"; then
        result=0
    else
        result=$?
    fi
    flock -u "$lock_fd"
    eval "exec ${lock_fd}>&-"
    return "$result"
}

ensure_pgbackrest() {
    pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info stanza-create
    pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info check
}

has_full_backup() {
    local backup_info

    if ! backup_info="$(pgbackrest --stanza="$PGBACKREST_STANZA" --output=json info 2>/dev/null)"; then
        return 1
    fi
    grep -Fq '"type":"full"' <<<"$backup_info"
}

queue_support_available() {
    local available
    if ! available="$(psql_tmdb -qAtc "
        SELECT to_regclass('ops.backup_requests') IS NOT NULL
           AND to_regprocedure('ops.claim_job_for_types(text,bigint,text[])') IS NOT NULL
           AND to_regprocedure('ops.complete_job(uuid,text,text)') IS NOT NULL
            AND to_regprocedure('ops.fail_job(uuid,text,text,bigint)') IS NOT NULL
           AND to_regprocedure('ops.fail_backup_request_and_job(uuid,text,text,bigint)') IS NOT NULL
           AND to_regprocedure('ops.submit_scheduled_backup(text,date)') IS NOT NULL
           AND to_regprocedure('ops.record_component_heartbeat(text,text)') IS NOT NULL
    " 2>/dev/null)"; then
        return 1
    fi
    [[ "$available" == t ]]
}

record_backup_heartbeat() {
    local state="$1"

    case "$state" in
        ready|degraded|failed)
            ;;
        *)
            log "event=component_heartbeat_rejected component=backup reason=invalid_state"
            return 64
            ;;
    esac
    psql_tmdb -qAt \
        --set=state="$state" \
        >/dev/null <<'SQL'
SELECT ops.record_component_heartbeat('backup', :'state');
SQL
}

claim_backup_job() {
    psql_tmdb -qAt -F $'\t' \
        --set=worker_id="$BACKUP_WORKER_ID" \
        --set=lease_microseconds="$JOB_LEASE_MICROSECONDS" <<'SQL'
SELECT job_id::text, job_type
FROM ops.claim_job_for_types(
    :'worker_id',
    :'lease_microseconds'::bigint,
    ARRAY['database.backup_full', 'database.backup_diff']::text[]
);
SQL
}

claim_backup_request() {
    local job_id="$1"

    psql_tmdb -qAt -F $'\t' \
        --set=job_id="$job_id" \
        --set=worker_id="$BACKUP_WORKER_ID" <<'SQL'
UPDATE ops.backup_requests AS request
SET status = 'running',
    started_at = pg_catalog.clock_timestamp(),
    finished_at = NULL,
    worker_id = :'worker_id',
    error_code = NULL,
    error_message = NULL,
    result = NULL
WHERE request.job_id = :'job_id'::uuid
  AND request.status IN ('queued', 'running')
RETURNING request.backup_type, request.request_source;
SQL
}

refresh_job_heartbeat() {
    local job_id="$1"

    psql_tmdb -qAt \
        --set=job_id="$job_id" \
        --set=worker_id="$BACKUP_WORKER_ID" \
        --set=lease_microseconds="$JOB_LEASE_MICROSECONDS" \
        >/dev/null <<'SQL'
SELECT ops.heartbeat_job(:'job_id'::uuid, :'worker_id', :'lease_microseconds'::bigint);
SQL
}

heartbeat_loop() {
    local job_id="$1"

    while sleep 30; do
        if ! refresh_job_heartbeat "$job_id" 2>/dev/null; then
            log "event=job_heartbeat_failed job_id=$job_id"
        fi
    done
}

fail_unpaired_job() {
    local job_id="$1"

    psql_tmdb -qAt \
        --set=job_id="$job_id" \
        --set=worker_id="$BACKUP_WORKER_ID" \
        --set=retry_microseconds="$JOB_RETRY_MICROSECONDS" \
        >/dev/null 2>&1 <<'SQL' || true
SELECT disposition
FROM ops.fail_job(
    :'job_id'::uuid,
    :'worker_id',
    'invalid_payload',
    :'retry_microseconds'::bigint
);
SQL
}

complete_backup_request_and_job() {
    local job_id="$1"
    local backup_type="$2"
    local result

    if [[ "$backup_type" == full ]]; then
        result="{\"backup_type\":\"full\",\"archive_checked\":true,\"verified\":true}"
    else
        result="{\"backup_type\":\"diff\",\"archive_checked\":true,\"verified\":false}"
    fi
    psql_tmdb -qAt \
        --set=job_id="$job_id" \
        --set=worker_id="$BACKUP_WORKER_ID" \
        --set=result="$result" <<'SQL'
WITH completed AS MATERIALIZED (
    SELECT ops.complete_job(:'job_id'::uuid, :'worker_id', :'result') AS status
)
UPDATE ops.backup_requests AS request
SET status = CASE WHEN completed.status = 'succeeded' THEN 'succeeded' ELSE 'failed' END,
    finished_at = pg_catalog.clock_timestamp(),
    error_code = CASE WHEN completed.status = 'succeeded' THEN NULL ELSE 'backup_failed' END,
    error_message = CASE WHEN completed.status = 'succeeded' THEN NULL ELSE 'cancelled' END,
    result = CASE WHEN completed.status = 'succeeded' THEN :'result'::jsonb ELSE NULL END
FROM completed
WHERE request.job_id = :'job_id'::uuid
  AND request.status = 'running'
  AND request.worker_id = :'worker_id'
RETURNING completed.status;
SQL
}

fail_backup_request_and_job() {
    local job_id="$1"
    local failure_step="$2"

    psql_tmdb -qAt \
        --set=job_id="$job_id" \
        --set=worker_id="$BACKUP_WORKER_ID" \
        --set=retry_microseconds="$JOB_RETRY_MICROSECONDS" \
        --set=failure_step="$failure_step" \
        <<'SQL'
SELECT ops.fail_backup_request_and_job(
    :'job_id'::uuid,
    :'worker_id',
    :'failure_step',
    :'retry_microseconds'::bigint
);
SQL
}

reconcile_terminal_backup_requests() {
    psql_tmdb -qAt <<'SQL'
UPDATE ops.backup_requests AS request
SET status = 'failed',
    finished_at = pg_catalog.clock_timestamp(),
    error_code = 'backup_failed',
    error_message = CASE
        WHEN job.status = 'cancelled' THEN 'cancelled'
        ELSE 'job_terminal'
    END,
    result = NULL
FROM ops.jobs AS job
WHERE request.job_id = job.id
  AND request.status IN ('queued', 'running')
  AND job.status IN ('cancelled', 'dead_letter');
SQL
}

process_one_backup_job() {
    local claimed
    local job_id
    local job_type
    local request
    local backup_type
    local request_source
    local expected_job_type
    local heartbeat_pid=""
    local completion_status

    if ! claimed="$(claim_backup_job)"; then
        log "event=backup_queue_poll_failed"
        return 1
    fi
    [[ -n "$claimed" ]] || return 0
    IFS=$'\t' read -r job_id job_type <<<"$claimed"

    if ! request="$(claim_backup_request "$job_id")"; then
        log "event=backup_request_claim_failed job_id=$job_id"
        fail_unpaired_job "$job_id"
        return 1
    fi
    if [[ -z "$request" ]]; then
        log "event=backup_request_missing job_id=$job_id"
        fail_unpaired_job "$job_id"
        return 1
    fi
    IFS=$'\t' read -r backup_type request_source <<<"$request"
    expected_job_type="database.backup_${backup_type}"
    if ! validate_backup_type "$backup_type" || [[ "$job_type" != "$expected_job_type" ]]; then
        log "event=backup_request_rejected job_id=$job_id reason=type_mismatch"
        fail_backup_request_and_job "$job_id" invalid_payload || true
        return 1
    fi

    heartbeat_loop "$job_id" &
    heartbeat_pid=$!
    if run_backup "$backup_type"; then
        if completion_status="$(complete_backup_request_and_job "$job_id" "$backup_type")"; then
            record_backup_heartbeat ready || true
            log "event=backup_completed job_id=$job_id type=$backup_type source=$request_source status=${completion_status:-unknown}"
        else
            log "event=backup_completion_record_failed job_id=$job_id"
        fi
    else
        record_backup_heartbeat degraded || true
        log "event=backup_failed job_id=$job_id type=$backup_type step=${last_failure_step:-unknown}"
        fail_backup_request_and_job "$job_id" "${last_failure_step:-unknown}" || true
    fi
    kill -TERM "$heartbeat_pid" >/dev/null 2>&1 || true
    wait "$heartbeat_pid" >/dev/null 2>&1 || true
}

schedule_type_for_date() {
    local local_date="$1"
    local day_of_week

    if [[ ! "$local_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        return 64
    fi
    day_of_week="$(TZ="$TZ" date --date="$local_date" +%u)"
    case "$day_of_week" in
        7)
            printf '%s\n' full
            ;;
        [1-5])
            printf '%s\n' diff
            ;;
        6)
            return 1
            ;;
        *)
            return 1
            ;;
    esac
}

schedule_state_matches() {
    local local_date="$1"
    local backup_type="$2"

    [[ -f "$SCHEDULE_STATE_FILE" ]] \
        && [[ "$(<"$SCHEDULE_STATE_FILE")" == "$local_date:$backup_type" ]]
}

record_schedule_submission() {
    local local_date="$1"
    local backup_type="$2"
    local temporary_state

    temporary_state="$(mktemp "${PGBACKREST_REPOSITORY}/.tmdb-pgbackrest-schedule.XXXXXX")"
    printf '%s:%s\n' "$local_date" "$backup_type" >"$temporary_state"
    mv -f "$temporary_state" "$SCHEDULE_STATE_FILE"
}

submit_scheduled_backup() {
    local local_date="$1"
    local backup_type="$2"

    psql_tmdb -qAt \
        --set=backup_type="$backup_type" \
        --set=scheduled_for="$local_date" \
        <<'SQL'
SELECT ops.submit_scheduled_backup(:'backup_type', :'scheduled_for'::date)::text;
SQL
}

run_scheduled_backup() {
    local local_date="$1"
    local backup_type="$2"
    local job_id

    if [[ "$backup_type" == diff ]] && ! has_full_backup; then
        backup_type=full
        log "event=scheduled_backup_upgraded reason=missing_full_backup date=$local_date"
    fi
    if schedule_state_matches "$local_date" "$backup_type"; then
        return 0
    fi
    if queue_support_available; then
        if job_id="$(submit_scheduled_backup "$local_date" "$backup_type")"; then
            record_schedule_submission "$local_date" "$backup_type"
            log "event=scheduled_backup_queued job_id=$job_id type=$backup_type date=$local_date"
            return 0
        fi
        log "event=scheduled_backup_queue_failed type=$backup_type date=$local_date"
        return 1
    fi

    if [[ "$queue_unavailable_logged" == false ]]; then
        # Backup state must always have a durable job/request pair.  The main
        # worker applies migrations shortly after startup; retry on the next
        # scheduler poll instead of making an untracked direct backup.
        log "event=backup_queue_unavailable action=waiting_for_migrations"
        queue_unavailable_logged=true
    fi
    return 1
}

scheduler() {
    local local_date
    local local_hour
    local backup_type

    log "event=scheduler_started timezone=$TZ"
    while true; do
        if queue_support_available; then
            if ! record_backup_heartbeat ready; then
                log "event=component_heartbeat_failed component=backup error_code=database_unavailable"
            fi
            reconcile_terminal_backup_requests || true
            process_one_backup_job || true
        fi
        local_date="$(date +%F)"
        local_hour="$(date +%H)"
        if [[ "$local_hour" == 05 ]] && backup_type="$(schedule_type_for_date "$local_date")"; then
            run_scheduled_backup "$local_date" "$backup_type" || true
            if queue_support_available; then
                process_one_backup_job || true
            fi
        fi
        sleep "$POLL_SECONDS"
    done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
        ensure)
            ensure_pgbackrest
            ;;
        backup)
            [[ $# == 2 ]] || { usage; exit 64; }
            run_backup "$2"
            ;;
        check)
            pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info check
            ;;
        verify)
            pgbackrest --stanza="$PGBACKREST_STANZA" --log-level-console=info verify
            ;;
        info)
            pgbackrest --stanza="$PGBACKREST_STANZA" info
            ;;
        scheduler)
            scheduler
            ;;
        schedule-type)
            [[ $# == 2 ]] || { usage; exit 64; }
            schedule_type_for_date "$2"
            ;;
        *)
            usage
            exit 64
            ;;
    esac
fi
