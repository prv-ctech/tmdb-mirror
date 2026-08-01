-- Durable scheduler checkpoints make restart/re-election idempotent.  The
-- job table still owns execution leases; this table only records that a
-- schedule slot has been expanded into jobs.
CREATE TABLE ops.scheduler_runs (
    schedule_key text NOT NULL,
    run_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (schedule_key, run_key),
    CONSTRAINT scheduler_runs_schedule_check CHECK (btrim(schedule_key) <> ''),
    CONSTRAINT scheduler_runs_key_check CHECK (btrim(run_key) <> '')
);

CREATE INDEX scheduler_runs_created_idx
    ON ops.scheduler_runs (created_at);

ALTER TABLE ops.scheduler_runs OWNER TO migrator;
GRANT SELECT, INSERT, DELETE ON ops.scheduler_runs TO ingest_writer;
