use std::borrow::Cow;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Extension, Path, RawQuery, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{FromRow, PgPool};
use tmdb_jobs::{JobId, JobStatus};
use tmdb_observability::{CatalogScope, Component, ComponentState, Metrics, QueueState};
use uuid::Uuid;

use crate::{RequestId, app::AdminState, problem};

const DEFAULT_JOB_LIMIT: u16 = 50;
const MAX_JOB_LIMIT: u16 = 100;
const MAX_QUERY_BYTES: usize = 2_048;
const MAX_CURSOR_CHARS: usize = 64;
const MAX_JOB_TYPE_CHARS: usize = 128;
const MAX_IDEMPOTENCY_KEY_CHARS: usize = 128;

/// Sanitized errors returned by the private administrative storage boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdminApiError {
    /// The API request was structurally invalid or exceeded a fixed bound.
    #[error("admin request is invalid")]
    InvalidInput,
    /// The request body exceeded the fixed administrative payload limit.
    #[error("admin request body is too large")]
    PayloadTooLarge,
    /// The request was structurally valid but cannot be performed.
    #[error("admin request is not allowed")]
    Rejected,
    /// No durable operation or job matches the requested identifier.
    #[error("admin job was not found")]
    NotFound,
    /// An idempotency key was previously used with another payload.
    #[error("idempotency key conflicts with an existing request")]
    IdempotencyConflict,
    /// The administrative database dependency could not complete the request.
    #[error("admin dependency is unavailable")]
    Unavailable,
}

/// Private administrative status summary. All timestamps in this API are UTC.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminStatus {
    pub build: AdminBuildStatus,
    pub database: AdminDatabaseStatus,
    pub pools: AdminPoolStatus,
    pub catalog: AdminCatalogCounts,
    pub queues: Vec<AdminQueueSummary>,
    pub ingest: AdminComponentHealth,
    pub media: AdminComponentHealth,
    pub upstream: AdminComponentHealth,
    pub backup: AdminBackupStatus,
}

/// Build and schema identity used to make diagnostics comparable across nodes.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminBuildStatus {
    pub version: String,
    pub schema_revision: Option<String>,
}

/// Bounded live database state. It contains no connection string or credential.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminDatabaseStatus {
    pub reachable: bool,
    pub size_bytes: i64,
    pub active_connections: i64,
}

/// Direct-pool state from the API process.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminPoolStatus {
    pub read_only_size: u32,
    pub read_only_idle: usize,
    pub read_write_size: u32,
    pub read_write_idle: usize,
}

/// Catalog totals by TMDB media namespace.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminCatalogCounts {
    pub movies: i64,
    pub tv: i64,
}

/// One durable queue count group.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminQueueSummary {
    pub job_type: String,
    /// Jobs that can still be claimed or are currently executing.
    pub active: i64,
    /// Retained rows for this job type, including terminal history.
    pub retained: i64,
    pub queued: i64,
    pub running: i64,
    pub retry_wait: i64,
    pub succeeded: i64,
    pub dead_letter: i64,
    pub cancelled: i64,
}

/// Health state intentionally kept independent from secret upstream details.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminComponentHealth {
    pub state: String,
    pub observed_at: Option<DateTime<Utc>>,
}

impl AdminComponentHealth {
    #[must_use]
    pub fn ready() -> Self {
        Self {
            state: "ready".to_owned(),
            observed_at: None,
        }
    }

    #[must_use]
    pub fn unknown() -> Self {
        Self {
            state: "unknown".to_owned(),
            observed_at: None,
        }
    }
}

/// Last durable backup state. Repository paths and command output are never exposed.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminBackupStatus {
    pub state: String,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
}

impl AdminBackupStatus {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            state: "unknown".to_owned(),
            last_success_at: None,
            last_failure_at: None,
        }
    }
}

/// Projects the bounded private status snapshot into Prometheus metric families.
///
/// This intentionally accepts only the fixed status fields returned by the
/// store. It never creates labels from raw job payloads, database errors, or
/// upstream URLs.
pub(crate) fn record_status_metrics(metrics: &Metrics, status: &AdminStatus) {
    metrics.set_catalog_count(CatalogScope::Movies, status.catalog.movies);
    metrics.set_catalog_count(CatalogScope::Tv, status.catalog.tv);

    for queue in &status.queues {
        metrics.set_queue_depth(&queue.job_type, QueueState::Queued, queue.queued);
        metrics.set_queue_depth(&queue.job_type, QueueState::Running, queue.running);
        metrics.set_queue_depth(&queue.job_type, QueueState::RetryWait, queue.retry_wait);
        metrics.set_queue_depth(&queue.job_type, QueueState::DeadLetter, queue.dead_letter);
    }

    metrics.set_component_state(Component::Worker, component_state(&status.ingest.state));
    metrics.set_component_state(Component::Media, component_state(&status.media.state));
    metrics.set_component_state(Component::Upstream, component_state(&status.upstream.state));
    metrics.set_component_state(Component::Backup, component_state(&status.backup.state));
    metrics.set_backup_timestamps(
        status
            .backup
            .last_success_at
            .map(|timestamp| timestamp.timestamp()),
        status
            .backup
            .last_failure_at
            .map(|timestamp| timestamp.timestamp()),
    );
}

fn component_state(value: &str) -> ComponentState {
    match value {
        "ready" => ComponentState::Ready,
        "degraded" => ComponentState::Degraded,
        "failed" => ComponentState::Failed,
        "stale" => ComponentState::Stale,
        _ => ComponentState::Unknown,
    }
}

/// One sanitized durable job record.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminJob {
    pub id: JobId,
    pub job_type: String,
    pub status: JobStatus,
    pub attempts: i32,
    pub max_attempts: i32,
    pub cancellation_requested: bool,
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Immutable job history detail. Request keys and raw job payloads are excluded.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminJobDetail {
    pub job: AdminJob,
    pub events: Vec<AdminJobEvent>,
}

/// One event in an immutable durable job audit trail.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminJobEvent {
    pub id: Uuid,
    pub event_kind: String,
    pub from_status: Option<JobStatus>,
    pub to_status: JobStatus,
    pub request_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Bounded list request parsed from the opaque public cursor contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminJobListRequest {
    pub limit: u16,
    pub cursor: Option<JobId>,
    pub status: Option<JobStatus>,
    pub job_type: Option<String>,
}

/// One bounded page of sanitized jobs.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminJobPage {
    pub data: Vec<AdminJob>,
    pub next_cursor: Option<String>,
}

/// A durable operation accepted by the private administrative API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminOperation {
    Scan {
        mode: AdminScanMode,
        media_types: Vec<AdminMediaType>,
    },
    MediaAudit {
        repair: bool,
    },
    Analyze,
    Backup {
        backup_kind: AdminBackupKind,
    },
}

impl AdminOperation {
    #[must_use]
    pub fn job_type(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "admin.scan",
            Self::MediaAudit { .. } => "admin.media_audit",
            Self::Analyze => "admin.analyze",
            Self::Backup {
                backup_kind: AdminBackupKind::Full,
            } => "database.backup_full",
            Self::Backup {
                backup_kind: AdminBackupKind::Differential,
            } => "database.backup_diff",
        }
    }
}

/// The bounded scan classes that never run implicitly on API restart.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminScanMode {
    FullSweep,
    MissingOnly,
    PruneCleanup,
    DailySync,
}

/// A catalog media namespace targeted by an explicit scan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminMediaType {
    Movie,
    Tv,
}

/// Backup kind supported by the existing `PostgreSQL` container.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminBackupKind {
    Full,
    Differential,
}

/// Durable administrative submission result.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSubmission {
    pub job_id: JobId,
    pub duplicate: bool,
}

/// Durable media-scan mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminMediaScanMode {
    Full,
    Missing,
    Audit,
}

/// Media-worker control action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminMediaWorkerAction {
    Start,
    Pause,
    Resume,
    Cancel,
}

/// Durable media-scan submission result.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMediaScanSubmission {
    pub run_id: Uuid,
    pub job_id: JobId,
    pub duplicate: bool,
}

/// One linked-job status group for a media scan.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMediaScanJobSummary {
    pub phase: String,
    pub queued: i64,
    pub running: i64,
    pub retry_wait: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub cancelled: i64,
}

/// Durable media-scan status.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMediaScanStatus {
    pub run_id: Uuid,
    pub job_id: JobId,
    pub mode: AdminMediaScanMode,
    pub repair: bool,
    pub phase: String,
    pub status: String,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub queued_count: i64,
    pub completed_count: i64,
    pub failed_count: i64,
    pub audited_count: i64,
    pub invalid_count: i64,
    pub repair_queued_count: i64,
    pub error_code: Option<String>,
    pub linked_jobs: Vec<AdminMediaScanJobSummary>,
}

/// Persistent media-worker control state.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMediaWorkerStatus {
    pub state: String,
    pub updated_at: DateTime<Utc>,
}

/// Main ingest-worker control uses the same durable state contract as media.
pub type AdminWorkerAction = AdminMediaWorkerAction;
/// Main ingest-worker status uses the same durable state shape as media.
pub type AdminWorkerStatus = AdminMediaWorkerStatus;

/// Object-safe private administrative storage boundary.
#[async_trait]
pub trait AdminApiStore: Send + Sync + 'static {
    async fn status(&self) -> Result<AdminStatus, AdminApiError>;

    async fn list_jobs(&self, request: AdminJobListRequest) -> Result<AdminJobPage, AdminApiError>;

    async fn get_job(&self, job_id: JobId) -> Result<Option<AdminJobDetail>, AdminApiError>;

    async fn submit(
        &self,
        operation: AdminOperation,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError>;

    async fn start_media_scan(
        &self,
        mode: AdminMediaScanMode,
        repair: bool,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminMediaScanSubmission, AdminApiError>;

    async fn get_media_scan(
        &self,
        run_id: Uuid,
    ) -> Result<Option<AdminMediaScanStatus>, AdminApiError>;

    async fn set_media_worker(
        &self,
        action: AdminMediaWorkerAction,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminMediaWorkerStatus, AdminApiError>;

    async fn media_worker(&self) -> Result<AdminMediaWorkerStatus, AdminApiError>;

    async fn set_worker(
        &self,
        action: AdminWorkerAction,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminWorkerStatus, AdminApiError>;

    async fn worker(&self) -> Result<AdminWorkerStatus, AdminApiError>;

    async fn cancel(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError>;

    async fn retry(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError>;
}

/// PostgreSQL-backed implementation of the private administrative boundary.
///
/// It has one bounded monitor pool for summaries and one bounded submitter
/// pool for durable operations. Public catalog reads use a separate API-reader
/// pool owned by the parent application.
#[derive(Clone, Debug)]
pub struct DatabaseAdminStore {
    read_pool: PgPool,
    write_pool: PgPool,
}

impl DatabaseAdminStore {
    #[must_use]
    pub const fn new(read_pool: PgPool, write_pool: PgPool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }
}

#[async_trait]
impl AdminApiStore for DatabaseAdminStore {
    #[allow(
        clippy::too_many_lines,
        reason = "the bounded status snapshot intentionally gathers each independent operational projection in one read boundary"
    )]
    async fn status(&self) -> Result<AdminStatus, AdminApiError> {
        let status: StatusRow = sqlx::query_as(
            "SELECT
                (SELECT schema_revision
                   FROM ops.readiness) AS schema_revision,
                pg_catalog.pg_database_size(pg_catalog.current_database())::bigint AS size_bytes,
                (SELECT pg_catalog.count(*)
                   FROM pg_catalog.pg_stat_activity
                  WHERE datname = pg_catalog.current_database())::bigint AS active_connections,
                (SELECT pg_catalog.count(*)
                   FROM catalog.titles
                  WHERE active AND media_type = 'movie')::bigint AS movies,
                (SELECT pg_catalog.count(*)
                   FROM catalog.titles
                  WHERE active AND media_type = 'tv')::bigint AS tv",
        )
        .fetch_one(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        let queues = sqlx::query_as::<_, QueueRow>(
            "SELECT job_type,
                    pg_catalog.count(*)::bigint AS retained,
                    pg_catalog.count(*) FILTER (
                        WHERE status IN ('queued', 'running', 'retry_wait')
                    )::bigint AS active,
                    pg_catalog.count(*) FILTER (WHERE status = 'queued')::bigint AS queued,
                    pg_catalog.count(*) FILTER (WHERE status = 'running')::bigint AS running,
                    pg_catalog.count(*) FILTER (WHERE status = 'retry_wait')::bigint AS retry_wait,
                    pg_catalog.count(*) FILTER (WHERE status = 'succeeded')::bigint AS succeeded,
                    pg_catalog.count(*) FILTER (WHERE status = 'dead_letter')::bigint AS dead_letter,
                    pg_catalog.count(*) FILTER (WHERE status = 'cancelled')::bigint AS cancelled
               FROM ops.jobs
              GROUP BY job_type
              ORDER BY job_type
              LIMIT 64",
        )
        .fetch_all(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        let backup: BackupRow = sqlx::query_as(
            "SELECT CASE
                        WHEN pg_catalog.bool_or(status = 'running') THEN 'running'
                        WHEN pg_catalog.bool_or(status = 'queued') THEN 'queued'
                        WHEN pg_catalog.bool_or(status = 'failed') THEN 'failed'
                        WHEN pg_catalog.bool_or(status = 'succeeded') THEN 'ready'
                        ELSE 'unknown'
                    END AS state,
                    pg_catalog.max(finished_at) FILTER (WHERE status = 'succeeded') AS last_success_at,
                    pg_catalog.max(finished_at) FILTER (WHERE status = 'failed') AS last_failure_at
               FROM ops.backup_requests",
        )
        .fetch_one(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        let components = sqlx::query_as::<_, ComponentRow>(
            "SELECT component, state, observed_at
               FROM ops.component_heartbeats
              ORDER BY component",
        )
        .fetch_all(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        let mut component_health = std::collections::BTreeMap::new();
        let now = Utc::now();
        for component in components {
            let stale = now
                .signed_duration_since(component.observed_at)
                .num_seconds()
                > 120;
            component_health.insert(
                component.component,
                AdminComponentHealth {
                    state: if stale {
                        "stale".to_owned()
                    } else {
                        component.state
                    },
                    observed_at: Some(component.observed_at),
                },
            );
        }

        Ok(AdminStatus {
            build: AdminBuildStatus {
                version: env!("CARGO_PKG_VERSION").to_owned(),
                schema_revision: status.schema_revision,
            },
            database: AdminDatabaseStatus {
                reachable: true,
                size_bytes: status.size_bytes,
                active_connections: status.active_connections,
            },
            pools: AdminPoolStatus {
                read_only_size: self.read_pool.size(),
                read_only_idle: self.read_pool.num_idle(),
                read_write_size: self.write_pool.size(),
                read_write_idle: self.write_pool.num_idle(),
            },
            catalog: AdminCatalogCounts {
                movies: status.movies,
                tv: status.tv,
            },
            queues: queues.into_iter().map(QueueRow::into_model).collect(),
            ingest: component_health
                .remove("worker")
                .unwrap_or_else(AdminComponentHealth::unknown),
            media: component_health
                .remove("media")
                .unwrap_or_else(AdminComponentHealth::unknown),
            upstream: component_health
                .remove("upstream")
                .unwrap_or_else(AdminComponentHealth::unknown),
            backup: AdminBackupStatus {
                state: backup.state,
                last_success_at: backup.last_success_at,
                last_failure_at: backup.last_failure_at,
            },
        })
    }

    async fn list_jobs(&self, request: AdminJobListRequest) -> Result<AdminJobPage, AdminApiError> {
        if let Some(cursor) = request.cursor {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM ops.jobs WHERE id = $1)")
                    .bind(cursor.as_uuid())
                    .fetch_one(&self.read_pool)
                    .await
                    .map_err(|error| map_database_error(&error))?;
            if !exists {
                return Err(AdminApiError::InvalidInput);
            }
        }
        let status = request.status.map(job_status_name).map(str::to_owned);
        let rows = sqlx::query_as::<_, JobRow>(
            "WITH page_cursor AS (
                SELECT created_at, id
                  FROM ops.jobs
                 WHERE id = $3
             )
             SELECT id, job_type, status, attempts, max_attempts, cancellation_requested,
                    error_code, created_at, updated_at, finished_at
               FROM ops.jobs AS job
              WHERE ($1::text IS NULL OR job.status = $1)
                AND ($2::text IS NULL OR job.job_type = $2)
                AND (
                    $3::uuid IS NULL
                    OR (job.created_at, job.id) < (
                        SELECT cursor.created_at, cursor.id FROM page_cursor AS cursor
                    )
                )
              ORDER BY job.created_at DESC, job.id DESC
              LIMIT $4",
        )
        .bind(status)
        .bind(request.job_type)
        .bind(request.cursor.map(JobId::as_uuid))
        .bind(i64::from(request.limit) + 1)
        .fetch_all(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        let limit = usize::from(request.limit);
        let has_next = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(JobRow::into_model)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = has_next
            .then(|| data.last().map(|job| job.id.as_uuid().to_string()))
            .flatten();
        Ok(AdminJobPage { data, next_cursor })
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<AdminJobDetail>, AdminApiError> {
        let Some(job) = sqlx::query_as::<_, JobRow>(
            "SELECT id, job_type, status, attempts, max_attempts, cancellation_requested,
                    error_code, created_at, updated_at, finished_at
               FROM ops.jobs
              WHERE id = $1",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        else {
            return Ok(None);
        };
        let events = sqlx::query_as::<_, JobEventRow>(
            "SELECT id, event_kind, from_status, to_status,
                    details ->> 'request_id' AS request_id, created_at
               FROM ops.job_events
              WHERE job_id = $1
              ORDER BY created_at, id
              LIMIT 500",
        )
        .bind(job_id.as_uuid())
        .fetch_all(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .into_iter()
        .map(JobEventRow::into_model)
        .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(AdminJobDetail {
            job: job.into_model()?,
            events,
        }))
    }

    async fn submit(
        &self,
        operation: AdminOperation,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        let (operation, payload) = operation_payload(operation);
        let request_id = parse_request_id(request_id)?;
        let payload = serde_json::to_string(&payload).map_err(|_| AdminApiError::Unavailable)?;
        let submission: SubmissionRow = sqlx::query_as(
            "SELECT job_id, was_duplicate
               FROM ops.submit_admin_job($1, $2, $3, $4, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(operation)
        .bind(payload)
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        Ok(submission.into_model())
    }

    async fn start_media_scan(
        &self,
        mode: AdminMediaScanMode,
        repair: bool,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminMediaScanSubmission, AdminApiError> {
        let request_id = parse_request_id(request_id)?;
        let payload = serde_json::to_string(&json!({
            "mode": mode,
            "repair": repair,
        }))
        .map_err(|_| AdminApiError::Unavailable)?;
        let submission: MediaScanSubmissionRow = sqlx::query_as(
            "SELECT job_id, run_id, was_duplicate
               FROM ops.submit_media_scan($1, $2, $3, $4)",
        )
        .bind(Uuid::now_v7())
        .bind(payload)
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        Ok(submission.into_model())
    }

    async fn get_media_scan(
        &self,
        run_id: Uuid,
    ) -> Result<Option<AdminMediaScanStatus>, AdminApiError> {
        let Some(run) = sqlx::query_as::<_, MediaScanRunRow>(
            "SELECT id, job_id, mode, repair, phase, status, requested_at, started_at,
                    finished_at, queued_count, completed_count, failed_count, audited_count,
                    invalid_count, repair_queued_count, error_code
               FROM ops.media_scan_runs
              WHERE id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        else {
            return Ok(None);
        };
        let linked_jobs = sqlx::query_as::<_, MediaScanJobSummaryRow>(
            "SELECT link.phase,
                    count(*) FILTER (WHERE job.status = 'queued')::bigint AS queued,
                    count(*) FILTER (WHERE job.status = 'running')::bigint AS running,
                    count(*) FILTER (WHERE job.status = 'retry_wait')::bigint AS retry_wait,
                    count(*) FILTER (WHERE job.status = 'succeeded')::bigint AS succeeded,
                    count(*) FILTER (WHERE job.status = 'dead_letter')::bigint AS failed,
                    count(*) FILTER (WHERE job.status = 'cancelled')::bigint AS cancelled
               FROM ops.media_scan_job_links AS link
               JOIN ops.jobs AS job ON job.id = link.job_id
              WHERE link.run_id = $1
              GROUP BY link.phase
              ORDER BY link.phase",
        )
        .bind(run_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .into_iter()
        .map(MediaScanJobSummaryRow::into_model)
        .collect();
        Ok(Some(run.into_model(linked_jobs)?))
    }

    async fn set_media_worker(
        &self,
        action: AdminMediaWorkerAction,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        let request_id = parse_request_id(request_id)?;
        let row: MediaWorkerStateRow = sqlx::query_as(
            "SELECT state
               FROM ops.set_worker_state('media', $1, $2, $3)",
        )
        .bind(media_worker_action_name(action))
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        self.media_worker().await.map(|mut status| {
            status.state = row.state;
            status
        })
    }

    async fn media_worker(&self) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        sqlx::query_as::<_, MediaWorkerStatusRow>(
            "SELECT state, updated_at
               FROM ops.worker_control
              WHERE worker_kind = 'media'",
        )
        .fetch_optional(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .map(MediaWorkerStatusRow::into_model)
        .ok_or(AdminApiError::Unavailable)
    }

    async fn set_worker(
        &self,
        action: AdminWorkerAction,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminWorkerStatus, AdminApiError> {
        let request_id = parse_request_id(request_id)?;
        let row: MediaWorkerStateRow = sqlx::query_as(
            "SELECT state
               FROM ops.set_worker_state('ingest', $1, $2, $3)",
        )
        .bind(media_worker_action_name(action))
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        self.worker().await.map(|mut status| {
            status.state = row.state;
            status
        })
    }

    async fn worker(&self) -> Result<AdminWorkerStatus, AdminApiError> {
        sqlx::query_as::<_, MediaWorkerStatusRow>(
            "SELECT state, updated_at
               FROM ops.worker_control
              WHERE worker_kind = 'ingest'",
        )
        .fetch_optional(&self.read_pool)
        .await
        .map_err(|error| map_database_error(&error))?
        .map(MediaWorkerStatusRow::into_model)
        .ok_or(AdminApiError::Unavailable)
    }

    async fn cancel(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        let request_id = parse_request_id(request_id)?;
        let submission: SubmissionRow = sqlx::query_as(
            "SELECT job_id, was_duplicate
               FROM ops.request_admin_job_cancel($1, $2, $3)",
        )
        .bind(job_id.as_uuid())
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        Ok(submission.into_model())
    }

    async fn retry(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        let request_id = parse_request_id(request_id)?;
        let submission: SubmissionRow = sqlx::query_as(
            "SELECT job_id, was_duplicate
               FROM ops.retry_admin_job($1, $2, $3, $4)",
        )
        .bind(Uuid::now_v7())
        .bind(job_id.as_uuid())
        .bind(idempotency_key)
        .bind(request_id)
        .fetch_one(&self.write_pool)
        .await
        .map_err(|error| map_database_error(&error))?;
        Ok(submission.into_model())
    }
}

#[derive(FromRow)]
struct StatusRow {
    schema_revision: Option<String>,
    size_bytes: i64,
    active_connections: i64,
    movies: i64,
    tv: i64,
}

#[derive(FromRow)]
struct QueueRow {
    job_type: String,
    retained: i64,
    active: i64,
    queued: i64,
    running: i64,
    retry_wait: i64,
    succeeded: i64,
    dead_letter: i64,
    cancelled: i64,
}

impl QueueRow {
    fn into_model(self) -> AdminQueueSummary {
        AdminQueueSummary {
            job_type: self.job_type,
            active: self.active,
            retained: self.retained,
            queued: self.queued,
            running: self.running,
            retry_wait: self.retry_wait,
            succeeded: self.succeeded,
            dead_letter: self.dead_letter,
            cancelled: self.cancelled,
        }
    }
}

#[derive(FromRow)]
struct BackupRow {
    state: String,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
}

#[derive(FromRow)]
struct ComponentRow {
    component: String,
    state: String,
    observed_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct JobRow {
    id: Uuid,
    job_type: String,
    status: String,
    attempts: i32,
    max_attempts: i32,
    cancellation_requested: bool,
    error_code: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn into_model(self) -> Result<AdminJob, AdminApiError> {
        Ok(AdminJob {
            id: self.id.into(),
            job_type: self.job_type,
            status: parse_job_status(&self.status)?,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            cancellation_requested: self.cancellation_requested,
            error_code: self.error_code,
            created_at: self.created_at,
            updated_at: self.updated_at,
            finished_at: self.finished_at,
        })
    }
}

#[derive(FromRow)]
struct JobEventRow {
    id: Uuid,
    event_kind: String,
    from_status: Option<String>,
    to_status: String,
    request_id: Option<String>,
    created_at: DateTime<Utc>,
}

impl JobEventRow {
    fn into_model(self) -> Result<AdminJobEvent, AdminApiError> {
        Ok(AdminJobEvent {
            id: self.id,
            event_kind: self.event_kind,
            from_status: self
                .from_status
                .map(|status| parse_job_status(&status))
                .transpose()?,
            to_status: parse_job_status(&self.to_status)?,
            request_id: self.request_id,
            created_at: self.created_at,
        })
    }
}

#[derive(FromRow)]
struct SubmissionRow {
    job_id: Uuid,
    was_duplicate: bool,
}

impl SubmissionRow {
    fn into_model(self) -> AdminSubmission {
        AdminSubmission {
            job_id: JobId::from(self.job_id),
            duplicate: self.was_duplicate,
        }
    }
}

#[derive(FromRow)]
struct MediaScanSubmissionRow {
    job_id: Uuid,
    run_id: Uuid,
    was_duplicate: bool,
}

impl MediaScanSubmissionRow {
    fn into_model(self) -> AdminMediaScanSubmission {
        AdminMediaScanSubmission {
            run_id: self.run_id,
            job_id: JobId::from(self.job_id),
            duplicate: self.was_duplicate,
        }
    }
}

#[derive(FromRow)]
struct MediaScanRunRow {
    id: Uuid,
    job_id: Uuid,
    mode: String,
    repair: bool,
    phase: String,
    status: String,
    requested_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    queued_count: i64,
    completed_count: i64,
    failed_count: i64,
    audited_count: i64,
    invalid_count: i64,
    repair_queued_count: i64,
    error_code: Option<String>,
}

impl MediaScanRunRow {
    fn into_model(
        self,
        linked_jobs: Vec<AdminMediaScanJobSummary>,
    ) -> Result<AdminMediaScanStatus, AdminApiError> {
        Ok(AdminMediaScanStatus {
            run_id: self.id,
            job_id: JobId::from(self.job_id),
            mode: parse_media_scan_mode(&self.mode)?,
            repair: self.repair,
            phase: self.phase,
            status: self.status,
            requested_at: self.requested_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            queued_count: self.queued_count,
            completed_count: self.completed_count,
            failed_count: self.failed_count,
            audited_count: self.audited_count,
            invalid_count: self.invalid_count,
            repair_queued_count: self.repair_queued_count,
            error_code: self.error_code,
            linked_jobs,
        })
    }
}

#[derive(FromRow)]
struct MediaScanJobSummaryRow {
    phase: String,
    queued: i64,
    running: i64,
    retry_wait: i64,
    succeeded: i64,
    failed: i64,
    cancelled: i64,
}

impl MediaScanJobSummaryRow {
    fn into_model(self) -> AdminMediaScanJobSummary {
        AdminMediaScanJobSummary {
            phase: self.phase,
            queued: self.queued,
            running: self.running,
            retry_wait: self.retry_wait,
            succeeded: self.succeeded,
            failed: self.failed,
            cancelled: self.cancelled,
        }
    }
}

#[derive(FromRow)]
struct MediaWorkerStateRow {
    state: String,
}

#[derive(FromRow)]
struct MediaWorkerStatusRow {
    state: String,
    updated_at: DateTime<Utc>,
}

impl MediaWorkerStatusRow {
    fn into_model(self) -> AdminMediaWorkerStatus {
        AdminMediaWorkerStatus {
            state: self.state,
            updated_at: self.updated_at,
        }
    }
}

fn operation_payload(operation: AdminOperation) -> (&'static str, serde_json::Value) {
    match operation {
        AdminOperation::Scan { mode, media_types } => (
            "admin.scan",
            json!({"mode": mode, "mediaTypes": media_types}),
        ),
        AdminOperation::MediaAudit { repair } => ("admin.media_audit", json!({"repair": repair})),
        AdminOperation::Analyze => ("admin.analyze", json!({})),
        AdminOperation::Backup {
            backup_kind: AdminBackupKind::Full,
        } => ("database.backup_full", json!({"type": "full"})),
        AdminOperation::Backup {
            backup_kind: AdminBackupKind::Differential,
        } => ("database.backup_diff", json!({"type": "diff"})),
    }
}

fn parse_request_id(request_id: &str) -> Result<Uuid, AdminApiError> {
    Uuid::parse_str(request_id).map_err(|_| AdminApiError::Unavailable)
}

fn parse_media_scan_mode(value: &str) -> Result<AdminMediaScanMode, AdminApiError> {
    match value {
        "full" => Ok(AdminMediaScanMode::Full),
        "missing" => Ok(AdminMediaScanMode::Missing),
        "audit" => Ok(AdminMediaScanMode::Audit),
        _ => Err(AdminApiError::Unavailable),
    }
}

const fn media_worker_action_name(action: AdminMediaWorkerAction) -> &'static str {
    match action {
        AdminMediaWorkerAction::Start => "start",
        AdminMediaWorkerAction::Pause => "pause",
        AdminMediaWorkerAction::Resume => "resume",
        AdminMediaWorkerAction::Cancel => "cancel",
    }
}

fn parse_job_status(value: &str) -> Result<JobStatus, AdminApiError> {
    match value {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "retry_wait" => Ok(JobStatus::RetryWait),
        "succeeded" => Ok(JobStatus::Succeeded),
        "dead_letter" => Ok(JobStatus::DeadLetter),
        "cancelled" => Ok(JobStatus::Cancelled),
        _ => Err(AdminApiError::Unavailable),
    }
}

const fn job_status_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::RetryWait => "retry_wait",
        JobStatus::Succeeded => "succeeded",
        JobStatus::DeadLetter => "dead_letter",
        JobStatus::Cancelled => "cancelled",
    }
}

fn map_database_error(error: &sqlx::Error) -> AdminApiError {
    match error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(Cow::into_owned)
        .as_deref()
    {
        Some("P0002") => AdminApiError::NotFound,
        Some("P0003") => AdminApiError::IdempotencyConflict,
        Some("22023" | "P0001") => AdminApiError::Rejected,
        _ => AdminApiError::Unavailable,
    }
}

/// Adds administrative operations to the already-private admin listener.
pub(crate) fn register_routes(router: Router<AdminState>) -> Router<AdminState> {
    router
        .route("/admin/v1/openapi.json", get(openapi))
        .route("/admin/v1/status", get(status))
        .route("/admin/v1/jobs", get(list_jobs))
        .route("/admin/v1/jobs/{job_id}", get(get_job))
        .route("/admin/v1/scans", post(start_scan))
        .route("/admin/v1/media/scans", post(start_media_scan))
        .route("/admin/v1/media/scans/{run_id}", get(get_media_scan))
        .route(
            "/admin/v1/media/worker",
            get(get_media_worker).post(set_media_worker),
        )
        .route("/admin/v1/worker", get(get_worker).post(set_worker))
        .route("/admin/v1/jobs/{job_id}/cancel", post(cancel_job))
        .route("/admin/v1/jobs/{job_id}/retry", post(retry_job))
        .route("/admin/v1/media/audits", post(start_media_audit))
        .route("/admin/v1/maintenance/analyze", post(start_analyze))
        .route("/admin/v1/backups", get(list_backups).post(start_backup))
}

#[allow(
    clippy::too_many_lines,
    reason = "the private OpenAPI document is intentionally a single auditable static route declaration"
)]
async fn openapi() -> Json<serde_json::Value> {
    Json(json!({
        "openapi": "3.1.0",
        "info": {
            "title": "TMDB Mirror private operations API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Available only through the private administrative listener. Every state-changing operation is an asynchronous durable job and requires an Idempotency-Key header."
        },
        "security": [{"AdminApiKey": []}, {"AdminBearer": []}],
        "paths": {
            "/admin/v1/openapi.json": {
                "get": {"summary": "Read this private API document", "responses": {"200": {"description": "OpenAPI document"}}}
            },
            "/admin/v1/status": {
                "get": {"summary": "Read bounded operational status", "responses": {"200": {"description": "Build, database, pools, catalog, queue, component, and backup state"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "503": {"$ref": "#/components/responses/Unavailable"}}}
            },
            "/admin/v1/jobs": {
                "get": {
                    "summary": "List durable jobs with bounded filters",
                    "parameters": [
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50}},
                        {"name": "cursor", "in": "query", "description": "Opaque cursor returned by the previous page", "schema": {"type": "string", "maxLength": 64}},
                        {"name": "status", "in": "query", "schema": {"type": "string"}},
                        {"name": "jobType", "in": "query", "schema": {"type": "string", "maxLength": 128}}
                    ],
                    "responses": {"200": {"description": "Sanitized job page"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}}
                }
            },
            "/admin/v1/jobs/{job_id}": {
                "get": {
                    "summary": "Read one durable job and its immutable audit events",
                    "parameters": [{"$ref": "#/components/parameters/JobId"}],
                    "responses": {"200": {"description": "Sanitized job detail"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "404": {"$ref": "#/components/responses/NotFound"}}
                }
            },
            "/admin/v1/scans": {
                "post": {
                    "summary": "Queue an explicit full_sweep, missing_only, prune_cleanup, or daily_sync catalog scan; never runs automatically on restart",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ScanRequest"}}}},
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/media/scans": {
                "post": {
                    "summary": "Queue a durable full, missing, or audit media scan",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MediaScanRequest"}}}},
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/media/scans/{run_id}": {
                "get": {
                    "summary": "Read one durable media-scan run",
                    "parameters": [{"$ref": "#/components/parameters/RunId"}],
                    "responses": {"200": {"description": "Media-scan phase, counters, and linked jobs"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "404": {"$ref": "#/components/responses/NotFound"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/media/worker": {
                "get": {
                    "summary": "Read persistent media-worker control state",
                    "responses": {"200": {"description": "Running, paused, or stopped"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                },
                "post": {
                    "summary": "Start, pause, resume, or cancel media work",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MediaWorkerRequest"}}}},
                    "responses": {"200": {"description": "Updated persistent worker state"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/worker": {
                "get": {
                    "summary": "Read persistent main ingest-worker control state",
                    "responses": {"200": {"description": "Running, paused, or stopped"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                },
                "post": {
                    "summary": "Start, pause, resume, or cancel main ingest work",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MediaWorkerRequest"}}}},
                    "responses": {"200": {"description": "Updated persistent worker state"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/jobs/{job_id}/cancel": {
                "post": {
                    "summary": "Request cancellation of an eligible durable job",
                    "parameters": [{"$ref": "#/components/parameters/JobId"}, {"$ref": "#/components/parameters/IdempotencyKey"}],
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "404": {"$ref": "#/components/responses/NotFound"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}}
                }
            },
            "/admin/v1/jobs/{job_id}/retry": {
                "post": {
                    "summary": "Queue an auditable retry job without changing historical work",
                    "parameters": [{"$ref": "#/components/parameters/JobId"}, {"$ref": "#/components/parameters/IdempotencyKey"}],
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "404": {"$ref": "#/components/responses/NotFound"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}}
                }
            },
            "/admin/v1/media/audits": {
                "post": {
                    "summary": "Queue a non-destructive local-media audit",
                    "description": "When repair is true, verified replacement downloads may be queued; no media is deleted.",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MediaAuditRequest"}}}},
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/maintenance/analyze": {
                "post": {
                    "summary": "Queue fixed allowlisted catalog statistics maintenance",
                    "description": "No raw SQL, shell, arbitrary reindex, or destructive maintenance is accepted.",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            },
            "/admin/v1/backups": {
                "get": {"summary": "Read backup health and last durable backup state", "responses": {"200": {"description": "Backup summary"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "503": {"$ref": "#/components/responses/Unavailable"}}},
                "post": {
                    "summary": "Queue a manual full or differential pgBackRest backup",
                    "description": "Restore is intentionally an offline recovery procedure, never an HTTP action.",
                    "parameters": [{"$ref": "#/components/parameters/IdempotencyKey"}],
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/BackupRequest"}}}},
                    "responses": {"202": {"$ref": "#/components/responses/Accepted"}, "400": {"$ref": "#/components/responses/BadRequest"}, "401": {"$ref": "#/components/responses/Unauthorized"}, "409": {"$ref": "#/components/responses/IdempotencyConflict"}, "422": {"$ref": "#/components/responses/Rejected"}, "503": {"$ref": "#/components/responses/Unavailable"}}
                }
            }
        },
        "components": {
            "securitySchemes": {
                "AdminApiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"},
                "AdminBearer": {"type": "http", "scheme": "bearer"}
            },
            "parameters": {
                "IdempotencyKey": {"name": "Idempotency-Key", "in": "header", "required": true, "schema": {"type": "string", "minLength": 1, "maxLength": 128}},
                "JobId": {"name": "job_id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}},
                "RunId": {"name": "run_id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}}
            },
            "schemas": {
                "ScanRequest": {"type": "object", "additionalProperties": false, "required": ["mode", "mediaTypes"], "properties": {"mode": {"type": "string", "enum": ["full_sweep", "missing_only", "prune_cleanup", "daily_sync"]}, "mediaTypes": {"type": "array", "minItems": 1, "maxItems": 2, "uniqueItems": true, "items": {"type": "string", "enum": ["movie", "tv"]}}}},
                "MediaScanRequest": {"type": "object", "additionalProperties": false, "required": ["mode"], "properties": {"mode": {"type": "string", "enum": ["full", "missing", "audit"]}, "repair": {"type": "boolean", "default": false}}},
                "MediaWorkerRequest": {"type": "object", "additionalProperties": false, "required": ["action"], "properties": {"action": {"type": "string", "enum": ["start", "pause", "resume", "cancel"]}}},
                "MediaAuditRequest": {"type": "object", "additionalProperties": false, "properties": {"repair": {"type": "boolean", "default": false}}},
                "BackupRequest": {"type": "object", "additionalProperties": false, "required": ["type"], "properties": {"type": {"type": "string", "enum": ["full", "differential"]}}}
            },
            "responses": {
                "Accepted": {"description": "Durable operation accepted for asynchronous execution"},
                "BadRequest": {"description": "Invalid bounded request"},
                "Unauthorized": {"description": "Existing admin key was missing or invalid"},
                "NotFound": {"description": "Durable job was not found"},
                "IdempotencyConflict": {"description": "Idempotency key was already used with a different payload"},
                "Rejected": {"description": "Valid request cannot be performed"},
                "Unavailable": {"description": "Administrative database dependency is unavailable"}
            }
        }
    }))
}

async fn status(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.status().await {
        Ok(status) => Json(Data { data: status }).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn list_jobs(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request = match parse_job_list(raw_query.as_deref()) {
        Ok(request) => request,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.list_jobs(request).await {
        Ok(page) => Json(page).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn get_job(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    Path(job_id): Path<String>,
) -> Response {
    let job_id = match parse_job_id(&job_id) {
        Ok(job_id) => job_id,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.get_job(job_id).await {
        Ok(Some(job)) => Json(Data { data: job }).into_response(),
        Ok(None) => failure(AdminApiError::NotFound, &request_id.0),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn start_scan(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<ScanRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    if payload.media_types.is_empty()
        || payload.media_types.len() > 2
        || has_duplicates(&payload.media_types)
    {
        return failure(AdminApiError::Rejected, &request_id.0);
    }
    submit(
        state,
        AdminOperation::Scan {
            mode: payload.mode,
            media_types: payload.media_types,
        },
        idempotency_key,
        &request_id.0,
    )
    .await
}

async fn start_media_scan(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<MediaScanRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    if payload.repair && payload.mode != AdminMediaScanMode::Audit {
        return failure(AdminApiError::Rejected, &request_id.0);
    }
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store
        .start_media_scan(payload.mode, payload.repair, idempotency_key, &request_id.0)
        .await
    {
        Ok(submission) => accepted_media_scan(submission),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn get_media_scan(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = Uuid::parse_str(&run_id) else {
        return failure(AdminApiError::InvalidInput, &request_id.0);
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.get_media_scan(run_id).await {
        Ok(Some(status)) => Json(Data { data: status }).into_response(),
        Ok(None) => failure(AdminApiError::NotFound, &request_id.0),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn set_media_worker(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<MediaWorkerRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store
        .set_media_worker(payload.action, idempotency_key, &request_id.0)
        .await
    {
        Ok(status) => Json(Data { data: status }).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn get_media_worker(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.media_worker().await {
        Ok(status) => Json(Data { data: status }).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn set_worker(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<MediaWorkerRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store
        .set_worker(payload.action, idempotency_key, &request_id.0)
        .await
    {
        Ok(status) => Json(Data { data: status }).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn get_worker(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.worker().await {
        Ok(status) => Json(Data { data: status }).into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn start_media_audit(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<MediaAuditRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    submit(
        state,
        AdminOperation::MediaAudit {
            repair: payload.repair,
        },
        idempotency_key,
        &request_id.0,
    )
    .await
}

async fn start_analyze(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    submit(
        state,
        AdminOperation::Analyze,
        idempotency_key,
        &request_id.0,
    )
    .await
}

async fn list_backups(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.status().await {
        Ok(status) => Json(Data {
            data: status.backup,
        })
        .into_response(),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn start_backup(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Json<BackupRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let payload = match bounded_json(payload) {
        Ok(payload) => payload,
        Err(error) => return failure(error, &request_id.0),
    };
    submit(
        state,
        AdminOperation::Backup {
            backup_kind: payload.backup_kind,
        },
        idempotency_key,
        &request_id.0,
    )
    .await
}

async fn cancel_job(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let job_id = match parse_job_id(&job_id) {
        Ok(job_id) => job_id,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.cancel(job_id, idempotency_key, &request_id.0).await {
        Ok(submission) => accepted(submission),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn retry_job(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Response {
    let idempotency_key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return failure(error, &request_id.0),
    };
    let job_id = match parse_job_id(&job_id) {
        Ok(job_id) => job_id,
        Err(error) => return failure(error, &request_id.0),
    };
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, &request_id.0);
    };
    match store.retry(job_id, idempotency_key, &request_id.0).await {
        Ok(submission) => accepted(submission),
        Err(error) => failure(error, &request_id.0),
    }
}

async fn submit(
    state: AdminState,
    operation: AdminOperation,
    idempotency_key: &str,
    request_id: &str,
) -> Response {
    let Some(store) = state.operations.as_deref() else {
        return failure(AdminApiError::Unavailable, request_id);
    };
    match store.submit(operation, idempotency_key, request_id).await {
        Ok(submission) => accepted(submission),
        Err(error) => failure(error, request_id),
    }
}

fn accepted(submission: AdminSubmission) -> Response {
    (StatusCode::ACCEPTED, Json(Data { data: submission })).into_response()
}

fn accepted_media_scan(submission: AdminMediaScanSubmission) -> Response {
    (StatusCode::ACCEPTED, Json(Data { data: submission })).into_response()
}

fn failure(error: AdminApiError, request_id: &str) -> Response {
    match error {
        AdminApiError::InvalidInput => problem::response(
            StatusCode::BAD_REQUEST,
            "Bad Request",
            "The administrative request is invalid.",
            request_id,
        ),
        AdminApiError::PayloadTooLarge => problem::response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Payload Too Large",
            "The administrative request body exceeds the maximum size.",
            request_id,
        ),
        AdminApiError::Rejected => problem::response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Unprocessable Entity",
            "The administrative request cannot be performed.",
            request_id,
        ),
        AdminApiError::NotFound => problem::response(
            StatusCode::NOT_FOUND,
            "Not Found",
            "The requested administrative job was not found.",
            request_id,
        ),
        AdminApiError::IdempotencyConflict => problem::response(
            StatusCode::CONFLICT,
            "Conflict",
            "The idempotency key was already used with a different request.",
            request_id,
        ),
        AdminApiError::Unavailable => problem::response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service Unavailable",
            "The administrative dependency is unavailable.",
            request_id,
        ),
    }
}

/// Maps axum's structured JSON rejection to the bounded public admin error
/// vocabulary. Only the body-limit rejection is surfaced as 413; malformed
/// JSON, missing content type, and schema mismatches remain 400.
fn bounded_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, AdminApiError> {
    match payload {
        Ok(Json(value)) => Ok(value),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(AdminApiError::PayloadTooLarge)
        }
        Err(_) => Err(AdminApiError::InvalidInput),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Data<T> {
    data: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScanRequest {
    mode: AdminScanMode,
    media_types: Vec<AdminMediaType>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaAuditRequest {
    #[serde(default)]
    repair: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaScanRequest {
    mode: AdminMediaScanMode,
    #[serde(default)]
    repair: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaWorkerRequest {
    action: AdminMediaWorkerAction,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRequest {
    #[serde(rename = "type")]
    backup_kind: AdminBackupKind,
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, AdminApiError> {
    let value = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or(AdminApiError::InvalidInput)?;
    if value.is_empty()
        || value.len() > MAX_IDEMPOTENCY_KEY_CHARS
        || !value.is_ascii()
        || value.chars().any(|character| character.is_ascii_control())
    {
        return Err(AdminApiError::InvalidInput);
    }
    Ok(value)
}

fn parse_job_id(value: &str) -> Result<JobId, AdminApiError> {
    Uuid::parse_str(value)
        .map(JobId::from)
        .map_err(|_| AdminApiError::InvalidInput)
}

fn parse_job_list(raw_query: Option<&str>) -> Result<AdminJobListRequest, AdminApiError> {
    let mut request = AdminJobListRequest {
        limit: DEFAULT_JOB_LIMIT,
        cursor: None,
        status: None,
        job_type: None,
    };
    let Some(raw_query) = raw_query else {
        return Ok(request);
    };
    if raw_query.len() > MAX_QUERY_BYTES {
        return Err(AdminApiError::InvalidInput);
    }
    let mut seen_limit = false;
    let mut seen_cursor = false;
    let mut seen_status = false;
    let mut seen_type = false;
    for component in raw_query
        .split('&')
        .filter(|component| !component.is_empty())
    {
        let (raw_key, raw_value) = component.split_once('=').unwrap_or((component, ""));
        let key = decode_query_component(raw_key)?;
        let value = decode_query_component(raw_value)?;
        match key.as_str() {
            "limit" if !seen_limit => {
                seen_limit = true;
                request.limit = value
                    .parse::<u16>()
                    .ok()
                    .filter(|limit| (1..=MAX_JOB_LIMIT).contains(limit))
                    .ok_or(AdminApiError::InvalidInput)?;
            }
            "cursor" if !seen_cursor => {
                seen_cursor = true;
                if value.is_empty() || value.len() > MAX_CURSOR_CHARS {
                    return Err(AdminApiError::InvalidInput);
                }
                request.cursor = Some(parse_job_id(&value)?);
            }
            "status" if !seen_status => {
                seen_status = true;
                request.status = Some(parse_status(&value)?);
            }
            "jobType" if !seen_type => {
                seen_type = true;
                if value.is_empty()
                    || value.len() > MAX_JOB_TYPE_CHARS
                    || value.chars().any(char::is_control)
                {
                    return Err(AdminApiError::InvalidInput);
                }
                request.job_type = Some(value);
            }
            _ => return Err(AdminApiError::InvalidInput),
        }
    }
    Ok(request)
}

fn parse_status(value: &str) -> Result<JobStatus, AdminApiError> {
    match value {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "retry_wait" => Ok(JobStatus::RetryWait),
        "succeeded" => Ok(JobStatus::Succeeded),
        "dead_letter" => Ok(JobStatus::DeadLetter),
        "cancelled" => Ok(JobStatus::Cancelled),
        _ => Err(AdminApiError::InvalidInput),
    }
}

fn decode_query_component(value: &str) -> Result<String, AdminApiError> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut characters = value.as_bytes().iter().copied();
    while let Some(byte) = characters.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = characters
                    .next()
                    .and_then(hex_value)
                    .ok_or(AdminApiError::InvalidInput)?;
                let low = characters
                    .next()
                    .and_then(hex_value)
                    .ok_or(AdminApiError::InvalidInput)?;
                bytes.push((high << 4) | low);
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8(bytes).map_err(|_| AdminApiError::InvalidInput)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn has_duplicates(values: &[AdminMediaType]) -> bool {
    values.windows(2).any(|pair| pair[0] == pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_list_parser_rejects_duplicate_and_unknown_parameters() {
        assert!(parse_job_list(Some("limit=1&limit=2")).is_err());
        assert!(parse_job_list(Some("unexpected=value")).is_err());
    }

    #[test]
    fn job_list_parser_accepts_the_documented_statuses() {
        for status in [
            "queued",
            "running",
            "retry_wait",
            "succeeded",
            "dead_letter",
            "cancelled",
        ] {
            assert!(parse_status(status).is_ok(), "{status}");
        }
    }

    #[test]
    fn job_list_parser_accepts_the_documented_job_type_parameter() {
        assert!(matches!(
            parse_job_list(Some("limit=10&status=succeeded&jobType=admin.analyze")),
            Ok(AdminJobListRequest {
                limit: 10,
                cursor: None,
                status: Some(JobStatus::Succeeded),
                job_type: Some(job_type),
            }) if job_type == "admin.analyze"
        ));
    }
}
