use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{Days, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tmdb_domain::MediaType;
use tmdb_jobs::{ClaimedJob, JobError, JobExecutionError, JobExecutor, JobRepository, NewJob};
use tmdb_upstream::{
    DailyExportParser, MAX_DAILY_EXPORT_BYTES, TmdbClient, TmdbClientError, TmdbTrendingItem,
};

#[path = "catalog_locks.rs"]
mod catalog_locks;
#[path = "catalog_write.rs"]
mod catalog_write;

/// Versioned durable job names accepted by the ingestion worker.
pub const REFRESH_MOVIE_JOB: &str = "ingest.refresh_movie";
/// Versioned durable job names accepted by the ingestion worker.
pub const REFRESH_TV_JOB: &str = "ingest.refresh_tv";
/// Refresh one TV season and its episode list from TMDB.
pub const REFRESH_SEASON_JOB: &str = "ingest.refresh_season";
/// Versioned durable job names accepted by the ingestion worker.
pub const CHANGES_SYNC_JOB: &str = "ingest.changes_sync";
/// Versioned durable job names accepted by the ingestion worker.
pub const DAILY_EXPORT_JOB: &str = "ingest.daily_export";
/// Refresh a typed TMDB trending window into the durable ranking table.
pub const TRENDING_REFRESH_JOB: &str = "ingest.trending";
/// Explicit administrative catalog scan coordinator. It is never enqueued by restart.
pub const ADMIN_SCAN_JOB: &str = "admin.scan";
/// Fixed allowlisted catalog statistics maintenance.
pub const ADMIN_ANALYZE_JOB: &str = "admin.analyze";
/// Current payload version for all ingestion jobs.
pub const INGEST_PAYLOAD_VERSION: i32 = 1;
const INGEST_JOB_TYPES: &[&str] = &[
    REFRESH_MOVIE_JOB,
    REFRESH_TV_JOB,
    REFRESH_SEASON_JOB,
    CHANGES_SYNC_JOB,
    DAILY_EXPORT_JOB,
    TRENDING_REFRESH_JOB,
    ADMIN_SCAN_JOB,
    ADMIN_ANALYZE_JOB,
];
const DAILY_EXPORT_REFRESH_PRIORITY: i16 = -100;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RefreshPayload {
    tmdb_id: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RefreshSeasonPayload {
    tv_id: u32,
    season_number: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ChangesPayload {
    media_type: MediaType,
    page: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DailyExportPayload {
    media_type: MediaType,
    url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct TrendingPayload {
    media_type: MediaType,
    trend_window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminScanMode {
    Full,
    Missing,
    Changes,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdminScanPayload {
    mode: AdminScanMode,
    media_types: Vec<MediaType>,
}

/// A validated, idempotent ingestion job payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestJob {
    /// Refresh one movie from TMDB.
    RefreshMovie { tmdb_id: u32 },
    /// Refresh one television series from TMDB.
    RefreshTv { tmdb_id: u32 },
    /// Fetch one TV season and its episodes.
    RefreshSeason { tv_id: u32, season_number: u16 },
    /// Fetch one page of media changes.
    ChangesSync { media_type: MediaType, page: u32 },
    /// Fetch and parse one daily ID export.
    DailyExport { media_type: MediaType, url: String },
    /// Refresh a named trending window for a single public media namespace.
    Trending {
        media_type: MediaType,
        trend_window: String,
    },
    /// Expand one explicit operational scan into safely bounded ingest jobs.
    AdminScan {
        mode: AdminScanMode,
        media_types: Vec<MediaType>,
    },
    /// Analyze only the fixed catalog/search relation allowlist.
    AdminAnalyze,
}

impl IngestJob {
    /// Returns a stable deduplication key for one logical unit of work.
    #[must_use]
    pub fn dedup_key(&self) -> String {
        match self {
            Self::RefreshMovie { tmdb_id } => format!("{REFRESH_MOVIE_JOB}:{tmdb_id}"),
            Self::RefreshTv { tmdb_id } => format!("{REFRESH_TV_JOB}:{tmdb_id}"),
            Self::RefreshSeason {
                tv_id,
                season_number,
            } => format!("{REFRESH_SEASON_JOB}:{tv_id}:{season_number}"),
            Self::ChangesSync { media_type, page } => {
                format!("{CHANGES_SYNC_JOB}:{media_type}:{page}")
            }
            Self::DailyExport { media_type, url } => {
                let mut digest = Sha256::new();
                digest.update(url.as_bytes());
                let digest = digest.finalize();
                format!("{DAILY_EXPORT_JOB}:{media_type}:{digest:x}")
            }
            Self::Trending {
                media_type,
                trend_window,
            } => format!("{TRENDING_REFRESH_JOB}:{media_type}:{trend_window}"),
            Self::AdminScan { mode, media_types } => {
                format!("{ADMIN_SCAN_JOB}:{mode:?}:{media_types:?}")
            }
            Self::AdminAnalyze => ADMIN_ANALYZE_JOB.to_owned(),
        }
    }
}

/// Validates a durable job's type, version, and bounded JSON object.
///
/// # Errors
///
/// Returns [`JobPayloadError`] when the type, version, or payload violates
/// the durable job contract.
pub fn parse_job(
    job_type: &str,
    payload_version: i32,
    payload: &Value,
) -> Result<IngestJob, JobPayloadError> {
    if payload_version != INGEST_PAYLOAD_VERSION {
        return Err(JobPayloadError::UnsupportedVersion);
    }
    match job_type {
        REFRESH_MOVIE_JOB => {
            let payload: RefreshPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::RefreshMovie {
                tmdb_id: payload.tmdb_id,
            })
        }
        REFRESH_TV_JOB => {
            let payload: RefreshPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::RefreshTv {
                tmdb_id: payload.tmdb_id,
            })
        }
        REFRESH_SEASON_JOB => {
            let payload: RefreshSeasonPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tv_id)?;
            Ok(IngestJob::RefreshSeason {
                tv_id: payload.tv_id,
                season_number: payload.season_number,
            })
        }
        CHANGES_SYNC_JOB => {
            let payload: ChangesPayload = parse_payload(payload)?;
            if payload.page == 0 {
                return Err(JobPayloadError::InvalidValue);
            }
            Ok(IngestJob::ChangesSync {
                media_type: payload.media_type,
                page: payload.page,
            })
        }
        DAILY_EXPORT_JOB => {
            let payload: DailyExportPayload = parse_payload(payload)?;
            validate_export_url(&payload.url)?;
            Ok(IngestJob::DailyExport {
                media_type: payload.media_type,
                url: payload.url,
            })
        }
        TRENDING_REFRESH_JOB => {
            let payload: TrendingPayload = parse_payload(payload)?;
            if !matches!(payload.trend_window.as_str(), "day" | "week") {
                return Err(JobPayloadError::InvalidValue);
            }
            Ok(IngestJob::Trending {
                media_type: payload.media_type,
                trend_window: payload.trend_window,
            })
        }
        ADMIN_SCAN_JOB => {
            let payload: AdminScanPayload = parse_payload(payload)?;
            if payload.media_types.is_empty()
                || payload.media_types.len() > 2
                || payload
                    .media_types
                    .windows(2)
                    .any(|pair| pair[0] == pair[1])
            {
                return Err(JobPayloadError::InvalidValue);
            }
            Ok(IngestJob::AdminScan {
                mode: payload.mode,
                media_types: payload.media_types,
            })
        }
        ADMIN_ANALYZE_JOB if payload == &serde_json::json!({}) => Ok(IngestJob::AdminAnalyze),
        ADMIN_ANALYZE_JOB => Err(JobPayloadError::InvalidPayload),
        _ => Err(JobPayloadError::UnknownJobType),
    }
}

fn parse_payload<T>(payload: &Value) -> Result<T, JobPayloadError>
where
    T: for<'de> Deserialize<'de>,
{
    if !payload.is_object() {
        return Err(JobPayloadError::InvalidPayload);
    }
    serde_json::from_value(payload.clone()).map_err(|_| JobPayloadError::InvalidPayload)
}

fn validate_tmdb_id(tmdb_id: u32) -> Result<(), JobPayloadError> {
    (tmdb_id > 0)
        .then_some(())
        .ok_or(JobPayloadError::InvalidValue)
}

fn validate_export_url(url: &str) -> Result<(), JobPayloadError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| JobPayloadError::InvalidValue)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.host_str() != Some("files.tmdb.org")
    {
        return Err(JobPayloadError::InvalidValue);
    }
    Ok(())
}

/// A sanitized payload validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobPayloadError {
    /// The durable job type is not registered by this worker.
    #[error("unknown ingestion job type")]
    UnknownJobType,
    /// The payload version is not supported by this worker.
    #[error("unsupported ingestion payload version")]
    UnsupportedVersion,
    /// The payload is not a bounded object with the expected fields.
    #[error("invalid ingestion job payload")]
    InvalidPayload,
    /// A field has a disallowed value.
    #[error("invalid ingestion job value")]
    InvalidValue,
}

/// Durable executor for upstream ingestion and catalog persistence.
#[derive(Clone, Debug)]
pub struct IngestExecutor {
    client: TmdbClient,
    export_parser: DailyExportParser,
    export_root: PathBuf,
    export_max_bytes: u64,
    database: Option<PgPool>,
    allow_local_media: bool,
}

impl IngestExecutor {
    /// Creates an executor that archives daily exports under the configured work root.
    #[must_use]
    pub fn with_export_root(client: TmdbClient, export_root: PathBuf) -> Self {
        Self {
            client,
            export_parser: DailyExportParser::default(),
            export_root,
            export_max_bytes: MAX_DAILY_EXPORT_BYTES,
            database: None,
            allow_local_media: false,
        }
    }

    /// Enables transactional catalog writes for this executor.
    #[must_use]
    pub fn with_database(mut self, database: PgPool) -> Self {
        self.database = Some(database);
        self
    }

    /// Enables transactional image-job creation for the local media mount.
    #[must_use]
    pub fn with_local_media(mut self, enabled: bool) -> Self {
        self.allow_local_media = enabled;
        self
    }

    /// Overrides the compressed daily-export bound after validating it.
    ///
    /// # Errors
    ///
    /// Returns [`JobExecutionError`] only when the configured bound is outside
    /// the upstream hard safety limit.
    pub fn with_export_max_bytes(
        mut self,
        export_max_bytes: u64,
    ) -> Result<Self, JobExecutionError> {
        if export_max_bytes == 0 || export_max_bytes > MAX_DAILY_EXPORT_BYTES {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
        self.export_max_bytes = export_max_bytes;
        Ok(self)
    }

    async fn record_upstream_state(&self, state: &'static str) {
        let Some(database) = &self.database else {
            return;
        };
        if sqlx::query("SELECT ops.record_component_heartbeat('upstream', $1)")
            .bind(state)
            .execute(database)
            .await
            .is_err()
        {
            tracing::warn!(
                event = "component_heartbeat_failed",
                component = "upstream",
                error_code = "database_unavailable",
            );
        }
    }
}

#[async_trait::async_trait]
impl JobExecutor for IngestExecutor {
    fn supported_job_types(&self) -> Option<&'static [&'static str]> {
        Some(INGEST_JOB_TYPES)
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError> {
        let parsed = parse_job(job.job_type(), job.payload_version(), job.payload())
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        let dedup_key = parsed.dedup_key();
        match parsed {
            IngestJob::RefreshMovie { tmdb_id } => {
                let movie = match self.client.fetch_movie(tmdb_id).await {
                    Ok(movie) => {
                        self.record_upstream_state("ready").await;
                        movie
                    }
                    Err(TmdbClientError::NotFound) => {
                        self.record_upstream_state("ready").await;
                        tracing::debug!(
                            event = "upstream_item_skipped",
                            operation = "movie_detail",
                            media_type = "movie",
                            tmdb_id,
                            reason = "not_found",
                        );
                        return Ok(serde_json::json!({
                            "media_type":"movie",
                            "tmdb_id":tmdb_id,
                            "skipped":"upstream_not_found",
                            "dedup_key":dedup_key
                        }));
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("movie_detail", "movie", tmdb_id, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    catalog_write::persist_movie_with_options(
                        database,
                        &movie,
                        self.allow_local_media,
                    )
                    .await?;
                }
                Ok(serde_json::json!({
                    "media_type":"movie",
                    "tmdb_id":movie.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::RefreshTv { tmdb_id } => {
                let series = match self.client.fetch_tv(tmdb_id).await {
                    Ok(series) => {
                        self.record_upstream_state("ready").await;
                        series
                    }
                    Err(TmdbClientError::NotFound) => {
                        self.record_upstream_state("ready").await;
                        tracing::debug!(
                            event = "upstream_item_skipped",
                            operation = "tv_detail",
                            media_type = "tv",
                            tmdb_id,
                            reason = "not_found",
                        );
                        return Ok(serde_json::json!({
                            "media_type":"tv",
                            "tmdb_id":tmdb_id,
                            "skipped":"upstream_not_found",
                            "dedup_key":dedup_key
                        }));
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("tv_detail", "tv", tmdb_id, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    catalog_write::persist_tv_with_options(
                        database,
                        &series,
                        self.allow_local_media,
                    )
                    .await?;
                }
                Ok(serde_json::json!({
                    "media_type":"tv",
                    "tmdb_id":series.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::ChangesSync { media_type, page } => {
                let change_page = match self.client.fetch_changes(media_type, page).await {
                    Ok(change_page) => {
                        self.record_upstream_state("ready").await;
                        change_page
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        tracing::warn!(
                            event = "upstream_request_failed",
                            operation = "changes_page",
                            media_type = media_type_name(media_type),
                            page,
                            failure_reason = upstream_error_reason(&error),
                            http_status = upstream_http_status(&error),
                        );
                        return Err(map_upstream_error(&error));
                    }
                };
                let changed_ids: Vec<u64> = change_page
                    .results
                    .iter()
                    .map(|changed| changed.id)
                    .collect();
                let detail_refresh_candidates = if let Some(database) = &self.database {
                    catalog_write::persist_changes(database, media_type, &change_page).await?;
                    let detail_refresh_candidates =
                        enqueue_refresh_jobs(database, media_type, &changed_ids).await?;
                    if change_page.total_pages > page {
                        let next_page = page.saturating_add(1);
                        let next_job = NewJob::new(
                            CHANGES_SYNC_JOB,
                            INGEST_PAYLOAD_VERSION,
                            serde_json::json!({
                                "media_type": media_type,
                                "page": next_page
                            }),
                            &format!("{CHANGES_SYNC_JOB}:{media_type}:{next_page}"),
                        )
                        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                        JobRepository::new(database.clone())
                            .submit(next_job)
                            .await
                            .map_err(|_| {
                                JobExecutionError::retry(
                                    "database_unavailable",
                                    Duration::from_secs(5),
                                )
                            })?;
                    }
                    detail_refresh_candidates
                } else {
                    0
                };
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "page": change_page.page,
                    "total_pages": change_page.total_pages,
                    "changed_ids": change_page.results.len(),
                    "detail_refresh_candidates": detail_refresh_candidates,
                    "next_page_queued": change_page.total_pages > page,
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::RefreshSeason {
                tv_id,
                season_number,
            } => {
                let season = match self.client.fetch_season(tv_id, season_number).await {
                    Ok(season) => {
                        self.record_upstream_state("ready").await;
                        season
                    }
                    Err(TmdbClientError::NotFound) => {
                        self.record_upstream_state("ready").await;
                        tracing::debug!(
                            event = "upstream_item_skipped",
                            operation = "season_detail",
                            media_type = "tv",
                            tmdb_id = tv_id,
                            season_number,
                            reason = "not_found",
                        );
                        return Ok(serde_json::json!({
                            "media_type":"tv",
                            "tv_id":tv_id,
                            "season_number":season_number,
                            "skipped":"upstream_not_found",
                            "dedup_key":dedup_key
                        }));
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure(
                            "season_detail",
                            "tv",
                            tv_id,
                            Some(season_number),
                            &error,
                        );
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    catalog_write::persist_season_with_options(
                        database,
                        tv_id,
                        &season,
                        self.allow_local_media,
                    )
                    .await?;
                }
                Ok(serde_json::json!({
                    "media_type":"tv",
                    "tv_id":tv_id,
                    "season_number":season_number,
                    "episodes":season.episodes.len(),
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::DailyExport { media_type, url } => {
                let digest = Sha256::digest(url.as_bytes());
                let destination = self
                    .export_root
                    .join(format!("{media_type}-{digest:x}.ndjson.gz"));
                tokio::fs::create_dir_all(&self.export_root)
                    .await
                    .map_err(|_| {
                        JobExecutionError::retry("export_storage", Duration::from_secs(30))
                    })?;
                let download = match self
                    .client
                    .fetch_daily_export_to_file(&url, &destination, self.export_max_bytes)
                    .await
                {
                    Ok(download) => {
                        self.record_upstream_state("ready").await;
                        download
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        tracing::warn!(
                            event = "upstream_request_failed",
                            operation = "daily_export",
                            media_type = media_type_name(media_type),
                            failure_reason = upstream_error_reason(&error),
                            http_status = upstream_http_status(&error),
                        );
                        return Err(map_upstream_error(&error));
                    }
                };
                let queue_summary = if let Some(database) = &self.database {
                    enqueue_daily_export_refresh_jobs(
                        database,
                        media_type,
                        self.export_parser,
                        destination,
                    )
                    .await?
                } else {
                    let parser = self.export_parser;
                    let records =
                        tokio::task::spawn_blocking(move || parser.count_file(&destination))
                            .await
                            .map_err(|_| {
                                JobExecutionError::retry("export_storage", Duration::from_secs(30))
                            })?
                            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                    ExportQueueSummary {
                        records,
                        detail_refresh_candidates: 0,
                    }
                };
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "records": queue_summary.records,
                    "detail_refresh_candidates": queue_summary.detail_refresh_candidates,
                    "dedup_key": dedup_key,
                    "bytes": download.bytes,
                    "sha256": hex_digest(&download.sha256)
                }))
            }
            IngestJob::Trending {
                media_type,
                trend_window,
            } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                let trend_page = match self.client.fetch_trending(media_type, &trend_window).await {
                    Ok(page) => {
                        self.record_upstream_state("ready").await;
                        page
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        tracing::warn!(
                            event = "upstream_request_failed",
                            operation = "trending",
                            media_type = media_type_name(media_type),
                            trend_window,
                            failure_reason = upstream_error_reason(&error),
                            http_status = upstream_http_status(&error),
                        );
                        return Err(map_upstream_error(&error));
                    }
                };
                let persisted =
                    persist_trending(database, media_type, &trend_window, &trend_page.results)
                        .await?;
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "trend_window": trend_window,
                    "upstream_items": trend_page.results.len(),
                    "persisted": persisted,
                    "dedup_key": dedup_key,
                }))
            }
            IngestJob::AdminScan { mode, media_types } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                let queued = match mode {
                    AdminScanMode::Full => {
                        let export_date = Utc::now()
                            .date_naive()
                            .checked_sub_days(Days::new(1))
                            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                        let mut queued = 0_usize;
                        for &media_type in &media_types {
                            let job = full_export_job(media_type, export_date)?;
                            if !JobRepository::new(database.clone())
                                .submit(job)
                                .await
                                .map_err(|_| {
                                    JobExecutionError::retry(
                                        "database_unavailable",
                                        Duration::from_secs(5),
                                    )
                                })?
                                .was_duplicate()
                            {
                                queued = queued.saturating_add(1);
                            }
                        }
                        queued
                    }
                    AdminScanMode::Changes => {
                        let mut queued = 0_usize;
                        for media_type in &media_types {
                            let job = NewJob::new(
                                CHANGES_SYNC_JOB,
                                INGEST_PAYLOAD_VERSION,
                                serde_json::json!({"media_type": media_type, "page": 1}),
                                &format!("{CHANGES_SYNC_JOB}:{media_type}:1"),
                            )
                            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                            if !JobRepository::new(database.clone())
                                .submit(job)
                                .await
                                .map_err(|_| {
                                    JobExecutionError::retry(
                                        "database_unavailable",
                                        Duration::from_secs(5),
                                    )
                                })?
                                .was_duplicate()
                            {
                                queued = queued.saturating_add(1);
                            }
                        }
                        queued
                    }
                    AdminScanMode::Missing => {
                        enqueue_missing_catalog_refresh_jobs(database, &media_types).await?
                    }
                };
                Ok(serde_json::json!({
                    "mode": mode,
                    "media_types": media_types,
                    "queued": queued,
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::AdminAnalyze => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                analyze_catalog(database).await?;
                Ok(serde_json::json!({
                    "analyzed": [
                        "catalog.titles",
                        "catalog.movie_details",
                        "catalog.tv_details",
                        "catalog.title_credits",
                        "search.search_documents"
                    ],
                    "dedup_key": dedup_key
                }))
            }
        }
    }
}

fn full_export_job(
    media_type: MediaType,
    export_date: NaiveDate,
) -> Result<NewJob, JobExecutionError> {
    let (media_type_name, file_prefix) = match media_type {
        MediaType::Movie => ("movie", "movie_ids"),
        MediaType::Tv => ("tv", "tv_series_ids"),
    };
    let date_text = export_date.format("%m_%d_%Y").to_string();
    NewJob::new(
        DAILY_EXPORT_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({
            "media_type": media_type_name,
            "url": format!("https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz")
        }),
        &format!("{DAILY_EXPORT_JOB}:{media_type_name}:{date_text}"),
    )
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

async fn enqueue_missing_catalog_refresh_jobs(
    database: &PgPool,
    media_types: &[MediaType],
) -> Result<usize, JobExecutionError> {
    const MAX_MISSING_REFRESHES: i64 = 10_000;
    let repository = JobRepository::new(database.clone());
    let mut queued = 0_usize;
    for media_type in media_types {
        let media_type_name = media_type_name(*media_type);
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT tmdb_id
               FROM catalog.titles
              WHERE media_type = $1
                AND active
                AND (source_updated_at IS NULL OR display_title IS NULL)
              ORDER BY id
              LIMIT $2",
        )
        .bind(media_type_name)
        .bind(MAX_MISSING_REFRESHES)
        .fetch_all(database)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
        for tmdb_id in ids {
            let tmdb_id = u32::try_from(tmdb_id)
                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            let job_type = match media_type {
                MediaType::Movie => REFRESH_MOVIE_JOB,
                MediaType::Tv => REFRESH_TV_JOB,
            };
            let job = NewJob::new(
                job_type,
                INGEST_PAYLOAD_VERSION,
                serde_json::json!({"tmdb_id": tmdb_id}),
                &format!("{job_type}:{tmdb_id}"),
            )
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            if !repository
                .submit(job)
                .await
                .map_err(|_| {
                    JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
                })?
                .was_duplicate()
            {
                queued = queued.saturating_add(1);
            }
        }
    }
    Ok(queued)
}

async fn analyze_catalog(database: &PgPool) -> Result<(), JobExecutionError> {
    // These are compile-time literals, not caller-controlled relation names.
    for statement in [
        "ANALYZE catalog.titles",
        "ANALYZE catalog.movie_details",
        "ANALYZE catalog.tv_details",
        "ANALYZE catalog.title_credits",
        "ANALYZE search.search_documents",
    ] {
        sqlx::query(statement)
            .execute(database)
            .await
            .map_err(|_| {
                JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
            })?;
    }
    Ok(())
}

async fn persist_trending(
    database: &PgPool,
    media_type: MediaType,
    trend_window: &str,
    items: &[TmdbTrendingItem],
) -> Result<usize, JobExecutionError> {
    if !matches!(trend_window, "day" | "week") || items.len() > 100 {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    let collected_for = Utc::now().date_naive();
    let mut transaction = database
        .begin()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    // Replace the complete scope inside this transaction. A successful TMDB
    // response is authoritative for its window; keeping same-day rows would
    // surface titles that disappeared from the later response. If insertion
    // fails, the transaction rolls back and preserves the prior list.
    sqlx::query(
        "DELETE FROM catalog.title_trends
          WHERE trend_window = $1
            AND media_type = $2",
    )
    .bind(trend_window)
    .bind(media_type_name(media_type))
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let mut persisted = 0_usize;
    for (offset, item) in items.iter().enumerate() {
        let tmdb_id = source_id(item.id)?;
        let rank = i32::try_from(offset + 1)
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        let score = item.popularity.filter(|score| score.is_finite());
        let affected = sqlx::query(
            "INSERT INTO catalog.title_trends (
                 trend_window, media_type, title_id, rank, score, collected_for, updated_at
             )
             SELECT $1, $2, title.id, $3, $4, $5, clock_timestamp()
               FROM catalog.titles AS title
              WHERE title.media_type = $2
                AND title.tmdb_id = $6
                AND title.active
             ON CONFLICT (trend_window, media_type, title_id) DO UPDATE
             SET rank = EXCLUDED.rank,
                 score = EXCLUDED.score,
                 collected_for = EXCLUDED.collected_for,
                 updated_at = clock_timestamp()",
        )
        .bind(trend_window)
        .bind(media_type_name(media_type))
        .bind(rank)
        .bind(score)
        .bind(collected_for)
        .bind(tmdb_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
        .rows_affected();
        persisted = persisted.saturating_add(usize::try_from(affected).unwrap_or(0));
    }
    transaction
        .commit()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(persisted)
}

fn hex_digest(value: &[u8; 32]) -> String {
    use std::fmt::Write;

    value
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

fn source_id(raw: u64) -> Result<i64, JobExecutionError> {
    let id = i64::try_from(raw).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    (id > 0)
        .then_some(id)
        .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))
}

fn parse_source_date(value: Option<&str>) -> Result<Option<NaiveDate>, JobExecutionError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(Some)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

fn normalize_language(value: &str) -> Result<String, JobExecutionError> {
    let value = value.trim().to_ascii_lowercase();
    if !(2..=3).contains(&value.len())
        || !value
            .chars()
            .all(|character| character.is_ascii_alphabetic())
    {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    Ok(value)
}

fn log_upstream_failure(
    operation: &'static str,
    media_type: &'static str,
    tmdb_id: u32,
    season_number: Option<u16>,
    error: &TmdbClientError,
) {
    tracing::warn!(
        event = "upstream_request_failed",
        operation,
        media_type,
        tmdb_id,
        season_number = season_number.unwrap_or(0),
        failure_reason = upstream_error_reason(error),
        http_status = upstream_http_status(error),
    );
}

fn media_type_name(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Tv => "tv",
    }
}

fn upstream_error_reason(error: &TmdbClientError) -> &'static str {
    match error {
        TmdbClientError::InvalidBaseUrl => "invalid_base_url",
        TmdbClientError::InvalidPath => "invalid_path",
        TmdbClientError::HttpClientBuild => "http_client_build_failed",
        TmdbClientError::Policy(_) => "request_policy_failed",
        TmdbClientError::Transport => "transport_failed",
        TmdbClientError::ResponseTooLarge => "response_too_large",
        TmdbClientError::ExportSizeLimit => "export_size_limit",
        TmdbClientError::InvalidExportDestination => "invalid_export_destination",
        TmdbClientError::ExportStorage => "export_storage_failed",
        TmdbClientError::RateLimited { .. } => "rate_limited",
        TmdbClientError::Unauthorized => "unauthorized",
        TmdbClientError::Forbidden { .. } => "forbidden",
        TmdbClientError::NotFound => "not_found",
        TmdbClientError::NotModified => "not_modified",
        TmdbClientError::UpstreamServer { .. } => "upstream_server_error",
        TmdbClientError::PermanentHttp { .. } => "permanent_http_error",
        TmdbClientError::MalformedJson { .. } => "malformed_json",
    }
}

fn upstream_http_status(error: &TmdbClientError) -> u16 {
    match error {
        TmdbClientError::Unauthorized => 401,
        TmdbClientError::Forbidden { .. } => 403,
        TmdbClientError::NotFound => 404,
        TmdbClientError::NotModified => 304,
        TmdbClientError::RateLimited { .. } => 429,
        TmdbClientError::UpstreamServer { status } | TmdbClientError::PermanentHttp { status } => {
            *status
        }
        _ => 0,
    }
}

fn map_upstream_error(error: &TmdbClientError) -> JobExecutionError {
    match error {
        TmdbClientError::RateLimited { retry_after } => JobExecutionError::retry(
            "rate_limited",
            Duration::from_secs(retry_after.as_ref().copied().unwrap_or(1).min(600)),
        ),
        TmdbClientError::Transport
        | TmdbClientError::ResponseTooLarge
        | TmdbClientError::UpstreamServer { .. } => {
            JobExecutionError::retry("upstream_unavailable", Duration::from_secs(5))
        }
        TmdbClientError::Unauthorized => JobExecutionError::dead_letter("upstream_unauthorized"),
        TmdbClientError::Forbidden { .. }
        | TmdbClientError::NotFound
        | TmdbClientError::NotModified
        | TmdbClientError::PermanentHttp { .. }
        | TmdbClientError::MalformedJson { .. }
        | TmdbClientError::InvalidBaseUrl
        | TmdbClientError::InvalidPath
        | TmdbClientError::HttpClientBuild
        | TmdbClientError::Policy(_)
        | TmdbClientError::ExportSizeLimit
        | TmdbClientError::InvalidExportDestination => {
            JobExecutionError::dead_letter("invalid_payload")
        }
        TmdbClientError::ExportStorage => {
            JobExecutionError::retry("export_storage", Duration::from_secs(30))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use sqlx::PgPool;

    use super::*;

    #[test]
    fn unauthorized_upstream_errors_dead_letter_with_an_actionable_code() {
        let error = map_upstream_error(&TmdbClientError::Unauthorized);
        assert!(error.is_terminal());
        assert_eq!(error.failure_code(), "upstream_unauthorized");
        assert_eq!(error.retry_delay(), Duration::ZERO);
    }

    #[test]
    fn upstream_not_found_is_classified_for_a_nonfatal_detail_skip() {
        assert_eq!(
            upstream_error_reason(&TmdbClientError::NotFound),
            "not_found"
        );
        assert_eq!(upstream_http_status(&TmdbClientError::NotFound), 404);
        assert_eq!(media_type_name(MediaType::Movie), "movie");
        assert_eq!(media_type_name(MediaType::Tv), "tv");
    }

    #[test]
    fn payloads_are_strict_and_dedup_keys_are_stable() -> Result<(), Box<dyn std::error::Error>> {
        let first = parse_job(REFRESH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":42}))?;
        let second = parse_job(REFRESH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":42}))?;
        assert_eq!(first, second);
        assert_eq!(first.dedup_key(), "ingest.refresh_movie:42");
        assert!(matches!(
            parse_job(REFRESH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":0})),
            Err(JobPayloadError::InvalidValue)
        ));
        assert!(matches!(
            parse_job(
                REFRESH_MOVIE_JOB,
                1,
                &serde_json::json!({"tmdb_id":42,"extra":true})
            ),
            Err(JobPayloadError::InvalidPayload)
        ));
        let season = parse_job(
            REFRESH_SEASON_JOB,
            1,
            &serde_json::json!({"tv_id":42,"season_number":0}),
        )?;
        assert_eq!(season.dedup_key(), "ingest.refresh_season:42:0");
        let year_number = parse_job(
            REFRESH_SEASON_JOB,
            1,
            &serde_json::json!({"tv_id":42,"season_number":2012}),
        )?;
        assert_eq!(year_number.dedup_key(), "ingest.refresh_season:42:2012");
        assert!(matches!(
            parse_job(
                REFRESH_SEASON_JOB,
                1,
                &serde_json::json!({"tv_id":42,"season_number":65536})
            ),
            Err(JobPayloadError::InvalidPayload)
        ));
        Ok(())
    }

    #[test]
    fn sync_and_export_payloads_require_safe_values() {
        assert!(
            parse_job(
                CHANGES_SYNC_JOB,
                1,
                &serde_json::json!({"media_type":"movie","page":1})
            )
            .is_ok()
        );
        assert!(matches!(
            parse_job(
                DAILY_EXPORT_JOB,
                1,
                &serde_json::json!({"media_type":"movie","url":"https://evil.example/export"})
            ),
            Err(JobPayloadError::InvalidValue)
        ));
        assert!(matches!(
            parse_job(DAILY_EXPORT_JOB, 2, &serde_json::json!({})),
            Err(JobPayloadError::UnsupportedVersion)
        ));
    }

    #[test]
    fn unknown_and_non_object_payloads_are_rejected() {
        assert!(matches!(
            parse_job("ingest.unknown", 1, &serde_json::json!({})),
            Err(JobPayloadError::UnknownJobType)
        ));
        assert!(matches!(
            parse_job(REFRESH_TV_JOB, 1, &serde_json::json!([])),
            Err(JobPayloadError::InvalidPayload)
        ));
    }

    #[test]
    fn source_fields_are_validated_before_database_writes() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(source_id(0).is_err());
        assert_eq!(source_id(42)?, 42);
        assert_eq!(
            parse_source_date(Some("2024-02-29"))?,
            Some(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).ok_or("date")?)
        );
        assert!(parse_source_date(Some("2024-02-30")).is_err());
        assert_eq!(normalize_language("EN")?, "en");
        assert!(normalize_language("e").is_err());
        assert!(normalize_language("eng!").is_err());
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn changed_ids_enqueue_idempotent_detail_refresh_jobs(pool: PgPool) -> sqlx::Result<()> {
        enqueue_refresh_jobs(&pool, MediaType::Movie, &[42, 42, 43])
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        let rows: Vec<(String, Value, String, i16)> = sqlx::query_as(
            "SELECT job_type, payload, dedup_key, priority
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_movie'
              ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, REFRESH_MOVIE_JOB);
        assert_eq!(rows[0].1["tmdb_id"], 42);
        assert_eq!(rows[0].2, "ingest.refresh_movie:42");
        assert_eq!(rows[1].1["tmdb_id"], 43);
        assert_eq!(rows[1].2, "ingest.refresh_movie:43");
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_enqueues_detail_refresh_jobs(pool: PgPool) -> sqlx::Result<()> {
        let export = tempfile::NamedTempFile::new()?;
        std::fs::write(
            export.path(),
            concat!(
                "{\"id\":51,\"adult\":false,\"video\":false}\n",
                "{\"id\":52,\"adult\":false,\"video\":false}\n",
                "{\"id\":51,\"adult\":false,\"video\":false}\n"
            ),
        )?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            DailyExportParser::default(),
            export.path().to_path_buf(),
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(summary.records, 3);
        assert_eq!(summary.detail_refresh_candidates, 2);
        let rows: Vec<(String, Value, String, i16)> = sqlx::query_as(
            "SELECT job_type, payload, dedup_key, priority
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_movie'
              ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1["tmdb_id"], 51);
        assert_eq!(rows[1].1["tmdb_id"], 52);
        assert_eq!(rows[0].3, DAILY_EXPORT_REFRESH_PRIORITY);
        assert_eq!(rows[1].3, DAILY_EXPORT_REFRESH_PRIORITY);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_does_not_requeue_loaded_catalog_titles(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('movie', 51, 'Already loaded')",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.movie_details (title_id)
             SELECT id
               FROM catalog.titles
              WHERE media_type = 'movie' AND tmdb_id = 51",
        )
        .execute(&pool)
        .await?;
        let export = tempfile::NamedTempFile::new()?;
        std::fs::write(
            export.path(),
            concat!(
                "{\"id\":51,\"adult\":false,\"video\":false}\n",
                "{\"id\":52,\"adult\":false,\"video\":false}\n"
            ),
        )?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            DailyExportParser::default(),
            export.path().to_path_buf(),
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(summary.records, 2);
        assert_eq!(summary.detail_refresh_candidates, 1);
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT (payload ->> 'tmdb_id')::bigint
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_movie'
              ORDER BY 1",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(ids, [52]);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn trending_refresh_replaces_the_complete_current_window(
        pool: PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('movie', 101, 'First trend'), ('movie', 102, 'Second trend')",
        )
        .execute(&pool)
        .await?;

        persist_trending(
            &pool,
            MediaType::Movie,
            "day",
            &[TmdbTrendingItem {
                id: 101,
                popularity: Some(10.0),
            }],
        )
        .await?;
        persist_trending(
            &pool,
            MediaType::Movie,
            "day",
            &[TmdbTrendingItem {
                id: 102,
                popularity: Some(20.0),
            }],
        )
        .await?;

        let trends: Vec<(i64, i32)> = sqlx::query_as(
            "SELECT title.tmdb_id, trend.rank
               FROM catalog.title_trends AS trend
               JOIN catalog.titles AS title ON title.id = trend.title_id
              WHERE trend.media_type = 'movie'
                AND trend.trend_window = 'day'
              ORDER BY trend.rank",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(trends, vec![(102, 1)]);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExportQueueSummary {
    records: usize,
    detail_refresh_candidates: usize,
}

async fn enqueue_refresh_jobs(
    pool: &PgPool,
    media_type: MediaType,
    tmdb_ids: &[u64],
) -> Result<usize, JobExecutionError> {
    enqueue_refresh_jobs_with_priority(pool, media_type, tmdb_ids, 0).await
}

async fn enqueue_refresh_jobs_with_priority(
    pool: &PgPool,
    media_type: MediaType,
    tmdb_ids: &[u64],
    priority: i16,
) -> Result<usize, JobExecutionError> {
    const SUBMISSION_BATCH_SIZE: usize = 500;

    let repository = JobRepository::new(pool.clone());
    let mut submitted = 0_usize;
    for ids in tmdb_ids.chunks(SUBMISSION_BATCH_SIZE) {
        let jobs = ids
            .iter()
            .copied()
            .map(|tmdb_id| refresh_job(media_type, tmdb_id, priority))
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = repository
            .submit_many(&jobs)
            .await
            .map_err(map_submission_error)?;
        submitted = submitted.saturating_add(
            outcomes
                .iter()
                .filter(|outcome| !outcome.was_duplicate())
                .count(),
        );
    }
    Ok(submitted)
}

async fn enqueue_missing_refresh_jobs(
    pool: &PgPool,
    media_type: MediaType,
    tmdb_ids: &[u64],
) -> Result<usize, JobExecutionError> {
    let validated_ids = tmdb_ids
        .iter()
        .copied()
        .map(validate_refresh_tmdb_id)
        .collect::<Result<Vec<_>, _>>()?;
    let catalogued_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT title.tmdb_id
           FROM catalog.titles AS title
          WHERE title.media_type = $1
            AND title.tmdb_id = ANY($2)
            AND (
                ($1 = 'movie' AND EXISTS (
                    SELECT 1
                      FROM catalog.movie_details AS detail
                     WHERE detail.title_id = title.id
                ))
                OR ($1 = 'tv' AND EXISTS (
                    SELECT 1
                      FROM catalog.tv_details AS detail
                     WHERE detail.title_id = title.id
                ))
            )",
    )
    .bind(media_type.to_string())
    .bind(
        validated_ids
            .iter()
            .map(|tmdb_id| i64::from(*tmdb_id))
            .collect::<Vec<_>>(),
    )
    .fetch_all(pool)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let catalogued_ids = catalogued_ids.into_iter().collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let missing = validated_ids
        .into_iter()
        .filter(|tmdb_id| seen.insert(*tmdb_id) && !catalogued_ids.contains(&i64::from(*tmdb_id)))
        .map(u64::from)
        .collect::<Vec<_>>();
    enqueue_refresh_jobs_with_priority(pool, media_type, &missing, DAILY_EXPORT_REFRESH_PRIORITY)
        .await
}

async fn enqueue_daily_export_refresh_jobs(
    pool: &PgPool,
    media_type: MediaType,
    parser: DailyExportParser,
    path: PathBuf,
) -> Result<ExportQueueSummary, JobExecutionError> {
    const SUBMISSION_BATCH_SIZE: usize = 500;
    const CHANNEL_CAPACITY: usize = 2;

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u64>>(CHANNEL_CAPACITY);
    let parser_task = tokio::task::spawn_blocking(move || {
        let mut sender = Some(sender);
        let mut batch = Vec::with_capacity(SUBMISSION_BATCH_SIZE);
        let result = parser.scan_file(path, |record| {
            if sender.is_none() {
                return;
            }
            batch.push(record.id);
            if batch.len() == SUBMISSION_BATCH_SIZE {
                let next = std::mem::replace(&mut batch, Vec::with_capacity(SUBMISSION_BATCH_SIZE));
                if sender
                    .as_ref()
                    .is_some_and(|sender| sender.blocking_send(next).is_err())
                {
                    sender = None;
                }
            }
        });
        if !batch.is_empty()
            && let Some(sender) = sender
        {
            let _ = sender.blocking_send(batch);
        }
        result
    });

    let mut received_records = 0_usize;
    let mut detail_refresh_candidates = 0_usize;
    let mut submission_error = None;
    while let Some(ids) = receiver.recv().await {
        received_records = received_records.saturating_add(ids.len());
        match enqueue_missing_refresh_jobs(pool, media_type, &ids).await {
            Ok(submitted) => {
                detail_refresh_candidates = detail_refresh_candidates.saturating_add(submitted);
            }
            Err(error) => {
                submission_error = Some(error);
                receiver.close();
                break;
            }
        }
    }
    drop(receiver);

    let parsed_records = parser_task
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    if let Some(error) = submission_error {
        return Err(error);
    }
    if parsed_records != received_records {
        return Err(JobExecutionError::retry(
            "export_queue_incomplete",
            Duration::from_secs(5),
        ));
    }
    Ok(ExportQueueSummary {
        records: received_records,
        detail_refresh_candidates,
    })
}

fn refresh_job(
    media_type: MediaType,
    tmdb_id: u64,
    priority: i16,
) -> Result<NewJob, JobExecutionError> {
    let tmdb_id = validate_refresh_tmdb_id(tmdb_id)?;
    let job_type = match media_type {
        MediaType::Movie => REFRESH_MOVIE_JOB,
        MediaType::Tv => REFRESH_TV_JOB,
    };
    NewJob::new(
        job_type,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({"tmdb_id": tmdb_id}),
        &format!("{job_type}:{tmdb_id}"),
    )
    .and_then(|job| job.with_priority(priority))
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

fn validate_refresh_tmdb_id(tmdb_id: u64) -> Result<u32, JobExecutionError> {
    u32::try_from(tmdb_id)
        .ok()
        .filter(|tmdb_id| *tmdb_id > 0)
        .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))
}

fn map_submission_error(error: JobError) -> JobExecutionError {
    match error {
        JobError::Validation(_) => JobExecutionError::dead_letter("invalid_payload"),
        JobError::NotFound | JobError::LeaseLost | JobError::Rejected | JobError::Database => {
            JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
        }
    }
}
