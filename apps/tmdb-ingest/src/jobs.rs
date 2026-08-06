use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{Days, Duration as ChronoDuration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use tmdb_db::TmdbDocumentRepository;
use tmdb_domain::MediaType;
use tmdb_jobs::{
    ClaimedJob, JobError, JobExecutionError, JobExecutor, JobRepository, NewJob, SubmitOutcome,
};
use tmdb_upstream::{
    DailyExportParser, EPISODE_DETAIL_QUERY_STRING, IMAGE_GALLERY_QUERY_STRING,
    MAX_DAILY_EXPORT_BYTES, MOVIE_DETAIL_QUERY_STRING, SEASON_DETAIL_QUERY_STRING,
    TV_DETAIL_QUERY_STRING, TmdbClient, TmdbClientError, TmdbImages, TmdbTrendingItem, TmdbVideos,
    VIDEO_GALLERY_QUERY_STRING,
};
use tokio::io::{AsyncBufReadExt, BufReader as TokioBufReader};
use tokio::task::JoinSet;
use uuid::Uuid;

#[path = "catalog_locks.rs"]
mod catalog_locks;
#[path = "catalog_write.rs"]
mod catalog_write;

/// Versioned durable job names accepted by the ingestion worker.
pub const REFRESH_MOVIE_JOB: &str = "ingest.refresh_movie";
/// Versioned durable job names accepted by the ingestion worker.
pub const REFRESH_TV_JOB: &str = "ingest.refresh_tv";
/// Enrich one movie after its fast metadata refresh is durable.
pub const ENRICH_MOVIE_JOB: &str = "ingest.enrich_movie";
/// Enrich one TV title after its fast metadata refresh is durable.
pub const ENRICH_TV_JOB: &str = "ingest.enrich_tv";
/// Refresh one TV season and its episode list from TMDB.
pub const REFRESH_SEASON_JOB: &str = "ingest.refresh_season";
/// Versioned durable job names accepted by the ingestion worker.
pub const CHANGES_SYNC_JOB: &str = "ingest.changes_sync";
/// Versioned durable job names accepted by the ingestion worker.
pub const DAILY_EXPORT_JOB: &str = "ingest.daily_export";
/// Refresh TMDB's image configuration document.
pub const CONFIGURATION_JOB: &str = "ingest.configuration";
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
    ENRICH_MOVIE_JOB,
    ENRICH_TV_JOB,
    REFRESH_SEASON_JOB,
    CHANGES_SYNC_JOB,
    DAILY_EXPORT_JOB,
    CONFIGURATION_JOB,
    TRENDING_REFRESH_JOB,
    ADMIN_SCAN_JOB,
    ADMIN_ANALYZE_JOB,
];
const TITLE_REFRESH_PRIORITY: i16 = 200;
const ENRICHMENT_PRIORITY: i16 = 50;
const CATALOG_PHASE_COORDINATOR_PRIORITY: i16 = 150;
const DAILY_EXPORT_COORDINATOR_PRIORITY: i16 = 300;
const CATALOG_PHASE_POLL_SECONDS: i64 = 2;
const MAX_PENDING_REFRESH_JOBS: i64 = 1_000;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshScope {
    /// Persist metadata and enqueue the normal bounded child work.
    #[default]
    Full,
    /// Persist metadata without enqueueing enrichment or season jobs.
    CatalogOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RefreshPayload {
    tmdb_id: u32,
    #[serde(default)]
    scope: RefreshScope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RefreshSeasonPayload {
    tv_id: u32,
    season_number: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ChangesPayload {
    media_type: MediaType,
    page: u32,
    #[serde(default)]
    start_date: Option<NaiveDate>,
    #[serde(default)]
    end_date: Option<NaiveDate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DailyExportPayload {
    media_type: MediaType,
    url: String,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    refresh_all: bool,
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
    FullSweep,
    MissingOnly,
    Recovery,
    PruneCleanup,
    DailySync,
    Reconcile,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminScanPhase {
    /// Submit the initial export or incremental jobs requested by the operator.
    #[default]
    Start,
    /// Backfill optional title metadata after all title census jobs finish.
    Enrichment,
    /// Backfill TV season and episode metadata after title enrichment finishes.
    Seasons,
    /// Wait for the final child jobs and durably advance synchronization state.
    Completion,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdminScanPayload {
    mode: AdminScanMode,
    media_types: Vec<MediaType>,
    #[serde(default)]
    phase: AdminScanPhase,
    #[serde(default)]
    cursor: u64,
    #[serde(default)]
    poll: u32,
    #[serde(default)]
    window_start: Option<NaiveDate>,
    #[serde(default)]
    window_end: Option<NaiveDate>,
}

/// A validated, idempotent ingestion job payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestJob {
    /// Refresh one movie from TMDB.
    RefreshMovie { tmdb_id: u32, scope: RefreshScope },
    /// Refresh one television series from TMDB.
    RefreshTv { tmdb_id: u32, scope: RefreshScope },
    /// Fetch optional movie gallery documents after the title census.
    EnrichMovie { tmdb_id: u32, scope: RefreshScope },
    /// Fetch optional television gallery documents after the title census.
    EnrichTv { tmdb_id: u32, scope: RefreshScope },
    /// Fetch one TV season and its episodes.
    RefreshSeason { tv_id: u32, season_number: u32 },
    /// Fetch one page of media changes.
    ChangesSync {
        media_type: MediaType,
        page: u32,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    },
    /// Fetch and parse one daily ID export.
    DailyExport {
        media_type: MediaType,
        url: String,
        offset: u64,
        refresh_all: bool,
    },
    /// Fetch TMDB's image configuration document.
    Configuration,
    /// Refresh a named trending window for a single public media namespace.
    Trending {
        media_type: MediaType,
        trend_window: String,
    },
    /// Expand one explicit operational scan into safely bounded ingest jobs.
    AdminScan {
        mode: AdminScanMode,
        media_types: Vec<MediaType>,
        phase: AdminScanPhase,
        cursor: u64,
        poll: u32,
        window_start: Option<NaiveDate>,
        window_end: Option<NaiveDate>,
    },
    /// Analyze only the fixed catalog/search relation allowlist.
    AdminAnalyze,
}

impl IngestJob {
    /// Returns a stable deduplication key for one logical unit of work.
    #[must_use]
    pub fn dedup_key(&self) -> String {
        match self {
            Self::RefreshMovie { tmdb_id, .. } => format!("{REFRESH_MOVIE_JOB}:{tmdb_id}"),
            Self::RefreshTv { tmdb_id, .. } => format!("{REFRESH_TV_JOB}:{tmdb_id}"),
            Self::EnrichMovie { tmdb_id, .. } => format!("{ENRICH_MOVIE_JOB}:{tmdb_id}"),
            Self::EnrichTv { tmdb_id, .. } => format!("{ENRICH_TV_JOB}:{tmdb_id}"),
            Self::RefreshSeason {
                tv_id,
                season_number,
                ..
            } => format!("{REFRESH_SEASON_JOB}:{tv_id}:{season_number}"),
            Self::ChangesSync {
                media_type,
                page,
                start_date,
                end_date,
            } => {
                format!("{CHANGES_SYNC_JOB}:{media_type}:{start_date:?}:{end_date:?}:{page}")
            }
            Self::DailyExport {
                media_type,
                url,
                offset,
                ..
            } => {
                let mut digest = Sha256::new();
                digest.update(url.as_bytes());
                let digest = digest.finalize();
                format!("{DAILY_EXPORT_JOB}:{media_type}:{digest:x}:{offset}")
            }
            Self::Configuration => CONFIGURATION_JOB.to_owned(),
            Self::Trending {
                media_type,
                trend_window,
            } => format!("{TRENDING_REFRESH_JOB}:{media_type}:{trend_window}"),
            Self::AdminScan {
                mode,
                media_types,
                phase,
                cursor,
                poll,
                window_start,
                window_end,
            } => {
                format!(
                    "{ADMIN_SCAN_JOB}:{mode:?}:{media_types:?}:{phase:?}:{cursor}:{poll}:{window_start:?}:{window_end:?}"
                )
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
#[allow(clippy::too_many_lines)]
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
                scope: payload.scope,
            })
        }
        REFRESH_TV_JOB => {
            let payload: RefreshPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::RefreshTv {
                tmdb_id: payload.tmdb_id,
                scope: payload.scope,
            })
        }
        ENRICH_MOVIE_JOB => {
            let payload: RefreshPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::EnrichMovie {
                tmdb_id: payload.tmdb_id,
                scope: payload.scope,
            })
        }
        ENRICH_TV_JOB => {
            let payload: RefreshPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::EnrichTv {
                tmdb_id: payload.tmdb_id,
                scope: payload.scope,
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
            if payload.page == 0
                || payload.start_date.is_some() != payload.end_date.is_some()
                || payload
                    .start_date
                    .zip(payload.end_date)
                    .is_some_and(|(start, end)| start > end || (end - start).num_days() > 13)
            {
                return Err(JobPayloadError::InvalidValue);
            }
            Ok(IngestJob::ChangesSync {
                media_type: payload.media_type,
                page: payload.page,
                start_date: payload.start_date,
                end_date: payload.end_date,
            })
        }
        DAILY_EXPORT_JOB => {
            let payload: DailyExportPayload = parse_payload(payload)?;
            validate_export_url(&payload.url)?;
            Ok(IngestJob::DailyExport {
                media_type: payload.media_type,
                url: payload.url,
                offset: payload.offset,
                refresh_all: payload.refresh_all,
            })
        }
        CONFIGURATION_JOB if payload == &serde_json::json!({}) => Ok(IngestJob::Configuration),
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
                || payload.cursor > i64::MAX as u64
                || (matches!(
                    payload.phase,
                    AdminScanPhase::Enrichment | AdminScanPhase::Seasons
                ) && !matches!(
                    payload.mode,
                    AdminScanMode::FullSweep
                        | AdminScanMode::Recovery
                        | AdminScanMode::Reconcile
                        | AdminScanMode::MissingOnly
                ))
                || (matches!(
                    payload.phase,
                    AdminScanPhase::Enrichment | AdminScanPhase::Seasons
                ) && payload.media_types.len() != 1)
                || (payload.phase == AdminScanPhase::Completion
                    && !matches!(
                        payload.mode,
                        AdminScanMode::FullSweep
                            | AdminScanMode::Recovery
                            | AdminScanMode::Reconcile
                            | AdminScanMode::DailySync
                            | AdminScanMode::MissingOnly
                    ))
                || (payload.phase == AdminScanPhase::Start && payload.poll != 0)
                || (payload.phase == AdminScanPhase::Start
                    && payload.mode != AdminScanMode::MissingOnly
                    && payload.cursor != 0)
                || (payload.window_start.is_some() != payload.window_end.is_some())
                || (payload.mode != AdminScanMode::DailySync
                    && (payload.window_start.is_some() || payload.window_end.is_some()))
                || payload
                    .window_start
                    .zip(payload.window_end)
                    .is_some_and(|(start, end)| start > end || (end - start).num_days() > 13)
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
                phase: payload.phase,
                cursor: payload.cursor,
                poll: payload.poll,
                window_start: payload.window_start,
                window_end: payload.window_end,
            })
        }
        ADMIN_ANALYZE_JOB if payload == &serde_json::json!({}) => Ok(IngestJob::AdminAnalyze),
        CONFIGURATION_JOB | ADMIN_ANALYZE_JOB => Err(JobPayloadError::InvalidPayload),
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
    upstream_ready_heartbeat: Arc<AtomicU64>,
    upstream_degraded_heartbeat: Arc<AtomicU64>,
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
            upstream_ready_heartbeat: Arc::new(AtomicU64::new(0)),
            upstream_degraded_heartbeat: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Enables transactional catalog writes for this executor.
    #[must_use]
    pub fn with_database(mut self, database: PgPool) -> Self {
        self.database = Some(database);
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
        let now = u64::try_from(Utc::now().timestamp()).unwrap_or_default();
        let (slot, interval_seconds) = if state == "ready" {
            (&self.upstream_ready_heartbeat, 5)
        } else {
            (&self.upstream_degraded_heartbeat, 1)
        };
        if !claim_heartbeat_slot(slot, now, interval_seconds) {
            return;
        }
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

fn claim_heartbeat_slot(slot: &AtomicU64, now: u64, interval_seconds: u64) -> bool {
    let next_write = slot.load(Ordering::Relaxed);
    now >= next_write
        && slot
            .compare_exchange(
                next_write,
                now.saturating_add(interval_seconds),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
}

const MAX_LINKED_DOCUMENTS: usize = 256;
const UPSTREAM_ENRICHMENT_CONCURRENCY: usize = 20;

const GLOBAL_DOCUMENT_PATHS: &[&str] = &[
    "configuration/countries",
    "configuration/jobs",
    "configuration/languages",
    "configuration/primary_translations",
    "configuration/timezones",
    "certification/movie/list",
    "certification/tv/list",
    "genre/movie/list",
    "genre/tv/list",
    "watch/providers/regions",
    "watch/providers/movie",
    "watch/providers/tv",
    "movie/changes",
    "movie/latest",
    "movie/now_playing",
    "movie/popular",
    "movie/top_rated",
    "movie/upcoming",
    "person/changes",
    "person/latest",
    "person/popular",
    "tv/airing_today",
    "tv/changes",
    "tv/latest",
    "tv/on_the_air",
    "tv/popular",
    "tv/top_rated",
    "trending/all/day",
    "trending/all/week",
    "trending/movie/day",
    "trending/movie/week",
    "trending/person/day",
    "trending/person/week",
    "trending/tv/day",
    "trending/tv/week",
];

#[derive(Clone, Debug)]
struct CapturedDocument {
    endpoint_path: String,
    query_string: String,
    response: Value,
}

fn optional_gallery_with_raw(
    result: Result<(Value, TmdbImages), TmdbClientError>,
) -> Result<(Option<Value>, TmdbImages), TmdbClientError> {
    match result {
        Ok((raw, images)) => Ok((Some(raw), images)),
        Err(TmdbClientError::NotFound) => Ok((None, TmdbImages::default())),
        Err(error) => Err(error),
    }
}

async fn capture_optional_documents(
    client: &TmdbClient,
    paths: Vec<String>,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let mut pending = paths.into_iter().enumerate();
    let mut tasks = JoinSet::new();
    for _ in 0..UPSTREAM_ENRICHMENT_CONCURRENCY {
        let Some((index, endpoint_path)) = pending.next() else {
            break;
        };
        let client = client.clone();
        tasks.spawn(async move {
            let result = client.fetch_document(&endpoint_path, &[]).await;
            (index, endpoint_path, result)
        });
    }

    let mut documents = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (index, endpoint_path, response) = result.map_err(|_| TmdbClientError::Transport)?;
        match response {
            Ok((response, query_string)) => documents.push((
                index,
                CapturedDocument {
                    endpoint_path,
                    query_string,
                    response,
                },
            )),
            Err(TmdbClientError::NotFound) => {}
            Err(error) => return Err(error),
        }
        if let Some((next_index, next_path)) = pending.next() {
            let client = client.clone();
            tasks.spawn(async move {
                let result = client.fetch_document(&next_path, &[]).await;
                (next_index, next_path, result)
            });
        }
    }
    documents.sort_by_key(|(index, _)| *index);
    Ok(documents
        .into_iter()
        .map(|(_, document)| document)
        .collect())
}

fn appended_named_documents(
    endpoint_prefix: &str,
    detail_raw: &Value,
    names: &[(&str, &str)],
) -> Vec<CapturedDocument> {
    names
        .iter()
        .filter_map(|(name, query_string)| {
            detail_raw
                .get(*name)
                .filter(|value| value.is_object())
                .cloned()
                .map(|response| CapturedDocument {
                    endpoint_path: format!("{endpoint_prefix}/{name}"),
                    query_string: (*query_string).to_owned(),
                    response,
                })
        })
        .collect()
}

const MOVIE_APPENDED_DOCUMENTS: &[(&str, &str)] = &[
    ("keywords", ""),
    ("credits", ""),
    ("translations", ""),
    ("alternative_titles", ""),
    ("external_ids", ""),
    ("videos", VIDEO_GALLERY_QUERY_STRING),
    ("release_dates", ""),
    ("images", IMAGE_GALLERY_QUERY_STRING),
];

const TV_APPENDED_DOCUMENTS: &[(&str, &str)] = &[
    ("keywords", ""),
    ("credits", ""),
    ("translations", ""),
    ("alternative_titles", ""),
    ("external_ids", ""),
    ("videos", VIDEO_GALLERY_QUERY_STRING),
    ("content_ratings", ""),
    ("images", IMAGE_GALLERY_QUERY_STRING),
];

const SEASON_APPENDED_DOCUMENTS: &[(&str, &str)] = &[
    ("account_states", ""),
    ("aggregate_credits", ""),
    ("credits", ""),
    ("external_ids", ""),
    ("translations", ""),
    ("videos", VIDEO_GALLERY_QUERY_STRING),
    ("watch/providers", ""),
    ("images", IMAGE_GALLERY_QUERY_STRING),
];

const EPISODE_APPENDED_DOCUMENTS: &[(&str, &str)] = &[
    ("account_states", ""),
    ("credits", ""),
    ("external_ids", ""),
    ("translations", ""),
    ("videos", VIDEO_GALLERY_QUERY_STRING),
    ("images", IMAGE_GALLERY_QUERY_STRING),
];

fn detail_documents(
    endpoint_path: &str,
    query_string: &str,
    response: Value,
) -> Vec<CapturedDocument> {
    vec![
        CapturedDocument {
            endpoint_path: endpoint_path.to_owned(),
            query_string: query_string.to_owned(),
            response: response.clone(),
        },
        CapturedDocument {
            endpoint_path: endpoint_path.to_owned(),
            query_string: String::new(),
            response,
        },
    ]
}

fn linked_document_paths(
    documents: &[CapturedDocument],
    detail_sources: &[(&str, &Value)],
) -> Vec<String> {
    let mut paths = BTreeSet::new();
    for document in documents {
        collect_linked_document_paths(&document.endpoint_path, &document.response, &mut paths);
    }
    for (endpoint_path, response) in detail_sources {
        collect_linked_document_paths(endpoint_path, response, &mut paths);
    }
    paths.into_iter().take(MAX_LINKED_DOCUMENTS).collect()
}

fn collect_linked_document_paths(
    endpoint_path: &str,
    response: &Value,
    paths: &mut BTreeSet<String>,
) {
    let is_credit_document = endpoint_path.ends_with("/credits")
        || endpoint_path.ends_with("/aggregate_credits")
        || response.get("cast").is_some()
        || response.get("crew").is_some()
        || response.get("credits").is_some()
        || response.get("aggregate_credits").is_some();
    if is_credit_document {
        for section in [response.get("cast"), response.get("crew")] {
            if let Some(rows) = section.and_then(Value::as_array) {
                for row in rows {
                    if let Some(credit_id) = row.get("credit_id").and_then(path_component) {
                        paths.insert(format!("credit/{credit_id}"));
                    }
                }
            }
        }
        for section_name in ["credits", "aggregate_credits"] {
            if let Some(credits) = response.get(section_name) {
                collect_linked_document_paths(endpoint_path, credits, paths);
            }
        }
    }

    if endpoint_path.ends_with("/reviews") {
        add_result_paths(response, "review", paths);
    }
    if endpoint_path.ends_with("/keywords") || response.get("keywords").is_some() {
        for rows in [response.get("keywords"), response.get("results")] {
            add_id_paths(rows, "keyword", paths);
        }
    }
    if endpoint_path.ends_with("/episode_groups") || response.get("episode_groups").is_some() {
        for rows in [response.get("episode_groups"), response.get("results")] {
            add_id_paths(rows, "tv/episode_group", paths);
        }
    }
}

fn add_result_paths(response: &Value, prefix: &str, paths: &mut BTreeSet<String>) {
    add_id_paths(response.get("results"), prefix, paths);
}

fn add_id_paths(value: Option<&Value>, prefix: &str, paths: &mut BTreeSet<String>) {
    let Some(rows) = value.and_then(|value| {
        value.as_array().or_else(|| {
            value
                .get("results")
                .and_then(Value::as_array)
                .or_else(|| value.get("keywords").and_then(Value::as_array))
        })
    }) else {
        return;
    };
    for row in rows {
        let Some(id) = row.get("id").and_then(path_component) else {
            continue;
        };
        paths.insert(format!("{prefix}/{id}"));
        if prefix == "keyword" {
            paths.insert(format!("{prefix}/{id}/movies"));
        }
    }
}

fn path_component(value: &Value) -> Option<String> {
    let value = if let Some(value) = value.as_str() {
        value.to_owned()
    } else {
        value.as_u64().filter(|id| *id > 0)?.to_string()
    };
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || value.chars().any(char::is_control)
        || value.contains(['/', '\\', '?', '#'])
    {
        return None;
    }
    Some(value)
}

async fn capture_linked_documents(
    client: &TmdbClient,
    documents: &mut Vec<CapturedDocument>,
    detail_sources: &[(&str, &Value)],
) -> Result<(), TmdbClientError> {
    let existing = documents
        .iter()
        .map(|document| format!("{}\0{}", document.endpoint_path, document.query_string))
        .collect::<HashSet<_>>();
    let paths = linked_document_paths(documents, detail_sources)
        .into_iter()
        .filter(|path| !existing.contains(&format!("{path}\0")))
        .collect();
    documents.extend(capture_optional_documents(client, paths).await?);
    Ok(())
}

fn upstream_id(raw: u64) -> Result<u32, TmdbClientError> {
    u32::try_from(raw).map_err(|_| TmdbClientError::InvalidPath)
}

async fn hydrate_movie_galleries(
    client: &TmdbClient,
    movie: &mut tmdb_upstream::TmdbMovie,
    detail_raw: &mut Value,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let movie_id = upstream_id(movie.id)?;
    let mut documents = appended_named_documents(
        &format!("movie/{movie_id}"),
        detail_raw,
        MOVIE_APPENDED_DOCUMENTS,
    );
    if detail_raw
        .get("images")
        .is_none_or(|value| !value.is_object())
    {
        let (raw, images) =
            optional_gallery_with_raw(client.fetch_movie_images_with_raw(movie_id).await)?;
        movie.images = images;
        if let Some(response) = raw {
            detail_raw["images"] = response.clone();
            documents.push(CapturedDocument {
                endpoint_path: format!("movie/{movie_id}/images"),
                query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
    }
    if detail_raw
        .get("videos")
        .is_none_or(|value| !value.is_object())
    {
        let (raw, videos) = match client.fetch_movie_videos_with_raw(movie_id).await {
            Ok((response, videos)) => (Some(response), videos),
            Err(TmdbClientError::NotFound) => (None, TmdbVideos::default()),
            Err(error) => return Err(error),
        };
        movie.videos = videos;
        if let Some(response) = raw {
            detail_raw["videos"] = response.clone();
            documents.push(CapturedDocument {
                endpoint_path: format!("movie/{movie_id}/videos"),
                query_string: VIDEO_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
    }
    documents.extend(
        capture_optional_documents(
            client,
            [
                "account_states",
                "changes",
                "lists",
                "recommendations",
                "reviews",
                "similar",
                "watch/providers",
            ]
            .into_iter()
            .map(|suffix| format!("movie/{movie_id}/{suffix}"))
            .collect(),
        )
        .await?,
    );
    capture_linked_documents(client, &mut documents, &[("movie detail", detail_raw)]).await?;
    Ok(documents)
}

async fn hydrate_tv_galleries(
    client: &TmdbClient,
    series: &mut tmdb_upstream::TmdbTv,
    detail_raw: &mut Value,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let series_id = upstream_id(series.id)?;
    let mut documents = appended_named_documents(
        &format!("tv/{series_id}"),
        detail_raw,
        TV_APPENDED_DOCUMENTS,
    );
    if detail_raw
        .get("images")
        .is_none_or(|value| !value.is_object())
    {
        let (raw, images) =
            optional_gallery_with_raw(client.fetch_tv_images_with_raw(series_id).await)?;
        series.images = images;
        if let Some(response) = raw {
            detail_raw["images"] = response.clone();
            documents.push(CapturedDocument {
                endpoint_path: format!("tv/{series_id}/images"),
                query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
    }
    if detail_raw
        .get("videos")
        .is_none_or(|value| !value.is_object())
    {
        let (raw, videos) = match client.fetch_tv_videos_with_raw(series_id).await {
            Ok((response, videos)) => (Some(response), videos),
            Err(TmdbClientError::NotFound) => (None, TmdbVideos::default()),
            Err(error) => return Err(error),
        };
        series.videos = videos;
        if let Some(response) = raw {
            detail_raw["videos"] = response.clone();
            documents.push(CapturedDocument {
                endpoint_path: format!("tv/{series_id}/videos"),
                query_string: VIDEO_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
    }
    documents.extend(
        capture_optional_documents(
            client,
            [
                "account_states",
                "aggregate_credits",
                "changes",
                "episode_groups",
                "lists",
                "recommendations",
                "reviews",
                "screened_theatrically",
                "similar",
                "watch/providers",
            ]
            .into_iter()
            .map(|suffix| format!("tv/{series_id}/{suffix}"))
            .collect(),
        )
        .await?,
    );
    capture_linked_documents(client, &mut documents, &[("tv detail", detail_raw)]).await?;
    Ok(documents)
}

async fn hydrate_season_galleries(
    client: &TmdbClient,
    tv_id: u32,
    season_detail_raw: &Value,
    season: &mut tmdb_upstream::TmdbSeason,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let season_number = season.season_number;
    let season_path = format!("tv/{tv_id}/season/{season_number}");
    let mut documents =
        appended_named_documents(&season_path, season_detail_raw, SEASON_APPENDED_DOCUMENTS);
    let (season_raw, images) = if season_detail_raw
        .get("images")
        .is_some_and(Value::is_object)
    {
        (None, season.images.clone())
    } else {
        optional_gallery_with_raw(
            client
                .fetch_season_images_with_raw(tv_id, season_number)
                .await,
        )?
    };
    season.images = images;
    if let Some(response) = season_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("{season_path}/images"),
            query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    if season.id > 0 {
        let season_id = upstream_id(season.id)?;
        documents.extend(
            capture_optional_documents(client, vec![format!("tv/season/{season_id}/changes")])
                .await?,
        );
    }
    let mut pending = season.episodes.clone().into_iter().enumerate();
    let mut tasks = JoinSet::new();
    for _ in 0..UPSTREAM_ENRICHMENT_CONCURRENCY {
        let Some((index, episode)) = pending.next() else {
            break;
        };
        let client = client.clone();
        tasks.spawn(
            async move { enrich_episode(client, tv_id, season_number, index, episode).await },
        );
    }
    let mut enriched_episodes = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let enriched = result.map_err(|_| TmdbClientError::Transport)??;
        enriched_episodes.push(enriched);
        if let Some((index, episode)) = pending.next() {
            let client = client.clone();
            tasks.spawn(async move {
                enrich_episode(client, tv_id, season_number, index, episode).await
            });
        }
    }
    enriched_episodes.sort_by_key(|(index, _, _)| *index);
    for (index, episode, episode_documents) in enriched_episodes {
        season.episodes[index] = episode;
        documents.extend(episode_documents);
    }
    Ok(documents)
}

async fn enrich_episode(
    client: TmdbClient,
    tv_id: u32,
    season_number: u32,
    index: usize,
    mut episode: tmdb_upstream::TmdbEpisode,
) -> Result<(usize, tmdb_upstream::TmdbEpisode, Vec<CapturedDocument>), TmdbClientError> {
    let episode_number = episode.episode_number;
    let mut documents = Vec::new();
    let mut has_appended_images = false;
    match client
        .fetch_episode_with_raw(tv_id, season_number, episode_number)
        .await
    {
        Ok((response, fetched_episode)) => {
            episode = fetched_episode;
            has_appended_images = response.get("images").is_some_and(Value::is_object);
            let endpoint_path =
                format!("tv/{tv_id}/season/{season_number}/episode/{episode_number}");
            documents.extend(appended_named_documents(
                &endpoint_path,
                &response,
                EPISODE_APPENDED_DOCUMENTS,
            ));
            documents.extend(detail_documents(
                &endpoint_path,
                EPISODE_DETAIL_QUERY_STRING,
                response,
            ));
        }
        Err(TmdbClientError::NotFound) => {}
        Err(error) => return Err(error),
    }
    if episode.id > 0 {
        let episode_id = upstream_id(episode.id)?;
        documents.extend(
            capture_optional_documents(&client, vec![format!("tv/episode/{episode_id}/changes")])
                .await?,
        );
    }
    if !has_appended_images {
        let (episode_raw, images) = optional_gallery_with_raw(
            client
                .fetch_episode_images_with_raw(tv_id, season_number, episode_number)
                .await,
        )?;
        episode.images = images;
        if let Some(response) = episode_raw {
            documents.push(CapturedDocument {
                endpoint_path: format!(
                    "tv/{tv_id}/season/{season_number}/episode/{episode_number}/images"
                ),
                query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
    }
    Ok((index, episode, documents))
}

async fn persist_documents(
    database: &PgPool,
    documents: &[CapturedDocument],
) -> Result<(), JobExecutionError> {
    let documents = documents
        .iter()
        .map(|document| {
            (
                document.endpoint_path.clone(),
                document.query_string.clone(),
                document.response.clone(),
            )
        })
        .collect::<Vec<_>>();
    TmdbDocumentRepository::new(database.clone())
        .upsert_many(&documents)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
}

struct IngestTiming {
    job_id: Uuid,
    job_type: String,
    started: Instant,
}

impl IngestTiming {
    fn new(job: &ClaimedJob) -> Self {
        Self {
            job_id: job.job_id().as_uuid(),
            job_type: job.job_type().to_owned(),
            started: Instant::now(),
        }
    }
}

impl Drop for IngestTiming {
    fn drop(&mut self) {
        tracing::debug!(
            event = "ingest_job_duration",
            job_id = %self.job_id,
            job_type = %self.job_type,
            duration_ms = self.started.elapsed().as_millis(),
        );
    }
}

async fn load_movie_enrichment_source(
    client: &TmdbClient,
    database: &PgPool,
    tmdb_id: u32,
) -> Result<Option<(Value, tmdb_upstream::TmdbMovie)>, JobExecutionError> {
    let endpoint_path = format!("movie/{tmdb_id}");
    if let Some(raw) = TmdbDocumentRepository::new(database.clone())
        .get(&endpoint_path, MOVIE_DETAIL_QUERY_STRING)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
        && let Ok(movie) = serde_json::from_value(raw.clone())
    {
        return Ok(Some((raw, movie)));
    }
    match client.fetch_movie_with_raw(tmdb_id).await {
        Ok(movie) => Ok(Some(movie)),
        Err(TmdbClientError::NotFound) => Ok(None),
        Err(error) => Err(map_upstream_error(&error)),
    }
}

async fn load_tv_enrichment_source(
    client: &TmdbClient,
    database: &PgPool,
    tmdb_id: u32,
) -> Result<Option<(Value, tmdb_upstream::TmdbTv)>, JobExecutionError> {
    let endpoint_path = format!("tv/{tmdb_id}");
    if let Some(raw) = TmdbDocumentRepository::new(database.clone())
        .get(&endpoint_path, TV_DETAIL_QUERY_STRING)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
        && let Ok(series) = serde_json::from_value(raw.clone())
    {
        return Ok(Some((raw, series)));
    }
    match client.fetch_tv_with_raw(tmdb_id).await {
        Ok(series) => Ok(Some(series)),
        Err(TmdbClientError::NotFound) => Ok(None),
        Err(error) => Err(map_upstream_error(&error)),
    }
}

#[async_trait::async_trait]
impl JobExecutor for IngestExecutor {
    fn supported_job_types(&self) -> Option<&'static [&'static str]> {
        Some(INGEST_JOB_TYPES)
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, job: ClaimedJob) -> Result<Value, JobExecutionError> {
        let _timing = IngestTiming::new(&job);
        let parsed = parse_job(job.job_type(), job.payload_version(), job.payload())
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        let dedup_key = parsed.dedup_key();
        match parsed {
            IngestJob::RefreshMovie { tmdb_id, scope } => {
                let (movie_raw, movie) = match self.client.fetch_movie_with_raw(tmdb_id).await {
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
                let mut movie_documents = detail_documents(
                    &format!("movie/{}", movie.id),
                    MOVIE_DETAIL_QUERY_STRING,
                    movie_raw,
                );
                let appended = appended_named_documents(
                    &format!("movie/{}", movie.id),
                    &movie_documents[0].response,
                    MOVIE_APPENDED_DOCUMENTS,
                );
                movie_documents.extend(appended);
                if let Some(database) = &self.database {
                    let options = match scope {
                        RefreshScope::Full => catalog_write::CatalogWriteOptions::title_refresh(),
                        RefreshScope::CatalogOnly => {
                            catalog_write::CatalogWriteOptions::CATALOG_ONLY
                        }
                    };
                    catalog_write::persist_movie_with_options(database, &movie, options).await?;
                    persist_documents(database, &movie_documents).await?;
                }
                Ok(serde_json::json!({
                    "media_type":"movie",
                    "tmdb_id":movie.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::RefreshTv { tmdb_id, scope } => {
                let (series_raw, series) = match self.client.fetch_tv_with_raw(tmdb_id).await {
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
                let mut series_documents = detail_documents(
                    &format!("tv/{}", series.id),
                    TV_DETAIL_QUERY_STRING,
                    series_raw,
                );
                let appended = appended_named_documents(
                    &format!("tv/{}", series.id),
                    &series_documents[0].response,
                    TV_APPENDED_DOCUMENTS,
                );
                series_documents.extend(appended);
                if let Some(database) = &self.database {
                    let options = match scope {
                        RefreshScope::Full => catalog_write::CatalogWriteOptions::title_refresh(),
                        RefreshScope::CatalogOnly => {
                            catalog_write::CatalogWriteOptions::CATALOG_ONLY
                        }
                    };
                    catalog_write::persist_tv_with_options(database, &series, options).await?;
                    persist_documents(database, &series_documents).await?;
                }
                Ok(serde_json::json!({
                    "media_type":"tv",
                    "tmdb_id":series.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::EnrichMovie { tmdb_id, scope } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                let endpoint_path = format!("movie/{tmdb_id}");
                let Some((mut movie_raw, mut movie)) =
                    load_movie_enrichment_source(&self.client, database, tmdb_id).await?
                else {
                    return Ok(serde_json::json!({
                        "media_type": "movie",
                        "tmdb_id": tmdb_id,
                        "skipped": "upstream_not_found",
                        "dedup_key": dedup_key
                    }));
                };
                let gallery_documents =
                    match hydrate_movie_galleries(&self.client, &mut movie, &mut movie_raw).await {
                        Ok(documents) => documents,
                        Err(error) => {
                            self.record_upstream_state("degraded").await;
                            log_upstream_failure(
                                "movie_enrichment",
                                "movie",
                                tmdb_id,
                                None,
                                &error,
                            );
                            return Err(map_upstream_error(&error));
                        }
                    };
                let mut documents =
                    detail_documents(&endpoint_path, MOVIE_DETAIL_QUERY_STRING, movie_raw);
                documents.extend(gallery_documents);
                let options = match scope {
                    RefreshScope::Full => catalog_write::CatalogWriteOptions::title_enrichment(),
                    RefreshScope::CatalogOnly => catalog_write::CatalogWriteOptions::CATALOG_ONLY,
                };
                catalog_write::persist_movie_with_options(database, &movie, options).await?;
                persist_documents(database, &documents).await?;
                catalog_write::mark_title_enriched(database, "movie", source_id(movie.id)?).await?;
                Ok(serde_json::json!({
                    "phase": "enrichment",
                    "media_type": "movie",
                    "tmdb_id": movie.id,
                    "documents": documents.len(),
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::EnrichTv { tmdb_id, scope } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                let endpoint_path = format!("tv/{tmdb_id}");
                let Some((mut series_raw, mut series)) =
                    load_tv_enrichment_source(&self.client, database, tmdb_id).await?
                else {
                    return Ok(serde_json::json!({
                        "media_type": "tv",
                        "tmdb_id": tmdb_id,
                        "skipped": "upstream_not_found",
                        "dedup_key": dedup_key
                    }));
                };
                let gallery_documents =
                    match hydrate_tv_galleries(&self.client, &mut series, &mut series_raw).await {
                        Ok(documents) => documents,
                        Err(error) => {
                            self.record_upstream_state("degraded").await;
                            log_upstream_failure("tv_enrichment", "tv", tmdb_id, None, &error);
                            return Err(map_upstream_error(&error));
                        }
                    };
                let mut documents =
                    detail_documents(&endpoint_path, TV_DETAIL_QUERY_STRING, series_raw);
                documents.extend(gallery_documents);
                let options = match scope {
                    RefreshScope::Full => catalog_write::CatalogWriteOptions::title_enrichment(),
                    RefreshScope::CatalogOnly => catalog_write::CatalogWriteOptions::CATALOG_ONLY,
                };
                catalog_write::persist_tv_with_options(database, &series, options).await?;
                persist_documents(database, &documents).await?;
                catalog_write::mark_title_enriched(database, "tv", source_id(series.id)?).await?;
                Ok(serde_json::json!({
                    "phase": "enrichment",
                    "media_type": "tv",
                    "tmdb_id": series.id,
                    "documents": documents.len(),
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::ChangesSync {
                media_type,
                page,
                start_date,
                end_date,
            } => {
                let (change_raw, change_page) = match self
                    .client
                    .fetch_changes_window_with_raw(media_type, page, start_date, end_date)
                    .await
                {
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
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("{media_type}/changes"),
                        &format!("page={page}"),
                        &change_raw,
                    )
                    .await?;
                    let detail_refresh_candidates = enqueue_refresh_jobs_with_priority(
                        database,
                        media_type,
                        &changed_ids,
                        TITLE_REFRESH_PRIORITY,
                        RefreshScope::Full,
                        "changes_queue_full",
                    )
                    .await?;
                    if change_page.total_pages > page {
                        let next_page = page.saturating_add(1);
                        let next_job = NewJob::new(
                            CHANGES_SYNC_JOB,
                            INGEST_PAYLOAD_VERSION,
                            serde_json::json!({
                                "media_type": media_type,
                                "page": next_page,
                                "start_date": start_date,
                                "end_date": end_date
                            }),
                            &format!(
                                "{CHANGES_SYNC_JOB}:{media_type}:{start_date:?}:{end_date:?}:{next_page}"
                            ),
                        )
                        .and_then(|job| job.with_max_attempts(100))
                        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                        submit_ingest_child_job(database, next_job).await?;
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
                let (season_raw, mut season) = match self
                    .client
                    .fetch_season_with_raw(tv_id, season_number)
                    .await
                {
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
                let mut season_documents = detail_documents(
                    &format!("tv/{tv_id}/season/{season_number}"),
                    SEASON_DETAIL_QUERY_STRING,
                    season_raw.clone(),
                );
                match hydrate_season_galleries(&self.client, tv_id, &season_raw, &mut season).await
                {
                    Ok(documents) => season_documents.extend(documents),
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure(
                            "season_gallery",
                            "tv",
                            tv_id,
                            Some(season_number),
                            &error,
                        );
                        return Err(map_upstream_error(&error));
                    }
                }
                if let Some(database) = &self.database {
                    catalog_write::persist_season(database, tv_id, &season).await?;
                    persist_documents(database, &season_documents).await?;
                    catalog_write::mark_season_enriched(
                        database,
                        i64::from(tv_id),
                        i32::try_from(season_number)
                            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?,
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
            IngestJob::DailyExport {
                media_type,
                url,
                offset,
                refresh_all,
            } => {
                let digest = Sha256::digest(url.as_bytes());
                let destination = self
                    .export_root
                    .join(format!("{media_type}-{digest:x}.ndjson.gz"));
                let ids_destination = destination.with_extension("ids");
                tokio::fs::create_dir_all(&self.export_root)
                    .await
                    .map_err(|_| {
                        JobExecutionError::retry("export_storage", Duration::from_secs(30))
                    })?;
                let download = if tokio::fs::try_exists(&destination).await.map_err(|_| {
                    JobExecutionError::retry("export_storage", Duration::from_secs(30))
                })? {
                    None
                } else {
                    match self
                        .client
                        .fetch_daily_export_to_file(&url, &destination, self.export_max_bytes)
                        .await
                    {
                        Ok(download) => {
                            self.record_upstream_state("ready").await;
                            Some(download)
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
                    }
                };
                let queue_summary = if let Some(database) = &self.database {
                    ensure_export_id_file(
                        self.export_parser,
                        destination.clone(),
                        ids_destination.clone(),
                    )
                    .await?;
                    enqueue_daily_export_refresh_jobs(
                        database,
                        media_type,
                        ids_destination,
                        &url,
                        offset,
                        refresh_all,
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
                        continued: false,
                        next_offset: None,
                    }
                };
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "records": queue_summary.records,
                    "detail_refresh_candidates": queue_summary.detail_refresh_candidates,
                    "continued": queue_summary.continued,
                    "next_offset": queue_summary.next_offset,
                    "dedup_key": dedup_key,
                    "bytes": download.as_ref().map_or(0, |download| download.bytes),
                    "sha256": download.as_ref().map(|download| hex_digest(&download.sha256))
                }))
            }
            IngestJob::Configuration => {
                let response = match self.client.fetch_configuration().await {
                    Ok(response) => {
                        self.record_upstream_state("ready").await;
                        response
                    }
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("configuration", "configuration", 0, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                let related_documents = match capture_optional_documents(
                    &self.client,
                    GLOBAL_DOCUMENT_PATHS
                        .iter()
                        .map(|path| (*path).to_owned())
                        .collect(),
                )
                .await
                {
                    Ok(documents) => documents,
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("global_documents", "configuration", 0, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    catalog_write::persist_tmdb_document(database, "configuration", "", &response)
                        .await?;
                    persist_documents(database, &related_documents).await?;
                }
                Ok(serde_json::json!({
                    "endpoint": "configuration",
                    "related_documents": related_documents.len(),
                    "dedup_key": dedup_key
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
                let (trend_raw, trend_page) = match self
                    .client
                    .fetch_trending_with_raw(media_type, &trend_window)
                    .await
                {
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
                catalog_write::persist_tmdb_document(
                    database,
                    &format!("trending/{media_type}/{trend_window}"),
                    "",
                    &trend_raw,
                )
                .await?;
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "trend_window": trend_window,
                    "upstream_items": trend_page.results.len(),
                    "persisted": persisted,
                    "dedup_key": dedup_key,
                }))
            }
            IngestJob::AdminScan {
                mode,
                media_types,
                phase,
                cursor,
                poll,
                window_start,
                window_end,
            } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                if phase == AdminScanPhase::Start
                    && mode != AdminScanMode::MissingOnly
                    && cursor != 0
                {
                    return Err(JobExecutionError::dead_letter("invalid_payload"));
                }
                if phase != AdminScanPhase::Start {
                    return execute_catalog_scan_phase(
                        database,
                        mode,
                        &media_types,
                        phase,
                        cursor,
                        poll,
                        window_start,
                        window_end,
                    )
                    .await;
                }
                let configuration_queued = if mode != AdminScanMode::PruneCleanup && cursor == 0 {
                    submit_configuration_refresh(database).await?
                } else {
                    0
                };
                let mut jobs_pruned = 0_i32;
                let queued = match mode {
                    AdminScanMode::FullSweep => {
                        let export_date = Utc::now()
                            .date_naive()
                            .checked_sub_days(Days::new(1))
                            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                        let mut queued = configuration_queued;
                        for &media_type in &media_types {
                            for trend_window in ["day", "week"] {
                                let job = NewJob::new(
                                    TRENDING_REFRESH_JOB,
                                    INGEST_PAYLOAD_VERSION,
                                    serde_json::json!({
                                        "media_type": media_type,
                                        "trend_window": trend_window
                                    }),
                                    &format!("{TRENDING_REFRESH_JOB}:{media_type}:{trend_window}"),
                                )
                                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                                if submit_ingest_child_job(database, job)
                                    .await?
                                    .is_some_and(|outcome| !outcome.was_duplicate())
                                {
                                    queued = queued.saturating_add(1);
                                }
                            }
                            let job = export_job(media_type, export_date, true)?;
                            if submit_ingest_child_job(database, job)
                                .await?
                                .is_some_and(|outcome| !outcome.was_duplicate())
                            {
                                queued = queued.saturating_add(1);
                            }
                        }
                        queued
                    }
                    AdminScanMode::DailySync => {
                        let Some((window_start, window_end)) =
                            catalog_sync_window(database, window_start, window_end).await?
                        else {
                            return Ok(serde_json::json!({
                                "mode": mode,
                                "media_types": media_types,
                                "phase": phase,
                                "queued": configuration_queued,
                                "full_sweep_required": true,
                                "dedup_key": dedup_key
                            }));
                        };
                        let mut queued = configuration_queued;
                        for media_type in &media_types {
                            let job = NewJob::new(
                                CHANGES_SYNC_JOB,
                                INGEST_PAYLOAD_VERSION,
                                serde_json::json!({
                                    "media_type": media_type,
                                    "page": 1,
                                    "start_date": window_start,
                                    "end_date": window_end
                                }),
                                &format!(
                                    "{CHANGES_SYNC_JOB}:{media_type}:{window_start}:{window_end}:1"
                                ),
                            )
                            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                            if submit_ingest_child_job(database, job)
                                .await?
                                .is_some_and(|outcome| !outcome.was_duplicate())
                            {
                                queued = queued.saturating_add(1);
                            }
                        }
                        if submit_ingest_child_job(
                            database,
                            admin_scan_job_with_window(
                                mode,
                                &media_types,
                                AdminScanPhase::Completion,
                                0,
                                0,
                                Some(window_start),
                                Some(window_end),
                            )?,
                        )
                        .await?
                        .is_some_and(|outcome| !outcome.was_duplicate())
                        {
                            queued = queued.saturating_add(1);
                        }
                        queued
                    }
                    AdminScanMode::PruneCleanup => {
                        let export_date = Utc::now()
                            .date_naive()
                            .checked_sub_days(Days::new(1))
                            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                        let mut deleted = 0_usize;
                        for &media_type in &media_types {
                            let (media_type_name, file_prefix) = match media_type {
                                MediaType::Movie => ("movie", "movie_ids"),
                                MediaType::Tv => ("tv", "tv_series_ids"),
                            };
                            let date_text = export_date.format("%m_%d_%Y").to_string();
                            let url = format!(
                                "https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz"
                            );
                            let digest = Sha256::digest(url.as_bytes());
                            let destination = self
                                .export_root
                                .join(format!("{media_type}-{digest:x}.ndjson.gz"));
                            let ids_destination = destination.with_extension("ids");
                            tokio::fs::create_dir_all(&self.export_root)
                                .await
                                .map_err(|_| {
                                    JobExecutionError::retry(
                                        "export_storage",
                                        Duration::from_secs(30),
                                    )
                                })?;
                            if !tokio::fs::try_exists(&destination).await.map_err(|_| {
                                JobExecutionError::retry("export_storage", Duration::from_secs(30))
                            })? {
                                match self
                                    .client
                                    .fetch_daily_export_to_file(
                                        &url,
                                        &destination,
                                        self.export_max_bytes,
                                    )
                                    .await
                                {
                                    Ok(_) => self.record_upstream_state("ready").await,
                                    Err(error) => {
                                        self.record_upstream_state("degraded").await;
                                        tracing::warn!(
                                            event = "upstream_request_failed",
                                            operation = "prune_export",
                                            media_type = media_type_name,
                                            failure_reason = upstream_error_reason(&error),
                                            http_status = upstream_http_status(&error),
                                        );
                                        return Err(map_upstream_error(&error));
                                    }
                                }
                            }
                            ensure_export_id_file(
                                self.export_parser,
                                destination,
                                ids_destination.clone(),
                            )
                            .await?;
                            let removed =
                                prune_catalog_titles(database, media_type, ids_destination).await?;
                            deleted = deleted.saturating_add(removed);
                        }
                        jobs_pruned = sqlx::query_scalar("SELECT ops.prune_finished_jobs($1, $2)")
                            .bind(Utc::now() - ChronoDuration::days(30))
                            .bind(10_000_i32)
                            .fetch_one(database)
                            .await
                            .map_err(|_| {
                                JobExecutionError::retry(
                                    "database_unavailable",
                                    Duration::from_secs(5),
                                )
                            })?;
                        deleted
                    }
                    AdminScanMode::MissingOnly => {
                        let batch =
                            enqueue_missing_catalog_refresh_batch(database, &media_types, cursor)
                                .await?;
                        if batch.done {
                            for &media_type in &media_types {
                                submit_ingest_child_job(
                                    database,
                                    admin_scan_job(
                                        mode,
                                        &[media_type],
                                        AdminScanPhase::Enrichment,
                                        0,
                                        0,
                                    )?,
                                )
                                .await?;
                            }
                        } else {
                            submit_ingest_child_job(
                                database,
                                admin_scan_job(
                                    mode,
                                    &media_types,
                                    AdminScanPhase::Start,
                                    batch.next_cursor,
                                    0,
                                )?,
                            )
                            .await?;
                        }
                        batch.queued.saturating_add(configuration_queued)
                    }
                    AdminScanMode::Recovery => {
                        let export_date = Utc::now()
                            .date_naive()
                            .checked_sub_days(Days::new(1))
                            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                        let mut queued = configuration_queued;
                        for &media_type in &media_types {
                            for job in [
                                export_job(media_type, export_date, false)?,
                                admin_scan_job(
                                    mode,
                                    &[media_type],
                                    AdminScanPhase::Enrichment,
                                    0,
                                    0,
                                )?,
                            ] {
                                if submit_ingest_child_job(database, job)
                                    .await?
                                    .is_some_and(|outcome| !outcome.was_duplicate())
                                {
                                    queued = queued.saturating_add(1);
                                }
                            }
                        }
                        queued
                    }
                    AdminScanMode::Reconcile => execute_reconcile_start(
                        &self.client,
                        self.export_parser,
                        &self.export_root,
                        self.export_max_bytes,
                        database,
                        &media_types,
                    )
                    .await?
                    .saturating_add(configuration_queued),
                };
                Ok(serde_json::json!({
                    "mode": mode,
                    "media_types": media_types,
                    "phase": phase,
                    "queued": queued,
                    "deleted": (matches!(mode, AdminScanMode::PruneCleanup)).then_some(queued),
                    "jobs_pruned": (matches!(mode, AdminScanMode::PruneCleanup))
                        .then_some(jobs_pruned),
                    "cursor": cursor,
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

async fn submit_configuration_refresh(database: &PgPool) -> Result<usize, JobExecutionError> {
    let job = NewJob::new(
        CONFIGURATION_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({}),
        CONFIGURATION_JOB,
    )
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    Ok(usize::from(
        submit_ingest_child_job(database, job)
            .await?
            .is_some_and(|outcome| !outcome.was_duplicate()),
    ))
}

fn export_job(
    media_type: MediaType,
    export_date: NaiveDate,
    refresh_all: bool,
) -> Result<NewJob, JobExecutionError> {
    let (media_type_name, file_prefix) = match media_type {
        MediaType::Movie => ("movie", "movie_ids"),
        MediaType::Tv => ("tv", "tv_series_ids"),
    };
    let date_text = export_date.format("%m_%d_%Y").to_string();
    let dedup_key = if refresh_all {
        format!("{DAILY_EXPORT_JOB}:{media_type_name}:{date_text}:0")
    } else {
        format!("{DAILY_EXPORT_JOB}:recovery:{media_type_name}:{date_text}:0")
    };
    NewJob::new(
        DAILY_EXPORT_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({
            "media_type": media_type_name,
            "url": format!("https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz"),
            "offset": 0,
            "refresh_all": refresh_all
        }),
        &dedup_key,
    )
    .and_then(|job| job.with_max_attempts(100))
    .and_then(|job| job.with_priority(DAILY_EXPORT_COORDINATOR_PRIORITY))
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

async fn catalog_sync_window(
    database: &PgPool,
    requested_start: Option<NaiveDate>,
    requested_end: Option<NaiveDate>,
) -> Result<Option<(NaiveDate, NaiveDate)>, JobExecutionError> {
    let state: (Option<NaiveDate>, bool) = sqlx::query_as(
        "SELECT last_successful_window_end, full_sweep_required
           FROM ops.catalog_sync_state
          WHERE mode = 'daily_sync'",
    )
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if state.1 {
        return Ok(None);
    }
    let end = requested_end.unwrap_or_else(|| Utc::now().date_naive());
    let Some(start) = requested_start.or(state.0) else {
        sqlx::query_scalar::<_, bool>("SELECT ops.mark_catalog_full_sweep_required()")
            .fetch_one(database)
            .await
            .map_err(|_| {
                JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
            })?;
        return Ok(None);
    };
    if start > end || (end - start).num_days() > 13 {
        sqlx::query_scalar::<_, bool>("SELECT ops.mark_catalog_full_sweep_required()")
            .fetch_one(database)
            .await
            .map_err(|_| {
                JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
            })?;
        return Ok(None);
    }
    Ok(Some((start, end)))
}

async fn execute_reconcile_start(
    client: &TmdbClient,
    parser: DailyExportParser,
    export_root: &Path,
    export_max_bytes: u64,
    database: &PgPool,
    media_types: &[MediaType],
) -> Result<usize, JobExecutionError> {
    let export_date = Utc::now()
        .date_naive()
        .checked_sub_days(Days::new(1))
        .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
    let mut queued = 0_usize;
    for &media_type in media_types {
        let (name, prefix) = match media_type {
            MediaType::Movie => ("movie", "movie_ids"),
            MediaType::Tv => ("tv", "tv_series_ids"),
        };
        let date_text = export_date.format("%m_%d_%Y").to_string();
        let url = format!("https://files.tmdb.org/p/exports/{prefix}_{date_text}.json.gz");
        let digest = Sha256::digest(url.as_bytes());
        let destination = export_root.join(format!("{media_type}-{digest:x}.ndjson.gz"));
        let ids_destination = destination.with_extension("ids");
        tokio::fs::create_dir_all(export_root)
            .await
            .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
        if !tokio::fs::try_exists(&destination)
            .await
            .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        {
            client
                .fetch_daily_export_to_file(&url, &destination, export_max_bytes)
                .await
                .map_err(|error| map_upstream_error(&error))?;
        }
        ensure_export_id_file(parser, destination, ids_destination.clone()).await?;
        let deactivated = reconcile_catalog_titles(database, media_type, ids_destination).await?;
        tracing::info!(
            event = "catalog_reconcile_progress",
            media_type = name,
            deactivated,
        );
        for job in [
            export_job(media_type, export_date, false)?,
            admin_scan_job(
                AdminScanMode::Reconcile,
                &[media_type],
                AdminScanPhase::Enrichment,
                0,
                0,
            )?,
        ] {
            if submit_ingest_child_job(database, job)
                .await?
                .is_some_and(|outcome| !outcome.was_duplicate())
            {
                queued = queued.saturating_add(1);
            }
        }
    }
    Ok(queued)
}

async fn reconcile_catalog_titles(
    database: &PgPool,
    media_type: MediaType,
    ids_path: PathBuf,
) -> Result<usize, JobExecutionError> {
    let mut transaction = database
        .begin()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    sqlx::query(
        "CREATE TEMP TABLE tmdb_reconcile_ids (
             tmdb_id bigint PRIMARY KEY
         ) ON COMMIT DROP",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let file = tokio::fs::File::open(ids_path)
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
    let mut lines = TokioBufReader::new(file).lines();
    let mut ids = Vec::with_capacity(500);
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
    {
        let id = line
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0)
            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
        ids.push(id);
        if ids.len() == 500 {
            sqlx::query(
                "INSERT INTO tmdb_reconcile_ids (tmdb_id)
                 SELECT id FROM unnest($1::bigint[]) AS values(id)
                 ON CONFLICT (tmdb_id) DO NOTHING",
            )
            .bind(&ids)
            .execute(&mut *transaction)
            .await
            .map_err(|_| {
                JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
            })?;
            ids.clear();
        }
    }
    if !ids.is_empty() {
        sqlx::query(
            "INSERT INTO tmdb_reconcile_ids (tmdb_id)
             SELECT id FROM unnest($1::bigint[]) AS values(id)
             ON CONFLICT (tmdb_id) DO NOTHING",
        )
        .bind(&ids)
        .execute(&mut *transaction)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    }
    sqlx::query(
        "UPDATE catalog.titles AS title
            SET active = true, updated_at = clock_timestamp()
          WHERE title.media_type = $1 AND NOT title.active
            AND EXISTS (
                SELECT 1 FROM tmdb_reconcile_ids AS keep
                 WHERE keep.tmdb_id = title.tmdb_id
            )",
    )
    .bind(media_type_name(media_type))
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let deactivated = sqlx::query(
        "UPDATE catalog.titles AS title
            SET active = false, updated_at = clock_timestamp()
          WHERE title.media_type = $1 AND title.active
            AND NOT EXISTS (
                SELECT 1 FROM tmdb_reconcile_ids AS keep
                 WHERE keep.tmdb_id = title.tmdb_id
            )",
    )
    .bind(media_type_name(media_type))
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
    .rows_affected();
    transaction
        .commit()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    usize::try_from(deactivated).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

async fn prune_catalog_titles(
    database: &PgPool,
    media_type: MediaType,
    ids_path: PathBuf,
) -> Result<usize, JobExecutionError> {
    let mut transaction = database
        .begin()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    sqlx::query(
        "CREATE TEMP TABLE tmdb_prune_ids (
             tmdb_id bigint PRIMARY KEY
         ) ON COMMIT DROP",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;

    let file = tokio::fs::File::open(ids_path)
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
    let mut lines = TokioBufReader::new(file).lines();
    let mut ids = Vec::with_capacity(500);
    loop {
        let Some(line) = lines
            .next_line()
            .await
            .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        else {
            break;
        };
        let tmdb_id = line
            .trim()
            .parse::<u32>()
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        if tmdb_id == 0 {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
        ids.push(i64::from(tmdb_id));
        if ids.len() == 500 {
            insert_prune_ids(&mut transaction, &ids).await?;
            ids.clear();
        }
    }
    if !ids.is_empty() {
        insert_prune_ids(&mut transaction, &ids).await?;
    }
    let deleted = sqlx::query(
        "DELETE FROM catalog.titles AS title
          WHERE title.media_type = $1
            AND NOT EXISTS (
                SELECT 1
                  FROM tmdb_prune_ids AS keep
                 WHERE keep.tmdb_id = title.tmdb_id
            )",
    )
    .bind(media_type_name(media_type))
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
    .rows_affected();
    transaction
        .commit()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    usize::try_from(deleted).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

async fn insert_prune_ids(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &[i64],
) -> Result<(), JobExecutionError> {
    sqlx::query(
        "INSERT INTO tmdb_prune_ids (tmdb_id)
         SELECT id FROM unnest($1::bigint[]) AS values(id)
         ON CONFLICT (tmdb_id) DO NOTHING",
    )
    .bind(ids)
    .execute(&mut **transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(())
}

const CATALOG_ENRICHMENT_BATCH_SIZE: usize = 100;
const CATALOG_SEASON_BATCH_SIZE: usize = 25;
const CATALOG_REFRESH_BATCH_SIZE: i64 = 500;

#[derive(Debug, FromRow)]
struct MissingCatalogRow {
    id: i64,
    tmdb_id: i64,
    media_type: String,
}

struct MissingCatalogBatch {
    queued: usize,
    next_cursor: u64,
    done: bool,
}

struct CatalogPhaseBatch {
    queued: usize,
    next_cursor: u64,
    done: bool,
}

#[derive(Debug, FromRow)]
struct CatalogEnrichmentRow {
    id: i64,
    tmdb_id: i64,
}

async fn enqueue_missing_catalog_refresh_batch(
    database: &PgPool,
    media_types: &[MediaType],
    cursor: u64,
) -> Result<MissingCatalogBatch, JobExecutionError> {
    let cursor_i64 =
        i64::try_from(cursor).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let media_type_names = media_types
        .iter()
        .copied()
        .map(media_type_name)
        .collect::<Vec<_>>();
    let mut rows: Vec<MissingCatalogRow> = sqlx::query_as(
        "SELECT id, tmdb_id, media_type
           FROM catalog.titles
          WHERE active
            AND media_type = ANY($1)
            AND id > $2
            AND (source_updated_at IS NULL OR display_title IS NULL)
          ORDER BY id
          LIMIT $3",
    )
    .bind(&media_type_names)
    .bind(cursor_i64)
    .bind(CATALOG_REFRESH_BATCH_SIZE + 1)
    .fetch_all(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;

    let batch_size = usize::try_from(CATALOG_REFRESH_BATCH_SIZE).unwrap_or(usize::MAX);
    let done = rows.len() <= batch_size;
    rows.truncate(batch_size);
    let jobs = rows
        .iter()
        .map(|row| {
            let tmdb_id = u32::try_from(row.tmdb_id)
                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            let job_type = match row.media_type.as_str() {
                "movie" => REFRESH_MOVIE_JOB,
                "tv" => REFRESH_TV_JOB,
                _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
            };
            NewJob::new(
                job_type,
                INGEST_PAYLOAD_VERSION,
                serde_json::json!({"tmdb_id": tmdb_id}),
                &format!("{job_type}:{tmdb_id}"),
            )
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let queued = submit_refresh_jobs_with_capacity(database, &jobs, "missing_queue_full").await?;
    let next_cursor = rows
        .last()
        .map(|row| u64::try_from(row.id))
        .transpose()
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?
        .unwrap_or(cursor);
    Ok(MissingCatalogBatch {
        queued,
        next_cursor,
        done,
    })
}

async fn enqueue_catalog_enrichment_batch(
    database: &PgPool,
    mode: AdminScanMode,
    media_type: MediaType,
    cursor: u64,
) -> Result<CatalogPhaseBatch, JobExecutionError> {
    let cursor_i64 =
        i64::try_from(cursor).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let limit = i64::try_from(CATALOG_ENRICHMENT_BATCH_SIZE + 1)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let query = match mode {
        AdminScanMode::FullSweep => {
            "SELECT id, tmdb_id
               FROM catalog.titles
              WHERE active
                AND media_type = $1
                AND id > $2
              ORDER BY id
              LIMIT $3"
        }
        AdminScanMode::Recovery | AdminScanMode::Reconcile | AdminScanMode::MissingOnly => {
            "SELECT id, tmdb_id
               FROM catalog.titles
              WHERE active
                AND media_type = $1
                AND id > $2
                AND enriched_at IS NULL
              ORDER BY id
              LIMIT $3"
        }
        AdminScanMode::PruneCleanup | AdminScanMode::DailySync => {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
    };
    let mut rows: Vec<CatalogEnrichmentRow> = sqlx::query_as(query)
        .bind(media_type_name(media_type))
        .bind(cursor_i64)
        .bind(limit)
        .fetch_all(database)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let done = rows.len() <= CATALOG_ENRICHMENT_BATCH_SIZE;
    rows.truncate(CATALOG_ENRICHMENT_BATCH_SIZE);

    let job_type = match media_type {
        MediaType::Movie => ENRICH_MOVIE_JOB,
        MediaType::Tv => ENRICH_TV_JOB,
    };
    let jobs = rows
        .iter()
        .map(|row| {
            let tmdb_id = u32::try_from(row.tmdb_id)
                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            NewJob::new(
                job_type,
                INGEST_PAYLOAD_VERSION,
                serde_json::json!({
                    "tmdb_id": tmdb_id,
                    "scope": RefreshScope::CatalogOnly
                }),
                &format!("{job_type}:{tmdb_id}"),
            )
            .and_then(|job| job.with_priority(ENRICHMENT_PRIORITY))
            .and_then(|job| job.with_max_attempts(8))
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let queued = if jobs.is_empty() {
        0
    } else {
        submit_ingest_child_jobs(database, &jobs)
            .await?
            .iter()
            .filter(|outcome| !outcome.was_duplicate())
            .count()
    };
    let next_cursor = rows
        .last()
        .map(|row| u64::try_from(row.id))
        .transpose()
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?
        .unwrap_or(cursor);
    Ok(CatalogPhaseBatch {
        queued,
        next_cursor,
        done,
    })
}

#[derive(Debug, FromRow)]
struct CatalogSeasonRow {
    id: i64,
    tmdb_id: i64,
    season_number: i32,
}

async fn enqueue_catalog_season_batch(
    database: &PgPool,
    mode: AdminScanMode,
    cursor: u64,
) -> Result<CatalogPhaseBatch, JobExecutionError> {
    let cursor_i64 =
        i64::try_from(cursor).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let limit = i64::try_from(CATALOG_SEASON_BATCH_SIZE + 1)
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let query = match mode {
        AdminScanMode::FullSweep => {
            "SELECT season.id, title.tmdb_id, season.season_number
               FROM catalog.seasons AS season
               JOIN catalog.titles AS title ON title.id = season.title_id
              WHERE title.active
                AND title.media_type = 'tv'
                AND season.id > $1
              ORDER BY season.id
              LIMIT $2"
        }
        AdminScanMode::Recovery | AdminScanMode::Reconcile | AdminScanMode::MissingOnly => {
            "SELECT season.id, title.tmdb_id, season.season_number
               FROM catalog.seasons AS season
               JOIN catalog.titles AS title ON title.id = season.title_id
              WHERE title.active
                AND title.media_type = 'tv'
                AND season.id > $1
                AND season.enriched_at IS NULL
              ORDER BY season.id
              LIMIT $2"
        }
        AdminScanMode::PruneCleanup | AdminScanMode::DailySync => {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
    };
    let mut rows: Vec<CatalogSeasonRow> = sqlx::query_as(query)
        .bind(cursor_i64)
        .bind(limit)
        .fetch_all(database)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let done = rows.len() <= CATALOG_SEASON_BATCH_SIZE;
    rows.truncate(CATALOG_SEASON_BATCH_SIZE);
    let jobs = rows
        .iter()
        .map(|row| {
            let tmdb_id = u32::try_from(row.tmdb_id)
                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            let season_number = u32::try_from(row.season_number)
                .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
            NewJob::new(
                REFRESH_SEASON_JOB,
                INGEST_PAYLOAD_VERSION,
                serde_json::json!({
                    "tv_id": tmdb_id,
                    "season_number": season_number,
                    "scope": RefreshScope::CatalogOnly
                }),
                &format!("{REFRESH_SEASON_JOB}:{tmdb_id}:{season_number}"),
            )
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let queued = if jobs.is_empty() {
        0
    } else {
        submit_ingest_child_jobs(database, &jobs)
            .await?
            .iter()
            .filter(|outcome| !outcome.was_duplicate())
            .count()
    };
    let next_cursor = rows
        .last()
        .map(|row| u64::try_from(row.id))
        .transpose()
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?
        .unwrap_or(cursor);
    Ok(CatalogPhaseBatch {
        queued,
        next_cursor,
        done,
    })
}

fn admin_scan_job(
    mode: AdminScanMode,
    media_types: &[MediaType],
    phase: AdminScanPhase,
    cursor: u64,
    poll: u32,
) -> Result<NewJob, JobExecutionError> {
    admin_scan_job_with_window(mode, media_types, phase, cursor, poll, None, None)
}

fn admin_scan_job_with_window(
    mode: AdminScanMode,
    media_types: &[MediaType],
    phase: AdminScanPhase,
    cursor: u64,
    poll: u32,
    window_start: Option<NaiveDate>,
    window_end: Option<NaiveDate>,
) -> Result<NewJob, JobExecutionError> {
    NewJob::new(
        ADMIN_SCAN_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({
            "mode": mode,
            "mediaTypes": media_types,
            "phase": phase,
            "cursor": cursor,
            "poll": poll,
            "windowStart": window_start,
            "windowEnd": window_end
        }),
        &format!(
            "{ADMIN_SCAN_JOB}:{mode:?}:{media_types:?}:{phase:?}:{cursor}:{poll}:{window_start:?}:{window_end:?}"
        ),
    )
    .and_then(|job| job.with_max_attempts(100))
    .and_then(|job| job.with_priority(CATALOG_PHASE_COORDINATOR_PRIORITY))
    .and_then(|job| {
        job.with_available_at(Utc::now() + ChronoDuration::seconds(CATALOG_PHASE_POLL_SECONDS))
    })
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the durable scan state is explicit and the phase transitions are clearest in one coordinator"
)]
async fn execute_catalog_scan_phase(
    database: &PgPool,
    mode: AdminScanMode,
    media_types: &[MediaType],
    phase: AdminScanPhase,
    cursor: u64,
    poll: u32,
    window_start: Option<NaiveDate>,
    window_end: Option<NaiveDate>,
) -> Result<Value, JobExecutionError> {
    if phase == AdminScanPhase::Completion {
        if !matches!(
            mode,
            AdminScanMode::FullSweep
                | AdminScanMode::Recovery
                | AdminScanMode::Reconcile
                | AdminScanMode::DailySync
                | AdminScanMode::MissingOnly
        ) {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
        if catalog_completion_has_active_work(database).await? {
            let next_poll = poll
                .checked_add(1)
                .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
            submit_ingest_child_job(
                database,
                admin_scan_job_with_window(
                    mode,
                    media_types,
                    phase,
                    cursor,
                    next_poll,
                    window_start,
                    window_end,
                )?,
            )
            .await?;
            return Ok(serde_json::json!({
                "mode": mode,
                "media_types": media_types,
                "phase": phase,
                "poll": poll,
                "waiting": true,
            }));
        }
        if catalog_completion_has_unresolved_failures(database, mode).await? {
            return Ok(serde_json::json!({
                "mode": mode,
                "media_types": media_types,
                "phase": phase,
                "completed": false,
                "watermark_advanced": false,
                "failure_reason": "unresolved_child_jobs",
            }));
        }
        let completed_for = window_end.unwrap_or_else(|| Utc::now().date_naive());
        let completed = if mode == AdminScanMode::Recovery {
            true
        } else {
            sqlx::query_scalar("SELECT ops.complete_catalog_sync($1, $2)")
                .bind(scan_mode_name(mode))
                .bind(completed_for)
                .fetch_one(database)
                .await
                .map_err(|_| {
                    JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
                })?
        };
        return Ok(serde_json::json!({
            "mode": mode,
            "media_types": media_types,
            "phase": phase,
            "completed_for": completed_for,
            "watermark_advanced": completed,
        }));
    }
    if !matches!(
        mode,
        AdminScanMode::FullSweep
            | AdminScanMode::Recovery
            | AdminScanMode::Reconcile
            | AdminScanMode::MissingOnly
    ) || media_types.len() != 1
    {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    if catalog_scan_phase_has_active_work(database, phase).await? {
        let next_poll = poll
            .checked_add(1)
            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
        submit_ingest_child_job(
            database,
            admin_scan_job_with_window(
                mode,
                media_types,
                phase,
                cursor,
                next_poll,
                window_start,
                window_end,
            )?,
        )
        .await?;
        return Ok(serde_json::json!({
            "mode": mode,
            "media_types": media_types,
            "phase": phase,
            "cursor": cursor,
            "poll": poll,
            "waiting": true,
        }));
    }

    let media_type = media_types[0];
    let batch = match phase {
        AdminScanPhase::Enrichment => {
            enqueue_catalog_enrichment_batch(database, mode, media_type, cursor).await?
        }
        AdminScanPhase::Seasons if media_type == MediaType::Tv => {
            enqueue_catalog_season_batch(database, mode, cursor).await?
        }
        AdminScanPhase::Start | AdminScanPhase::Seasons | AdminScanPhase::Completion => {
            return Err(JobExecutionError::dead_letter("invalid_payload"));
        }
    };

    if !batch.done {
        submit_ingest_child_job(
            database,
            admin_scan_job_with_window(
                mode,
                media_types,
                phase,
                batch.next_cursor,
                0,
                window_start,
                window_end,
            )?,
        )
        .await?;
    } else if phase == AdminScanPhase::Enrichment && media_type == MediaType::Tv {
        submit_ingest_child_job(
            database,
            admin_scan_job(mode, media_types, AdminScanPhase::Seasons, 0, 0)?,
        )
        .await?;
    } else {
        submit_ingest_child_job(
            database,
            admin_scan_job_with_window(
                mode,
                media_types,
                AdminScanPhase::Completion,
                0,
                0,
                window_start,
                window_end,
            )?,
        )
        .await?;
    }

    Ok(serde_json::json!({
        "mode": mode,
        "media_types": media_types,
        "phase": phase,
        "queued": batch.queued,
        "cursor": cursor,
        "next_cursor": batch.next_cursor,
        "continued": !batch.done,
    }))
}

async fn catalog_completion_has_active_work(database: &PgPool) -> Result<bool, JobExecutionError> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM ops.jobs
              WHERE status IN ('queued', 'running', 'retry_wait')
                AND job_type LIKE 'ingest.%'
         )",
    )
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
}

async fn catalog_completion_has_unresolved_failures(
    database: &PgPool,
    mode: AdminScanMode,
) -> Result<bool, JobExecutionError> {
    sqlx::query_scalar(
        "WITH root AS (
             SELECT pg_catalog.max(job.created_at) AS started_at
               FROM ops.jobs AS job
              WHERE job.job_type = 'admin.scan'
                AND job.payload ->> 'mode' = $1
                AND NOT (job.payload ? 'phase')
         )
         SELECT root.started_at IS NULL OR EXISTS (
             SELECT 1
               FROM ops.jobs AS failed
              WHERE failed.status = 'dead_letter'
                AND failed.job_type LIKE 'ingest.%'
                AND failed.created_at >= root.started_at
                AND NOT EXISTS (
                    SELECT 1
                      FROM ops.jobs AS recovered
                     WHERE recovered.job_type = failed.job_type
                       AND recovered.dedup_key = failed.dedup_key
                       AND recovered.status = 'succeeded'
                       AND recovered.finished_at > failed.finished_at
                )
         )
           FROM root",
    )
    .bind(scan_mode_name(mode))
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
}

const fn scan_mode_name(mode: AdminScanMode) -> &'static str {
    match mode {
        AdminScanMode::FullSweep => "full_sweep",
        AdminScanMode::MissingOnly => "missing_only",
        AdminScanMode::Recovery => "recovery",
        AdminScanMode::PruneCleanup => "prune_cleanup",
        AdminScanMode::DailySync => "daily_sync",
        AdminScanMode::Reconcile => "reconcile",
    }
}

async fn catalog_scan_phase_has_active_work(
    database: &PgPool,
    phase: AdminScanPhase,
) -> Result<bool, JobExecutionError> {
    let include_seasons = phase == AdminScanPhase::Seasons;
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.jobs
              WHERE status IN ('queued', 'running', 'retry_wait')
                AND (
                    job_type = 'ingest.daily_export'
                    OR (
                        job_type IN (
                            'ingest.refresh_movie', 'ingest.refresh_tv',
                            'ingest.enrich_movie', 'ingest.enrich_tv'
                        )
                        AND payload ->> 'scope' = 'catalog_only'
                    )
                    OR (
                        $1
                        AND job_type = 'ingest.refresh_season'
                        AND payload ->> 'scope' = 'catalog_only'
                    )
                )
         )",
    )
    .bind(include_seasons)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
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
    season_number: Option<u32>,
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

    async fn enable_ingest_worker(pool: &PgPool) -> sqlx::Result<()> {
        sqlx::query(
            "UPDATE ops.worker_control
                SET state = 'running', updated_at = clock_timestamp()
              WHERE worker_kind = 'ingest'",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

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
    fn optional_gallery_not_found_is_empty_but_other_errors_are_preserved() {
        assert!(matches!(
            optional_gallery_with_raw(Err(TmdbClientError::NotFound)),
            Ok((None, gallery)) if gallery.posters.is_empty()
        ));
        assert!(matches!(
            optional_gallery_with_raw(Err(TmdbClientError::Unauthorized)),
            Err(TmdbClientError::Unauthorized)
        ));
    }

    #[test]
    fn linked_tmdb_documents_are_bounded_and_deduplicated() {
        let detail = serde_json::json!({
            "credits": {
                "cast": [
                    {"id": 10, "credit_id": "credit-b"},
                    {"id": 11, "credit_id": "credit-a"},
                    {"id": 12, "credit_id": "credit-b"}
                ],
                "crew": [{"id": 13, "credit_id": "credit-c"}]
            },
            "keywords": {"keywords": [{"id": 20}, {"id": 20}]},
            "episode_groups": {"results": [{"id": "group-1"}]}
        });
        let reviews = serde_json::json!({
            "results": [{"id": "review-1"}, {"id": "review-1"}]
        });
        let documents = vec![CapturedDocument {
            endpoint_path: "movie/42/reviews".to_owned(),
            query_string: String::new(),
            response: reviews,
        }];

        let paths = linked_document_paths(&documents, &[("movie/42", &detail)]);

        assert_eq!(
            paths,
            vec![
                "credit/credit-a",
                "credit/credit-b",
                "credit/credit-c",
                "keyword/20",
                "keyword/20/movies",
                "review/review-1",
                "tv/episode_group/group-1"
            ]
        );
    }

    #[test]
    fn linked_tmdb_documents_ignore_unsafe_identifiers() {
        let detail = serde_json::json!({
            "credits": {"cast": [{"credit_id": "../secret"}]},
            "keywords": {"keywords": [{"id": 0}, {"id": -1}]},
            "episode_groups": {"results": [{"id": "group/unsafe"}]}
        });

        assert!(linked_document_paths(&[], &[("tv/1", &detail)]).is_empty());
    }

    #[test]
    fn appended_movie_documents_are_captured_without_extra_requests() {
        let raw = serde_json::json!({
            "keywords": {"keywords": []},
            "credits": {"cast": [], "crew": []},
            "translations": {"translations": []},
            "alternative_titles": {"titles": []},
            "external_ids": {},
            "images": {"posters": []},
            "videos": {"results": []},
            "release_dates": {"results": []}
        });
        let documents = appended_named_documents("movie/42", &raw, MOVIE_APPENDED_DOCUMENTS);
        let paths = documents
            .iter()
            .map(|document| document.endpoint_path.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            [
                "movie/42/keywords",
                "movie/42/credits",
                "movie/42/translations",
                "movie/42/alternative_titles",
                "movie/42/external_ids",
                "movie/42/videos",
                "movie/42/release_dates",
                "movie/42/images",
            ]
        );
        assert_eq!(documents[5].query_string, VIDEO_GALLERY_QUERY_STRING);
        assert_eq!(documents[7].query_string, IMAGE_GALLERY_QUERY_STRING);
    }

    #[test]
    fn appended_season_and_episode_documents_keep_local_routes() {
        let raw = serde_json::json!({
            "credits": {"cast": []},
            "videos": {"results": []},
            "watch/providers": {"results": []}
        });
        let season = appended_named_documents("tv/42/season/1", &raw, SEASON_APPENDED_DOCUMENTS);
        assert_eq!(season.len(), 3);
        assert_eq!(season[0].endpoint_path, "tv/42/season/1/credits");
        assert_eq!(season[0].query_string, "");
        assert_eq!(season[1].endpoint_path, "tv/42/season/1/videos");
        assert_eq!(season[1].query_string, VIDEO_GALLERY_QUERY_STRING);
        assert_eq!(season[2].endpoint_path, "tv/42/season/1/watch/providers");

        let episode =
            appended_named_documents("tv/42/season/1/episode/1", &raw, EPISODE_APPENDED_DOCUMENTS);
        assert_eq!(episode.len(), 2);
        assert_eq!(episode[0].endpoint_path, "tv/42/season/1/episode/1/credits");
        assert_eq!(episode[1].endpoint_path, "tv/42/season/1/episode/1/videos");
    }

    #[test]
    fn detail_documents_keep_the_public_empty_query_without_an_extra_request() {
        let documents = detail_documents(
            "movie/42",
            MOVIE_DETAIL_QUERY_STRING,
            serde_json::json!({"id": 42, "images": {"posters": []}}),
        );

        assert_eq!(documents.len(), 2);
        assert_eq!(documents[0].query_string, MOVIE_DETAIL_QUERY_STRING);
        assert_eq!(documents[1].query_string, "");
        assert_eq!(documents[0].response, documents[1].response);
    }

    #[test]
    fn payloads_are_strict_and_dedup_keys_are_stable() -> Result<(), Box<dyn std::error::Error>> {
        let first = parse_job(REFRESH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":42}))?;
        let second = parse_job(REFRESH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":42}))?;
        assert_eq!(first, second);
        assert_eq!(first.dedup_key(), "ingest.refresh_movie:42");
        assert_eq!(
            parse_job(ENRICH_MOVIE_JOB, 1, &serde_json::json!({"tmdb_id":42}))?.dedup_key(),
            "ingest.enrich_movie:42"
        );
        assert_eq!(
            parse_job(ENRICH_TV_JOB, 1, &serde_json::json!({"tmdb_id":42}))?.dedup_key(),
            "ingest.enrich_tv:42"
        );
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
        let large_number = parse_job(
            REFRESH_SEASON_JOB,
            1,
            &serde_json::json!({"tv_id":134_819,"season_number":120_120_224}),
        )?;
        assert_eq!(
            large_number.dedup_key(),
            "ingest.refresh_season:134819:120120224"
        );
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
    fn catalog_scan_modes_are_explicit_and_recovery_phases_are_internal() {
        for mode in [
            "full_sweep",
            "missing_only",
            "recovery",
            "prune_cleanup",
            "daily_sync",
        ] {
            let job = parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": mode,
                    "mediaTypes": ["movie"],
                    "cursor": 0
                }),
            );
            assert!(job.is_ok(), "mode {mode} was rejected");
        }
        assert!(matches!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "missing_only",
                    "mediaTypes": ["movie"],
                    "cursor": i64::MAX as u64 + 1
                }),
            ),
            Err(JobPayloadError::InvalidValue)
        ));
        assert!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "full_sweep",
                    "mediaTypes": ["tv"],
                    "phase": "enrichment",
                    "cursor": 42
                }),
            )
            .is_ok()
        );
        assert!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "missing_only",
                    "mediaTypes": ["movie"],
                    "cursor": 42
                }),
            )
            .is_ok()
        );
        assert!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "recovery",
                    "mediaTypes": ["movie"],
                    "phase": "enrichment",
                    "cursor": 42,
                    "poll": 7
                }),
            )
            .is_ok()
        );
        assert!(matches!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "daily_sync",
                    "mediaTypes": ["tv"],
                    "phase": "enrichment"
                }),
            ),
            Err(JobPayloadError::InvalidValue)
        ));
        assert!(matches!(
            parse_job(
                ADMIN_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "mode": "recovery",
                    "mediaTypes": ["movie", "tv"],
                    "phase": "enrichment"
                }),
            ),
            Err(JobPayloadError::InvalidValue)
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

    #[test]
    fn upstream_heartbeat_claims_are_throttled() {
        let slot = AtomicU64::new(0);
        assert!(claim_heartbeat_slot(&slot, 10, 5));
        assert!(!claim_heartbeat_slot(&slot, 12, 5));
        assert!(claim_heartbeat_slot(&slot, 15, 5));
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn changed_ids_enqueue_idempotent_detail_refresh_jobs(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        enqueue_refresh_jobs_with_priority(
            &pool,
            MediaType::Movie,
            &[42, 42, 43],
            TITLE_REFRESH_PRIORITY,
            RefreshScope::Full,
            "changes_queue_full",
        )
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
    async fn stopped_ingest_worker_rejects_child_refresh_fanout(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query(
            "UPDATE ops.worker_control
                SET state = 'stopped', updated_at = clock_timestamp()
              WHERE worker_kind = 'ingest'",
        )
        .execute(&pool)
        .await?;
        let submitted = enqueue_refresh_jobs_with_priority(
            &pool,
            MediaType::Movie,
            &[42],
            TITLE_REFRESH_PRIORITY,
            RefreshScope::Full,
            "changes_queue_full",
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(submitted, 0);
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.refresh_movie'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn ingest_cancel_catches_an_inflight_child_submission(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        let mut transaction = pool.begin().await?;
        assert!(
            ingest_child_submissions_enabled(&mut transaction)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
        );

        let cancel_pool = pool.clone();
        let cancel = tokio::spawn(async move {
            sqlx::query_scalar::<_, String>(
                "SELECT state
                   FROM ops.set_worker_state(
                       'ingest', 'cancel', 'inflight-child-cancel', gen_random_uuid()
                   )",
            )
            .fetch_one(&cancel_pool)
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!cancel.is_finished());

        let job = NewJob::new(
            REFRESH_MOVIE_JOB,
            INGEST_PAYLOAD_VERSION,
            serde_json::json!({"tmdb_id": 42}),
            "inflight-child-refresh",
        )
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        JobRepository::submit_many_in_transaction(&mut transaction, &[job])
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        transaction.commit().await?;

        let state = cancel
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))??;
        assert_eq!(state, "stopped");
        let job_status: String = sqlx::query_scalar(
            "SELECT status FROM ops.jobs WHERE dedup_key = 'inflight-child-refresh'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(job_status, "cancelled");
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_enqueues_detail_refresh_jobs(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        let export = tempfile::NamedTempFile::new()?;
        std::fs::write(export.path(), "51\n52\n51\n")?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            false,
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
        assert_eq!(rows[0].1["scope"], "catalog_only");
        assert_eq!(rows[1].1["scope"], "catalog_only");
        assert_eq!(rows[0].3, TITLE_REFRESH_PRIORITY);
        assert_eq!(rows[1].3, TITLE_REFRESH_PRIORITY);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_does_not_requeue_loaded_catalog_titles(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (
                 media_type, tmdb_id, display_title, source_updated_at
             ) VALUES ('movie', 51, 'Already loaded', clock_timestamp())",
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
        std::fs::write(export.path(), "51\n52\n")?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            false,
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
    async fn full_export_requeues_loaded_catalog_titles(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
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
        std::fs::write(export.path(), "51\n52\n")?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            true,
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(summary.detail_refresh_candidates, 2);
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT (payload ->> 'tmdb_id')::bigint
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_movie'
              ORDER BY 1",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(ids, [51, 52]);

        let phase_payload: Value = sqlx::query_scalar(
            "SELECT payload
               FROM ops.jobs
              WHERE job_type = 'admin.scan'
                AND payload ->> 'phase' = 'enrichment'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(phase_payload["mode"], "full_sweep");
        assert_eq!(phase_payload["mediaTypes"], serde_json::json!(["movie"]));
        assert_eq!(phase_payload["cursor"], 0);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_continuation_keeps_refresh_queue_bounded(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        let export = tempfile::NamedTempFile::new()?;
        let contents = (1..=501)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(export.path(), contents)?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            false,
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(summary.records, DAILY_EXPORT_BATCH_SIZE);
        assert_eq!(summary.detail_refresh_candidates, DAILY_EXPORT_BATCH_SIZE);
        assert!(summary.continued);
        let refresh_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.refresh_movie'",
        )
        .fetch_one(&pool)
        .await?;
        let continuation_priorities: Vec<i16> = sqlx::query_scalar(
            "SELECT priority FROM ops.jobs WHERE job_type = 'ingest.daily_export'",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(refresh_count, 500);
        assert_eq!(continuation_priorities, [DAILY_EXPORT_COORDINATOR_PRIORITY]);
        assert!(continuation_priorities[0] > TITLE_REFRESH_PRIORITY);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_waits_when_the_refresh_queue_is_full(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        let repository = JobRepository::new(pool.clone());
        let jobs = (1..=MAX_PENDING_REFRESH_JOBS)
            .map(|tmdb_id| {
                NewJob::new(
                    REFRESH_MOVIE_JOB,
                    INGEST_PAYLOAD_VERSION,
                    serde_json::json!({"tmdb_id": tmdb_id}),
                    &format!("{REFRESH_MOVIE_JOB}:{tmdb_id}"),
                )
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for batch in jobs.chunks(500) {
            repository
                .submit_many(batch)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        }

        let export = tempfile::NamedTempFile::new()?;
        std::fs::write(export.path(), "1001\n")?;
        let result = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            false,
        )
        .await;

        assert!(matches!(
            result,
            Err(error) if error.failure_code() == "export_queue_incomplete"
        ));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn concurrent_daily_exports_report_backpressure_instead_of_database_failure(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        let repository = JobRepository::new(pool.clone());
        let existing = (1..=500)
            .map(|tmdb_id| {
                NewJob::new(
                    REFRESH_MOVIE_JOB,
                    INGEST_PAYLOAD_VERSION,
                    serde_json::json!({"tmdb_id": tmdb_id}),
                    &format!("existing-refresh:{tmdb_id}"),
                )
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        repository
            .submit_many(&existing)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        let first_export = tempfile::NamedTempFile::new()?;
        let second_export = tempfile::NamedTempFile::new()?;
        std::fs::write(
            first_export.path(),
            (1_001..=1_500)
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )?;
        std::fs::write(
            second_export.path(),
            (2_001..=2_500)
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )?;

        let (first, second) = tokio::join!(
            enqueue_daily_export_refresh_jobs(
                &pool,
                MediaType::Movie,
                first_export.path().to_path_buf(),
                "https://files.tmdb.org/movie-first.json.gz",
                0,
                true,
            ),
            enqueue_daily_export_refresh_jobs(
                &pool,
                MediaType::Tv,
                second_export.path().to_path_buf(),
                "https://files.tmdb.org/tv-second.json.gz",
                0,
                true,
            ),
        );
        let results = [first, second];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(error) if error.failure_code() == "export_queue_incomplete"
                ))
                .count(),
            1
        );
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_title_census_is_not_blocked_by_enrichment_backlog(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO ops.jobs (
                 id, job_type, payload_version, payload, status, dedup_key
             )
             SELECT gen_random_uuid(), 'ingest.enrich_movie', 1,
                    jsonb_build_object('tmdb_id', series), 'queued',
                    'enrichment-capacity-fixture-' || series::text
               FROM generate_series(1, 1000) AS series",
        )
        .execute(&pool)
        .await?;
        let export = tempfile::NamedTempFile::new()?;
        std::fs::write(export.path(), "1001\n")?;

        let summary = enqueue_daily_export_refresh_jobs(
            &pool,
            MediaType::Movie,
            export.path().to_path_buf(),
            "https://files.tmdb.org/movie.json.gz",
            0,
            true,
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(summary.detail_refresh_candidates, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_scan_configuration_refresh_is_deduplicated_while_active(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;

        let first = submit_configuration_refresh(&pool)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let second = submit_configuration_refresh(&pool)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(first, 1);
        assert_eq!(second, 0);
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.configuration'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn missing_catalog_refreshes_are_batched_and_resumable(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id)
             SELECT 'movie', generate_series(1, 501)",
        )
        .execute(&pool)
        .await?;

        let first = enqueue_missing_catalog_refresh_batch(&pool, &[MediaType::Movie], 0)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(first.queued, 500);
        assert!(!first.done);

        let second =
            enqueue_missing_catalog_refresh_batch(&pool, &[MediaType::Movie], first.next_cursor)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(second.queued, 1);
        assert!(second.done);

        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.refresh_movie'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 501);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_enrichment_phase_queues_bounded_catalog_only_jobs(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             SELECT 'movie', series, 'fixture-' || series::text
               FROM generate_series(1, 101) AS series",
        )
        .execute(&pool)
        .await?;

        let batch =
            enqueue_catalog_enrichment_batch(&pool, AdminScanMode::FullSweep, MediaType::Movie, 0)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let scopes: Vec<String> = sqlx::query_scalar(
            "SELECT payload ->> 'scope'
               FROM ops.jobs
              WHERE job_type = 'ingest.enrich_movie'
              ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(batch.queued, CATALOG_ENRICHMENT_BATCH_SIZE);
        assert_eq!(batch.next_cursor, 100);
        assert!(!batch.done);
        assert_eq!(scopes.len(), CATALOG_ENRICHMENT_BATCH_SIZE);
        assert!(scopes.iter().all(|scope| scope == "catalog_only"));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_season_phase_queues_bounded_catalog_only_jobs(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('tv', 700, 'season fixture')",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.seasons (id, title_id, media_type, season_number)
             SELECT 1000 + series, title.id, 'tv', series - 1
               FROM generate_series(1, 26) AS series
               CROSS JOIN catalog.titles AS title
              WHERE title.media_type = 'tv' AND title.tmdb_id = 700",
        )
        .execute(&pool)
        .await?;

        let batch = enqueue_catalog_season_batch(&pool, AdminScanMode::FullSweep, 0)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let scopes: Vec<String> = sqlx::query_scalar(
            "SELECT payload ->> 'scope'
               FROM ops.jobs
              WHERE job_type = 'ingest.refresh_season'
              ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(batch.queued, CATALOG_SEASON_BATCH_SIZE);
        assert_eq!(batch.next_cursor, 1025);
        assert!(!batch.done);
        assert_eq!(scopes.len(), CATALOG_SEASON_BATCH_SIZE);
        assert!(scopes.iter().all(|scope| scope == "catalog_only"));
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_phase_wait_succeeds_and_queues_a_continuation(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO ops.jobs (
                 id, job_type, payload_version, payload, status, dedup_key
             ) VALUES (
                 gen_random_uuid(), 'ingest.enrich_movie', 1,
                 '{\"tmdb_id\":42,\"scope\":\"catalog_only\"}'::jsonb,
                 'queued', 'active-enrichment-fixture'
             )",
        )
        .execute(&pool)
        .await?;

        let result = execute_catalog_scan_phase(
            &pool,
            AdminScanMode::FullSweep,
            &[MediaType::Movie],
            AdminScanPhase::Enrichment,
            0,
            0,
            None,
            None,
        )
        .await;
        let summary = result.map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(summary["waiting"], true);
        let continuations: i64 = sqlx::query_scalar(
            "SELECT count(*)
               FROM ops.jobs
              WHERE job_type = 'admin.scan'
                AND status = 'queued'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(continuations, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn missing_export_requeues_an_incomplete_catalog_title(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('movie', 51, 'Incomplete')",
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

        let queued =
            enqueue_missing_refresh_jobs(&pool, MediaType::Movie, &[51], "missing_queue_full")
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(queued, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn recovery_export_requeues_an_unresolved_dead_letter(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (
                 media_type, tmdb_id, display_title, source_updated_at
             ) VALUES ('movie', 51, 'Stale complete title', clock_timestamp() - interval '1 hour')",
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
        sqlx::query(
            "INSERT INTO ops.jobs (
                 id, job_type, payload_version, payload, status, attempts,
                 max_attempts, dedup_key, error_code, error_message, finished_at
             ) VALUES (
                 gen_random_uuid(), 'ingest.refresh_movie', 1,
                 '{\"tmdb_id\":51,\"scope\":\"catalog_only\"}'::jsonb,
                 'dead_letter', 8, 8, 'dead-refresh-51', 'upstream_unavailable',
                 'upstream unavailable', clock_timestamp()
             )",
        )
        .execute(&pool)
        .await?;

        let queued =
            enqueue_missing_refresh_jobs(&pool, MediaType::Movie, &[51], "missing_queue_full")
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(queued, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn enrichment_completion_state_is_durable(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('movie', 51, 'Complete')",
        )
        .execute(&pool)
        .await?;
        catalog_write::mark_title_enriched(&pool, "movie", 51)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let enriched: bool = sqlx::query_scalar(
            "SELECT enriched_at IS NOT NULL
               FROM catalog.titles
              WHERE media_type = 'movie' AND tmdb_id = 51",
        )
        .fetch_one(&pool)
        .await?;

        assert!(enriched);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn catalog_watermark_does_not_advance_past_an_unresolved_child_dead_letter(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO ops.jobs (
                 id, job_type, payload_version, payload, status, attempts,
                 max_attempts, dedup_key, created_at, updated_at, finished_at
             ) VALUES (
                 gen_random_uuid(), 'admin.scan', 1,
                 '{\"mode\":\"missing_only\",\"mediaTypes\":[\"movie\",\"tv\"]}',
                 'succeeded', 1, 100, 'watermark-root',
                 clock_timestamp() - interval '1 minute',
                 clock_timestamp() - interval '1 minute',
                 clock_timestamp() - interval '1 minute'
             ), (
                 gen_random_uuid(), 'ingest.refresh_movie', 1,
                 '{\"tmdb_id\":51}', 'dead_letter', 8, 8,
                 'watermark-dead-child', clock_timestamp(), clock_timestamp(),
                 clock_timestamp()
             )",
        )
        .execute(&pool)
        .await?;
        let result = execute_catalog_scan_phase(
            &pool,
            AdminScanMode::MissingOnly,
            &[MediaType::Movie, MediaType::Tv],
            AdminScanPhase::Completion,
            0,
            0,
            None,
            None,
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(result["watermark_advanced"], false);
        assert_eq!(result["failure_reason"], "unresolved_child_jobs");
        let watermark: Option<NaiveDate> = sqlx::query_scalar(
            "SELECT last_successful_window_end
               FROM ops.catalog_sync_state WHERE mode = 'missing_only'",
        )
        .fetch_one(&pool)
        .await?;
        assert!(watermark.is_none());
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn recovery_enrichment_queues_only_incomplete_titles(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (
                 media_type, tmdb_id, display_title, enriched_at
             ) VALUES
                 ('movie', 51, 'Incomplete', NULL),
                 ('movie', 52, 'Complete', clock_timestamp())",
        )
        .execute(&pool)
        .await?;

        let batch =
            enqueue_catalog_enrichment_batch(&pool, AdminScanMode::Recovery, MediaType::Movie, 0)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(batch.queued, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn recovery_seasons_queue_only_incomplete_seasons(pool: PgPool) -> sqlx::Result<()> {
        enable_ingest_worker(&pool).await?;
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('tv', 700, 'Recovery fixture')",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.seasons (
                 id, title_id, media_type, season_number, enriched_at
             )
             SELECT 1001, title.id, 'tv', 1, NULL
               FROM catalog.titles AS title
              WHERE title.media_type = 'tv' AND title.tmdb_id = 700
             UNION ALL
             SELECT 1002, title.id, 'tv', 2, clock_timestamp()
               FROM catalog.titles AS title
              WHERE title.media_type = 'tv' AND title.tmdb_id = 700",
        )
        .execute(&pool)
        .await?;

        let batch = enqueue_catalog_season_batch(&pool, AdminScanMode::Recovery, 0)
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        assert_eq!(batch.queued, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn prune_cleanup_deletes_only_ids_absent_from_the_export(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO catalog.titles (media_type, tmdb_id, display_title)
             VALUES ('movie', 1, 'keep'), ('movie', 2, 'remove'), ('tv', 2, 'keep-tv')",
        )
        .execute(&pool)
        .await?;
        let ids = tempfile::NamedTempFile::new()?;
        std::fs::write(ids.path(), "1\n")?;

        let deleted = prune_catalog_titles(&pool, MediaType::Movie, ids.path().to_path_buf())
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        assert_eq!(deleted, 1);
        let movies: Vec<i64> = sqlx::query_scalar(
            "SELECT tmdb_id FROM catalog.titles WHERE media_type = 'movie' ORDER BY tmdb_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(movies, [1]);
        let tvs: Vec<i64> = sqlx::query_scalar(
            "SELECT tmdb_id FROM catalog.titles WHERE media_type = 'tv' ORDER BY tmdb_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(tvs, [2]);
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
    continued: bool,
    next_offset: Option<u64>,
}

const DAILY_EXPORT_BATCH_SIZE: usize = 500;

async fn ensure_export_id_file(
    parser: DailyExportParser,
    source: PathBuf,
    destination: PathBuf,
) -> Result<(), JobExecutionError> {
    if tokio::fs::try_exists(&destination)
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
    {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || parser.write_id_file(source, destination))
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportIdBatch {
    ids: Vec<u64>,
    next_offset: u64,
    finished: bool,
}

fn read_export_id_batch(path: PathBuf, offset: u64) -> Result<ExportIdBatch, JobExecutionError> {
    let file = File::open(path)
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
    let file_length = file
        .metadata()
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        .len();
    if offset > file_length {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    let mut reader = BufReader::new(file);
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
    let mut ids = Vec::with_capacity(DAILY_EXPORT_BATCH_SIZE);
    let mut line = String::new();
    while ids.len() < DAILY_EXPORT_BATCH_SIZE {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
        if read == 0 {
            break;
        }
        let id = line
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|id| *id > 0)
            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
        ids.push(id);
    }
    let next_offset = reader
        .stream_position()
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?;
    let finished = reader
        .fill_buf()
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        .is_empty();
    Ok(ExportIdBatch {
        ids,
        next_offset,
        finished,
    })
}

fn export_dedup_key(media_type: MediaType, url: &str, offset: u64, refresh_all: bool) -> String {
    let digest = Sha256::digest(url.as_bytes());
    if refresh_all {
        format!("{DAILY_EXPORT_JOB}:{media_type}:{digest:x}:{offset}")
    } else {
        format!("{DAILY_EXPORT_JOB}:recovery:{media_type}:{digest:x}:{offset}")
    }
}

async fn enqueue_refresh_jobs_with_priority(
    pool: &PgPool,
    media_type: MediaType,
    tmdb_ids: &[u64],
    priority: i16,
    scope: RefreshScope,
    capacity_failure_code: &'static str,
) -> Result<usize, JobExecutionError> {
    const SUBMISSION_BATCH_SIZE: usize = 500;

    let mut submitted = 0_usize;
    for ids in tmdb_ids.chunks(SUBMISSION_BATCH_SIZE) {
        let jobs = ids
            .iter()
            .copied()
            .map(|tmdb_id| refresh_job(media_type, tmdb_id, priority, scope))
            .collect::<Result<Vec<_>, _>>()?;
        submitted = submitted.saturating_add(
            submit_refresh_jobs_with_capacity(pool, &jobs, capacity_failure_code).await?,
        );
    }
    Ok(submitted)
}

async fn ingest_child_submissions_enabled(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, JobExecutionError> {
    sqlx::query_scalar("SELECT ops.ingest_child_submissions_enabled()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
}

async fn submit_ingest_child_jobs(
    pool: &PgPool,
    jobs: &[NewJob],
) -> Result<Vec<SubmitOutcome>, JobExecutionError> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if !ingest_child_submissions_enabled(&mut transaction).await? {
        transaction.rollback().await.map_err(|_| {
            JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
        })?;
        return Ok(Vec::new());
    }
    let outcomes = JobRepository::submit_many_in_transaction(&mut transaction, jobs)
        .await
        .map_err(map_submission_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(outcomes)
}

async fn submit_ingest_child_job(
    pool: &PgPool,
    job: NewJob,
) -> Result<Option<SubmitOutcome>, JobExecutionError> {
    Ok(submit_ingest_child_jobs(pool, &[job]).await?.pop())
}

async fn enqueue_missing_refresh_jobs(
    pool: &PgPool,
    media_type: MediaType,
    tmdb_ids: &[u64],
    capacity_failure_code: &'static str,
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
            AND title.source_updated_at IS NOT NULL
            AND title.display_title IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                  FROM ops.jobs AS failed
                 WHERE failed.status = 'dead_letter'
                   AND failed.job_type = CASE $1
                       WHEN 'movie' THEN 'ingest.refresh_movie'
                       WHEN 'tv' THEN 'ingest.refresh_tv'
                   END
                   AND failed.payload ->> 'tmdb_id' = title.tmdb_id::text
                   AND failed.finished_at > title.source_updated_at
            )
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
    enqueue_refresh_jobs_with_priority(
        pool,
        media_type,
        &missing,
        TITLE_REFRESH_PRIORITY,
        RefreshScope::CatalogOnly,
        capacity_failure_code,
    )
    .await
}

async fn enqueue_daily_export_refresh_jobs(
    pool: &PgPool,
    media_type: MediaType,
    path: PathBuf,
    url: &str,
    offset: u64,
    refresh_all: bool,
) -> Result<ExportQueueSummary, JobExecutionError> {
    let batch = tokio::task::spawn_blocking(move || read_export_id_batch(path, offset))
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))??;
    let detail_refresh_candidates = if refresh_all {
        enqueue_refresh_jobs_with_priority(
            pool,
            media_type,
            &batch.ids,
            TITLE_REFRESH_PRIORITY,
            RefreshScope::CatalogOnly,
            "export_queue_incomplete",
        )
        .await?
    } else {
        enqueue_missing_refresh_jobs(pool, media_type, &batch.ids, "export_queue_incomplete")
            .await?
    };
    if !batch.finished {
        let continuation = NewJob::new(
            DAILY_EXPORT_JOB,
            INGEST_PAYLOAD_VERSION,
            serde_json::json!({
                "media_type": media_type,
                "url": url,
                "offset": batch.next_offset,
                "refresh_all": refresh_all
            }),
            &export_dedup_key(media_type, url, batch.next_offset, refresh_all),
        )
        .and_then(|job| job.with_max_attempts(100))
        .and_then(|job| job.with_priority(DAILY_EXPORT_COORDINATOR_PRIORITY))
        .and_then(|job| job.with_available_at(Utc::now() + ChronoDuration::seconds(1)))
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        submit_ingest_child_job(pool, continuation).await?;
    } else if refresh_all {
        submit_ingest_child_job(
            pool,
            admin_scan_job(
                AdminScanMode::FullSweep,
                &[media_type],
                AdminScanPhase::Enrichment,
                0,
                0,
            )?,
        )
        .await?;
    }
    Ok(ExportQueueSummary {
        records: batch.ids.len(),
        detail_refresh_candidates,
        continued: !batch.finished,
        next_offset: (!batch.finished).then_some(batch.next_offset),
    })
}

async fn submit_refresh_jobs_with_capacity(
    pool: &PgPool,
    jobs: &[NewJob],
    failure_code: &'static str,
) -> Result<usize, JobExecutionError> {
    if jobs.is_empty() {
        return Ok(0);
    }
    let reserve =
        i64::try_from(jobs.len()).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if !ingest_child_submissions_enabled(&mut transaction).await? {
        transaction.rollback().await.map_err(|_| {
            JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
        })?;
        return Ok(0);
    }
    sqlx::query(
        "SELECT pg_catalog.pg_advisory_xact_lock(
             pg_catalog.hashtextextended('queue:capacity', 0)
         )",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let pending_refreshes: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM ops.jobs
          WHERE job_type IN ('ingest.refresh_movie', 'ingest.refresh_tv')
            AND status IN ('queued', 'running', 'retry_wait')",
    )
    .fetch_one(&mut *transaction)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if pending_refreshes.saturating_add(reserve) > MAX_PENDING_REFRESH_JOBS {
        return Err(JobExecutionError::retry(
            failure_code,
            Duration::from_secs(10),
        ));
    }
    let outcomes = JobRepository::submit_many_in_transaction(&mut transaction, jobs)
        .await
        .map_err(map_submission_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(outcomes
        .iter()
        .filter(|outcome| !outcome.was_duplicate())
        .count())
}

fn refresh_job(
    media_type: MediaType,
    tmdb_id: u64,
    priority: i16,
    scope: RefreshScope,
) -> Result<NewJob, JobExecutionError> {
    let tmdb_id = validate_refresh_tmdb_id(tmdb_id)?;
    let job_type = match media_type {
        MediaType::Movie => REFRESH_MOVIE_JOB,
        MediaType::Tv => REFRESH_TV_JOB,
    };
    NewJob::new(
        job_type,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({"tmdb_id": tmdb_id, "scope": scope}),
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
