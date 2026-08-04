use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Days, Duration as ChronoDuration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use tmdb_domain::MediaType;
use tmdb_jobs::{ClaimedJob, JobError, JobExecutionError, JobExecutor, JobRepository, NewJob};
use tmdb_upstream::{
    DailyExportParser, IMAGE_GALLERY_QUERY_STRING, MAX_DAILY_EXPORT_BYTES,
    MOVIE_DETAIL_QUERY_STRING, TV_DETAIL_QUERY_STRING, TmdbClient, TmdbClientError, TmdbImages,
    TmdbTrendingItem, TmdbVideos, VIDEO_GALLERY_QUERY_STRING,
};
use tokio::io::{AsyncBufReadExt, BufReader as TokioBufReader};
use uuid::Uuid;

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
/// Refresh TMDB's image configuration document.
pub const CONFIGURATION_JOB: &str = "ingest.configuration";
/// Refresh a typed TMDB trending window into the durable ranking table.
pub const TRENDING_REFRESH_JOB: &str = "ingest.trending";
/// Refresh one reusable people, company, network, or collection gallery.
pub const REFRESH_REUSABLE_GALLERY_JOB: &str = "ingest.refresh_reusable_gallery";
/// Explicit administrative catalog scan coordinator. It is never enqueued by restart.
pub const ADMIN_SCAN_JOB: &str = "admin.scan";
/// Durable media scan coordinator.
pub const ADMIN_MEDIA_SCAN_JOB: &str = "admin.media_scan";
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
    CONFIGURATION_JOB,
    TRENDING_REFRESH_JOB,
    REFRESH_REUSABLE_GALLERY_JOB,
    ADMIN_SCAN_JOB,
    ADMIN_MEDIA_SCAN_JOB,
    ADMIN_ANALYZE_JOB,
];
const DAILY_EXPORT_REFRESH_PRIORITY: i16 = -100;
const MAX_PENDING_REFRESH_JOBS: i64 = 1_000;

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
pub enum ReusableGalleryEntity {
    Person,
    Company,
    Network,
    Collection,
}

impl ReusableGalleryEntity {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Company => "company",
            Self::Network => "network",
            Self::Collection => "collection",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReusableGalleryPayload {
    entity_type: ReusableGalleryEntity,
    tmdb_id: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaScanMode {
    Full,
    Missing,
    Audit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaScanPayload {
    run_id: uuid::Uuid,
    mode: MediaScanMode,
    #[serde(default)]
    repair: bool,
    #[serde(default)]
    step: u32,
    #[serde(default)]
    cursor: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminScanMode {
    FullSweep,
    MissingOnly,
    PruneCleanup,
    DailySync,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdminScanPayload {
    mode: AdminScanMode,
    media_types: Vec<MediaType>,
    #[serde(default)]
    cursor: u64,
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
    /// Refresh one reusable entity's dedicated TMDB gallery.
    RefreshReusableGallery {
        entity_type: ReusableGalleryEntity,
        tmdb_id: u32,
    },
    /// Coordinate a durable full, missing, or audit media scan.
    MediaScan {
        run_id: uuid::Uuid,
        mode: MediaScanMode,
        repair: bool,
        step: u32,
        cursor: u64,
    },
    /// Expand one explicit operational scan into safely bounded ingest jobs.
    AdminScan {
        mode: AdminScanMode,
        media_types: Vec<MediaType>,
        cursor: u64,
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
            Self::RefreshReusableGallery {
                entity_type,
                tmdb_id,
            } => format!(
                "{REFRESH_REUSABLE_GALLERY_JOB}:{}:{tmdb_id}",
                entity_type.as_str()
            ),
            Self::MediaScan {
                run_id,
                step,
                cursor,
                ..
            } => {
                format!("{ADMIN_MEDIA_SCAN_JOB}:{run_id}:{step}:{cursor}")
            }
            Self::AdminScan {
                mode,
                media_types,
                cursor,
            } => {
                format!("{ADMIN_SCAN_JOB}:{mode:?}:{media_types:?}:{cursor}")
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
                offset: payload.offset,
                refresh_all: payload.refresh_all,
            })
        }
        CONFIGURATION_JOB if payload == &serde_json::json!({}) => Ok(IngestJob::Configuration),
        CONFIGURATION_JOB => Err(JobPayloadError::InvalidPayload),
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
        REFRESH_REUSABLE_GALLERY_JOB => {
            let payload: ReusableGalleryPayload = parse_payload(payload)?;
            validate_tmdb_id(payload.tmdb_id)?;
            Ok(IngestJob::RefreshReusableGallery {
                entity_type: payload.entity_type,
                tmdb_id: payload.tmdb_id,
            })
        }
        ADMIN_MEDIA_SCAN_JOB => {
            let payload: MediaScanPayload = parse_payload(payload)?;
            if payload.run_id.is_nil() || payload.step > 100_000 {
                return Err(JobPayloadError::InvalidValue);
            }
            Ok(IngestJob::MediaScan {
                run_id: payload.run_id,
                mode: payload.mode,
                repair: payload.repair,
                step: payload.step,
                cursor: payload.cursor,
            })
        }
        ADMIN_SCAN_JOB => {
            let payload: AdminScanPayload = parse_payload(payload)?;
            if payload.media_types.is_empty()
                || payload.media_types.len() > 2
                || payload.cursor > i64::MAX as u64
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
                cursor: payload.cursor,
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

const MAX_SHARED_GALLERY_ENTITIES: usize = 256;
const MAX_LINKED_DOCUMENTS: usize = 256;

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

fn optional_gallery(
    result: Result<TmdbImages, TmdbClientError>,
) -> Result<TmdbImages, TmdbClientError> {
    match result {
        Err(TmdbClientError::NotFound) => Ok(TmdbImages::default()),
        other => other,
    }
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
    let mut documents = Vec::new();
    for endpoint_path in paths {
        match client.fetch_document(&endpoint_path, &[]).await {
            Ok((response, query_string)) => documents.push(CapturedDocument {
                endpoint_path,
                query_string,
                response,
            }),
            Err(TmdbClientError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(documents)
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
    } else if let Some(id) = value.as_u64().filter(|id| *id > 0) {
        id.to_string()
    } else {
        return None;
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

async fn fetch_reusable_gallery(
    client: &TmdbClient,
    entity_type: ReusableGalleryEntity,
    tmdb_id: u32,
) -> Result<(TmdbImages, Vec<CapturedDocument>), TmdbClientError> {
    let path = format!("{}/{tmdb_id}/images", entity_type.as_str());
    let (raw, images) = match entity_type {
        ReusableGalleryEntity::Person => {
            optional_gallery_with_raw(client.fetch_person_images_with_raw(tmdb_id).await)?
        }
        ReusableGalleryEntity::Company => {
            optional_gallery_with_raw(client.fetch_company_images_with_raw(tmdb_id).await)?
        }
        ReusableGalleryEntity::Network => {
            optional_gallery_with_raw(client.fetch_network_images_with_raw(tmdb_id).await)?
        }
        ReusableGalleryEntity::Collection => {
            optional_gallery_with_raw(client.fetch_collection_images_with_raw(tmdb_id).await)?
        }
    };
    let mut documents = Vec::new();
    if let Some(response) = raw {
        documents.push(CapturedDocument {
            endpoint_path: path,
            query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    let detail_path = format!("{}/{tmdb_id}", entity_type.as_str());
    documents.extend(capture_optional_documents(client, vec![detail_path]).await?);
    let suffixes: &[&str] = match entity_type {
        ReusableGalleryEntity::Person => &[
            "changes",
            "combined_credits",
            "external_ids",
            "movie_credits",
            "tagged_images",
            "translations",
            "tv_credits",
        ],
        ReusableGalleryEntity::Company => &["alternative_names"],
        ReusableGalleryEntity::Network => &["alternative_names"],
        ReusableGalleryEntity::Collection => &["translations"],
    };
    documents.extend(
        capture_optional_documents(
            client,
            suffixes
                .iter()
                .map(|suffix| format!("{}/{tmdb_id}/{suffix}", entity_type.as_str()))
                .collect(),
        )
        .await?,
    );
    Ok((images, documents))
}

async fn hydrate_movie_galleries(
    client: &TmdbClient,
    movie: &mut tmdb_upstream::TmdbMovie,
    detail_raw: &Value,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let mut documents = Vec::new();
    let movie_id = upstream_id(movie.id)?;
    let (image_raw, images) =
        optional_gallery_with_raw(client.fetch_movie_images_with_raw(movie_id).await)?;
    movie.images = images;
    if let Some(response) = image_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("movie/{movie_id}/images"),
            query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    let (video_raw, videos) = match client.fetch_movie_videos_with_raw(movie_id).await {
        Ok((response, videos)) => (Some(response), videos),
        Err(TmdbClientError::NotFound) => (None, TmdbVideos::default()),
        Err(error) => return Err(error),
    };
    movie.videos = videos;
    if let Some(response) = video_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("movie/{movie_id}/videos"),
            query_string: VIDEO_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    documents.extend(
        capture_optional_documents(
            client,
            [
                "account_states",
                "alternative_titles",
                "changes",
                "credits",
                "external_ids",
                "keywords",
                "lists",
                "recommendations",
                "release_dates",
                "reviews",
                "similar",
                "translations",
                "watch/providers",
            ]
            .into_iter()
            .map(|suffix| format!("movie/{movie_id}/{suffix}"))
            .collect(),
        )
        .await?,
    );
    capture_linked_documents(client, &mut documents, &[("movie detail", detail_raw)]).await?;
    hydrate_credit_galleries(client, &mut movie.credits).await?;
    for company in movie
        .production_companies
        .iter_mut()
        .take(MAX_SHARED_GALLERY_ENTITIES)
    {
        let company_id = upstream_id(company.id)?;
        company.images = optional_gallery(client.fetch_company_images(company_id).await)?;
    }
    if let Some(collection) = movie.belongs_to_collection.as_mut() {
        let collection_id = upstream_id(collection.id)?;
        collection.images = optional_gallery(client.fetch_collection_images(collection_id).await)?;
    }
    Ok(documents)
}

async fn hydrate_tv_galleries(
    client: &TmdbClient,
    series: &mut tmdb_upstream::TmdbTv,
    detail_raw: &Value,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let mut documents = Vec::new();
    let series_id = upstream_id(series.id)?;
    let (image_raw, images) =
        optional_gallery_with_raw(client.fetch_tv_images_with_raw(series_id).await)?;
    series.images = images;
    if let Some(response) = image_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("tv/{series_id}/images"),
            query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    let (video_raw, videos) = match client.fetch_tv_videos_with_raw(series_id).await {
        Ok((response, videos)) => (Some(response), videos),
        Err(TmdbClientError::NotFound) => (None, TmdbVideos::default()),
        Err(error) => return Err(error),
    };
    series.videos = videos;
    if let Some(response) = video_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("tv/{series_id}/videos"),
            query_string: VIDEO_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    documents.extend(
        capture_optional_documents(
            client,
            [
                "account_states",
                "aggregate_credits",
                "alternative_titles",
                "changes",
                "content_ratings",
                "credits",
                "episode_groups",
                "external_ids",
                "keywords",
                "lists",
                "recommendations",
                "reviews",
                "screened_theatrically",
                "similar",
                "translations",
                "watch/providers",
            ]
            .into_iter()
            .map(|suffix| format!("tv/{series_id}/{suffix}"))
            .collect(),
        )
        .await?,
    );
    capture_linked_documents(client, &mut documents, &[("tv detail", detail_raw)]).await?;
    hydrate_credit_galleries(client, &mut series.credits).await?;
    for company in series
        .production_companies
        .iter_mut()
        .take(MAX_SHARED_GALLERY_ENTITIES)
    {
        let company_id = upstream_id(company.id)?;
        company.images = optional_gallery(client.fetch_company_images(company_id).await)?;
    }
    for network in series.networks.iter_mut().take(MAX_SHARED_GALLERY_ENTITIES) {
        let network_id = upstream_id(network.id)?;
        network.images = optional_gallery(client.fetch_network_images(network_id).await)?;
    }
    Ok(documents)
}

async fn hydrate_credit_galleries(
    client: &TmdbClient,
    credits: &mut tmdb_upstream::TmdbCredits,
) -> Result<(), TmdbClientError> {
    let mut cache = HashMap::<u64, TmdbImages>::new();
    hydrate_credit_galleries_with_cache(client, credits, &mut cache).await
}

async fn hydrate_credit_galleries_with_cache(
    client: &TmdbClient,
    credits: &mut tmdb_upstream::TmdbCredits,
    cache: &mut HashMap<u64, TmdbImages>,
) -> Result<(), TmdbClientError> {
    for credit in credits
        .cast
        .iter()
        .chain(credits.crew.iter())
        .take(MAX_SHARED_GALLERY_ENTITIES)
    {
        if cache.len() >= MAX_SHARED_GALLERY_ENTITIES && !cache.contains_key(&credit.id) {
            continue;
        }
        let person_id = upstream_id(credit.id)?;
        if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry(credit.id) {
            let images = optional_gallery(client.fetch_person_images(person_id).await)?;
            entry.insert(images);
        }
    }
    for credit in credits.cast.iter_mut().chain(credits.crew.iter_mut()) {
        if let Some(images) = cache.get(&credit.id) {
            credit.images = images.clone();
        }
    }
    Ok(())
}

async fn hydrate_season_galleries(
    client: &TmdbClient,
    tv_id: u32,
    season: &mut tmdb_upstream::TmdbSeason,
) -> Result<Vec<CapturedDocument>, TmdbClientError> {
    let mut documents = Vec::new();
    let (season_raw, images) = optional_gallery_with_raw(
        client
            .fetch_season_images_with_raw(tv_id, season.season_number)
            .await,
    )?;
    season.images = images;
    if let Some(response) = season_raw {
        documents.push(CapturedDocument {
            endpoint_path: format!("tv/{tv_id}/season/{}/images", season.season_number),
            query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
            response,
        });
    }
    if season.id > 0 {
        let season_id = upstream_id(season.id)?;
        documents.extend(
            capture_optional_documents(
                client,
                vec![format!("tv/season/{season_id}/changes")],
            )
            .await?,
        );
    }
    documents.extend(
        capture_optional_documents(
            client,
            [
                "account_states",
                "aggregate_credits",
                "credits",
                "external_ids",
                "translations",
                "videos",
                "watch/providers",
            ]
            .into_iter()
            .map(|suffix| format!("tv/{tv_id}/season/{}/{suffix}", season.season_number))
            .collect(),
        )
        .await?,
    );
    let mut person_cache = HashMap::<u64, TmdbImages>::new();
    for episode in &mut season.episodes {
        let episode_detail = match client
            .fetch_episode_with_raw(tv_id, season.season_number, episode.episode_number)
            .await
        {
            Ok((response, _)) => Some(response),
            Err(TmdbClientError::NotFound) => None,
            Err(error) => return Err(error),
        };
        if let Some(response) = episode_detail {
            documents.push(CapturedDocument {
                endpoint_path: format!(
                    "tv/{tv_id}/season/{}/episode/{}",
                    season.season_number, episode.episode_number
                ),
                query_string: String::new(),
                response,
            });
        }
        if episode.id > 0 {
            let episode_id = upstream_id(episode.id)?;
            documents.extend(
                capture_optional_documents(
                    client,
                    vec![format!("tv/episode/{episode_id}/changes")],
                )
                .await?,
            );
        }
        let (episode_raw, images) = optional_gallery_with_raw(
            client
                .fetch_episode_images_with_raw(tv_id, season.season_number, episode.episode_number)
                .await,
        )?;
        episode.images = images;
        if let Some(response) = episode_raw {
            documents.push(CapturedDocument {
                endpoint_path: format!(
                    "tv/{tv_id}/season/{}/episode/{}/images",
                    season.season_number, episode.episode_number
                ),
                query_string: IMAGE_GALLERY_QUERY_STRING.to_owned(),
                response,
            });
        }
        documents.extend(
            capture_optional_documents(
                client,
                [
                    "account_states",
                    "credits",
                    "external_ids",
                    "translations",
                    "videos",
                ]
                .into_iter()
                .map(|suffix| {
                    format!(
                        "tv/{tv_id}/season/{}/episode/{}/{}",
                        season.season_number, episode.episode_number, suffix
                    )
                })
                .collect(),
            )
            .await?,
        );
        hydrate_credit_galleries_with_cache(client, &mut episode.credits, &mut person_cache)
            .await?;
    }
    Ok(documents)
}

async fn persist_documents(
    database: &PgPool,
    documents: &[CapturedDocument],
) -> Result<(), JobExecutionError> {
    for document in documents {
        catalog_write::persist_tmdb_document(
            database,
            &document.endpoint_path,
            &document.query_string,
            &document.response,
        )
        .await?;
    }
    Ok(())
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
                let (movie_raw, mut movie) = match self.client.fetch_movie_with_raw(tmdb_id).await {
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
                let movie_documents = match hydrate_movie_galleries(
                    &self.client,
                    &mut movie,
                    &movie_raw,
                )
                .await
                {
                    Ok(documents) => documents,
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("movie_gallery", "movie", tmdb_id, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    let base_response = match self.client.fetch_movie_base(tmdb_id).await {
                        Ok(response) => response,
                        Err(error) => {
                            self.record_upstream_state("degraded").await;
                            log_upstream_failure("movie_base", "movie", tmdb_id, None, &error);
                            return Err(map_upstream_error(&error));
                        }
                    };
                    catalog_write::persist_movie_with_options(
                        database,
                        &movie,
                        self.allow_local_media,
                    )
                    .await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("movie/{}", movie.id),
                        MOVIE_DETAIL_QUERY_STRING,
                        &movie_raw,
                    )
                    .await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("movie/{}", movie.id),
                        "",
                        &base_response,
                    )
                    .await?;
                    persist_documents(database, &movie_documents).await?;
                }
                Ok(serde_json::json!({
                    "media_type":"movie",
                    "tmdb_id":movie.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::RefreshTv { tmdb_id } => {
                let (series_raw, mut series) = match self.client.fetch_tv_with_raw(tmdb_id).await {
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
                let series_documents = match hydrate_tv_galleries(
                    &self.client,
                    &mut series,
                    &series_raw,
                )
                .await
                {
                    Ok(documents) => documents,
                    Err(error) => {
                        self.record_upstream_state("degraded").await;
                        log_upstream_failure("tv_gallery", "tv", tmdb_id, None, &error);
                        return Err(map_upstream_error(&error));
                    }
                };
                if let Some(database) = &self.database {
                    let base_response = match self.client.fetch_tv_base(tmdb_id).await {
                        Ok(response) => response,
                        Err(error) => {
                            self.record_upstream_state("degraded").await;
                            log_upstream_failure("tv_base", "tv", tmdb_id, None, &error);
                            return Err(map_upstream_error(&error));
                        }
                    };
                    catalog_write::persist_tv_with_options(
                        database,
                        &series,
                        self.allow_local_media,
                    )
                    .await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("tv/{}", series.id),
                        TV_DETAIL_QUERY_STRING,
                        &series_raw,
                    )
                    .await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("tv/{}", series.id),
                        "",
                        &base_response,
                    )
                    .await?;
                    persist_documents(database, &series_documents).await?;
                }
                Ok(serde_json::json!({
                    "media_type":"tv",
                    "tmdb_id":series.id,
                    "dedup_key":dedup_key
                }))
            }
            IngestJob::ChangesSync { media_type, page } => {
                let (change_raw, change_page) =
                    match self.client.fetch_changes_with_raw(media_type, page).await {
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
                    ensure_refresh_queue_capacity(
                        database,
                        changed_ids.len(),
                        "changes_queue_full",
                    )
                    .await?;
                    catalog_write::persist_changes(database, media_type, &change_page).await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("{media_type}/changes"),
                        &format!("page={page}"),
                        &change_raw,
                    )
                    .await?;
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
                        .and_then(|job| job.with_max_attempts(100))
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
                let season_documents =
                    match hydrate_season_galleries(&self.client, tv_id, &mut season).await {
                        Ok(documents) => documents,
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
                    };
                if let Some(database) = &self.database {
                    catalog_write::persist_season_with_options(
                        database,
                        tv_id,
                        &season,
                        self.allow_local_media,
                    )
                    .await?;
                    catalog_write::persist_tmdb_document(
                        database,
                        &format!("tv/{tv_id}/season/{season_number}"),
                        "",
                        &season_raw,
                    )
                    .await?;
                    persist_documents(database, &season_documents).await?;
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
            IngestJob::RefreshReusableGallery {
                entity_type,
                tmdb_id,
            } => {
                let (images, documents) =
                    match fetch_reusable_gallery(&self.client, entity_type, tmdb_id).await {
                        Ok(images) => {
                            self.record_upstream_state("ready").await;
                            images
                        }
                        Err(error) => {
                            self.record_upstream_state("degraded").await;
                            log_upstream_failure(
                                "reusable_gallery",
                                entity_type.as_str(),
                                tmdb_id,
                                None,
                                &error,
                            );
                            return Err(map_upstream_error(&error));
                        }
                    };
                if let Some(database) = &self.database {
                    catalog_write::enqueue_reusable_gallery(
                        database,
                        entity_type.as_str(),
                        i64::from(tmdb_id),
                        &images,
                        self.allow_local_media,
                    )
                    .await?;
                    persist_documents(database, &documents).await?;
                }
                Ok(serde_json::json!({
                    "entity_type": entity_type,
                    "tmdb_id": tmdb_id,
                    "poster_count": images.posters.len(),
                    "backdrop_count": images.backdrops.len(),
                    "logo_count": images.logos.len(),
                    "profile_count": images.profiles.len(),
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::MediaScan {
                run_id,
                mode,
                repair,
                step,
                cursor,
            } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                execute_media_scan(
                    database,
                    run_id,
                    mode,
                    repair,
                    step,
                    cursor,
                    self.allow_local_media,
                )
                .await
            }
            IngestJob::AdminScan {
                mode,
                media_types,
                cursor,
            } => {
                let Some(database) = &self.database else {
                    return Err(JobExecutionError::retry(
                        "database_unavailable",
                        Duration::from_secs(5),
                    ));
                };
                if !matches!(mode, AdminScanMode::MissingOnly) && cursor != 0 {
                    return Err(JobExecutionError::dead_letter("invalid_payload"));
                }
                let mut jobs_pruned = 0_i32;
                let queued = match mode {
                    AdminScanMode::FullSweep => {
                        let export_date = Utc::now()
                            .date_naive()
                            .checked_sub_days(Days::new(1))
                            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                        let mut queued = 0_usize;
                        let configuration = NewJob::new(
                            CONFIGURATION_JOB,
                            INGEST_PAYLOAD_VERSION,
                            serde_json::json!({}),
                            CONFIGURATION_JOB,
                        )
                        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                        if !JobRepository::new(database.clone())
                            .submit(configuration)
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
                    AdminScanMode::DailySync => {
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
                        if !batch.done {
                            let continuation =
                                admin_scan_job(mode, &media_types, batch.next_cursor)?;
                            JobRepository::new(database.clone())
                                .submit(continuation)
                                .await
                                .map_err(map_submission_error)?;
                        }
                        batch.queued
                    }
                };
                Ok(serde_json::json!({
                    "mode": mode,
                    "media_types": media_types,
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
            "url": format!("https://files.tmdb.org/p/exports/{file_prefix}_{date_text}.json.gz"),
            "offset": 0,
            "refresh_all": true
        }),
        &format!("{DAILY_EXPORT_JOB}:{media_type_name}:{date_text}:0"),
    )
    .and_then(|job| job.with_max_attempts(100))
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
}

const MEDIA_SCAN_POLL_SECONDS: i64 = 5;

#[derive(Debug, FromRow)]
struct MediaScanState {
    mode: String,
    repair: bool,
    phase: String,
    status: String,
    requested_at: DateTime<Utc>,
}

async fn execute_media_scan(
    database: &PgPool,
    run_id: Uuid,
    mode: MediaScanMode,
    repair: bool,
    step: u32,
    cursor: u64,
    allow_local_media: bool,
) -> Result<Value, JobExecutionError> {
    let Some(run) = sqlx::query_as::<_, MediaScanState>(
        "SELECT mode, repair, phase, status, requested_at
           FROM ops.media_scan_runs
          WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?
    else {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    };
    if run.mode != media_scan_mode_name(mode) || run.repair != repair {
        return Err(JobExecutionError::dead_letter("invalid_payload"));
    }
    if matches!(run.status.as_str(), "succeeded" | "failed" | "cancelled") {
        return Ok(serde_json::json!({
            "runId": run_id,
            "status": run.status,
            "phase": run.phase,
            "step": step
        }));
    }
    if !allow_local_media {
        finish_media_scan(database, run_id, true, Some("local_media_disabled")).await?;
        return Ok(serde_json::json!({
            "runId": run_id,
            "status": "succeeded",
            "skipped": "local_media_disabled",
            "step": step
        }));
    }

    match (mode, run.phase.as_str()) {
        (MediaScanMode::Full | MediaScanMode::Missing, "queued") => {
            let queued = enqueue_catalog_scan(database, run_id, catalog_scan_mode(mode)).await?;
            add_scan_queued_count(database, run_id, queued).await?;
            set_media_scan_phase(database, run_id, "catalog").await?;
            queue_media_scan_followup(database, run_id, mode, repair, step, 0).await?;
        }
        (MediaScanMode::Audit, "queued") => {
            let queued = enqueue_media_audit(database, run_id, repair).await?;
            add_scan_queued_count(database, run_id, queued).await?;
            set_media_scan_phase(database, run_id, "audit").await?;
            queue_media_scan_followup(database, run_id, mode, repair, step, cursor).await?;
        }
        (MediaScanMode::Full | MediaScanMode::Missing, "catalog") => {
            if catalog_scan_pending(database, run_id, run.requested_at).await? {
                queue_media_scan_followup(database, run_id, mode, repair, step, cursor).await?;
            } else {
                let mut queued = 0_usize;
                if mode == MediaScanMode::Missing {
                    queued =
                        queued.saturating_add(enqueue_media_audit(database, run_id, true).await?);
                }
                let batch = enqueue_media_work_batch(
                    database,
                    run_id,
                    cursor,
                    mode == MediaScanMode::Missing,
                )
                .await?;
                queued = queued.saturating_add(batch.queued);
                add_scan_queued_count(database, run_id, queued).await?;
                set_media_scan_phase(database, run_id, "media").await?;
                queue_media_scan_followup(database, run_id, mode, repair, step, batch.next_cursor)
                    .await?;
            }
        }
        (MediaScanMode::Full | MediaScanMode::Missing, "media") => {
            if media_scan_pending(database, run_id, run.requested_at).await? {
                queue_media_scan_followup(database, run_id, mode, repair, step, cursor).await?;
            } else {
                let batch = enqueue_media_work_batch(
                    database,
                    run_id,
                    cursor,
                    mode == MediaScanMode::Missing,
                )
                .await?;
                add_scan_queued_count(database, run_id, batch.queued).await?;
                if batch.done {
                    finish_media_scan(database, run_id, true, None).await?;
                } else {
                    queue_media_scan_followup(
                        database,
                        run_id,
                        mode,
                        repair,
                        step,
                        batch.next_cursor,
                    )
                    .await?;
                }
            }
        }
        (MediaScanMode::Audit, "audit") => {
            if audit_scan_pending(database, run_id).await? {
                queue_media_scan_followup(database, run_id, mode, repair, step, cursor).await?;
            } else {
                finish_media_scan(database, run_id, true, None).await?;
            }
        }
        _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
    }
    Ok(serde_json::json!({
        "runId": run_id,
        "mode": mode,
        "phase": "durable",
        "step": step
    }))
}

fn media_scan_mode_name(mode: MediaScanMode) -> &'static str {
    match mode {
        MediaScanMode::Full => "full",
        MediaScanMode::Missing => "missing",
        MediaScanMode::Audit => "audit",
    }
}

fn catalog_scan_mode(mode: MediaScanMode) -> AdminScanMode {
    match mode {
        MediaScanMode::Full => AdminScanMode::FullSweep,
        MediaScanMode::Missing => AdminScanMode::MissingOnly,
        MediaScanMode::Audit => unreachable!(),
    }
}

async fn enqueue_catalog_scan(
    database: &PgPool,
    run_id: Uuid,
    mode: AdminScanMode,
) -> Result<usize, JobExecutionError> {
    let job = NewJob::new(
        ADMIN_SCAN_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({"mode": mode, "mediaTypes": ["movie", "tv"]}),
        &format!("{ADMIN_SCAN_JOB}:{run_id}:catalog"),
    )
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let linked = submit_and_link_scan_job(database, run_id, "catalog", job).await?;
    Ok(usize::from(linked))
}

async fn enqueue_media_audit(
    database: &PgPool,
    run_id: Uuid,
    repair: bool,
) -> Result<usize, JobExecutionError> {
    let job = NewJob::new(
        "admin.media_audit",
        1,
        serde_json::json!({"repair": repair}),
        &format!("admin.media_audit:{run_id}"),
    )
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let linked = submit_and_link_scan_job(database, run_id, "audit", job).await?;
    Ok(usize::from(linked))
}

async fn queue_media_scan_followup(
    database: &PgPool,
    run_id: Uuid,
    mode: MediaScanMode,
    repair: bool,
    step: u32,
    cursor: u64,
) -> Result<(), JobExecutionError> {
    let next_step = step
        .checked_add(1)
        .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
    let job = NewJob::new(
        ADMIN_MEDIA_SCAN_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({
            "runId": run_id,
            "mode": mode,
            "repair": repair,
            "step": next_step,
            "cursor": cursor
        }),
        &format!("{ADMIN_MEDIA_SCAN_JOB}:{run_id}:{next_step}"),
    )
    .and_then(|job| {
        job.with_available_at(Utc::now() + ChronoDuration::seconds(MEDIA_SCAN_POLL_SECONDS))
    })
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    JobRepository::new(database.clone())
        .submit(job)
        .await
        .map_err(map_submission_error)?;
    Ok(())
}

async fn submit_and_link_scan_job(
    database: &PgPool,
    run_id: Uuid,
    phase: &str,
    job: NewJob,
) -> Result<bool, JobExecutionError> {
    let outcome = JobRepository::new(database.clone())
        .submit(job)
        .await
        .map_err(map_submission_error)?;
    sqlx::query(
        "INSERT INTO ops.media_scan_job_links (run_id, job_id, phase)
         VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING",
    )
    .bind(run_id)
    .bind(outcome.job_id().as_uuid())
    .bind(phase)
    .execute(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(!outcome.was_duplicate())
}

async fn add_scan_queued_count(
    database: &PgPool,
    run_id: Uuid,
    count: usize,
) -> Result<(), JobExecutionError> {
    let count =
        i64::try_from(count).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    sqlx::query(
        "UPDATE ops.media_scan_runs
            SET queued_count = queued_count + $2
          WHERE id = $1",
    )
    .bind(run_id)
    .bind(count)
    .execute(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(())
}

async fn set_media_scan_phase(
    database: &PgPool,
    run_id: Uuid,
    phase: &str,
) -> Result<(), JobExecutionError> {
    sqlx::query(
        "UPDATE ops.media_scan_runs
            SET status = 'running', phase = $2,
                started_at = COALESCE(started_at, pg_catalog.clock_timestamp()),
                error_code = NULL
          WHERE id = $1",
    )
    .bind(run_id)
    .bind(phase)
    .execute(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(())
}

async fn catalog_scan_pending(
    database: &PgPool,
    run_id: Uuid,
    requested_at: DateTime<Utc>,
) -> Result<bool, JobExecutionError> {
    let linked_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.media_scan_job_links AS link
               JOIN ops.job_status AS job ON job.id = link.job_id
              WHERE link.run_id = $1
                AND link.phase = 'catalog'
                AND job.status IN ('queued', 'running', 'retry_wait')
         )",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if linked_pending {
        return Ok(true);
    }
    let generated_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.media_scan_job_status AS job
              WHERE job.created_at >= $1
                AND job.job_type IN (
                    'ingest.daily_export', 'ingest.refresh_movie',
                    'ingest.refresh_tv', 'ingest.refresh_season'
                )
                AND job.status IN ('queued', 'running', 'retry_wait')
         )",
    )
    .bind(requested_at)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(generated_pending)
}

async fn media_scan_pending(
    database: &PgPool,
    run_id: Uuid,
    requested_at: DateTime<Utc>,
) -> Result<bool, JobExecutionError> {
    let linked_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.media_scan_job_links AS link
               JOIN ops.job_status AS job ON job.id = link.job_id
              WHERE link.run_id = $1
                AND link.phase = 'media'
                AND job.status IN ('queued', 'running', 'retry_wait')
         )",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if linked_pending {
        return Ok(true);
    }
    let generated_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.media_scan_job_status AS job
              WHERE job.created_at >= $1
                AND job.job_type IN (
                    'image.download', 'ingest.refresh_movie',
                    'ingest.refresh_tv', 'ingest.refresh_season',
                    'ingest.refresh_reusable_gallery', 'admin.media_audit'
                )
                AND job.status IN ('queued', 'running', 'retry_wait')
         )",
    )
    .bind(requested_at)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(generated_pending)
}

async fn audit_scan_pending(database: &PgPool, run_id: Uuid) -> Result<bool, JobExecutionError> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM ops.media_scan_job_links AS link
               JOIN ops.job_status AS job ON job.id = link.job_id
              WHERE link.run_id = $1
                AND link.phase = 'audit'
                AND job.status IN ('queued', 'running', 'retry_wait')
         )",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))
}

async fn finish_media_scan(
    database: &PgPool,
    run_id: Uuid,
    success: bool,
    error_code: Option<&str>,
) -> Result<(), JobExecutionError> {
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             count(*) FILTER (WHERE job.status = 'succeeded')::bigint,
             count(*) FILTER (WHERE job.status = 'dead_letter')::bigint,
             count(*) FILTER (WHERE job.status = 'cancelled')::bigint,
             count(*) FILTER (WHERE job.status IN ('queued', 'running', 'retry_wait'))::bigint
           FROM ops.media_scan_job_links AS link
           JOIN ops.media_scan_job_status AS job ON job.id = link.job_id
          WHERE link.run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let audit_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             COALESCE(SUM(CASE
                 WHEN jsonb_typeof(job.result_summary -> 'audited') = 'number'
                 THEN (job.result_summary ->> 'audited')::bigint
                 ELSE 0
             END), 0)::bigint,
             COALESCE(SUM(CASE
                 WHEN jsonb_typeof(job.result_summary -> 'invalid') = 'number'
                 THEN (job.result_summary ->> 'invalid')::bigint
                 ELSE 0
             END), 0)::bigint,
             COALESCE(SUM(CASE
                 WHEN jsonb_typeof(job.result_summary -> 'repairQueued') = 'number'
                 THEN (job.result_summary ->> 'repairQueued')::bigint
                 ELSE 0
             END), 0)::bigint
           FROM ops.media_scan_job_links AS link
           JOIN ops.media_scan_job_status AS job ON job.id = link.job_id
          WHERE link.run_id = $1
            AND link.phase = 'audit'
            AND job.status = 'succeeded'",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let status = if !success || counts.1 > 0 {
        "failed"
    } else if counts.2 > 0 {
        "cancelled"
    } else {
        "succeeded"
    };
    sqlx::query(
        "UPDATE ops.media_scan_runs
            SET status = $2,
                phase = 'completed',
                completed_count = $3,
                failed_count = $4,
                audited_count = $5,
                invalid_count = $6,
                repair_queued_count = $7,
                finished_at = pg_catalog.clock_timestamp(),
                error_code = $8
          WHERE id = $1",
    )
    .bind(run_id)
    .bind(status)
    .bind(counts.0)
    .bind(counts.1)
    .bind(audit_counts.0)
    .bind(audit_counts.1)
    .bind(audit_counts.2)
    .bind(error_code.or_else(|| (counts.1 > 0).then_some("linked_job_failed")))
    .execute(database)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    Ok(())
}

const MEDIA_WORK_BATCH_SIZE: i64 = 500;

#[derive(Debug, FromRow)]
struct MediaWorkRow {
    kind: i16,
    media_type: Option<String>,
    tmdb_id: Option<i64>,
    season_number: Option<i32>,
    entity_type: Option<String>,
    entity_id: Option<i64>,
}

struct MediaWorkBatch {
    queued: usize,
    next_cursor: u64,
    done: bool,
}

async fn enqueue_media_work_batch(
    database: &PgPool,
    run_id: Uuid,
    cursor: u64,
    missing_only: bool,
) -> Result<MediaWorkBatch, JobExecutionError> {
    let sql_cursor =
        i64::try_from(cursor).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let title_filter = if missing_only {
        "AND NOT EXISTS (
             SELECT 1 FROM assets.image_assets AS asset
              WHERE asset.title_id = title.id
                AND asset.image_kind = 'poster'
                AND asset.status = 'ready'
         )"
    } else {
        ""
    };
    let season_filter = if missing_only {
        "AND (
             NOT EXISTS (
                 SELECT 1 FROM assets.image_assets AS asset
                  WHERE asset.season_id = season.id
                    AND asset.image_kind = 'poster'
                    AND asset.status = 'ready'
             )
             OR EXISTS (
                 SELECT 1 FROM catalog.episodes AS episode
                  WHERE episode.season_id = season.id
                    AND NOT EXISTS (
                        SELECT 1 FROM assets.image_assets AS asset
                         WHERE asset.episode_id = episode.id
                           AND asset.image_kind = 'still'
                           AND asset.status = 'ready'
                    )
             )
         )"
    } else {
        ""
    };
    let reusable_filter = if missing_only {
        "AND NOT EXISTS (
             SELECT 1 FROM assets.image_assets AS asset
              WHERE asset.person_id = person.id
                AND asset.image_kind = 'profile'
                AND asset.status = 'ready'
         )"
    } else {
        ""
    };
    let company_filter = if missing_only {
        "AND NOT EXISTS (
             SELECT 1 FROM assets.image_assets AS asset
              WHERE asset.company_id = company.id
                AND asset.image_kind = 'logo'
                AND asset.status = 'ready'
         )"
    } else {
        ""
    };
    let network_filter = if missing_only {
        "AND NOT EXISTS (
             SELECT 1 FROM assets.image_assets AS asset
              WHERE asset.network_id = network.id
                AND asset.image_kind = 'logo'
                AND asset.status = 'ready'
         )"
    } else {
        ""
    };
    let collection_filter = if missing_only {
        "AND NOT EXISTS (
             SELECT 1 FROM assets.image_assets AS asset
              WHERE asset.collection_id = collection.id
                AND asset.image_kind IN ('poster', 'backdrop')
                AND asset.status = 'ready'
         )"
    } else {
        ""
    };
    let query = format!(
        "SELECT kind, media_type, tmdb_id, season_number, entity_type, entity_id
           FROM (
                SELECT 0::smallint AS kind, title.media_type, title.tmdb_id,
                       NULL::integer AS season_number, NULL::text AS entity_type,
                       NULL::bigint AS entity_id, title.id AS sort_id
                  FROM catalog.titles AS title
                 WHERE title.active {title_filter}
                UNION ALL
                SELECT 1::smallint, 'tv', title.tmdb_id, season.season_number,
                       NULL::text, NULL::bigint, season.id
                  FROM catalog.seasons AS season
                  JOIN catalog.titles AS title ON title.id = season.title_id
                 WHERE title.active {season_filter}
                UNION ALL
                SELECT 2::smallint, NULL::text, NULL::bigint, NULL::integer,
                       'person', person.id, person.id
                  FROM catalog.people AS person
                 WHERE true {reusable_filter}
                UNION ALL
                SELECT 3::smallint, NULL::text, NULL::bigint, NULL::integer,
                       'company', company.id, company.id
                  FROM catalog.companies AS company
                 WHERE true {company_filter}
                UNION ALL
                SELECT 4::smallint, NULL::text, NULL::bigint, NULL::integer,
                       'network', network.id, network.id
                  FROM catalog.networks AS network
                 WHERE true {network_filter}
                UNION ALL
                SELECT 5::smallint, NULL::text, NULL::bigint, NULL::integer,
                       'collection', collection.id, collection.id
                  FROM catalog.collections AS collection
                 WHERE true {collection_filter}
           ) AS work
          ORDER BY kind, sort_id
          OFFSET $1 LIMIT $2"
    );
    let rows: Vec<MediaWorkRow> = sqlx::query_as(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sql_cursor)
        .bind(MEDIA_WORK_BATCH_SIZE + 1)
        .fetch_all(database)
        .await
        .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    let done = rows.len() <= usize::try_from(MEDIA_WORK_BATCH_SIZE).unwrap_or(usize::MAX);
    let rows = rows
        .into_iter()
        .take(usize::try_from(MEDIA_WORK_BATCH_SIZE).unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    let mut queued = 0_usize;
    for row in &rows {
        let (job_type, payload, dedup_key) = match row.kind {
            0 => {
                let media_type = row
                    .media_type
                    .as_deref()
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                let tmdb_id = row
                    .tmdb_id
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                let job_type = match media_type {
                    "movie" => REFRESH_MOVIE_JOB,
                    "tv" => REFRESH_TV_JOB,
                    _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
                };
                (
                    job_type,
                    serde_json::json!({"tmdb_id": tmdb_id}),
                    format!("{job_type}:{tmdb_id}"),
                )
            }
            1 => {
                let tmdb_id = row
                    .tmdb_id
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                let season_number = row
                    .season_number
                    .and_then(|value| u16::try_from(value).ok())
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                (
                    REFRESH_SEASON_JOB,
                    serde_json::json!({"tv_id": tmdb_id, "season_number": season_number}),
                    format!("{REFRESH_SEASON_JOB}:{tmdb_id}:{season_number}"),
                )
            }
            2..=5 => {
                let entity_type = row
                    .entity_type
                    .as_deref()
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                let tmdb_id = row
                    .entity_id
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?;
                (
                    REFRESH_REUSABLE_GALLERY_JOB,
                    serde_json::json!({"entityType": entity_type, "tmdbId": tmdb_id}),
                    format!("{REFRESH_REUSABLE_GALLERY_JOB}:{entity_type}:{tmdb_id}"),
                )
            }
            _ => return Err(JobExecutionError::dead_letter("invalid_payload")),
        };
        let job = NewJob::new(job_type, INGEST_PAYLOAD_VERSION, payload, &dedup_key)
            .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        if submit_and_link_scan_job(database, run_id, "media", job).await? {
            queued = queued.saturating_add(1);
        }
    }
    let processed =
        u64::try_from(rows.len()).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    Ok(MediaWorkBatch {
        queued,
        next_cursor: cursor
            .checked_add(processed)
            .ok_or_else(|| JobExecutionError::dead_letter("invalid_payload"))?,
        done,
    })
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

async fn enqueue_missing_catalog_refresh_batch(
    database: &PgPool,
    media_types: &[MediaType],
    cursor: u64,
) -> Result<MissingCatalogBatch, JobExecutionError> {
    let cursor =
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
    .bind(cursor)
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
    ensure_refresh_queue_capacity(database, jobs.len(), "missing_queue_full").await?;
    let outcomes = JobRepository::new(database.clone())
        .submit_many(&jobs)
        .await
        .map_err(map_submission_error)?;
    let next_cursor = rows
        .last()
        .map(|row| u64::try_from(row.id))
        .transpose()
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?
        .unwrap_or(cursor as u64);
    Ok(MissingCatalogBatch {
        queued: outcomes
            .iter()
            .filter(|outcome| !outcome.was_duplicate())
            .count(),
        next_cursor,
        done,
    })
}

fn admin_scan_job(
    mode: AdminScanMode,
    media_types: &[MediaType],
    cursor: u64,
) -> Result<NewJob, JobExecutionError> {
    NewJob::new(
        ADMIN_SCAN_JOB,
        INGEST_PAYLOAD_VERSION,
        serde_json::json!({
            "mode": mode,
            "mediaTypes": media_types,
            "cursor": cursor
        }),
        &format!("{ADMIN_SCAN_JOB}:{mode:?}:{media_types:?}:{cursor}"),
    )
    .and_then(|job| job.with_max_attempts(100))
    .and_then(|job| job.with_available_at(Utc::now() + ChronoDuration::seconds(1)))
    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))
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
    fn optional_gallery_not_found_is_empty_but_other_errors_are_preserved() {
        assert!(matches!(
            optional_gallery(Err(TmdbClientError::NotFound)),
            Ok(gallery) if gallery.posters.is_empty()
        ));
        assert!(matches!(
            optional_gallery(Err(TmdbClientError::Unauthorized)),
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
    fn reusable_gallery_payloads_are_strict_and_use_tmdb_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let job = parse_job(
            REFRESH_REUSABLE_GALLERY_JOB,
            INGEST_PAYLOAD_VERSION,
            &serde_json::json!({"entityType":"person","tmdbId":1_373_074}),
        )?;
        assert_eq!(
            job,
            IngestJob::RefreshReusableGallery {
                entity_type: ReusableGalleryEntity::Person,
                tmdb_id: 1_373_074,
            }
        );
        assert_eq!(
            job.dedup_key(),
            "ingest.refresh_reusable_gallery:person:1373074"
        );
        assert!(matches!(
            parse_job(
                REFRESH_REUSABLE_GALLERY_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({"entityType":"person","tmdbId":1_373_074,"extra":"rejected"}),
            ),
            Err(JobPayloadError::InvalidPayload)
        ));
        Ok(())
    }

    #[test]
    fn media_scan_payloads_cover_each_mode_and_reject_unknown_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = Uuid::now_v7();
        assert_eq!(
            catalog_scan_mode(MediaScanMode::Full),
            AdminScanMode::FullSweep
        );
        assert_eq!(
            catalog_scan_mode(MediaScanMode::Missing),
            AdminScanMode::MissingOnly
        );
        for (mode, expected) in [
            ("full", MediaScanMode::Full),
            ("missing", MediaScanMode::Missing),
            ("audit", MediaScanMode::Audit),
        ] {
            let job = parse_job(
                ADMIN_MEDIA_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({
                    "runId": run_id,
                    "mode": mode,
                    "repair": mode == "audit",
                    "step": 0
                }),
            )?;
            assert!(matches!(
                job,
                IngestJob::MediaScan {
                    run_id: parsed,
                    mode: parsed_mode,
                    ..
                } if parsed == run_id && parsed_mode == expected
            ));
        }
        assert!(matches!(
            parse_job(
                ADMIN_MEDIA_SCAN_JOB,
                INGEST_PAYLOAD_VERSION,
                &serde_json::json!({"runId": run_id, "mode":"audit", "unexpected":true}),
            ),
            Err(JobPayloadError::InvalidPayload)
        ));
        Ok(())
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
    fn catalog_scan_modes_are_explicit_and_missing_scan_has_a_cursor() {
        for mode in ["full_sweep", "missing_only", "prune_cleanup", "daily_sync"] {
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
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_continuation_keeps_refresh_queue_bounded(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let export = tempfile::NamedTempFile::new()?;
        let contents = (1..=501).map(|id| format!("{id}\n")).collect::<String>();
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
        let continuation_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.daily_export'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(refresh_count, 500);
        assert_eq!(continuation_count, 1);
        Ok(())
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn daily_export_waits_when_the_refresh_queue_is_full(pool: PgPool) -> sqlx::Result<()> {
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
    async fn missing_catalog_refreshes_are_batched_and_resumable(pool: PgPool) -> sqlx::Result<()> {
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
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ops.jobs WHERE job_type = 'ingest.refresh_movie'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 500);

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

fn export_dedup_key(media_type: MediaType, url: &str, offset: u64) -> String {
    let digest = Sha256::digest(url.as_bytes());
    format!("{DAILY_EXPORT_JOB}:{media_type}:{digest:x}:{offset}")
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
    path: PathBuf,
    url: &str,
    offset: u64,
    refresh_all: bool,
) -> Result<ExportQueueSummary, JobExecutionError> {
    ensure_refresh_queue_capacity(pool, DAILY_EXPORT_BATCH_SIZE, "export_queue_incomplete").await?;
    let batch = tokio::task::spawn_blocking(move || read_export_id_batch(path, offset))
        .await
        .map_err(|_| JobExecutionError::retry("export_storage", Duration::from_secs(30)))?
        .map_err(|error| error)?;
    let detail_refresh_candidates = if refresh_all {
        enqueue_refresh_jobs_with_priority(
            pool,
            media_type,
            &batch.ids,
            DAILY_EXPORT_REFRESH_PRIORITY,
        )
        .await?
    } else {
        enqueue_missing_refresh_jobs(pool, media_type, &batch.ids).await?
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
            &export_dedup_key(media_type, url, batch.next_offset),
        )
        .and_then(|job| job.with_max_attempts(100))
        .and_then(|job| job.with_priority(DAILY_EXPORT_REFRESH_PRIORITY))
        .and_then(|job| job.with_available_at(Utc::now() + ChronoDuration::seconds(1)))
        .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
        JobRepository::new(pool.clone())
            .submit(continuation)
            .await
            .map_err(map_submission_error)?;
    }
    Ok(ExportQueueSummary {
        records: batch.ids.len(),
        detail_refresh_candidates,
        continued: !batch.finished,
        next_offset: (!batch.finished).then_some(batch.next_offset),
    })
}

async fn ensure_refresh_queue_capacity(
    pool: &PgPool,
    reserve: usize,
    failure_code: &'static str,
) -> Result<(), JobExecutionError> {
    let reserve =
        i64::try_from(reserve).map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
    let pending_refreshes: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM ops.media_scan_job_status
          WHERE job_type IN (
                    'ingest.refresh_movie', 'ingest.refresh_tv',
                    'ingest.refresh_season', 'ingest.refresh_reusable_gallery'
                )
            AND status IN ('queued', 'running', 'retry_wait')",
    )
    .fetch_one(pool)
    .await
    .map_err(|_| JobExecutionError::retry("database_unavailable", Duration::from_secs(5)))?;
    if pending_refreshes.saturating_add(reserve) > MAX_PENDING_REFRESH_JOBS {
        return Err(JobExecutionError::retry(
            failure_code,
            Duration::from_secs(10),
        ));
    }
    Ok(())
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
