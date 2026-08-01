use std::path::PathBuf;
use std::time::Duration;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tmdb_domain::MediaType;
use tmdb_jobs::{ClaimedJob, JobExecutionError, JobExecutor, JobRepository, NewJob};
use tmdb_upstream::{
    DailyExportParser, MAX_DAILY_EXPORT_BYTES, TmdbClient, TmdbClientError, TmdbKeyword,
};

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
/// Current payload version for all ingestion jobs.
pub const INGEST_PAYLOAD_VERSION: i32 = 1;
const INGEST_JOB_TYPES: &[&str] = &[
    REFRESH_MOVIE_JOB,
    REFRESH_TV_JOB,
    REFRESH_SEASON_JOB,
    CHANGES_SYNC_JOB,
    DAILY_EXPORT_JOB,
];

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
                let movie = self
                    .client
                    .fetch_movie(tmdb_id)
                    .await
                    .map_err(|error| map_upstream_error(&error))?;
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
                let series = self
                    .client
                    .fetch_tv(tmdb_id)
                    .await
                    .map_err(|error| map_upstream_error(&error))?;
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
                let change_page = self
                    .client
                    .fetch_changes(media_type, page)
                    .await
                    .map_err(|error| map_upstream_error(&error))?;
                if let Some(database) = &self.database {
                    catalog_write::persist_changes(database, media_type, &change_page).await?;
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
                }
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "page": change_page.page,
                    "total_pages": change_page.total_pages,
                    "changed_ids": change_page.results.len(),
                    "next_page_queued": change_page.total_pages > page,
                    "dedup_key": dedup_key
                }))
            }
            IngestJob::RefreshSeason {
                tv_id,
                season_number,
            } => {
                let season = self
                    .client
                    .fetch_season(tv_id, season_number)
                    .await
                    .map_err(|error| map_upstream_error(&error))?;
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
                let download = self
                    .client
                    .fetch_daily_export_to_file(&url, &destination, self.export_max_bytes)
                    .await
                    .map_err(|error| map_upstream_error(&error))?;
                let parser = self.export_parser;
                let records = tokio::task::spawn_blocking(move || parser.count_file(&destination))
                    .await
                    .map_err(|_| {
                        JobExecutionError::retry("export_storage", Duration::from_secs(30))
                    })?
                    .map_err(|_| JobExecutionError::dead_letter("invalid_payload"))?;
                Ok(serde_json::json!({
                    "media_type": media_type,
                    "records": records,
                    "dedup_key": dedup_key,
                    "bytes": download.bytes,
                    "sha256": hex_digest(&download.sha256)
                }))
            }
        }
    }
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

fn contains_anime_keyword(keywords: &[TmdbKeyword]) -> bool {
    keywords
        .iter()
        .any(|keyword| keyword.id == u64::from(tmdb_domain::ANIME_KEYWORD_ID))
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
    use super::*;

    #[test]
    fn unauthorized_upstream_errors_dead_letter_with_an_actionable_code() {
        let error = map_upstream_error(&TmdbClientError::Unauthorized);
        assert!(error.is_terminal());
        assert_eq!(error.failure_code(), "upstream_unauthorized");
        assert_eq!(error.retry_delay(), Duration::ZERO);
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

    #[test]
    fn anime_scope_uses_the_tmdb_keyword_for_both_media_types() {
        let keywords = vec![TmdbKeyword {
            id: tmdb_domain::ANIME_KEYWORD_ID.into(),
            name: Some("anime".to_owned()),
        }];
        assert!(contains_anime_keyword(&keywords));
        assert!(!contains_anime_keyword(&[TmdbKeyword {
            id: 42,
            name: Some("animation".to_owned()),
        }]));
    }
}
