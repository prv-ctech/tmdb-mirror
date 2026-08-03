//! Structured telemetry and bounded Prometheus metrics for the TMDB services.
//!
//! The public types in this crate deliberately collapse request dimensions to
//! small, reviewable sets before they reach the metrics registry.  Route
//! templates are allow-listed here; raw paths, identifiers, query strings, and
//! error text never become metric labels.

use std::{
    sync::{Arc, Once},
    time::Duration,
};

use prometheus_client::{
    encoding::{EncodeLabelSet, text},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram, info::Info},
    registry::{Registry, Unit},
};
use thiserror::Error;
use tracing_subscriber::EnvFilter;

const DEFAULT_HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0,
];
const MAX_LABEL_LENGTH: usize = 128;
const UNMATCHED_ROUTE: &str = "<unmatched>";

fn request_histogram() -> Histogram {
    Histogram::new(DEFAULT_HISTOGRAM_BUCKETS.to_vec())
}

/// Listener class used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Listener {
    /// Public API listener.
    Public,
    /// Private administrative listener.
    Admin,
}

impl Listener {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Admin => "admin",
        }
    }
}

/// HTTP method class used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MethodClass {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Other,
}

impl MethodClass {
    #[must_use]
    pub fn from_method(method: &str) -> Self {
        match method {
            value if value.eq_ignore_ascii_case("GET") => Self::Get,
            value if value.eq_ignore_ascii_case("POST") => Self::Post,
            value if value.eq_ignore_ascii_case("PUT") => Self::Put,
            value if value.eq_ignore_ascii_case("PATCH") => Self::Patch,
            value if value.eq_ignore_ascii_case("DELETE") => Self::Delete,
            value if value.eq_ignore_ascii_case("HEAD") => Self::Head,
            value if value.eq_ignore_ascii_case("OPTIONS") => Self::Options,
            _ => Self::Other,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Other => "OTHER",
        }
    }
}

/// HTTP status class used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StatusClass {
    Informational,
    Success,
    Redirection,
    ClientError,
    ServerError,
    Other,
}

impl StatusClass {
    #[must_use]
    pub const fn from_status(status: u16) -> Self {
        match status {
            100..=199 => Self::Informational,
            200..=299 => Self::Success,
            300..=399 => Self::Redirection,
            400..=499 => Self::ClientError,
            500..=599 => Self::ServerError,
            _ => Self::Other,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Informational => "1xx",
            Self::Success => "2xx",
            Self::Redirection => "3xx",
            Self::ClientError => "4xx",
            Self::ServerError => "5xx",
            Self::Other => "other",
        }
    }
}

/// Readiness failure class used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReadinessFailureReason {
    ProbeFailed,
    ProbeTimeout,
}

impl ReadinessFailureReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProbeFailed => "probe_failed",
            Self::ProbeTimeout => "probe_timeout",
        }
    }
}

/// Pool role class used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PoolRole {
    ApiReader,
    Migrator,
    Worker,
    Other,
}

impl PoolRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiReader => "api_reader",
            Self::Migrator => "migrator",
            Self::Worker => "worker",
            Self::Other => "other",
        }
    }
}

/// Pool acquisition outcome used as a bounded metric label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PoolAcquireOutcome {
    Success,
    Timeout,
    Error,
}

/// A fixed component class reported by operational health metrics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Component {
    /// Main migration, ingest, scheduling, and maintenance worker.
    Worker,
    /// Local media download and verification worker.
    Media,
    /// TMDB/Trawl upstream access.
    Upstream,
    /// PostgreSQL-local pgBackRest runner.
    Backup,
}

impl Component {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Media => "media",
            Self::Upstream => "upstream",
            Self::Backup => "backup",
        }
    }
}

/// Bounded state emitted for an operational component.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ComponentState {
    Ready,
    Degraded,
    Failed,
    Stale,
    Unknown,
}

impl ComponentState {
    const ALL: [Self; 5] = [
        Self::Ready,
        Self::Degraded,
        Self::Failed,
        Self::Stale,
        Self::Unknown,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
            Self::Stale => "stale",
            Self::Unknown => "unknown",
        }
    }
}

/// Durable job queue state reported in bounded operational summaries.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueueState {
    Queued,
    Running,
    RetryWait,
    DeadLetter,
}

impl QueueState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::RetryWait => "retry_wait",
            Self::DeadLetter => "dead_letter",
        }
    }
}

/// Fixed catalog count scopes used by metrics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CatalogScope {
    Movies,
    Tv,
    AnimeMovies,
    AnimeTv,
}

impl CatalogScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Movies => "movies",
            Self::Tv => "tv",
            Self::AnimeMovies => "anime_movies",
            Self::AnimeTv => "anime_tv",
        }
    }
}

impl PoolAcquireOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

/// The dimensions needed to observe one completed HTTP request.
#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
pub struct HttpRequestLabels {
    listener: String,
    method: String,
    route: String,
    status_class: String,
}

impl HttpRequestLabels {
    /// Creates labels after collapsing method, status, and route values.
    #[must_use]
    pub fn new(listener: Listener, method: MethodClass, route: &str, status: StatusClass) -> Self {
        Self {
            listener: listener.as_str().to_owned(),
            method: method.as_str().to_owned(),
            route: normalize_route(route),
            status_class: status.as_str().to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct ReadinessLabels {
    reason: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct PoolLabels {
    role: String,
    outcome: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct ComponentStateLabels {
    component: String,
    state: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct QueueLabels {
    job_type: String,
    state: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct CatalogLabels {
    scope: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct BuildLabels {
    service: String,
    version: String,
    revision: String,
}

fn normalize_route(route: &str) -> String {
    let unversioned = route.strip_prefix("/v1").unwrap_or(route);
    if matches!(
        unversioned,
        "/health/live"
            | "/health/ready"
            | "/metrics"
            | "/openapi.json"
            | "/movies"
            | "/movies/popular"
            | "/movies/recent"
            | "/movies/top-rated"
            | "/movies/{tmdb_id}"
            | "/movies/{tmdb_id}/translations"
            | "/movies/{tmdb_id}/alternate-titles"
            | "/movies/{tmdb_id}/external-ids"
            | "/movies/{tmdb_id}/videos"
            | "/movies/{tmdb_id}/release-dates"
            | "/movies/{tmdb_id}/credits"
            | "/movies/{tmdb_id}/images"
            | "/tv"
            | "/tv/popular"
            | "/tv/recent"
            | "/tv/top-rated"
            | "/tv/{tmdb_id}"
            | "/tv/{tmdb_id}/translations"
            | "/tv/{tmdb_id}/alternate-titles"
            | "/tv/{tmdb_id}/external-ids"
            | "/tv/{tmdb_id}/videos"
            | "/tv/{tmdb_id}/certifications"
            | "/tv/{tmdb_id}/credits"
            | "/tv/{tmdb_id}/images"
            | "/tv/{tmdb_id}/seasons"
            | "/tv/{tmdb_id}/seasons/{season_number}"
            | "/tv/{tmdb_id}/seasons/{season_number}/episodes"
            | "/tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}"
            | "/anime"
            | "/anime/popular"
            | "/anime/recent"
            | "/anime/top-rated"
            | "/anime/{media_type}/{tmdb_id}"
            | "/anime/{media_type}/{tmdb_id}/translations"
            | "/anime/{media_type}/{tmdb_id}/alternate-titles"
            | "/anime/{media_type}/{tmdb_id}/external-ids"
            | "/anime/{media_type}/{tmdb_id}/videos"
            | "/anime/{media_type}/{tmdb_id}/release-dates"
            | "/anime/{media_type}/{tmdb_id}/credits"
            | "/anime/{media_type}/{tmdb_id}/images"
            | "/trending/{trend_window}"
            | "/anime/trending/{trend_window}"
            | "/calendar/movies"
            | "/calendar/tv"
            | "/search"
            | "/genres"
            | "/languages"
            | "/keywords"
            | "/tags"
            | "/people"
            | "/companies"
            | "/networks"
            | "/collections"
            | "/admin/v1/openapi.json"
            | "/admin/v1/status"
            | "/admin/v1/jobs"
            | "/admin/v1/jobs/{job_id}"
            | "/admin/v1/scans"
            | "/admin/v1/media/scans"
            | "/admin/v1/media/scans/{run_id}"
            | "/admin/v1/media/worker"
            | "/admin/v1/jobs/{job_id}/cancel"
            | "/admin/v1/jobs/{job_id}/retry"
            | "/admin/v1/media/audits"
            | "/admin/v1/maintenance/analyze"
            | "/admin/v1/backups"
            | "/__test/panic"
            | "/__test/block"
            | "/__test/large"
    ) {
        route.to_owned()
    } else {
        UNMATCHED_ROUTE.to_owned()
    }
}

fn normalize_job_type(job_type: &str) -> &'static str {
    match job_type {
        "system.noop" => "system.noop",
        "image.download" => "image.download",
        "ingest.refresh_movie" => "ingest.refresh_movie",
        "ingest.refresh_tv" => "ingest.refresh_tv",
        "ingest.refresh_season" => "ingest.refresh_season",
        "ingest.changes" => "ingest.changes",
        "ingest.changes_sync" => "ingest.changes_sync",
        "ingest.daily_export" => "ingest.daily_export",
        "ingest.trending" => "ingest.trending",
        "ingest.refresh_reusable_gallery" => "ingest.refresh_reusable_gallery",
        "admin.scan" => "admin.scan",
        "admin.media_scan" => "admin.media_scan",
        "admin.media_audit" => "admin.media_audit",
        "admin.analyze" => "admin.analyze",
        "database.backup_full" => "database.backup_full",
        "database.backup_diff" => "database.backup_diff",
        _ => "other",
    }
}

fn bounded_identity(value: &str) -> String {
    if value.is_empty() || value.len() > MAX_LABEL_LENGTH || value.chars().any(char::is_control) {
        "<invalid>".to_owned()
    } else {
        value.to_owned()
    }
}

/// Encoding failure for the private `OpenMetrics` endpoint.
#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("metrics encoding failed")]
    Encoding(#[from] std::fmt::Error),
}

/// Shared metrics registry and hot-path metric handles.
#[derive(Clone, Debug)]
pub struct Metrics {
    registry: Arc<Registry>,
    http_requests: Family<HttpRequestLabels, Counter>,
    http_duration: Family<HttpRequestLabels, Histogram, fn() -> Histogram>,
    readiness_failures: Family<ReadinessLabels, Counter>,
    pool_acquire_duration: Family<PoolLabels, Histogram, fn() -> Histogram>,
    component_state: Family<ComponentStateLabels, Gauge>,
    queue_depth: Family<QueueLabels, Gauge>,
    catalog_titles: Family<CatalogLabels, Gauge>,
    backup_last_success_timestamp: Gauge,
    backup_last_failure_timestamp: Gauge,
}

impl Metrics {
    /// Builds and registers the complete Task 7 metric set once.
    #[must_use]
    pub fn new(service: &str, version: &str, revision: &str) -> Self {
        let http_requests = Family::default();
        let http_duration = Family::new_with_constructor(request_histogram as fn() -> Histogram);
        let readiness_failures = Family::default();
        let pool_acquire_duration =
            Family::new_with_constructor(request_histogram as fn() -> Histogram);
        let component_state = Family::default();
        let queue_depth = Family::default();
        let catalog_titles = Family::default();
        let backup_last_success_timestamp = Gauge::default();
        let backup_last_failure_timestamp = Gauge::default();
        let build = Info::new(BuildLabels {
            service: bounded_identity(service),
            version: bounded_identity(version),
            revision: bounded_identity(revision),
        });

        let mut registry = Registry::default();
        registry.register(
            "tmdb_http_requests",
            "Completed HTTP requests",
            http_requests.clone(),
        );
        registry.register_with_unit(
            "tmdb_http_request_duration",
            "End-to-end HTTP request latency",
            Unit::Seconds,
            http_duration.clone(),
        );
        registry.register(
            "tmdb_readiness_failures",
            "Failed readiness checks",
            readiness_failures.clone(),
        );
        registry.register_with_unit(
            "tmdb_db_pool_acquire_duration",
            "Database pool acquisition latency",
            Unit::Seconds,
            pool_acquire_duration.clone(),
        );
        registry.register(
            "tmdb_component_state",
            "Current bounded component state; exactly one state per component is one",
            component_state.clone(),
        );
        registry.register(
            "tmdb_queue_depth",
            "Durable job count by bounded job type and status",
            queue_depth.clone(),
        );
        registry.register(
            "tmdb_catalog_titles",
            "Catalog title count by bounded media scope",
            catalog_titles.clone(),
        );
        registry.register_with_unit(
            "tmdb_backup_last_success_timestamp",
            "Unix timestamp of the latest successful backup; zero means none recorded",
            Unit::Seconds,
            backup_last_success_timestamp.clone(),
        );
        registry.register_with_unit(
            "tmdb_backup_last_failure_timestamp",
            "Unix timestamp of the latest failed backup; zero means none recorded",
            Unit::Seconds,
            backup_last_failure_timestamp.clone(),
        );
        registry.register("tmdb_build", "Immutable build identity", build);

        Self {
            registry: Arc::new(registry),
            http_requests,
            http_duration,
            readiness_failures,
            pool_acquire_duration,
            component_state,
            queue_depth,
            catalog_titles,
            backup_last_success_timestamp,
            backup_last_failure_timestamp,
        }
    }

    /// Records one completed HTTP request and its end-to-end duration.
    pub fn observe_http(&self, labels: &HttpRequestLabels, duration: Duration) {
        self.http_requests.get_or_create(labels).inc();
        self.http_duration
            .get_or_create(labels)
            .observe(duration.as_secs_f64());
    }

    /// Records one bounded readiness failure.
    pub fn inc_readiness_failure(&self, reason: ReadinessFailureReason) {
        self.readiness_failures
            .get_or_create(&ReadinessLabels {
                reason: reason.as_str().to_owned(),
            })
            .inc();
    }

    /// Records a measured database pool acquisition.
    pub fn observe_pool_acquire(
        &self,
        role: PoolRole,
        outcome: PoolAcquireOutcome,
        duration: Duration,
    ) {
        self.pool_acquire_duration
            .get_or_create(&PoolLabels {
                role: role.as_str().to_owned(),
                outcome: outcome.as_str().to_owned(),
            })
            .observe(duration.as_secs_f64());
    }

    /// Sets one current component state, clearing the other fixed state gauges.
    pub fn set_component_state(&self, component: Component, current: ComponentState) {
        for state in ComponentState::ALL {
            self.component_state
                .get_or_create(&ComponentStateLabels {
                    component: component.as_str().to_owned(),
                    state: state.as_str().to_owned(),
                })
                .set(i64::from(state == current));
        }
    }

    /// Sets the observed queue depth after collapsing unknown job types to `other`.
    pub fn set_queue_depth(&self, job_type: &str, state: QueueState, count: i64) {
        self.queue_depth
            .get_or_create(&QueueLabels {
                job_type: normalize_job_type(job_type).to_owned(),
                state: state.as_str().to_owned(),
            })
            .set(count.max(0));
    }

    /// Sets a catalog title count for one fixed scope.
    pub fn set_catalog_count(&self, scope: CatalogScope, count: i64) {
        self.catalog_titles
            .get_or_create(&CatalogLabels {
                scope: scope.as_str().to_owned(),
            })
            .set(count.max(0));
    }

    /// Sets latest durable backup outcome timestamps in Unix seconds.
    ///
    /// A value of `None` is represented as zero so a recovered service cannot
    /// accidentally continue exposing a stale success/failure timestamp.
    pub fn set_backup_timestamps(&self, last_success: Option<i64>, last_failure: Option<i64>) {
        self.backup_last_success_timestamp
            .set(last_success.unwrap_or_default().max(0));
        self.backup_last_failure_timestamp
            .set(last_failure.unwrap_or_default().max(0));
    }

    /// Encodes the complete `OpenMetrics` exposition, including `# EOF`.
    ///
    /// # Errors
    ///
    /// Returns an error if the encoder cannot write the exposition.
    pub fn encode(&self) -> Result<String, MetricsError> {
        let mut encoded = String::new();
        text::encode(&mut encoded, &self.registry)?;
        Ok(encoded)
    }
}

/// Log output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogFormat {
    /// Structured JSON suitable for production collection.
    Json,
    /// Colorized human-readable output for an interactive terminal.
    Pretty,
}

/// Failure while building or installing the process-wide tracing subscriber.
#[derive(Debug, Error)]
pub enum InitTracingError {
    #[error("invalid tracing filter")]
    InvalidFilter(#[source] tracing_subscriber::filter::ParseError),
    #[error("TMDB_LOG_FORMAT must be pretty or json")]
    InvalidLogFormat,
    #[error("TMDB_LOG_LEVEL must be error, warn, info, debug, or trace")]
    InvalidLogLevel,
    #[error("tracing subscriber is already initialized")]
    AlreadyInitialized,
}

/// Initializes tracing using the deployment's human-readable log settings.
///
/// `TMDB_LOG_FORMAT` defaults to `pretty`; set it to `json` only when a log
/// collector needs structured JSON. `RUST_LOG` remains an advanced complete
/// filter override. Without it, `TMDB_LOG_LEVEL` defaults to `info`.
///
/// # Errors
///
/// Returns an error for an invalid log format/level or an already-initialized
/// process-wide subscriber.
pub fn init_tracing_from_env(service_name: &str) -> Result<(), InitTracingError> {
    let configured_format = std::env::var("TMDB_LOG_FORMAT").ok();
    init_tracing(
        service_name,
        parse_log_format(configured_format.as_deref())?,
    )
}

fn parse_log_format(value: Option<&str>) -> Result<LogFormat, InitTracingError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("pretty") => Ok(LogFormat::Pretty),
        Some("json") => Ok(LogFormat::Json),
        Some(_) => Err(InitTracingError::InvalidLogFormat),
    }
}

fn default_filter(service_name: &str) -> Result<String, InitTracingError> {
    let configured_level = std::env::var("TMDB_LOG_LEVEL").ok();
    let level = parse_log_level(configured_level.as_deref())?;
    // SQLx emits full query text for routine slow-statement warnings. The
    // services emit bounded lifecycle and database outcomes themselves, so
    // default terminal logs keep SQLx errors but suppress raw query noise.
    Ok(format!("{level},{service_name}={level},sqlx=error"))
}

fn parse_log_level(value: Option<&str>) -> Result<String, InitTracingError> {
    let level = value
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "info".to_owned());
    if !matches!(
        level.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    ) {
        return Err(InitTracingError::InvalidLogLevel);
    }
    Ok(level)
}

/// Initializes tracing using `RUST_LOG` or the `TMDB_LOG_LEVEL` INFO default.
///
/// # Errors
///
/// Returns an error when the process-wide subscriber is already installed.
pub fn init_tracing(service_name: &str, format: LogFormat) -> Result<(), InitTracingError> {
    init_tracing_with_filter(service_name, format, None)
}

/// Initializes tracing with an explicit filter, used by tests and controlled launchers.
///
/// # Errors
///
/// Returns an error when the filter is invalid or the process-wide subscriber
/// is already installed.
pub fn init_tracing_with_filter(
    service_name: &str,
    format: LogFormat,
    explicit_filter: Option<&str>,
) -> Result<(), InitTracingError> {
    let filter_value = explicit_filter
        .map(str::to_owned)
        .or_else(|| std::env::var("RUST_LOG").ok())
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| default_filter(service_name), Ok)?;
    let filter = EnvFilter::try_new(filter_value).map_err(InitTracingError::InvalidFilter)?;

    install_sanitized_panic_hook();
    let result = match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_target(false)
            .try_init(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .compact()
            .with_ansi(true)
            .with_env_filter(filter)
            .with_target(false)
            .with_file(false)
            .with_line_number(false)
            .try_init(),
    };
    result.map_err(|_| InitTracingError::AlreadyInitialized)
}

#[cfg(test)]
mod log_tests {
    use super::*;

    #[test]
    fn terminal_logs_default_to_pretty_and_accept_json() {
        assert!(matches!(parse_log_format(None), Ok(LogFormat::Pretty)));
        assert!(matches!(parse_log_format(Some("")), Ok(LogFormat::Pretty)));
        assert!(matches!(
            parse_log_format(Some("json")),
            Ok(LogFormat::Json)
        ));
    }

    #[test]
    fn invalid_terminal_log_format_is_rejected() {
        assert!(matches!(
            parse_log_format(Some("xml")),
            Err(InitTracingError::InvalidLogFormat)
        ));
    }

    #[test]
    fn terminal_log_level_is_bounded_and_case_insensitive() {
        assert!(matches!(
            parse_log_level(Some(" DEBUG ")),
            Ok(level) if level == "debug"
        ));
        assert!(matches!(
            parse_log_level(Some("verbose")),
            Err(InitTracingError::InvalidLogLevel)
        ));
    }
}

fn install_sanitized_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        std::panic::set_hook(Box::new(|_| {
            eprintln!("event=panic detail=redacted");
        }));
    });
}

/// Input for one structured request-completion event.
#[derive(Clone, Debug)]
pub struct HttpRequestLog<'a> {
    /// Service name.
    pub service: &'a str,
    /// Service build version.
    pub version: &'a str,
    /// Correlation request identifier.
    pub request_id: &'a str,
    /// Listener class.
    pub listener: Listener,
    /// Normalized method class.
    pub method: MethodClass,
    /// Matched route template.
    pub route: &'a str,
    /// HTTP status code.
    pub status: u16,
    /// End-to-end request duration.
    pub duration: Duration,
    /// Bounded outcome code.
    pub outcome: &'a str,
}

/// Emits a structured request completion event without untrusted values.
pub fn log_http_request(event: &HttpRequestLog<'_>) {
    let safe_outcome = bounded_identity(event.outcome);
    let status_class = StatusClass::from_status(event.status).as_str();
    if event.status >= 400 {
        tracing::warn!(
            event = "http_request_complete",
            service = %bounded_identity(event.service),
            version = %bounded_identity(event.version),
            request_id = %bounded_identity(event.request_id),
            listener = event.listener.as_str(),
            method = event.method.as_str(),
            route = %normalize_route(event.route),
            status = event.status,
            status_class,
            duration_ms = event.duration.as_secs_f64() * 1000.0,
            outcome = %safe_outcome,
        );
    } else {
        tracing::debug!(
            event = "http_request_complete",
            service = %bounded_identity(event.service),
            version = %bounded_identity(event.version),
            request_id = %bounded_identity(event.request_id),
            listener = event.listener.as_str(),
            method = event.method.as_str(),
            route = %normalize_route(event.route),
            status = event.status,
            status_class,
            duration_ms = event.duration.as_secs_f64() * 1000.0,
            outcome = %safe_outcome,
        );
    }
}
