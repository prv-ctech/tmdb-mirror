use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode, header::RETRY_AFTER};
use reqwest::{Client, Url};
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tmdb_domain::MediaType;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

use crate::policy::{PolicyError, RequestGate, RetryPolicy};
use crate::{
    ChangeHistory, ChangePage, TmdbImages, TmdbMovie, TmdbSeason, TmdbTrendingPage, TmdbTv,
    TmdbVideos,
};

/// Hard upper bound for one streamed daily export download.
pub const MAX_DAILY_EXPORT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Metadata returned after a streamed daily export is atomically published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DailyExportDownload {
    /// Number of compressed bytes written to the destination.
    pub bytes: u64,
    /// SHA-256 digest of the exact published bytes.
    pub sha256: [u8; 32],
}

/// A bounded upstream response classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseClass {
    /// A successful response body can be decoded.
    Success,
    /// The conditional request has no changed body.
    NotModified,
    /// TMDB asked the caller to slow down.
    RateLimited { retry_after: Option<Duration> },
    /// A server or gateway failure may succeed on a bounded retry.
    TransientServer { status: u16 },
    /// The access token was rejected; no retry is safe.
    Unauthorized,
    /// Access was denied; challenge detection is separate.
    Forbidden,
    /// The requested TMDB entity does not exist.
    NotFound,
    /// Other client failures are not retried.
    PermanentClient { status: u16 },
}

/// Whether a response is eligible for the user's existing Trawl service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrawlDecision {
    /// The URL, status, or body did not satisfy the strict fallback policy.
    NotEligible,
    /// A tested challenge signature was found on an allowlisted TMDB host.
    ChallengeDetected,
}

/// A reusable, bounded TMDB HTTP client.
#[derive(Clone)]
pub struct TmdbClient {
    http: Client,
    base_url: Url,
    token: SecretString,
    policy: RetryPolicy,
    gate: std::sync::Arc<RequestGate>,
}

impl fmt::Debug for TmdbClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TmdbClient")
            .field("base_url", &self.base_url)
            .field("token", &"[REDACTED]")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl TmdbClient {
    /// Constructs a client from an HTTP(S) base URL and secret bearer token.
    ///
    /// A URL may include a path prefix for deterministic test servers, but it
    /// cannot contain credentials, a query, or a fragment.
    ///
    /// # Errors
    ///
    /// Returns a sanitized configuration error when the URL or HTTP client
    /// cannot be created.
    pub fn new(
        base_url: &str,
        token: SecretString,
        policy: RetryPolicy,
    ) -> Result<Self, TmdbClientError> {
        let mut base_url = Url::parse(base_url).map_err(|_| TmdbClientError::InvalidBaseUrl)?;
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || base_url.username() != ""
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(TmdbClientError::InvalidBaseUrl);
        }
        if !base_url.path().ends_with('/') {
            let mut path = base_url.path().to_owned();
            path.push('/');
            base_url.set_path(&path);
        }
        let http = Client::builder()
            .timeout(policy.request_timeout)
            .user_agent("tmdb-rust-ingest/0.1")
            .build()
            .map_err(|_| TmdbClientError::HttpClientBuild)?;
        Ok(Self {
            http,
            base_url,
            token,
            gate: RequestGate::new(policy.rate_limit),
            policy,
        })
    }

    /// Returns the validated policy without exposing the bearer token.
    #[must_use]
    pub const fn policy(&self) -> RetryPolicy {
        self.policy
    }

    /// Fetches a typed movie detail response.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_movie(&self, tmdb_id: u32) -> Result<TmdbMovie, TmdbClientError> {
        if tmdb_id == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(
            &format!("movie/{tmdb_id}"),
            &[ (
                "append_to_response",
                "keywords,credits,translations,alternative_titles,external_ids,videos,release_dates"
                    .to_owned(),
            )],
            true,
        )
        .await
    }

    /// Fetches a typed television detail response.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_tv(&self, tmdb_id: u32) -> Result<TmdbTv, TmdbClientError> {
        if tmdb_id == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(
            &format!("tv/{tmdb_id}"),
            &[ (
                "append_to_response",
                "keywords,credits,translations,alternative_titles,external_ids,videos,content_ratings"
                    .to_owned(),
            )],
            true,
        )
        .await
    }

    /// Fetches a full TV season, including episodes and episode credits.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_season(
        &self,
        tv_id: u32,
        season_number: u16,
    ) -> Result<TmdbSeason, TmdbClientError> {
        if tv_id == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(
            &format!("tv/{tv_id}/season/{season_number}"),
            &[("append_to_response", "credits".to_owned())],
            true,
        )
        .await
    }

    /// Fetches the complete English and untagged movie image gallery.
    pub async fn fetch_movie_images(&self, tmdb_id: u32) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("movie/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches the complete English and untagged TV image gallery.
    pub async fn fetch_tv_images(&self, tmdb_id: u32) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("tv/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches a season image gallery, including season zero specials.
    pub async fn fetch_season_images(
        &self,
        tv_id: u32,
        season_number: u16,
    ) -> Result<TmdbImages, TmdbClientError> {
        if tv_id == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_images(&format!("tv/{tv_id}/season/{season_number}/images"), true)
            .await
    }

    /// Fetches an episode image gallery.
    pub async fn fetch_episode_images(
        &self,
        tv_id: u32,
        season_number: u16,
        episode_number: u16,
    ) -> Result<TmdbImages, TmdbClientError> {
        if tv_id == 0 || episode_number == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_images(
            &format!("tv/{tv_id}/season/{season_number}/episode/{episode_number}/images"),
            true,
        )
        .await
    }

    /// Fetches a person's profile image gallery.
    pub async fn fetch_person_images(&self, tmdb_id: u32) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("person/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches a production-company logo gallery.
    pub async fn fetch_company_images(&self, tmdb_id: u32) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("company/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches a broadcast-network logo gallery.
    pub async fn fetch_network_images(&self, tmdb_id: u32) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("network/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches a collection poster/backdrop gallery.
    pub async fn fetch_collection_images(
        &self,
        tmdb_id: u32,
    ) -> Result<TmdbImages, TmdbClientError> {
        self.fetch_images(&format!("collection/{tmdb_id}/images"), tmdb_id != 0)
            .await
    }

    /// Fetches all title-level movie video records.
    pub async fn fetch_movie_videos(&self, tmdb_id: u32) -> Result<TmdbVideos, TmdbClientError> {
        self.fetch_videos(&format!("movie/{tmdb_id}/videos"), tmdb_id != 0)
            .await
    }

    /// Fetches all title-level TV video records.
    pub async fn fetch_tv_videos(&self, tmdb_id: u32) -> Result<TmdbVideos, TmdbClientError> {
        self.fetch_videos(&format!("tv/{tmdb_id}/videos"), tmdb_id != 0)
            .await
    }

    async fn fetch_images(
        &self,
        path: &str,
        valid_id: bool,
    ) -> Result<TmdbImages, TmdbClientError> {
        if !valid_id {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(
            path,
            &[
                ("language", "en-US".to_owned()),
                ("include_image_language", "en,null".to_owned()),
            ],
            true,
        )
        .await
    }

    async fn fetch_videos(
        &self,
        path: &str,
        valid_id: bool,
    ) -> Result<TmdbVideos, TmdbClientError> {
        if !valid_id {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(
            path,
            &[
                ("language", "en-US".to_owned()),
                ("include_video_language", "en,null".to_owned()),
            ],
            true,
        )
        .await
    }

    /// Fetches one page of changed IDs for movies or television.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_changes(
        &self,
        media_type: MediaType,
        page: u32,
    ) -> Result<ChangePage, TmdbClientError> {
        if page == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        let path = format!("{media_type}/changes");
        self.fetch_json(&path, &[("page", page.to_string())], true)
            .await
    }

    /// Fetches the first page of a typed TMDB trending feed.
    ///
    /// The caller must select one concrete movie/TV namespace and either the
    /// current `day` or `week` window. This avoids an ambiguous mixed feed and
    /// keeps the returned rank deterministic for persistence.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_trending(
        &self,
        media_type: MediaType,
        trend_window: &str,
    ) -> Result<TmdbTrendingPage, TmdbClientError> {
        if !matches!(trend_window, "day" | "week") {
            return Err(TmdbClientError::InvalidPath);
        }
        self.fetch_json(&format!("trending/{media_type}/{trend_window}"), &[], true)
            .await
    }

    /// Fetches one page of field-level changes for a specific entity.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or JSON-decoding error.
    pub async fn fetch_entity_changes(
        &self,
        media_type: MediaType,
        tmdb_id: u32,
        page: u32,
    ) -> Result<ChangeHistory, TmdbClientError> {
        if tmdb_id == 0 || page == 0 {
            return Err(TmdbClientError::InvalidPath);
        }
        let path = format!("{media_type}/{tmdb_id}/changes");
        self.fetch_json(&path, &[("page", page.to_string())], true)
            .await
    }

    /// Downloads a daily export body without sending the bearer token.
    ///
    /// The caller should pass the bytes to [`crate::parse_daily_export`] or a
    /// bounded [`crate::DailyExportParser`].
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, or URL validation error.
    pub async fn fetch_daily_export(&self, url: &str) -> Result<Vec<u8>, TmdbClientError> {
        let url = Url::parse(url).map_err(|_| TmdbClientError::InvalidBaseUrl)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TmdbClientError::InvalidBaseUrl);
        }
        self.request_bytes(url, false).await
    }

    /// Streams a daily export to an atomically published file.
    ///
    /// The normal JSON response bound is intentionally small. Daily exports
    /// are much larger, so this method applies a separate explicit byte cap,
    /// retries a failed stream from the beginning, and never exposes a partial
    /// destination file. The caller owns the destination directory and should
    /// parse the published file with [`crate::DailyExportParser`].
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, URL, storage, or size-limit error.
    #[allow(clippy::too_many_lines)]
    pub async fn fetch_daily_export_to_file(
        &self,
        url: &str,
        destination: impl AsRef<Path>,
        max_bytes: u64,
    ) -> Result<DailyExportDownload, TmdbClientError> {
        let url = Url::parse(url).map_err(|_| TmdbClientError::InvalidBaseUrl)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TmdbClientError::InvalidBaseUrl);
        }
        if max_bytes == 0 || max_bytes > MAX_DAILY_EXPORT_BYTES {
            return Err(TmdbClientError::ExportSizeLimit);
        }
        let destination = destination.as_ref();
        if destination.as_os_str().is_empty() || destination.file_name().is_none() {
            return Err(TmdbClientError::InvalidExportDestination);
        }

        for attempt in 1..=self.policy.max_attempts.get() {
            let permit = self.gate.acquire().await.map_err(TmdbClientError::Policy)?;
            let (mut temporary, temporary_path) = create_temporary_file(destination).await?;
            let response = self.http.get(url.clone()).send().await;
            let response = match response {
                Ok(response) => response,
                Err(_) if attempt < self.policy.max_attempts.get() => {
                    drop(permit);
                    remove_temporary_file(&temporary_path).await;
                    sleep(self.policy.backoff(attempt)).await;
                    continue;
                }
                Err(_) => {
                    drop(permit);
                    remove_temporary_file(&temporary_path).await;
                    return Err(TmdbClientError::Transport);
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            if !status.is_success() {
                let body = match read_bounded(response, self.policy.max_response_bytes).await {
                    Ok(body) => body,
                    Err(error) => {
                        drop(permit);
                        remove_temporary_file(&temporary_path).await;
                        return Err(error);
                    }
                };
                drop(permit);
                remove_temporary_file(&temporary_path).await;
                match classify_response(status, &headers, &body) {
                    ResponseClass::NotModified => return Err(TmdbClientError::NotModified),
                    ResponseClass::RateLimited { retry_after } => {
                        if attempt < self.policy.max_attempts.get() {
                            let retry_delay = retry_after
                                .unwrap_or_else(|| self.policy.backoff(attempt))
                                .min(self.policy.backoff_max);
                            sleep(retry_delay).await;
                            continue;
                        }
                        return Err(TmdbClientError::RateLimited {
                            retry_after: retry_after.map(|value| value.as_secs()),
                        });
                    }
                    ResponseClass::TransientServer { status } => {
                        if attempt < self.policy.max_attempts.get() {
                            sleep(self.policy.backoff(attempt)).await;
                            continue;
                        }
                        return Err(TmdbClientError::UpstreamServer { status });
                    }
                    ResponseClass::Unauthorized => return Err(TmdbClientError::Unauthorized),
                    ResponseClass::Forbidden => {
                        let trawl = trawl_decision(&url, status, &headers, &body);
                        return Err(TmdbClientError::Forbidden { trawl });
                    }
                    ResponseClass::NotFound => return Err(TmdbClientError::NotFound),
                    ResponseClass::PermanentClient { status } => {
                        return Err(TmdbClientError::PermanentHttp { status });
                    }
                    ResponseClass::Success => return Err(TmdbClientError::Transport),
                }
            }

            let mut response = response;
            let mut bytes_written = 0_u64;
            let mut digest = Sha256::new();
            let mut stream_failed = false;
            loop {
                let Ok(next_chunk) = response.chunk().await else {
                    stream_failed = true;
                    break;
                };
                let Some(chunk) = next_chunk else {
                    break;
                };
                let chunk_len =
                    u64::try_from(chunk.len()).map_err(|_| TmdbClientError::ExportSizeLimit)?;
                bytes_written = bytes_written
                    .checked_add(chunk_len)
                    .ok_or(TmdbClientError::ExportSizeLimit)?;
                if bytes_written > max_bytes {
                    drop(permit);
                    remove_temporary_file(&temporary_path).await;
                    return Err(TmdbClientError::ExportSizeLimit);
                }
                if temporary.write_all(&chunk).await.is_err() {
                    drop(permit);
                    remove_temporary_file(&temporary_path).await;
                    return Err(TmdbClientError::ExportStorage);
                }
                digest.update(&chunk);
            }
            if stream_failed {
                drop(permit);
                remove_temporary_file(&temporary_path).await;
                if attempt < self.policy.max_attempts.get() {
                    sleep(self.policy.backoff(attempt)).await;
                    continue;
                }
                return Err(TmdbClientError::Transport);
            }
            if temporary.sync_all().await.is_err() {
                drop(permit);
                remove_temporary_file(&temporary_path).await;
                return Err(TmdbClientError::ExportStorage);
            }
            drop(permit);
            if fs::rename(&temporary_path, destination).await.is_err() {
                remove_temporary_file(&temporary_path).await;
                return Err(TmdbClientError::ExportStorage);
            }
            return Ok(DailyExportDownload {
                bytes: bytes_written,
                sha256: digest.finalize().into(),
            });
        }
        Err(TmdbClientError::Transport)
    }

    /// Performs a bounded GET for a caller-owned path and query.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP, policy, URL, or JSON-decoding error.
    pub async fn fetch_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        with_bearer: bool,
    ) -> Result<T, TmdbClientError>
    where
        T: DeserializeOwned,
    {
        if path.is_empty()
            || path.starts_with('/')
            || path.starts_with("//")
            || path.contains("://")
            || path.contains(['?', '#'])
            || path.chars().any(char::is_control)
        {
            return Err(TmdbClientError::InvalidPath);
        }
        let url = self
            .base_url
            .join(path)
            .map_err(|_| TmdbClientError::InvalidPath)?;
        let mut url = url;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.clear();
            for (name, value) in query {
                pairs.append_pair(name, value);
            }
        }
        let bytes = self.request_bytes(url, with_bearer).await?;
        serde_json::from_slice(&bytes).map_err(|_| TmdbClientError::MalformedJson {
            endpoint: bounded_endpoint(path),
        })
    }

    async fn request_bytes(&self, url: Url, with_bearer: bool) -> Result<Vec<u8>, TmdbClientError> {
        for attempt in 1..=self.policy.max_attempts.get() {
            let permit = self.gate.acquire().await.map_err(TmdbClientError::Policy)?;
            let mut request = self.http.get(url.clone());
            if with_bearer {
                request = request.bearer_auth(self.token.expose_secret());
            }
            let response = request.send().await;
            let response = match response {
                Ok(response) => response,
                Err(_) if attempt < self.policy.max_attempts.get() => {
                    drop(permit);
                    sleep(self.policy.backoff(attempt)).await;
                    continue;
                }
                Err(_) => {
                    drop(permit);
                    return Err(TmdbClientError::Transport);
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            let body = read_bounded(response, self.policy.max_response_bytes).await?;
            drop(permit);
            match classify_response(status, &headers, &body) {
                ResponseClass::Success => return Ok(body),
                ResponseClass::NotModified => return Err(TmdbClientError::NotModified),
                ResponseClass::RateLimited { retry_after } => {
                    if attempt < self.policy.max_attempts.get() {
                        let retry_delay = retry_after
                            .unwrap_or_else(|| self.policy.backoff(attempt))
                            .min(self.policy.backoff_max);
                        sleep(retry_delay).await;
                        continue;
                    }
                    return Err(TmdbClientError::RateLimited {
                        retry_after: retry_after.map(|value| value.as_secs()),
                    });
                }
                ResponseClass::TransientServer { status } => {
                    if attempt < self.policy.max_attempts.get() {
                        sleep(self.policy.backoff(attempt)).await;
                        continue;
                    }
                    return Err(TmdbClientError::UpstreamServer { status });
                }
                ResponseClass::Unauthorized => return Err(TmdbClientError::Unauthorized),
                ResponseClass::Forbidden => {
                    let trawl = trawl_decision(&url, status, &headers, &body);
                    return Err(TmdbClientError::Forbidden { trawl });
                }
                ResponseClass::NotFound => return Err(TmdbClientError::NotFound),
                ResponseClass::PermanentClient { status } => {
                    return Err(TmdbClientError::PermanentHttp { status });
                }
            }
        }
        Err(TmdbClientError::Transport)
    }
}

async fn read_bounded(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, TmdbClientError> {
    let mut body = Vec::with_capacity(max_bytes.min(64 * 1024));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| TmdbClientError::Transport)?
    {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(TmdbClientError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Classifies an HTTP response without retaining its body.
#[must_use]
pub fn classify_response(status: StatusCode, headers: &HeaderMap, body: &[u8]) -> ResponseClass {
    match status.as_u16() {
        200..=226 => ResponseClass::Success,
        304 => ResponseClass::NotModified,
        429 => ResponseClass::RateLimited {
            retry_after: parse_retry_after(headers),
        },
        401 => ResponseClass::Unauthorized,
        403 => ResponseClass::Forbidden,
        404 => ResponseClass::NotFound,
        status if (500..=599).contains(&status) => ResponseClass::TransientServer { status },
        status => {
            let _ = body;
            ResponseClass::PermanentClient { status }
        }
    }
}

/// Applies the strict Trawl fallback policy to one already-received response.
#[must_use]
pub fn trawl_decision(
    url: &Url,
    status: StatusCode,
    _headers: &HeaderMap,
    body: &[u8],
) -> TrawlDecision {
    if !allowlisted_host(url.host_str())
        || matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::UNAUTHORIZED | StatusCode::NOT_FOUND
        )
    {
        return TrawlDecision::NotEligible;
    }
    let body = String::from_utf8_lossy(&body[..body.len().min(16 * 1024)]).to_ascii_lowercase();
    let signatures = [
        "cf-chl-",
        "challenge-platform",
        "just a moment...",
        "captcha",
        "checking your browser",
    ];
    if signatures.iter().any(|signature| body.contains(signature)) {
        TrawlDecision::ChallengeDetected
    } else {
        TrawlDecision::NotEligible
    }
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let timestamp = httpdate::parse_http_date(value).ok()?;
    Some(
        timestamp
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn allowlisted_host(host: Option<&str>) -> bool {
    matches!(
        host,
        Some("api.themoviedb.org" | "files.tmdb.org" | "image.tmdb.org" | "www.themoviedb.org")
    )
}

fn bounded_endpoint(endpoint: &str) -> String {
    endpoint
        .chars()
        .take(128)
        .collect::<String>()
        .replace(['\n', '\r'], "")
}

/// Sanitized transport failure. No response body, authorization header, or
/// token is retained in any variant.
#[derive(Debug, thiserror::Error)]
pub enum TmdbClientError {
    /// Base URL was not a credential-free HTTP(S) URL.
    #[error("TMDB base URL is invalid")]
    InvalidBaseUrl,
    /// A path or query could not be joined to the configured base URL.
    #[error("TMDB request path is invalid")]
    InvalidPath,
    /// The local HTTP client could not be built.
    #[error("TMDB HTTP client could not be built")]
    HttpClientBuild,
    /// The request limiter policy was invalid or closed.
    #[error(transparent)]
    Policy(#[from] PolicyError),
    /// The network failed after the bounded retry budget.
    #[error("TMDB transport failed")]
    Transport,
    /// The response exceeded the configured memory bound.
    #[error("TMDB response is too large")]
    ResponseTooLarge,
    /// A streamed daily export exceeded its explicit size bound.
    #[error("TMDB daily export exceeds the configured size limit")]
    ExportSizeLimit,
    /// The destination path for a streamed daily export was invalid.
    #[error("TMDB daily export destination is invalid")]
    InvalidExportDestination,
    /// The streamed daily export could not be persisted atomically.
    #[error("TMDB daily export storage failed")]
    ExportStorage,
    /// TMDB requested slower traffic after the retry budget was exhausted.
    #[error("TMDB rate limit persisted")]
    RateLimited { retry_after: Option<u64> },
    /// The configured access token was rejected.
    #[error("TMDB authentication failed")]
    Unauthorized,
    /// TMDB denied access; the decision reports whether a challenge was detected.
    #[error("TMDB access was forbidden")]
    Forbidden { trawl: TrawlDecision },
    /// No entity exists for the requested ID.
    #[error("TMDB resource was not found")]
    NotFound,
    /// The upstream body was not changed.
    #[error("TMDB resource was not modified")]
    NotModified,
    /// A server-side response persisted after bounded retries.
    #[error("TMDB server error")]
    UpstreamServer { status: u16 },
    /// A non-retryable client response was received.
    #[error("TMDB request was rejected")]
    PermanentHttp { status: u16 },
    /// A successful body was not valid JSON for the requested type.
    #[error("TMDB response JSON is malformed at {endpoint}")]
    MalformedJson { endpoint: String },
}

async fn create_temporary_file(destination: &Path) -> Result<(File, PathBuf), TmdbClientError> {
    for _ in 0..3 {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut temporary = destination.as_os_str().to_os_string();
        temporary.push(format!(".part.{}.{}", std::process::id(), counter));
        let temporary_path = PathBuf::from(temporary);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .await
        {
            Ok(file) => return Ok((file, temporary_path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(TmdbClientError::ExportStorage),
        }
    }
    Err(TmdbClientError::ExportStorage)
}

async fn remove_temporary_file(path: &Path) {
    let _ = fs::remove_file(path).await;
}
