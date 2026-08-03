//! Image download and publication boundaries.
//!
//! The image worker deliberately separates three concerns:
//!
//! * [`ImageJobPayload`] validates the durable job boundary;
//! * [`ImageDownloader`] enforces the network policy and, only for a tested
//!   challenge response, asks the configured Trawl instance for a fallback;
//! * [`ImageStore`] writes a scratch copy and then publishes a content
//!   addressed semantic path atomically on the permanent filesystem.
//!
//! This module does not perform a full pixel decode.  MIME types, byte limits,
//! format signatures, and bounded dimensions are enforced here; a full
//! decoder can be added as a separate, versioned policy without changing the
//! download or storage contracts.

use std::collections::BTreeSet;
use std::fmt;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use image::{GenericImageView, ImageFormat as RasterFormat, imageops::FilterType};
use reqwest::redirect::Policy;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tmdb_media::{
    AssetVariant, ImageFormat, ReusableEntity, TitleScope, optimized_reusable_asset,
    optimized_title_asset, reusable_asset, title_asset,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

/// The durable job type handled by this worker.
pub const IMAGE_JOB_TYPE: &str = "image.download";
/// The first version of the image job payload contract.
pub const IMAGE_JOB_PAYLOAD_VERSION: i32 = 1;
const MAX_PATH_CHARS: usize = 512;
const MAX_SOURCE_URL_CHARS: usize = 2_048;
const MAX_LANGUAGE_CHARS: usize = 32;
const MAX_REVISION_CHARS: usize = 128;
const MAX_ASSET_INDEX: u16 = 99;
const MAX_IMAGE_DIMENSION: u32 = 16_384;
const MAX_IMAGE_PIXELS: u64 = 67_108_864;

/// Entity class represented by an image job.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageEntityType {
    /// A movie title.
    Movie,
    /// A television title.
    Tv,
    /// A television season.
    Season,
    /// A television episode.
    Episode,
    /// A person.
    Person,
    /// A collection.
    Collection,
    /// A production company or studio.
    Company,
    /// A television network.
    Network,
}

/// Image role in the TMDB catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageKind {
    /// Movie or television poster.
    Poster,
    /// Movie or television backdrop.
    Backdrop,
    /// Episode or season still.
    Still,
    /// Person profile image.
    Profile,
    /// Company or network logo.
    Logo,
    /// A source image that does not fit a known primary role.
    Other,
}

/// Payload stored in an `image.download` durable job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageJobPayload {
    /// Version of this payload schema.
    pub schema_version: i32,
    /// Entity owning the image.
    pub entity_type: ImageEntityType,
    /// Positive TMDB identifier for the entity.
    pub entity_id: i64,
    /// Catalog role of the image.
    pub kind: ImageKind,
    /// Original TMDB image path (for example `/abc123.jpg`).
    pub tmdb_path: String,
    /// Direct source URL.  The downloader separately applies its host policy.
    pub source_url: String,
    /// Optional source language code.
    pub language: Option<String>,
    /// Optional source/catalog revision.
    pub source_revision: Option<String>,
    /// Whether the title belongs to the explicit anime partition.
    #[serde(default)]
    pub anime: bool,
    /// Season number for season/episode assets.
    #[serde(default)]
    pub season_number: Option<u16>,
    /// Episode number for episode assets.
    #[serde(default)]
    pub episode_number: Option<u16>,
    /// Parent TV TMDB identifier for season and episode assets.
    #[serde(default)]
    pub title_tmdb_id: Option<i64>,
    /// Stable one-based source-image position for deterministic gallery names.
    ///
    /// Version-one jobs written before this field existed deserialize as the
    /// primary asset (`1`), so a deployed queue does not need a migration.
    #[serde(default = "default_asset_index")]
    pub asset_index: u16,
}

const fn default_asset_index() -> u16 {
    1
}

impl ImageJobPayload {
    /// Constructs an image job payload.
    ///
    /// Season and episode payloads must be completed with
    /// [`Self::with_tv_position`] before they can be serialized or executed.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] when any identifier, path, URL, or
    /// optional field violates the job contract.
    pub fn new(
        entity_type: ImageEntityType,
        entity_id: i64,
        kind: ImageKind,
        tmdb_path: impl Into<String>,
        source_url: impl Into<String>,
        language: Option<String>,
        source_revision: Option<String>,
    ) -> Result<Self, ImagePayloadError> {
        let payload = Self {
            schema_version: IMAGE_JOB_PAYLOAD_VERSION,
            entity_type,
            entity_id,
            kind,
            tmdb_path: tmdb_path.into(),
            source_url: source_url.into(),
            language,
            source_revision,
            anime: false,
            season_number: None,
            episode_number: None,
            title_tmdb_id: None,
            asset_index: default_asset_index(),
        };
        payload.validate_common()?;
        if !matches!(
            entity_type,
            ImageEntityType::Season | ImageEntityType::Episode
        ) {
            payload.validate_position()?;
        }
        Ok(payload)
    }

    /// Adds the parent TV identity and position to a season/episode payload.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] when the parent identifier or position is
    /// invalid for the entity type.
    pub fn with_tv_position(
        mut self,
        title_tmdb_id: i64,
        season_number: u16,
        episode_number: Option<u16>,
    ) -> Result<Self, ImagePayloadError> {
        self.title_tmdb_id = Some(title_tmdb_id);
        self.season_number = Some(season_number);
        self.episode_number = episode_number;
        self.validate()?;
        Ok(self)
    }

    /// Selects a deterministic non-primary gallery position.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError::InvalidAssetIndex`] for zero or an
    /// unbounded position. The fixed upper bound keeps path fan-out bounded.
    pub fn with_asset_index(mut self, asset_index: u16) -> Result<Self, ImagePayloadError> {
        self.asset_index = asset_index;
        self.validate()?;
        Ok(self)
    }

    /// Constructs a title-scoped payload with the explicit anime partition.
    /// Existing version-one jobs remain compatible because the additional
    /// fields are optional during deserialization.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] when any supplied field is invalid.
    #[allow(clippy::too_many_arguments)]
    pub fn new_scoped(
        entity_type: ImageEntityType,
        entity_id: i64,
        kind: ImageKind,
        tmdb_path: impl Into<String>,
        source_url: impl Into<String>,
        language: Option<String>,
        source_revision: Option<String>,
        anime: bool,
        season_number: Option<u16>,
        episode_number: Option<u16>,
    ) -> Result<Self, ImagePayloadError> {
        let mut payload = Self::new(
            entity_type,
            entity_id,
            kind,
            tmdb_path,
            source_url,
            language,
            source_revision,
        )?;
        payload.anime = anime;
        payload.season_number = season_number;
        payload.episode_number = episode_number;
        payload.validate()?;
        Ok(payload)
    }

    /// Validates a deserialized payload before it is executed.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] for an unsupported version or unsafe
    /// payload field.
    pub fn validate(&self) -> Result<(), ImagePayloadError> {
        self.validate_common()?;
        self.validate_position()
    }

    fn validate_common(&self) -> Result<(), ImagePayloadError> {
        if self.schema_version != IMAGE_JOB_PAYLOAD_VERSION {
            return Err(ImagePayloadError::UnsupportedVersion);
        }
        if self.entity_id <= 0 {
            return Err(ImagePayloadError::InvalidEntityId);
        }
        if !(1..=MAX_ASSET_INDEX).contains(&self.asset_index) {
            return Err(ImagePayloadError::InvalidAssetIndex);
        }
        validate_tmdb_path(&self.tmdb_path)?;
        let source = parse_source_url(&self.source_url)?;
        if source.fragment().is_some() {
            return Err(ImagePayloadError::InvalidSourceUrl);
        }
        if let Some(language) = &self.language {
            validate_text(
                language,
                MAX_LANGUAGE_CHARS,
                ImagePayloadError::InvalidLanguage,
            )?;
        }
        if let Some(revision) = &self.source_revision {
            validate_text(
                revision,
                MAX_REVISION_CHARS,
                ImagePayloadError::InvalidSourceRevision,
            )?;
        }
        Ok(())
    }

    fn validate_position(&self) -> Result<(), ImagePayloadError> {
        if matches!(
            self.entity_type,
            ImageEntityType::Season | ImageEntityType::Episode
        ) {
            let Some(_season) = self.season_number else {
                return Err(ImagePayloadError::InvalidSeasonNumber);
            };
            if self.title_tmdb_id.is_none_or(|id| id <= 0) {
                return Err(ImagePayloadError::InvalidSeasonNumber);
            }
        }
        if self.entity_type == ImageEntityType::Episode {
            let Some(episode) = self.episode_number else {
                return Err(ImagePayloadError::InvalidEpisodeNumber);
            };
            if episode == 0 {
                return Err(ImagePayloadError::InvalidEpisodeNumber);
            }
        }
        if self.entity_type != ImageEntityType::Episode && self.episode_number.is_some() {
            return Err(ImagePayloadError::InvalidEpisodeNumber);
        }
        if !matches!(
            self.entity_type,
            ImageEntityType::Season | ImageEntityType::Episode
        ) && self.title_tmdb_id.is_some()
        {
            return Err(ImagePayloadError::InvalidSeasonNumber);
        }
        Ok(())
    }

    /// Deserializes and validates a JSON job payload.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError::MalformedJson`] when the value does not
    /// match the payload shape, or a validation error for unsafe fields.
    pub fn from_json(value: &Value) -> Result<Self, ImagePayloadError> {
        let payload: Self =
            serde_json::from_value(value.clone()).map_err(|_| ImagePayloadError::MalformedJson)?;
        payload.validate()?;
        Ok(payload)
    }

    /// Serializes this payload for [`tmdb_jobs::NewJob`].
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] if the payload is no longer valid or
    /// cannot be serialized.
    pub fn to_json(&self) -> Result<Value, ImagePayloadError> {
        self.validate()?;
        serde_json::to_value(self).map_err(|_| ImagePayloadError::MalformedJson)
    }

    /// Returns the source URL after validation.
    ///
    /// # Errors
    ///
    /// Returns [`ImagePayloadError`] when the source URL or payload is invalid.
    pub fn source_url(&self) -> Result<Url, ImagePayloadError> {
        self.validate()?;
        parse_source_url(&self.source_url)
    }
}

/// Payload validation failure.  Values are intentionally generic so errors
/// cannot echo source URLs or other untrusted strings into logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum ImagePayloadError {
    /// The payload could not be deserialized as an object.
    #[error("image job payload is malformed")]
    MalformedJson,
    /// The worker does not understand the payload version.
    #[error("image job payload version is unsupported")]
    UnsupportedVersion,
    /// Entity identifiers must be positive.
    #[error("image entity identifier is invalid")]
    InvalidEntityId,
    /// The source image path is not a safe TMDB path.
    #[error("image TMDB path is invalid")]
    InvalidTmdbPath,
    /// The source URL is malformed or embeds credentials.
    #[error("image source URL is invalid")]
    InvalidSourceUrl,
    /// The optional language was invalid.
    #[error("image language is invalid")]
    InvalidLanguage,
    /// The optional source revision was invalid.
    #[error("image source revision is invalid")]
    InvalidSourceRevision,
    /// A season number was required or invalid.
    #[error("image season number is invalid")]
    InvalidSeasonNumber,
    /// An episode number was supplied for a non-episode or was invalid.
    #[error("image episode number is invalid")]
    InvalidEpisodeNumber,
    /// The source image position was not a bounded positive number.
    #[error("image asset index is invalid")]
    InvalidAssetIndex,
}

fn validate_tmdb_path(value: &str) -> Result<(), ImagePayloadError> {
    if value.is_empty()
        || value.chars().count() > MAX_PATH_CHARS
        || !value.starts_with('/')
        || value.chars().any(char::is_control)
        || value.contains('\\')
        || value.split('/').any(|part| part == ".." || part == ".")
    {
        return Err(ImagePayloadError::InvalidTmdbPath);
    }
    Ok(())
}

fn validate_text<E: Copy>(value: &str, max_chars: usize, error: E) -> Result<(), E> {
    if value.is_empty()
        || value.chars().count() > max_chars
        || value.chars().any(char::is_control)
        || value != value.trim()
    {
        return Err(error);
    }
    Ok(())
}

fn parse_source_url(value: &str) -> Result<Url, ImagePayloadError> {
    if value.is_empty() || value.chars().count() > MAX_SOURCE_URL_CHARS {
        return Err(ImagePayloadError::InvalidSourceUrl);
    }
    let url = Url::parse(value).map_err(|_| ImagePayloadError::InvalidSourceUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(ImagePayloadError::InvalidSourceUrl);
    }
    Ok(url)
}

/// Download source selected after direct TMDB or Trawl fallback retrieval.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageSource {
    /// The configured direct source.
    Direct,
    /// The existing, externally managed Trawl instance.
    Trawl,
}

/// SHA-256 digest of an image body.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ImageDigest([u8; 32]);

impl ImageDigest {
    /// Computes the digest without retaining the input.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hasher.finalize().into())
    }

    /// Returns the lower-case hexadecimal digest.
    #[must_use]
    pub fn as_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }
}

impl fmt::Debug for ImageDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ImageDigest")
            .field(&self.as_hex())
            .finish()
    }
}

/// Validated response body from an HTTP transport.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// URL that produced this response.
    pub url: Url,
    /// Optional raw Content-Type value.
    pub content_type: Option<String>,
    /// Optional Content-Length value.
    pub content_length: Option<u64>,
    /// Optional redirect location resolved against [`Self::url`].
    pub location: Option<Url>,
    /// Bytes read before completion or policy cap.
    pub body: Vec<u8>,
    /// Whether the body stream completed.
    pub body_state: BodyState,
}

/// State of a response body stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyState {
    /// The upstream ended normally.
    Complete,
    /// The transport stopped at the configured byte limit.
    Limited,
    /// The stream returned an I/O error.
    Failed,
}

/// Transport-level failure.  It intentionally carries no URL or upstream
/// body, which keeps logs free of credentials and challenge HTML.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum TransportError {
    /// The request or body exceeded its timeout.
    #[error("image request timed out")]
    Timeout,
    /// The request could not be started.
    #[error("image request failed")]
    Request,
    /// The response body could not be read.
    #[error("image response body could not be read")]
    BodyRead,
}

/// A single-request HTTP transport.  Redirects are handled by
/// [`ImageDownloader`] so every hop is checked against the host policy.
#[async_trait]
pub trait ImageTransport: Send + Sync {
    /// Fetches one URL with redirects disabled.
    async fn get(
        &self,
        url: &Url,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError>;
}

/// Trawl is deliberately a separate boundary: direct requests are attempted
/// first and this trait is called only after a challenge signature is found.
#[async_trait]
pub trait TrawlFallback: Send + Sync {
    /// Fetches the target through the already configured Trawl service.
    async fn fetch(
        &self,
        target: &Url,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError>;
}

/// Reqwest transport used for direct image requests.
#[derive(Clone, Debug)]
pub struct ReqwestTransport {
    client: Client,
}

impl ReqwestTransport {
    /// Builds a client with redirects disabled and no ambient proxy headers.
    ///
    /// # Errors
    ///
    /// Returns [`ImageError`] if the HTTP client cannot be constructed.
    pub fn new() -> Result<Self, ImageError> {
        let client = Client::builder()
            .redirect(Policy::none())
            .build()
            .map_err(|_| ImageError::Transport(TransportError::Request))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl ImageTransport for ReqwestTransport {
    async fn get(
        &self,
        url: &Url,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError> {
        let response = self
            .client
            .get(url.clone())
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    TransportError::Timeout
                } else {
                    TransportError::Request
                }
            })?;
        Self::read_response(response, max_bytes).await
    }
}

impl ReqwestTransport {
    async fn post_json(
        &self,
        url: &Url,
        payload: &Value,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError> {
        let response = self
            .client
            .post(url.clone())
            .json(payload)
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    TransportError::Timeout
                } else {
                    TransportError::Request
                }
            })?;
        Self::read_response(response, max_bytes).await
    }

    async fn read_response(
        response: reqwest::Response,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError> {
        let status = response.status().as_u16();
        let response_url = response.url().clone();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| response_url.join(value).ok());

        let mut body = Vec::new();
        let mut body_state = BodyState::Complete;
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                TransportError::Timeout
            } else {
                TransportError::BodyRead
            }
        })? {
            let remaining = max_bytes.saturating_sub(body.len());
            if chunk.len() > remaining {
                body.extend_from_slice(&chunk[..remaining]);
                body_state = BodyState::Limited;
                break;
            }
            body.extend_from_slice(&chunk);
        }

        Ok(HttpResponse {
            status,
            url: response_url,
            content_type,
            content_length,
            location,
            body,
            body_state,
        })
    }
}

/// HTTP Trawl fallback adapter.  It uses one existing service and never
/// starts or provisions another Trawl instance.
#[derive(Clone, Debug)]
pub struct HttpTrawlFallback {
    base_url: Url,
    transport: ReqwestTransport,
}

impl HttpTrawlFallback {
    /// Creates an adapter for a bare Trawl base URL (without credentials or a
    /// query string).  The deployed service is expected to expose `/scrape`.
    ///
    /// # Errors
    ///
    /// Returns [`ImageError::InvalidTrawlUrl`] for a URL with credentials,
    /// query state, or an unsupported scheme.
    pub fn new(base_url: Url) -> Result<Self, ImageError> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(ImageError::InvalidTrawlUrl);
        }
        Ok(Self {
            base_url,
            transport: ReqwestTransport::new()?,
        })
    }
}

#[async_trait]
impl TrawlFallback for HttpTrawlFallback {
    async fn fetch(
        &self,
        target: &Url,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<HttpResponse, TransportError> {
        let endpoint = self
            .base_url
            .join("scrape")
            .map_err(|_| TransportError::Request)?;
        let timeout_ms = timeout_millis(timeout);
        let request = json!({
            "url": target.as_str(),
            "maxTimeout": timeout_ms,
        });
        let response = self
            .transport
            .post_json(
                &endpoint,
                &request,
                timeout,
                trawl_response_limit(max_bytes),
            )
            .await?;
        if response.body_state != BodyState::Complete {
            return Err(TransportError::BodyRead);
        }
        let envelope: Value =
            serde_json::from_slice(&response.body).map_err(|_| TransportError::Request)?;
        let status = envelope
            .get("statusCode")
            .and_then(Value::as_u64)
            .and_then(|status| u16::try_from(status).ok())
            .unwrap_or(response.status);
        if !(200..300).contains(&status) {
            return Ok(HttpResponse {
                status,
                url: target.clone(),
                content_type: None,
                content_length: None,
                location: None,
                body: Vec::new(),
                body_state: BodyState::Complete,
            });
        }
        let body = envelope
            .get("body")
            .ok_or(TransportError::Request)
            .and_then(|body| parse_trawl_body(body, max_bytes))?;
        let content_type = envelope
            .get("contentType")
            .and_then(Value::as_str)
            .or_else(|| {
                envelope
                    .get("responseHeaders")
                    .and_then(Value::as_object)
                    .and_then(|headers| headers.get("content-type"))
                    .and_then(Value::as_str)
            })
            .map(str::to_owned);
        Ok(HttpResponse {
            status,
            url: target.clone(),
            content_type,
            content_length: Some(body.len() as u64),
            location: None,
            body,
            body_state: BodyState::Complete,
        })
    }
}

const MAX_TRAWL_JSON_BYTES: usize = 512 * 1024 * 1024;

fn timeout_millis(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
}

fn trawl_response_limit(max_bytes: usize) -> usize {
    max_bytes
        .saturating_mul(12)
        .saturating_add(1024 * 1024)
        .min(MAX_TRAWL_JSON_BYTES)
}

fn parse_trawl_body(value: &Value, max_bytes: usize) -> Result<Vec<u8>, TransportError> {
    match value {
        Value::Array(values) => {
            if values.len() > max_bytes {
                return Err(TransportError::Request);
            }
            values
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or(TransportError::Request)
                })
                .collect()
        }
        Value::Object(values) => {
            if values.len() > max_bytes {
                return Err(TransportError::Request);
            }
            let mut body = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                let byte = values
                    .get(&index.to_string())
                    .and_then(Value::as_u64)
                    .and_then(|value| u8::try_from(value).ok())
                    .ok_or(TransportError::Request)?;
                body.push(byte);
            }
            Ok(body)
        }
        _ => Err(TransportError::Request),
    }
}

/// Network and response policy for image retrieval.
#[derive(Clone, Debug)]
pub struct DownloadPolicy {
    /// Maximum accepted body size.
    pub max_bytes: usize,
    /// Maximum number of checked redirects.
    pub max_redirects: usize,
    /// Per-request timeout.
    pub timeout: Duration,
    allowed_hosts: BTreeSet<String>,
}

impl DownloadPolicy {
    /// Builds a policy with explicit limits and an exact host allowlist.
    ///
    /// # Errors
    ///
    /// Returns [`ImageError::InvalidPolicy`] when limits or allowlist entries
    /// are empty, oversized, or malformed.
    pub fn new(
        max_bytes: usize,
        max_redirects: usize,
        timeout: Duration,
        allowed_hosts: impl IntoIterator<Item = String>,
    ) -> Result<Self, ImageError> {
        if max_bytes == 0 || max_bytes > 1_024 * 1_024 * 1_024 || timeout.is_zero() {
            return Err(ImageError::InvalidPolicy);
        }
        let allowed_hosts = allowed_hosts
            .into_iter()
            .map(|host| host.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if allowed_hosts.is_empty()
            || allowed_hosts.iter().any(|host| {
                host.is_empty()
                    || host.contains('/')
                    || host.contains('@')
                    || host.chars().any(char::is_control)
            })
        {
            return Err(ImageError::InvalidPolicy);
        }
        Ok(Self {
            max_bytes,
            max_redirects,
            timeout,
            allowed_hosts,
        })
    }

    fn allows(&self, url: &Url) -> bool {
        url.host_str()
            .is_some_and(|host| self.allowed_hosts.contains(&host.to_ascii_lowercase()))
            && matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
    }
}

/// Result of a validated download before publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DownloadedImage {
    /// Complete body bytes.
    pub body: Vec<u8>,
    /// Canonical MIME type without parameters.
    pub mime_type: String,
    /// Final checked direct or fallback URL.
    pub final_url: Url,
    /// Retrieval path used.
    pub source: ImageSource,
    /// Content digest.
    pub digest: ImageDigest,
    /// Width read from the bounded image header.
    pub width: u32,
    /// Height read from the bounded image header.
    pub height: u32,
}

/// Downloader result plus optional fallback path information.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageMetadata {
    /// Entity class.
    pub entity_type: ImageEntityType,
    /// Entity identifier.
    pub entity_id: i64,
    /// Image role.
    pub kind: ImageKind,
    /// Original TMDB path.
    pub tmdb_path: String,
    /// Source language, if any.
    pub language: Option<String>,
    /// Ingestion revision, if any.
    pub source_revision: Option<String>,
    /// Original URL requested by the job.
    pub source_url: String,
    /// Canonical MIME type.
    pub mime_type: String,
    /// Number of bytes published.
    pub byte_size: u64,
    /// Width read from the bounded image header.
    pub width: u32,
    /// Height read from the bounded image header.
    pub height: u32,
    /// Lower-case content digest.
    pub sha256: String,
    /// Relative published path below the image root.
    pub storage_path: String,
    /// MIME type of the downloaded source bytes.
    #[serde(default)]
    pub source_mime_type: String,
    /// Byte size of the downloaded source bytes.
    #[serde(default)]
    pub source_byte_size: u64,
    /// Source image width.
    #[serde(default)]
    pub source_width: u32,
    /// Source image height.
    #[serde(default)]
    pub source_height: u32,
    /// SHA-256 of the exact downloaded source bytes.
    #[serde(default)]
    pub source_sha256: String,
    /// Root source path, absent for optimized-only episode thumbnails.
    #[serde(default)]
    pub source_storage_path: Option<String>,
    /// Direct or Trawl retrieval path.
    pub source: ImageSource,
    /// Verified optimized representations of this image. Episode thumbnails
    /// are optimized-only and therefore have no root source representation.
    #[serde(default)]
    pub variants: Vec<ImageVariantMetadata>,
}

/// One verified optimized public representation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageVariantMetadata {
    /// Stable bounded key such as `jpeg_w640` or `png_w500`.
    pub key: String,
    /// Safe relative path below the fixed `/media` root.
    pub storage_path: String,
    /// Canonical public MIME type.
    pub mime_type: String,
    /// Encoded byte size of this representation.
    pub byte_size: u64,
    /// Decoded display width of this representation.
    pub width: u32,
    /// Decoded display height of this representation.
    pub height: u32,
    /// SHA-256 of the optimized representation.
    pub sha256: String,
}

/// Publication outcome.  `deduplicated` means the destination already held
/// the same published file.  The current worker returns this metadata
/// in the durable-job result; the image worker persists this metadata in
/// `assets.image_assets` after the file publication succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredImage {
    /// Safe metadata to persist after publication.
    pub metadata: ImageMetadata,
    /// Whether no destination rewrite was needed.
    pub deduplicated: bool,
}

/// Safe image publication error.
#[derive(Debug, Error)]
pub enum StorageError {
    /// A root was relative, normalized incorrectly, or overlapped.
    #[error("image storage root is invalid")]
    InvalidRoot,
    /// The durable job payload was invalid.
    #[error("image job payload is invalid")]
    InvalidPayload,
    /// The downloaded body digest did not match the published source bytes.
    #[error("image body digest is invalid")]
    DigestMismatch,
    /// Filesystem operation failed without exposing a path.
    #[error("image storage operation failed")]
    Io {
        /// Fixed publication stage that failed.
        operation: StorageOperation,
        /// Underlying I/O error. Callers must log only its error kind.
        #[source]
        source: std::io::Error,
    },
    /// A destination exists but is not a regular file.
    #[error("image storage destination conflicts with an existing entry")]
    DestinationConflict,
    /// A downloaded image could not be converted into deterministic derivatives.
    #[error("image derivative generation failed")]
    Derivative,
}

/// Fixed media-publication stages used in safe terminal diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageOperation {
    /// Create the bounded `/config/media/images` scratch directory.
    PrepareScratchDirectory,
    /// Create a unique scratch image file.
    CreateScratchFile,
    /// Write downloaded bytes to the scratch image file.
    WriteScratchFile,
    /// Sync a scratch image file before publication.
    SyncScratchFile,
    /// Check whether the final destination already exists.
    CheckDestination,
    /// Read existing destination metadata.
    ReadDestinationMetadata,
    /// Hash an existing destination to verify deduplication.
    VerifyExistingDigest,
    /// Create the final destination directory.
    PrepareDestinationDirectory,
    /// Copy scratch content into a destination-local temporary file.
    CopyToDestination,
    /// Sync the destination-local temporary file.
    SyncDestinationFile,
    /// Atomically rename the temporary file into its final destination.
    PublishDestination,
}

impl StorageOperation {
    /// Returns the stable, safe diagnostic value for this stage.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PrepareScratchDirectory => "prepare_scratch_directory",
            Self::CreateScratchFile => "create_scratch_file",
            Self::WriteScratchFile => "write_scratch_file",
            Self::SyncScratchFile => "sync_scratch_file",
            Self::CheckDestination => "check_destination",
            Self::ReadDestinationMetadata => "read_destination_metadata",
            Self::VerifyExistingDigest => "verify_existing_digest",
            Self::PrepareDestinationDirectory => "prepare_destination_directory",
            Self::CopyToDestination => "copy_to_destination",
            Self::SyncDestinationFile => "sync_destination_file",
            Self::PublishDestination => "publish_destination",
        }
    }
}

impl StorageError {
    fn io(operation: StorageOperation, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}

/// Scratch/permanent image store.  Scratch and permanent roots are checked to
/// be disjoint; publication uses a temporary file under the permanent root so
/// the final rename remains atomic even when the roots are different mounts.
#[derive(Clone, Debug)]
pub struct ImageStore {
    work_root: PathBuf,
    image_root: PathBuf,
}

impl ImageStore {
    /// Creates the fixed `/config/media` to `/media` store used by the media
    /// container.  No host filesystem path is read from the environment.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidRoot`] only if the fixed contract is
    /// changed to an invalid layout.
    pub fn fixed() -> Result<Self, StorageError> {
        Self::with_semantic_layout(
            PathBuf::from(tmdb_media::MEDIA_WORK_ROOT),
            PathBuf::from(tmdb_media::MEDIA_ROOT),
        )
    }

    /// Creates a semantic-layout store for isolated tests.  Production code
    /// should use [`Self::fixed`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidRoot`] when either path is invalid or
    /// the roots overlap.
    pub fn with_semantic_layout(
        work_root: impl Into<PathBuf>,
        image_root: impl Into<PathBuf>,
    ) -> Result<Self, StorageError> {
        let work_root = validate_root(work_root.into())?;
        let image_root = validate_root(image_root.into())?;
        if work_root.starts_with(&image_root) || image_root.starts_with(&work_root) {
            return Err(StorageError::InvalidRoot);
        }
        Ok(Self {
            work_root,
            image_root,
        })
    }

    /// Publishes bytes and constructs metadata.  The caller must pass a body
    /// that has already passed [`ImageDownloader`] policy checks.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when scratch or permanent publication fails.
    #[allow(
        clippy::too_many_lines,
        reason = "source and optimized publication share one validated atomic-publication boundary"
    )]
    pub async fn publish(
        &self,
        payload: &ImageJobPayload,
        image: &DownloadedImage,
    ) -> Result<StoredImage, StorageError> {
        payload
            .validate()
            .map_err(|_| StorageError::InvalidPayload)?;
        if ImageDigest::of(&image.body) != image.digest {
            return Err(StorageError::DigestMismatch);
        }
        let digest = image.digest;
        let scratch_dir = self.work_root.join("images");
        tokio::fs::create_dir_all(&scratch_dir)
            .await
            .map_err(|error| StorageError::io(StorageOperation::PrepareScratchDirectory, error))?;
        let publication = {
            let derivative = tokio::task::spawn_blocking({
                let body = image.body.clone();
                let payload = payload.clone();
                move || encode_derivative(&body, &payload)
            })
            .await
            .map_err(|_| StorageError::Derivative)??;
            let derivative_relative = semantic_derivative_path(payload, derivative.width_hint)
                .map_err(|()| StorageError::InvalidPayload)?;
            let derivative_destination = self.image_root.join(&derivative_relative);
            let derivative_digest = ImageDigest::of(&derivative.bytes);
            let derivative_scratch = scratch_dir.join(format!(
                ".derivative-{}-{}.part",
                derivative.key,
                Uuid::now_v7()
            ));
            let derivative_outcome = self
                .publish_inner(
                    &derivative_scratch,
                    &derivative_destination,
                    &derivative.bytes,
                    derivative_digest,
                    true,
                )
                .await;
            let _ = tokio::fs::remove_file(&derivative_scratch).await;
            let derivative_outcome = derivative_outcome?;
            let derivative_variant = ImageVariantMetadata {
                key: derivative.key.to_owned(),
                storage_path: derivative_relative.to_string_lossy().replace('\\', "/"),
                mime_type: derivative.mime_type.to_owned(),
                byte_size: derivative.bytes.len() as u64,
                width: derivative.width,
                height: derivative.height,
                sha256: derivative_digest.as_hex(),
            };
            let optimized_only = payload.entity_type == ImageEntityType::Episode;
            let (
                relative,
                mime_type,
                byte_size,
                width,
                height,
                sha256,
                outcome,
                variants,
                source_relative,
            ) = if optimized_only {
                (
                    derivative_relative,
                    derivative.mime_type.to_owned(),
                    derivative.bytes.len() as u64,
                    derivative.width,
                    derivative.height,
                    derivative_digest.as_hex(),
                    derivative_outcome,
                    Vec::new(),
                    None,
                )
            } else {
                let source_relative = semantic_path(payload, &image.mime_type)
                    .map_err(|()| StorageError::InvalidPayload)?;
                let source_destination = self.image_root.join(&source_relative);
                let source_scratch = scratch_dir.join(format!(
                    ".source-{}.{}.part",
                    digest.as_hex(),
                    Uuid::now_v7()
                ));
                let source_outcome = self
                    .publish_inner(
                        &source_scratch,
                        &source_destination,
                        &image.body,
                        digest,
                        true,
                    )
                    .await;
                let _ = tokio::fs::remove_file(&source_scratch).await;
                (
                    source_relative.clone(),
                    image.mime_type.clone(),
                    image.body.len() as u64,
                    image.width,
                    image.height,
                    digest.as_hex(),
                    source_outcome?,
                    vec![derivative_variant],
                    Some(source_relative.clone()),
                )
            };
            PublicPublication {
                relative,
                mime_type,
                byte_size,
                width,
                height,
                sha256,
                outcome,
                variants,
                source_relative,
            }
        };
        let deduplicated = matches!(publication.outcome, PublishOutcome::AlreadyPresent);
        Ok(StoredImage {
            metadata: ImageMetadata {
                entity_type: payload.entity_type,
                entity_id: payload.entity_id,
                kind: payload.kind,
                tmdb_path: payload.tmdb_path.clone(),
                language: payload.language.clone(),
                source_revision: payload.source_revision.clone(),
                source_url: payload.source_url.clone(),
                mime_type: publication.mime_type,
                byte_size: publication.byte_size,
                width: publication.width,
                height: publication.height,
                sha256: publication.sha256,
                storage_path: publication.relative.to_string_lossy().replace('\\', "/"),
                source_mime_type: image.mime_type.clone(),
                source_byte_size: image.body.len() as u64,
                source_width: image.width,
                source_height: image.height,
                source_sha256: digest.as_hex(),
                source_storage_path: publication
                    .source_relative
                    .map(|path| path.to_string_lossy().replace('\\', "/")),
                source: image.source,
                variants: publication.variants,
            },
            deduplicated,
        })
    }

    async fn publish_inner(
        &self,
        scratch: &Path,
        destination: &Path,
        body: &[u8],
        digest: ImageDigest,
        replace_existing: bool,
    ) -> Result<PublishOutcome, StorageError> {
        let mut scratch_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(scratch)
            .await
            .map_err(|error| StorageError::io(StorageOperation::CreateScratchFile, error))?;
        scratch_file
            .write_all(body)
            .await
            .map_err(|error| StorageError::io(StorageOperation::WriteScratchFile, error))?;
        scratch_file
            .sync_all()
            .await
            .map_err(|error| StorageError::io(StorageOperation::SyncScratchFile, error))?;
        drop(scratch_file);

        match tokio::fs::symlink_metadata(destination).await {
            Ok(metadata) => {
                if !metadata.file_type().is_file() {
                    return Err(StorageError::DestinationConflict);
                }
                if file_matches_digest(destination, digest).await? {
                    return Ok(PublishOutcome::AlreadyPresent);
                }
                if !replace_existing {
                    return Err(StorageError::DestinationConflict);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::io(StorageOperation::CheckDestination, error));
            }
        }

        let parent = destination.parent().ok_or(StorageError::InvalidRoot)?;
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            StorageError::io(StorageOperation::PrepareDestinationDirectory, error)
        })?;
        let temporary = parent.join(format!(".{}.{}.tmp", Uuid::now_v7(), Uuid::now_v7()));
        let copy_result = tokio::fs::copy(scratch, &temporary).await;
        if let Err(error) = copy_result {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(StorageError::io(StorageOperation::CopyToDestination, error));
        }
        let outcome = async {
            let temporary_file = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&temporary)
                .await
                .map_err(|error| StorageError::io(StorageOperation::SyncDestinationFile, error))?;
            temporary_file
                .sync_all()
                .await
                .map_err(|error| StorageError::io(StorageOperation::SyncDestinationFile, error))?;
            drop(temporary_file);
            if replace_existing {
                tokio::fs::rename(&temporary, destination)
                    .await
                    .map(|()| PublishOutcome::Published)
                    .map_err(|error| StorageError::io(StorageOperation::PublishDestination, error))
            } else {
                match tokio::fs::hard_link(&temporary, destination).await {
                    Ok(()) => Ok(PublishOutcome::Published),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = tokio::fs::symlink_metadata(destination).await.map_err(
                            |metadata_error| {
                                StorageError::io(
                                    StorageOperation::ReadDestinationMetadata,
                                    metadata_error,
                                )
                            },
                        )?;
                        if !metadata.file_type().is_file() {
                            return Err(StorageError::DestinationConflict);
                        }
                        if file_matches_digest(destination, digest).await? {
                            Ok(PublishOutcome::AlreadyPresent)
                        } else {
                            Err(StorageError::DestinationConflict)
                        }
                    }
                    Err(error) => Err(StorageError::io(
                        StorageOperation::PublishDestination,
                        error,
                    )),
                }
            }
        }
        .await;
        if outcome.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        outcome
    }
}

fn semantic_path(payload: &ImageJobPayload, mime_type: &str) -> Result<PathBuf, ()> {
    let format = match mime_type {
        "image/webp" => ImageFormat::Webp,
        "image/png" => ImageFormat::Png,
        "image/gif" => ImageFormat::Gif,
        "image/jpeg" => ImageFormat::Jpeg,
        _ => return Err(()),
    };
    let variant = match payload.kind {
        ImageKind::Poster | ImageKind::Other => match payload.entity_type {
            ImageEntityType::Season => AssetVariant::SeasonPoster {
                season: payload.season_number.ok_or(())?,
                index: payload.asset_index,
            },
            _ => AssetVariant::Poster {
                index: payload.asset_index,
            },
        },
        ImageKind::Backdrop => AssetVariant::Backdrop {
            index: payload.asset_index,
        },
        ImageKind::Profile => AssetVariant::Profile {
            index: payload.asset_index,
        },
        ImageKind::Logo => AssetVariant::Logo {
            index: payload.asset_index,
        },
        ImageKind::Still => match payload.entity_type {
            ImageEntityType::Season => AssetVariant::SeasonPoster {
                season: payload.season_number.ok_or(())?,
                index: payload.asset_index,
            },
            ImageEntityType::Episode => AssetVariant::EpisodeThumbnail {
                season: payload.season_number.ok_or(())?,
                episode: payload.episode_number.ok_or(())?,
                index: payload.asset_index,
            },
            _ => return Err(()),
        },
    };
    match payload.entity_type {
        ImageEntityType::Movie => title_asset(
            if payload.anime {
                TitleScope::AnimeMovie
            } else {
                TitleScope::Movie
            },
            payload.entity_id,
            variant,
            format,
        )
        .map_err(|_| ()),
        ImageEntityType::Tv => title_asset(
            if payload.anime {
                TitleScope::AnimeTv
            } else {
                TitleScope::Tv
            },
            payload.entity_id,
            variant,
            format,
        )
        .map_err(|_| ()),
        ImageEntityType::Season | ImageEntityType::Episode => title_asset(
            if payload.anime {
                TitleScope::AnimeTv
            } else {
                TitleScope::Tv
            },
            payload.title_tmdb_id.ok_or(())?,
            variant,
            format,
        )
        .map_err(|_| ()),
        ImageEntityType::Person => {
            reusable_asset(ReusableEntity::Person, payload.entity_id, variant, format)
                .map_err(|_| ())
        }
        ImageEntityType::Network => {
            reusable_asset(ReusableEntity::Network, payload.entity_id, variant, format)
                .map_err(|_| ())
        }
        ImageEntityType::Company => {
            reusable_asset(ReusableEntity::Company, payload.entity_id, variant, format)
                .map_err(|_| ())
        }
        ImageEntityType::Collection => reusable_asset(
            ReusableEntity::Collection,
            payload.entity_id,
            variant,
            format,
        )
        .map_err(|_| ()),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "each entity kind maps directly to the fixed public media contract"
)]
fn semantic_derivative_path(payload: &ImageJobPayload, width_hint: u32) -> Result<PathBuf, ()> {
    let width = width_hint;
    let format = if payload.kind == ImageKind::Logo {
        ImageFormat::Png
    } else {
        ImageFormat::Jpeg
    };
    let variant = match payload.kind {
        ImageKind::Poster | ImageKind::Other => match payload.entity_type {
            ImageEntityType::Season => AssetVariant::SeasonPoster {
                season: payload.season_number.ok_or(())?,
                index: payload.asset_index,
            },
            _ => AssetVariant::Poster {
                index: payload.asset_index,
            },
        },
        ImageKind::Backdrop => AssetVariant::Backdrop {
            index: payload.asset_index,
        },
        ImageKind::Logo => AssetVariant::Logo {
            index: payload.asset_index,
        },
        ImageKind::Profile => AssetVariant::Profile {
            index: payload.asset_index,
        },
        ImageKind::Still => match payload.entity_type {
            ImageEntityType::Season => AssetVariant::SeasonPoster {
                season: payload.season_number.ok_or(())?,
                index: payload.asset_index,
            },
            ImageEntityType::Episode => AssetVariant::EpisodeThumbnail {
                season: payload.season_number.ok_or(())?,
                episode: payload.episode_number.ok_or(())?,
                index: payload.asset_index,
            },
            _ => return Err(()),
        },
    };
    match payload.entity_type {
        ImageEntityType::Movie => optimized_title_asset(
            if payload.anime {
                TitleScope::AnimeMovie
            } else {
                TitleScope::Movie
            },
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Tv => optimized_title_asset(
            if payload.anime {
                TitleScope::AnimeTv
            } else {
                TitleScope::Tv
            },
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Season | ImageEntityType::Episode => optimized_title_asset(
            if payload.anime {
                TitleScope::AnimeTv
            } else {
                TitleScope::Tv
            },
            payload.title_tmdb_id.ok_or(())?,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Person => optimized_reusable_asset(
            ReusableEntity::Person,
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Network => optimized_reusable_asset(
            ReusableEntity::Network,
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Company => optimized_reusable_asset(
            ReusableEntity::Company,
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
        ImageEntityType::Collection => optimized_reusable_asset(
            ReusableEntity::Collection,
            payload.entity_id,
            variant,
            format,
            width,
        )
        .map_err(|_| ()),
    }
}

#[derive(Clone, Debug)]
struct EncodedDerivative {
    key: &'static str,
    mime_type: &'static str,
    width_hint: u32,
    width: u32,
    height: u32,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct PublicPublication {
    relative: PathBuf,
    mime_type: String,
    byte_size: u64,
    width: u32,
    height: u32,
    sha256: String,
    outcome: PublishOutcome,
    variants: Vec<ImageVariantMetadata>,
    source_relative: Option<PathBuf>,
}

fn encode_derivative(
    body: &[u8],
    payload: &ImageJobPayload,
) -> Result<EncodedDerivative, StorageError> {
    let decoded = image::load_from_memory(body).map_err(|_| StorageError::Derivative)?;
    let (width, _height) = decoded.dimensions();
    let width_limit = match payload.kind {
        ImageKind::Backdrop => 1_280,
        ImageKind::Logo => 500,
        ImageKind::Profile | ImageKind::Poster | ImageKind::Still | ImageKind::Other => 640,
    };
    let output_width = width.min(width_limit);
    let decoded = if width > output_width {
        decoded.resize(output_width, u32::MAX, FilterType::Lanczos3)
    } else {
        decoded
    };
    let (output_width, output_height) = decoded.dimensions();
    if payload.kind == ImageKind::Logo {
        let mut encoded = Cursor::new(Vec::new());
        decoded
            .write_to(&mut encoded, RasterFormat::Png)
            .map_err(|_| StorageError::Derivative)?;
        Ok(EncodedDerivative {
            key: "png_w500",
            mime_type: "image/png",
            width_hint: 500,
            width: output_width,
            height: output_height,
            bytes: encoded.into_inner(),
        })
    } else {
        let mut output = Cursor::new(Vec::new());
        let mut jpeg_encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, 85);
        jpeg_encoder
            .encode_image(&decoded)
            .map_err(|_| StorageError::Derivative)?;
        let key = match payload.kind {
            ImageKind::Backdrop => "jpeg_w1280",
            ImageKind::Profile | ImageKind::Poster | ImageKind::Still | ImageKind::Other => {
                "jpeg_w640"
            }
            ImageKind::Logo => unreachable!("logo derivatives use the PNG branch"),
        };
        Ok(EncodedDerivative {
            key,
            mime_type: "image/jpeg",
            width_hint: width_limit,
            width: output_width,
            height: output_height,
            bytes: output.into_inner(),
        })
    }
}

async fn file_matches_digest(path: &Path, expected: ImageDigest) -> Result<bool, StorageError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| StorageError::io(StorageOperation::VerifyExistingDigest, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| StorageError::io(StorageOperation::VerifyExistingDigest, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ImageDigest(hasher.finalize().into()) == expected)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishOutcome {
    Published,
    AlreadyPresent,
}

fn validate_root(path: PathBuf) -> Result<PathBuf, StorageError> {
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().contains(&0) {
        return Err(StorageError::InvalidRoot);
    }
    let mut normal = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => normal = true,
            Component::CurDir | Component::ParentDir => return Err(StorageError::InvalidRoot),
            Component::Prefix(_) | Component::RootDir => {}
        }
    }
    if !normal || path.components().collect::<PathBuf>() != path {
        return Err(StorageError::InvalidRoot);
    }
    Ok(path)
}

/// Errors raised by the downloader or its Trawl boundary.
#[derive(Debug, Eq, PartialEq, Error)]
pub enum ImageError {
    /// A network policy itself was invalid.
    #[error("image download policy is invalid")]
    InvalidPolicy,
    /// The configured Trawl URL is invalid.
    #[error("Trawl fallback URL is invalid")]
    InvalidTrawlUrl,
    /// A source or redirect URL is not allowed.
    #[error("image source URL is not allowed")]
    DisallowedHost,
    /// The redirect response omitted a usable Location header.
    #[error("image redirect is invalid")]
    InvalidRedirect,
    /// Too many redirects were encountered.
    #[error("image redirect limit exceeded")]
    RedirectLimit,
    /// The upstream returned a tested browser challenge.
    #[error("image upstream challenge detected")]
    ChallengeDetected,
    /// Direct challenge fallback was not configured or failed to produce a usable response.
    #[error("Trawl fallback unavailable")]
    FallbackUnavailable,
    /// The status was not a successful image response.
    #[error("image upstream returned a rejected HTTP status")]
    HttpStatus(u16),
    /// The content type is not on the image allowlist.
    #[error("image content type is not allowed")]
    UnsupportedMime,
    /// The image signature or bounded header is invalid.
    #[error("image bytes are invalid")]
    InvalidImage,
    /// Image dimensions or pixel count exceed the bounded policy.
    #[error("image dimensions exceed the configured limit")]
    ImageTooLarge,
    /// The body exceeded the configured byte cap.
    #[error("image body exceeds the configured limit")]
    TooLarge,
    /// Content-Length did not match the complete body.
    #[error("image response body is truncated")]
    Truncated,
    /// The body transport failed.
    #[error("image response body could not be read")]
    BodyRead,
    /// The transport failed before a response arrived.
    #[error(transparent)]
    Transport(TransportError),
}

/// Bounded image downloader with optional challenge-only Trawl fallback.
#[derive(Clone)]
pub struct ImageDownloader<T, F = Arc<dyn TrawlFallback>> {
    transport: T,
    fallback: Option<F>,
    policy: DownloadPolicy,
}

impl<T> fmt::Debug for ImageDownloader<T, Arc<dyn TrawlFallback>>
where
    T: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ImageDownloader")
            .field("transport", &self.transport)
            .field("fallback_configured", &self.fallback.is_some())
            .field("policy", &self.policy)
            .finish()
    }
}

impl<T> ImageDownloader<T, Arc<dyn TrawlFallback>>
where
    T: ImageTransport,
{
    /// Creates a direct-only downloader.
    #[must_use]
    pub fn new(transport: T, policy: DownloadPolicy) -> Self {
        Self {
            transport,
            fallback: None,
            policy,
        }
    }

    /// Adds the one configured Trawl fallback boundary.
    #[must_use]
    pub fn with_fallback<F>(self, fallback: Arc<F>) -> ImageDownloader<T, Arc<dyn TrawlFallback>>
    where
        F: TrawlFallback + 'static,
    {
        ImageDownloader {
            transport: self.transport,
            fallback: Some(fallback),
            policy: self.policy,
        }
    }

    /// Downloads and validates one image job.
    ///
    /// # Errors
    ///
    /// Returns [`ImageError`] when the URL, response, redirect, challenge,
    /// MIME type, byte limit, or transport policy fails.
    pub async fn download(&self, payload: &ImageJobPayload) -> Result<DownloadedImage, ImageError> {
        let source_url = payload
            .source_url()
            .map_err(|_| ImageError::DisallowedHost)?;
        if !self.policy.allows(&source_url) {
            return Err(ImageError::DisallowedHost);
        }
        match self.fetch_direct(source_url.clone()).await {
            Ok(response) => self.validate_success(response, ImageSource::Direct),
            Err(ImageError::ChallengeDetected) => {
                let Some(fallback) = &self.fallback else {
                    return Err(ImageError::FallbackUnavailable);
                };
                let response = fallback
                    .fetch(&source_url, self.policy.timeout, self.policy.max_bytes)
                    .await
                    .map_err(ImageError::Transport)?;
                if detect_challenge(&response) {
                    return Err(ImageError::ChallengeDetected);
                }
                self.validate_success(response, ImageSource::Trawl)
            }
            Err(error) => Err(error),
        }
    }

    async fn fetch_direct(&self, mut url: Url) -> Result<HttpResponse, ImageError> {
        for redirect_count in 0..=self.policy.max_redirects {
            let response = self
                .transport
                .get(&url, self.policy.timeout, self.policy.max_bytes)
                .await
                .map_err(ImageError::Transport)?;
            if (300..400).contains(&response.status) {
                if redirect_count == self.policy.max_redirects {
                    return Err(ImageError::RedirectLimit);
                }
                let Some(next) = response.location else {
                    return Err(ImageError::InvalidRedirect);
                };
                if !self.policy.allows(&next) {
                    return Err(ImageError::DisallowedHost);
                }
                url = next;
                continue;
            }
            if detect_challenge(&response) {
                return Err(ImageError::ChallengeDetected);
            }
            return Ok(response);
        }
        Err(ImageError::RedirectLimit)
    }

    fn validate_success(
        &self,
        response: HttpResponse,
        source: ImageSource,
    ) -> Result<DownloadedImage, ImageError> {
        if !(200..300).contains(&response.status) {
            return Err(ImageError::HttpStatus(response.status));
        }
        let mime_type = response
            .content_type
            .as_deref()
            .and_then(canonical_mime)
            .ok_or(ImageError::UnsupportedMime)?;
        if !is_allowed_mime(mime_type) {
            return Err(ImageError::UnsupportedMime);
        }
        if response.body_state == BodyState::Limited || response.body.len() > self.policy.max_bytes
        {
            return Err(ImageError::TooLarge);
        }
        if response.body_state == BodyState::Failed {
            return Err(ImageError::BodyRead);
        }
        if response
            .content_length
            .is_some_and(|length| length > self.policy.max_bytes as u64)
        {
            return Err(ImageError::TooLarge);
        }
        if response
            .content_length
            .is_some_and(|length| length != response.body.len() as u64)
        {
            return Err(ImageError::Truncated);
        }
        let dimensions = validate_image_header(mime_type, &response.body)?;
        let digest = ImageDigest::of(&response.body);
        Ok(DownloadedImage {
            body: response.body,
            mime_type: mime_type.to_owned(),
            final_url: response.url,
            source,
            digest,
            width: dimensions.width,
            height: dimensions.height,
        })
    }
}

fn detect_challenge(response: &HttpResponse) -> bool {
    if !matches!(response.status, 403 | 503) {
        return false;
    }
    let body = String::from_utf8_lossy(&response.body).to_ascii_lowercase();
    [
        "cf-chl-",
        "challenge-platform",
        "just a moment",
        "attention required",
        "captcha",
    ]
    .iter()
    .any(|signature| body.contains(signature))
}

fn canonical_mime(value: &str) -> Option<&str> {
    value
        .split(';')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn is_allowed_mime(value: &str) -> bool {
    matches!(
        value,
        "image/jpeg" | "image/png" | "image/webp" | "image/gif"
    )
}

/// Dimensions extracted from a bounded, format-specific image header.  This
/// is intentionally not a full decoder: it proves the advertised image
/// framing and rejects decompression-bomb dimensions before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageDimensions {
    /// Width in pixels.
    pub width: u32,
    /// Height in pixels.
    pub height: u32,
}

fn validate_image_header(mime_type: &str, body: &[u8]) -> Result<ImageDimensions, ImageError> {
    let dimensions = match mime_type {
        "image/png" => parse_png_dimensions(body),
        "image/gif" => parse_gif_dimensions(body),
        "image/jpeg" => parse_jpeg_dimensions(body),
        "image/webp" => parse_webp_dimensions(body),
        _ => return Err(ImageError::UnsupportedMime),
    }
    .ok_or(ImageError::InvalidImage)?;
    if dimensions.width == 0
        || dimensions.height == 0
        || dimensions.width > MAX_IMAGE_DIMENSION
        || dimensions.height > MAX_IMAGE_DIMENSION
        || u64::from(dimensions.width) * u64::from(dimensions.height) > MAX_IMAGE_PIXELS
    {
        return Err(ImageError::ImageTooLarge);
    }
    Ok(dimensions)
}

fn parse_png_dimensions(body: &[u8]) -> Option<ImageDimensions> {
    (body.len() >= 24 && body.starts_with(b"\x89PNG\r\n\x1a\n") && &body[12..16] == b"IHDR").then(
        || ImageDimensions {
            width: u32::from_be_bytes([body[16], body[17], body[18], body[19]]),
            height: u32::from_be_bytes([body[20], body[21], body[22], body[23]]),
        },
    )
}

fn parse_gif_dimensions(body: &[u8]) -> Option<ImageDimensions> {
    (body.len() >= 10 && (&body[..6] == b"GIF87a" || &body[..6] == b"GIF89a")).then(|| {
        ImageDimensions {
            width: u32::from(u16::from_le_bytes([body[6], body[7]])),
            height: u32::from(u16::from_le_bytes([body[8], body[9]])),
        }
    })
}

fn parse_jpeg_dimensions(body: &[u8]) -> Option<ImageDimensions> {
    if body.len() < 4 || body[0] != 0xff || body[1] != 0xd8 {
        return None;
    }
    let mut index = 2;
    while index + 1 < body.len() {
        while index < body.len() && body[index] == 0xff {
            index += 1;
        }
        if index >= body.len() {
            return None;
        }
        let marker = body[index];
        index += 1;
        if marker == 0xd9 || marker == 0xda {
            return None;
        }
        if marker == 0xd8 || marker == 0x01 || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        if index + 2 > body.len() {
            return None;
        }
        let segment_length =
            usize::from(u16::from_be_bytes(body[index..index + 2].try_into().ok()?));
        if segment_length < 2 || index + segment_length > body.len() {
            return None;
        }
        if is_jpeg_start_of_frame(marker) {
            if segment_length < 7 {
                return None;
            }
            return Some(ImageDimensions {
                height: u32::from(u16::from_be_bytes(
                    body[index + 3..index + 5].try_into().ok()?,
                )),
                width: u32::from(u16::from_be_bytes(
                    body[index + 5..index + 7].try_into().ok()?,
                )),
            });
        }
        index += segment_length;
    }
    None
}

fn is_jpeg_start_of_frame(marker: u8) -> bool {
    matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf)
}

fn parse_webp_dimensions(body: &[u8]) -> Option<ImageDimensions> {
    if body.len() < 20 || &body[..4] != b"RIFF" || &body[8..12] != b"WEBP" {
        return None;
    }
    let chunk_size = usize::try_from(u32::from_le_bytes(body[16..20].try_into().ok()?)).ok()?;
    let data_start = 20_usize;
    let data_end = data_start.checked_add(chunk_size)?;
    if data_end > body.len() {
        return None;
    }
    match &body[12..16] {
        b"VP8X" if chunk_size >= 10 => {
            let width =
                1 + u32::from(body[24]) + (u32::from(body[25]) << 8) + (u32::from(body[26]) << 16);
            let height =
                1 + u32::from(body[27]) + (u32::from(body[28]) << 8) + (u32::from(body[29]) << 16);
            Some(ImageDimensions { width, height })
        }
        b"VP8 " if chunk_size >= 10 && body[23..26] == [0x9d, 0x01, 0x2a] => {
            let width = u32::from(u16::from_le_bytes(body[26..28].try_into().ok()?)) & 0x3fff;
            let height = u32::from(u16::from_le_bytes(body[28..30].try_into().ok()?)) & 0x3fff;
            Some(ImageDimensions { width, height })
        }
        b"VP8L" if chunk_size >= 5 && body[20] == 0x2f => {
            let bits = u32::from_le_bytes([body[21], body[22], body[23], body[24]]);
            Some(ImageDimensions {
                width: (bits & 0x3fff) + 1,
                height: ((bits >> 14) & 0x3fff) + 1,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    const PNG: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f,
        0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0xf8,
        0xcf, 0xc0, 0xf0, 0x1f, 0x00, 0x05, 0x00, 0x01, 0xff, 0x89, 0x99, 0x3d, 0x1d, 0x00, 0x00,
        0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];

    fn webp_vp8() -> Vec<u8> {
        vec![
            b'R', b'I', b'F', b'F', 0x16, 0x00, 0x00, 0x00, b'W', b'E', b'B', b'P', b'V', b'P',
            b'8', b' ', 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00,
            0x01, 0x00,
        ]
    }

    fn test_policy(host: &str, max_bytes: usize) -> Result<DownloadPolicy, ImageError> {
        DownloadPolicy::new(max_bytes, 2, Duration::from_secs(5), [host.to_owned()])
    }

    fn url(value: &str) -> Result<Url, ImageError> {
        Url::parse(value).map_err(|_| ImageError::DisallowedHost)
    }

    fn response(
        value: &str,
        status: u16,
        mime: Option<&str>,
        body: &[u8],
        content_length: Option<u64>,
        body_state: BodyState,
    ) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status,
            url: Url::parse(value).map_err(|_| TransportError::Request)?,
            content_type: mime.map(str::to_owned),
            content_length,
            location: None,
            body: body.to_vec(),
            body_state,
        })
    }

    #[derive(Debug)]
    struct QueueTransport {
        responses: Mutex<VecDeque<Result<HttpResponse, TransportError>>>,
    }

    impl QueueTransport {
        fn new(responses: Vec<Result<HttpResponse, TransportError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
            }
        }
    }

    #[async_trait]
    impl ImageTransport for QueueTransport {
        async fn get(
            &self,
            _url: &Url,
            _timeout: Duration,
            _max_bytes: usize,
        ) -> Result<HttpResponse, TransportError> {
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or(Err(TransportError::Request))
        }
    }

    #[derive(Debug)]
    struct CountingFallback {
        calls: Mutex<usize>,
        response: Mutex<Option<Result<HttpResponse, TransportError>>>,
    }

    #[async_trait]
    impl TrawlFallback for CountingFallback {
        async fn fetch(
            &self,
            _target: &Url,
            _timeout: Duration,
            _max_bytes: usize,
        ) -> Result<HttpResponse, TransportError> {
            *self.calls.lock().await += 1;
            self.response
                .lock()
                .await
                .take()
                .unwrap_or(Err(TransportError::Request))
        }
    }

    fn payload() -> Result<ImageJobPayload, ImagePayloadError> {
        ImageJobPayload::new(
            ImageEntityType::Movie,
            123,
            ImageKind::Poster,
            "/abc.jpg",
            "http://images.test/abc.jpg",
            Some("en".to_owned()),
            Some("r1".to_owned()),
        )
    }

    #[tokio::test]
    async fn valid_image_from_mock_http_server_is_downloaded_and_published()
    -> Result<(), Box<dyn std::error::Error>> {
        let (address, server) = spawn_server(200, "image/png", PNG, &[]).await?;
        let policy = test_policy(&address.ip().to_string(), 1024)?;
        let downloader = ImageDownloader::new(ReqwestTransport::new()?, policy);
        let mut job = payload()?;
        job.source_url = format!("http://{address}/abc.png");
        let image = downloader.download(&job).await?;
        assert_eq!(image.mime_type, "image/png");
        assert_eq!(image.body, PNG);
        assert_eq!((image.width, image.height), (1, 1));
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let stored = store.publish(&job, &image).await?;
        assert!(!stored.deduplicated);
        assert!(images.path().join(&stored.metadata.storage_path).is_file());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn wrong_mime_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let transport = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("text/html"),
            PNG,
            Some(PNG.len() as u64),
            BodyState::Complete,
        )]);
        let downloader = ImageDownloader::new(transport, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::UnsupportedMime)
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_bytes_and_oversized_dimensions_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("image/png"),
            b"not an image",
            Some(12),
            BodyState::Complete,
        )]);
        let downloader = ImageDownloader::new(invalid, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::InvalidImage)
        );

        let mut oversized = PNG.to_vec();
        oversized[16..20].copy_from_slice(&[0x00, 0x00, 0x40, 0x00]);
        oversized[20..24].copy_from_slice(&[0x00, 0x00, 0x40, 0x00]);
        let oversized_transport = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("image/png"),
            &oversized,
            Some(oversized.len() as u64),
            BodyState::Complete,
        )]);
        let downloader =
            ImageDownloader::new(oversized_transport, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::ImageTooLarge)
        );
        Ok(())
    }

    #[tokio::test]
    async fn webp_vp8_header_dimensions_are_validated() -> Result<(), Box<dyn std::error::Error>> {
        let webp = webp_vp8();
        let transport = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("image/webp"),
            &webp,
            Some(webp.len() as u64),
            BodyState::Complete,
        )]);
        let downloader = ImageDownloader::new(transport, test_policy("images.test", 1024)?);
        let image = downloader.download(&payload()?).await?;
        assert_eq!((image.width, image.height), (1, 1));
        Ok(())
    }

    #[tokio::test]
    async fn truncation_and_size_limits_are_classified() -> Result<(), Box<dyn std::error::Error>> {
        let truncated = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("image/png"),
            PNG,
            Some((PNG.len() + 2) as u64),
            BodyState::Complete,
        )]);
        let downloader = ImageDownloader::new(truncated, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::Truncated)
        );

        let too_large = QueueTransport::new(vec![response(
            "http://images.test/abc",
            200,
            Some("image/png"),
            PNG,
            Some(PNG.len() as u64),
            BodyState::Limited,
        )]);
        let downloader = ImageDownloader::new(too_large, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::TooLarge)
        );
        Ok(())
    }

    #[tokio::test]
    async fn redirects_are_bounded_and_rechecked() -> Result<(), Box<dyn std::error::Error>> {
        let mut redirect = response(
            "http://images.test/old",
            302,
            None,
            &[],
            Some(0),
            BodyState::Complete,
        )?;
        redirect.location = Some(url("http://images.test/new")?);
        let transport = QueueTransport::new(vec![
            Ok(redirect),
            response(
                "http://images.test/new",
                200,
                Some("image/png"),
                PNG,
                Some(PNG.len() as u64),
                BodyState::Complete,
            ),
        ]);
        let downloader = ImageDownloader::new(transport, test_policy("images.test", 1024)?);
        let image = downloader.download(&payload()?).await?;
        assert_eq!(image.final_url.as_str(), "http://images.test/new");

        let mut disallowed = response(
            "http://images.test/old",
            302,
            None,
            &[],
            Some(0),
            BodyState::Complete,
        )?;
        disallowed.location = Some(url("http://evil.test/new")?);
        let transport = QueueTransport::new(vec![Ok(disallowed)]);
        let downloader = ImageDownloader::new(transport, test_policy("images.test", 1024)?);
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::DisallowedHost)
        );
        Ok(())
    }

    #[tokio::test]
    async fn challenge_uses_trawl_but_rate_limit_does_not() -> Result<(), Box<dyn std::error::Error>>
    {
        let challenge = response(
            "http://images.test/abc",
            403,
            Some("text/html"),
            b"Just a moment... cf-chl-abc",
            None,
            BodyState::Complete,
        );
        let fallback = Arc::new(CountingFallback {
            calls: Mutex::new(0),
            response: Mutex::new(Some(response(
                "http://images.test/abc",
                200,
                Some("image/png"),
                PNG,
                Some(PNG.len() as u64),
                BodyState::Complete,
            ))),
        });
        let downloader = ImageDownloader::new(
            QueueTransport::new(vec![challenge]),
            test_policy("images.test", 1024)?,
        )
        .with_fallback(fallback.clone());
        let image = downloader.download(&payload()?).await?;
        assert_eq!(image.source, ImageSource::Trawl);
        assert_eq!(*fallback.calls.lock().await, 1);

        let fallback = Arc::new(CountingFallback {
            calls: Mutex::new(0),
            response: Mutex::new(None),
        });
        let rate_limited = response(
            "http://images.test/abc",
            429,
            Some("text/html"),
            b"slow down",
            None,
            BodyState::Complete,
        );
        let downloader = ImageDownloader::new(
            QueueTransport::new(vec![rate_limited]),
            test_policy("images.test", 1024)?,
        )
        .with_fallback(fallback.clone());
        assert_eq!(
            downloader.download(&payload()?).await,
            Err(ImageError::HttpStatus(429))
        );
        assert_eq!(*fallback.calls.lock().await, 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_trawl_api_posts_json_and_decodes_binary_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let (address, seen, server) = spawn_trawl_server(PNG).await?;
        let fallback = HttpTrawlFallback::new(Url::parse(&format!("http://{address}"))?)?;
        let challenge = response(
            "http://images.test/abc",
            403,
            Some("text/html"),
            b"Just a moment... cf-chl-abc",
            None,
            BodyState::Complete,
        );
        let downloader = ImageDownloader::new(
            QueueTransport::new(vec![challenge]),
            test_policy("images.test", 1024)?,
        )
        .with_fallback(Arc::new(fallback));
        let image = downloader.download(&payload()?).await?;
        assert_eq!(image.source, ImageSource::Trawl);
        assert_eq!(image.body, PNG);
        let request = seen.lock().await.clone();
        assert!(request.starts_with(b"POST /scrape HTTP/"));
        assert!(
            request
                .windows(b"content-type: application/json".len())
                .any(|window| window == b"content-type: application/json")
        );
        assert!(
            request
                .windows(b"\"url\":\"http://images.test/abc.jpg\"".len())
                .any(|window| window == b"\"url\":\"http://images.test/abc.jpg\"")
        );
        server.abort();
        Ok(())
    }

    #[test]
    fn payload_and_trawl_urls_reject_traversal_and_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            ImageJobPayload::new(
                ImageEntityType::Movie,
                1,
                ImageKind::Poster,
                "/../escape.jpg",
                "https://images.test/a.jpg",
                None,
                None,
            ),
            Err(ImagePayloadError::InvalidTmdbPath)
        );
        assert_eq!(
            ImageJobPayload::new(
                ImageEntityType::Movie,
                1,
                ImageKind::Poster,
                "/ok.jpg",
                "https://user:pass@images.test/a.jpg",
                None,
                None,
            ),
            Err(ImagePayloadError::InvalidSourceUrl)
        );
        assert!(matches!(
            HttpTrawlFallback::new(url("http://trawl.test/?token=secret")?),
            Err(ImageError::InvalidTrawlUrl)
        ));
        Ok(())
    }

    #[test]
    fn positioned_episode_payload_is_valid_after_builder_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = ImageJobPayload::new(
            ImageEntityType::Episode,
            300,
            ImageKind::Still,
            "/shared-still.jpg",
            "https://image.tmdb.org/t/p/original/shared-still.jpg",
            None,
            None,
        )?
        .with_tv_position(100, 1, Some(1))?;
        assert_eq!(payload.title_tmdb_id, Some(100));
        assert_eq!(payload.season_number, Some(1));
        assert_eq!(payload.episode_number, Some(1));
        assert!(payload.to_json().is_ok());
        Ok(())
    }

    #[test]
    fn season_zero_payload_is_valid_after_builder_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = ImageJobPayload::new(
            ImageEntityType::Season,
            200,
            ImageKind::Poster,
            "/specials.jpg",
            "https://image.tmdb.org/t/p/original/specials.jpg",
            None,
            None,
        )?
        .with_tv_position(100, 0, None)?;
        assert_eq!(payload.season_number, Some(0));
        assert!(payload.to_json().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn semantic_publication_deduplicates_unchanged_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let job = payload()?;
        let image = DownloadedImage {
            body: PNG.to_vec(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(PNG),
            width: 1,
            height: 1,
        };
        let first = store.publish(&job, &image).await?;
        let second = store.publish(&job, &image).await?;
        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(first.metadata.sha256, second.metadata.sha256);
        Ok(())
    }

    #[tokio::test]
    async fn semantic_store_writes_root_source_and_one_optimized_derivative()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let mut job = payload()?;
        job.anime = true;
        let image = DownloadedImage {
            body: PNG.to_vec(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(PNG),
            width: 1,
            height: 1,
        };
        let stored = store.publish(&job, &image).await?;
        assert_eq!(
            stored.metadata.storage_path,
            "anime/movie/123/posters/poster.png"
        );
        assert_eq!(stored.metadata.mime_type, "image/png");
        assert!(images.path().join(&stored.metadata.storage_path).is_file());
        assert_eq!(stored.metadata.variants.len(), 1);
        assert_eq!(
            stored.metadata.variants[0].storage_path,
            "anime/movie/123/optimized/posters/poster-w640.jpg"
        );
        assert!(!images.path().join(".private").exists());
        Ok(())
    }

    #[tokio::test]
    async fn episode_thumbnail_is_optimized_only_and_supports_specials()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let job = ImageJobPayload::new(
            ImageEntityType::Episode,
            300,
            ImageKind::Still,
            "/special-still.png",
            "https://image.tmdb.org/t/p/original/special-still.png",
            None,
            None,
        )?
        .with_tv_position(100, 0, Some(1))?;
        let image = DownloadedImage {
            body: PNG.to_vec(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(PNG),
            width: 1,
            height: 1,
        };

        let stored = store.publish(&job, &image).await?;
        assert_eq!(
            stored.metadata.storage_path,
            "tv/100/optimized/thumbnails/season-specials-episode01-thumbnails-w640.jpg"
        );
        assert!(stored.metadata.source_storage_path.is_none());
        assert!(stored.metadata.variants.is_empty());
        assert_eq!(stored.metadata.source_sha256, ImageDigest::of(PNG).as_hex());
        assert!(images.path().join(&stored.metadata.storage_path).is_file());
        Ok(())
    }

    #[tokio::test]
    async fn webp_source_bytes_are_preserved_with_a_jpeg_derivative()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let mut encoded = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 1).write_to(&mut encoded, RasterFormat::WebP)?;
        let source = encoded.into_inner();
        let job = payload()?;
        let image = DownloadedImage {
            body: source.clone(),
            mime_type: "image/webp".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(&source),
            width: 2,
            height: 1,
        };

        let stored = store.publish(&job, &image).await?;
        assert_eq!(
            stored.metadata.storage_path,
            "movies/123/posters/poster.webp"
        );
        assert_eq!(stored.metadata.source_mime_type, "image/webp");
        assert_eq!(stored.metadata.variants.len(), 1);
        assert_eq!(stored.metadata.variants[0].mime_type, "image/jpeg");
        assert_eq!(
            tokio::fs::read(images.path().join(&stored.metadata.storage_path)).await?,
            source
        );
        Ok(())
    }

    #[tokio::test]
    async fn semantic_store_replaces_changed_public_derivatives_atomically()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let job = payload()?;
        let first = DownloadedImage {
            body: PNG.to_vec(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(PNG),
            width: 1,
            height: 1,
        };
        let first_stored = store.publish(&job, &first).await?;

        let mut second_body = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 1).write_to(&mut second_body, RasterFormat::Png)?;
        let second_body = second_body.into_inner();
        let second = DownloadedImage {
            body: second_body.clone(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(&second_body),
            width: 2,
            height: 1,
        };
        let second_stored = store.publish(&job, &second).await?;

        assert!(!second_stored.deduplicated);
        assert_eq!(
            second_stored.metadata.storage_path,
            first_stored.metadata.storage_path
        );
        assert_ne!(second_stored.metadata.sha256, first_stored.metadata.sha256);
        let public_path = images.path().join(&second_stored.metadata.storage_path);
        let public_bytes = tokio::fs::read(public_path).await?;
        assert_eq!(
            ImageDigest::of(&public_bytes).as_hex(),
            second_stored.metadata.sha256
        );
        assert!(!images.path().join(".private").exists());
        Ok(())
    }

    #[tokio::test]
    async fn semantic_store_generates_one_bounded_derivative()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), images.path())?;
        let job = payload()?.with_asset_index(2)?;
        let mut source = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(1_600, 900).write_to(&mut source, RasterFormat::Png)?;
        let source = source.into_inner();
        let image = DownloadedImage {
            body: source.clone(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(&source),
            width: 1_600,
            height: 900,
        };

        let stored = store.publish(&job, &image).await?;
        assert_eq!(
            stored.metadata.storage_path,
            "movies/123/posters/poster-02.png"
        );
        assert_eq!(stored.metadata.variants.len(), 1);
        assert!(
            stored
                .metadata
                .variants
                .iter()
                .any(|variant| variant.storage_path
                    == "movies/123/optimized/posters/poster-02-w640.jpg"
                    && variant.width == 640
                    && variant.height == 360)
        );
        for variant in &stored.metadata.variants {
            let decoded = image::load_from_memory(
                &tokio::fs::read(images.path().join(&variant.storage_path)).await?,
            )?;
            assert_eq!(decoded.dimensions(), (variant.width, variant.height));
        }
        assert!(!images.path().join(".private").exists());
        Ok(())
    }

    #[test]
    fn gallery_index_is_bounded_and_backward_compatible() -> Result<(), Box<dyn std::error::Error>>
    {
        let job = payload()?;
        assert_eq!(job.asset_index, 1);
        assert_eq!(
            job.with_asset_index(0),
            Err(ImagePayloadError::InvalidAssetIndex)
        );
        assert_eq!(
            payload()?.with_asset_index(MAX_ASSET_INDEX + 1),
            Err(ImagePayloadError::InvalidAssetIndex)
        );
        Ok(())
    }

    #[tokio::test]
    async fn atomic_publication_failure_cleans_scratch() -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let images = tempfile::tempdir()?;
        let image_root = images.path().join("images");
        tokio::fs::write(&image_root, b"not a directory").await?;
        let store = ImageStore::with_semantic_layout(work.path().join("work"), image_root)?;
        let job = payload()?;
        let image = DownloadedImage {
            body: PNG.to_vec(),
            mime_type: "image/png".to_owned(),
            final_url: job.source_url()?,
            source: ImageSource::Direct,
            digest: ImageDigest::of(PNG),
            width: 1,
            height: 1,
        };
        assert!(store.publish(&job, &image).await.is_err());
        let scratch_dir = work.path().join("work").join("images");
        if tokio::fs::try_exists(&scratch_dir).await? {
            let mut entries = tokio::fs::read_dir(scratch_dir).await?;
            assert!(entries.next_entry().await?.is_none());
        }
        Ok(())
    }

    async fn spawn_server(
        status: u16,
        mime: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let mime = mime.to_owned();
        let body = body.to_vec();
        let headers = extra_headers
            .iter()
            .fold(String::new(), |mut output, (name, value)| {
                output.push_str(name);
                output.push_str(": ");
                output.push_str(value);
                output.push_str("\r\n");
                output
            });
        let task = tokio::spawn(async move {
            let accepted = listener.accept().await;
            let Ok((mut socket, _)) = accepted else {
                return;
            };
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: {mime}\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        });
        Ok((address, task))
    }

    async fn spawn_trawl_server(
        body: &[u8],
    ) -> Result<
        (SocketAddr, Arc<Mutex<Vec<u8>>>, tokio::task::JoinHandle<()>),
        Box<dyn std::error::Error>,
    > {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let mut body_object = serde_json::Map::new();
        for (index, byte) in body.iter().copied().enumerate() {
            body_object.insert(index.to_string(), Value::from(byte));
        }
        let response_body = serde_json::to_vec(&json!({
            "url": "http://images.test/abc",
            "statusCode": 200,
            "body": Value::Object(body_object),
            "contentType": "image/png",
        }))?;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let task_seen = seen.clone();
        let task = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let count = socket.read(&mut chunk).await.unwrap_or(0);
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..count]);
                    let Some(header_end) =
                        request.windows(4).position(|window| window == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    let body_start = header_end + 4;
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers.lines().find_map(|line| {
                        line.strip_prefix("Content-Length:")
                            .or_else(|| line.strip_prefix("content-length:"))
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    });
                    if content_length.is_some_and(|length| request.len() >= body_start + length)
                        || request[body_start..].ends_with(b"0\r\n\r\n")
                    {
                        break;
                    }
                }
            })
            .await;
            task_seen.lock().await.extend_from_slice(&request);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(&response_body).await;
        });
        Ok((address, seen, task))
    }
}
