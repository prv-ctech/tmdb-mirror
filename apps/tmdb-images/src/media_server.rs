//! Embedded read-only HTTP server for the public `/media` mount.
//!
//! The handler owns the public boundary instead of delegating the directory to
//! a generic file server so it can reject hidden paths, prevent path escape,
//! and provide deterministic conditional GETs for image clients.

use std::{
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    time::UNIX_EPOCH,
};

use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
struct MediaState {
    media_root: PathBuf,
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn media_router(media_root: PathBuf) -> Router {
    Router::new()
        .route("/health/live", get(health))
        // Keep the conventional container probe alias for operators and the
        // stress harness while retaining the shared `/health/live` contract.
        .route("/healthz", get(health))
        .route("/media/{*path}", get(media_file))
        .with_state(MediaState { media_root })
}

async fn media_file(
    State(state): State<MediaState>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !tmdb_media::is_public_relative(&path) {
        return not_found();
    }
    let Ok(canonical_root) = tokio::fs::canonicalize(&state.media_root).await else {
        return not_found();
    };
    let candidate = state.media_root.join(&path);
    let canonical_file = match tokio::fs::canonicalize(candidate).await {
        Ok(file) if file.starts_with(&canonical_root) => file,
        Ok(_) | Err(_) => return not_found(),
    };
    let metadata = match tokio::fs::metadata(&canonical_file).await {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) | Err(_) => return not_found(),
    };
    let etag = etag_for_metadata(&metadata);
    if if_none_match(&headers, &etag) {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
            .body(Body::empty())
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }
    let Ok(bytes) = tokio::fs::read(&canonical_file).await else {
        return not_found();
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type(&canonical_file))
        .header(header::CONTENT_LENGTH, bytes.len())
        .header(header::ETAG, etag)
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .body(Body::from(bytes))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn etag_for_metadata(metadata: &std::fs::Metadata) -> String {
    let modified_nanos = metadata
        .modified()
        .ok()
        .and_then(|timestamp| timestamp.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos());
    // A weak validator correctly communicates equality of the representation
    // without exposing a filesystem location or trusting a request-supplied
    // name. Publication is atomic, so size/mtime change as a unit.
    format!("W/\"{:x}-{:x}\"", metadata.len(), modified_nanos)
}

fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || candidate == etag)
        })
}

fn content_type(path: &FsPath) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        _ => "application/octet-stream",
    }
}

/// Runs the embedded static server until the worker cancellation token fires.
pub async fn run(
    bind: SocketAddr,
    media_root: PathBuf,
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let app = media_router(media_root);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(event = "media_server_started", address = %bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn public_mount_is_fixed() {
        assert_eq!(tmdb_media::MEDIA_ROOT, "/media");
        assert!(!FsPath::new(tmdb_media::MEDIA_ROOT).is_relative());
    }

    #[tokio::test]
    async fn serves_a_public_file_with_a_conditional_etag() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let movie = root.path().join("movies/1");
        tokio::fs::create_dir_all(&movie).await?;
        tokio::fs::create_dir_all(movie.join("posters")).await?;
        tokio::fs::write(movie.join("posters/poster.jpg"), b"public-jpeg").await?;
        let app = media_router(root.path().to_path_buf());

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/media/movies/1/posters/poster.jpg")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers()[header::CONTENT_TYPE], "image/jpeg");
        let etag = first.headers()[header::ETAG].to_str()?.to_owned();

        let second = app
            .oneshot(
                Request::builder()
                    .uri("/media/movies/1/posters/poster.jpg")
                    .header(header::IF_NONE_MATCH, etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
        Ok(())
    }

    #[tokio::test]
    async fn private_paths_and_escape_attempts_are_not_served()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        tokio::fs::create_dir_all(root.path().join(".private")).await?;
        tokio::fs::write(root.path().join(".private/original"), b"private").await?;
        let app = media_router(root.path().to_path_buf());
        for uri in [
            "/media/.private/original",
            "/media/%2e%2e/.private/original",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        Ok(())
    }
}
