//! Embedded read-only HTTP server for the public `/media` mount.

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::{Router, http::StatusCode, routing::get};
use tokio_util::sync::CancellationToken;
use tower_http::services::ServeDir;

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// Runs the embedded static server until the worker cancellation token fires.
pub async fn run(
    bind: SocketAddr,
    media_root: PathBuf,
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let app = Router::new()
        .route("/health/live", get(health))
        // Keep the conventional container probe alias for operators and the
        // stress harness while retaining the shared `/health/live` contract.
        .route("/healthz", get(health))
        // Original masters are private deduplication objects, never public
        // downloads even though they live below the permanent media mount.
        .route("/media/.masters", get(not_found))
        .route("/media/.masters/{*path}", get(not_found))
        .nest_service("/media", ServeDir::new(media_root));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(event = "media_server_started", address = %bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn public_mount_is_fixed() {
        assert_eq!(tmdb_media::MEDIA_ROOT, "/media");
        assert!(!Path::new(tmdb_media::MEDIA_ROOT).is_relative());
    }
}
