use std::time::Duration;

use axum::{
    Json,
    extract::State,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{ApiState, RequestId, problem};

// Readiness shares the bounded direct PostgreSQL read pool with catalog traffic.
// A sub-second deadline flaps under a full 100-client burst even while the
// database is healthy, so allow one normal pool wait plus the probe query.
const READINESS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Serialize)]
pub struct LiveResponse {
    pub service: &'static str,
    pub version: &'static str,
    pub status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadyResponse {
    pub service: &'static str,
    pub version: &'static str,
    pub status: &'static str,
    pub database: DatabaseReadyResponse,
}

#[derive(Clone, Debug, Serialize)]
pub struct DatabaseReadyResponse {
    #[serde(rename = "postgresMajor")]
    pub postgres_major: u16,
    #[serde(rename = "schemaRevision")]
    pub schema_revision: String,
    pub extensions: Vec<String>,
}

pub async fn live() -> impl IntoResponse {
    Json(LiveResponse {
        service: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        status: "live",
    })
}

pub async fn ready(
    State(state): State<ApiState>,
    request_id: Option<axum::extract::Extension<RequestId>>,
) -> Response {
    let request_id = request_id.map_or_else(String::new, |value| value.0.0);
    match tokio::time::timeout(READINESS_TIMEOUT, state.probe.check()).await {
        Ok(Ok(report)) => {
            tracing::info!(event = "readiness", outcome = "ready");
            Json(ReadyResponse {
                service: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
                status: "ready",
                database: DatabaseReadyResponse {
                    postgres_major: report.postgres_major,
                    schema_revision: report.schema_revision,
                    extensions: report.extensions,
                },
            })
            .into_response()
        }
        Ok(Err(_)) => {
            tracing::info!(event = "readiness", outcome = "probe_failed");
            state
                .metrics
                .inc_readiness_failure(tmdb_observability::ReadinessFailureReason::ProbeFailed);
            problem::response(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                "A required dependency is unavailable.",
                &request_id,
            )
        }
        Err(_) => {
            tracing::info!(event = "readiness", outcome = "probe_timeout");
            state
                .metrics
                .inc_readiness_failure(tmdb_observability::ReadinessFailureReason::ProbeTimeout);
            problem::response(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                "A required dependency is unavailable.",
                &request_id,
            )
        }
    }
}
