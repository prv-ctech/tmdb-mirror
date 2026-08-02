use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use secrecy::SecretString;
use sqlx::PgPool;
use tmdb_api::{DatabaseAdminStore, build_admin_router_with_operations_and_auth};
use tmdb_observability::Metrics;
use tower::ServiceExt;

const ADMIN_KEY: &str = "database-backed-admin-test-key-at-least-32-characters";

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn database_backed_admin_routes_are_durable_and_idempotent(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = build_admin_router_with_operations_and_auth(
        Metrics::new("tmdb-api", "test", "test"),
        Some(SecretString::from(ADMIN_KEY.to_owned())),
        Arc::new(DatabaseAdminStore::new(pool.clone(), pool.clone())),
    );

    let response = app
        .clone()
        .oneshot(
            Request::get("/admin/v1/status")
                .header("x-api-key", ADMIN_KEY)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let status: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 32 * 1024).await?)?;
    assert_eq!(status["data"]["build"]["schemaRevision"], "0026");
    assert_eq!(status["data"]["database"]["reachable"], true);

    let scan_request = || {
        Request::post("/admin/v1/scans")
            .header("x-api-key", ADMIN_KEY)
            .header("idempotency-key", "database-scan-1")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"mode":"full","mediaTypes":["movie","tv"]}"#))
    };
    let response = app.clone().oneshot(scan_request()?).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let scan: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4 * 1024).await?)?;
    let scan_id = scan["data"]["jobId"]
        .as_str()
        .ok_or("missing durable scan job ID")?
        .to_owned();
    assert_eq!(scan["data"]["duplicate"], false);

    let response = app.clone().oneshot(scan_request()?).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let duplicate: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4 * 1024).await?)?;
    assert_eq!(duplicate["data"]["jobId"], scan_id);
    assert_eq!(duplicate["data"]["duplicate"], true);

    let response = app
        .clone()
        .oneshot(
            Request::post("/admin/v1/scans")
                .header("x-api-key", ADMIN_KEY)
                .header("idempotency-key", "database-scan-1")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"mode":"missing","mediaTypes":["movie"]}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/admin/v1/jobs/{scan_id}"))
                .header("x-api-key", ADMIN_KEY)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let detail: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 32 * 1024).await?)?;
    assert_eq!(detail["data"]["job"]["jobType"], "admin.scan");
    assert!(detail["data"]["events"].is_array());

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/admin/v1/jobs/{scan_id}/cancel"))
                .header("x-api-key", ADMIN_KEY)
                .header("idempotency-key", "database-cancel-1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/admin/v1/jobs/{scan_id}/retry"))
                .header("x-api-key", ADMIN_KEY)
                .header("idempotency-key", "database-retry-1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let retry: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4 * 1024).await?)?;
    assert_ne!(retry["data"]["jobId"], scan_id);

    for (path, key, body) in [
        (
            "/admin/v1/media/audits",
            "database-audit-1",
            r#"{"repair":true}"#,
        ),
        ("/admin/v1/maintenance/analyze", "database-analyze-1", ""),
        (
            "/admin/v1/backups",
            "database-backup-1",
            r#"{"type":"full"}"#,
        ),
    ] {
        let mut request = Request::post(path)
            .header("x-api-key", ADMIN_KEY)
            .header("idempotency-key", key);
        if !body.is_empty() {
            request = request.header(header::CONTENT_TYPE, "application/json");
        }
        let response = app.clone().oneshot(request.body(Body::from(body))?).await?;
        assert_eq!(response.status(), StatusCode::ACCEPTED, "{path}");
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/admin/v1/jobs?limit=10&status=queued")
                .header("x-api-key", ADMIN_KEY)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::get("/admin/v1/backups")
                .header("x-api-key", ADMIN_KEY)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let backup: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4 * 1024).await?)?;
    assert_eq!(backup["data"]["state"], "queued");

    let durable_counts: (i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM ops.admin_requests),
             (SELECT count(*) FROM ops.backup_requests)",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(durable_counts, (6, 1));
    Ok(())
}
