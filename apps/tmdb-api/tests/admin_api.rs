use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::Utc;
use secrecy::SecretString;
use tmdb_api::{
    AdminApiError, AdminApiStore, AdminBackupStatus, AdminBuildStatus, AdminCatalogCounts,
    AdminComponentHealth, AdminDatabaseStatus, AdminJob, AdminJobDetail, AdminJobEvent,
    AdminJobListRequest, AdminJobPage, AdminMediaRequestItem, AdminMediaRequestOutcome,
    AdminMediaRequestStatus, AdminMediaRequestSubmission, AdminMediaWorkerAction,
    AdminMediaWorkerStatus, AdminOperation, AdminPoolStatus, AdminScanMode, AdminStatus,
    AdminSubmission, build_admin_router_with_operations_and_auth,
};
use tmdb_jobs::{JobId, JobStatus};
use tmdb_observability::Metrics;
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Debug)]
struct FakeAdminStore {
    job_id: JobId,
}

impl FakeAdminStore {
    fn new() -> Self {
        Self {
            job_id: JobId::from(Uuid::now_v7()),
        }
    }
}

#[async_trait]
impl AdminApiStore for FakeAdminStore {
    async fn status(&self) -> Result<AdminStatus, AdminApiError> {
        Ok(AdminStatus {
            build: AdminBuildStatus {
                version: "test".to_owned(),
                schema_revision: Some("0053".to_owned()),
            },
            database: AdminDatabaseStatus {
                reachable: true,
                size_bytes: 42,
                active_connections: 1,
            },
            pools: AdminPoolStatus {
                read_only_size: 2,
                read_only_idle: 1,
                read_write_size: 1,
                read_write_idle: 1,
            },
            catalog: AdminCatalogCounts {
                movies: 3,
                tv: 4,
                full_sweep_required: false,
            },
            queues: Vec::new(),
            active_catalog_work: Vec::new(),
            ingest: AdminComponentHealth::ready(),
            media: AdminComponentHealth::ready(),
            upstream: AdminComponentHealth::unknown(),
            backup: AdminBackupStatus::unknown(),
        })
    }

    async fn list_jobs(&self, request: AdminJobListRequest) -> Result<AdminJobPage, AdminApiError> {
        assert_eq!(request.limit, 1);
        Ok(AdminJobPage {
            data: vec![self.job("admin.scan")],
            next_cursor: None,
        })
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<AdminJobDetail>, AdminApiError> {
        Ok((job_id == self.job_id).then(|| AdminJobDetail {
            job: self.job("admin.scan"),
            events: vec![AdminJobEvent {
                id: Uuid::now_v7(),
                event_kind: "submitted".to_owned(),
                from_status: None,
                to_status: JobStatus::Queued,
                request_id: None,
                created_at: Utc::now(),
            }],
        }))
    }

    async fn submit(
        &self,
        operation: AdminOperation,
        idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        let AdminOperation::Scan { mode, .. } = operation else {
            return Err(AdminApiError::InvalidInput);
        };
        let expected = match idempotency_key {
            "scan-1" => AdminScanMode::FullSweep,
            "recovery-1" => AdminScanMode::Recovery,
            _ => return Err(AdminApiError::InvalidInput),
        };
        assert_eq!(mode, expected);
        Ok(AdminSubmission {
            job_id: self.job_id,
            duplicate: false,
        })
    }

    async fn submit_media_request(
        &self,
        items: &[AdminMediaRequestItem],
        idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminMediaRequestOutcome, AdminApiError> {
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].tmdb_id, 42);
        assert_eq!(idempotency_key, "media-request-1");
        Ok(AdminMediaRequestOutcome::Accepted(
            AdminMediaRequestSubmission {
                request_id: Uuid::nil(),
                duplicate: false,
            },
        ))
    }

    async fn get_media_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AdminMediaRequestStatus>, AdminApiError> {
        Ok(
            (request_id == Uuid::nil()).then(|| AdminMediaRequestStatus {
                request_id,
                status: "queued".to_owned(),
                requested_at: Utc::now(),
                started_at: None,
                finished_at: None,
                title_count: 1,
                source_assets_found: 0,
                queued_count: 0,
                downloading_count: 0,
                ready_count: 0,
                reused_count: 0,
                deleted_count: 0,
                failed_count: 0,
                catalog_incomplete_count: 0,
            }),
        )
    }

    async fn set_media_worker(
        &self,
        _action: AdminMediaWorkerAction,
        _idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        Err(AdminApiError::Unavailable)
    }

    async fn media_worker(&self) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        Err(AdminApiError::Unavailable)
    }

    async fn set_worker(
        &self,
        _action: AdminMediaWorkerAction,
        _idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        Err(AdminApiError::Unavailable)
    }

    async fn worker(&self) -> Result<AdminMediaWorkerStatus, AdminApiError> {
        Err(AdminApiError::Unavailable)
    }

    async fn cancel(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        assert_eq!(job_id, self.job_id);
        assert_eq!(idempotency_key, "cancel-1");
        Ok(AdminSubmission {
            job_id,
            duplicate: false,
        })
    }

    async fn retry(
        &self,
        job_id: JobId,
        idempotency_key: &str,
        _request_id: &str,
    ) -> Result<AdminSubmission, AdminApiError> {
        assert_eq!(job_id, self.job_id);
        assert_eq!(idempotency_key, "retry-1");
        Ok(AdminSubmission {
            job_id,
            duplicate: false,
        })
    }
}

impl FakeAdminStore {
    fn job(&self, job_type: &str) -> AdminJob {
        AdminJob {
            id: self.job_id,
            job_type: job_type.to_owned(),
            status: JobStatus::Queued,
            attempts: 0,
            max_attempts: 3,
            cancellation_requested: false,
            error_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            finished_at: None,
        }
    }
}

fn app() -> (axum::Router, JobId) {
    let store = Arc::new(FakeAdminStore::new());
    let job_id = store.job_id;
    let router = build_admin_router_with_operations_and_auth(
        Metrics::new("tmdb-api", "test", "test"),
        Some(SecretString::from(
            "a-test-key-that-is-long-enough-to-be-valid".to_owned(),
        )),
        store,
    );
    (router, job_id)
}

#[tokio::test]
async fn admin_operations_require_existing_admin_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, _) = app();
    let response = app
        .oneshot(Request::get("/admin/v1/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
    Ok(())
}

#[tokio::test]
async fn media_controls_require_authentication_and_validate_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, _) = app();
    let response = app
        .clone()
        .oneshot(Request::get("/admin/v1/media/worker").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(Request::get("/admin/v1/worker").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(
            Request::post("/admin/v1/media/requests")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "media-request-invalid")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"items":[]}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let response = app
        .clone()
        .oneshot(
            Request::post("/admin/v1/media/worker")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"action":"pause"}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            Request::get("/admin/v1/media/requests/not-a-uuid")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn private_openapi_documents_every_admin_operation() -> Result<(), Box<dyn std::error::Error>>
{
    let (app, _) = app();
    let response = app
        .oneshot(
            Request::get("/admin/v1/openapi.json")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["openapi"], "3.1.0");
    for path in [
        "/admin/v1/status",
        "/admin/v1/jobs",
        "/admin/v1/scans",
        "/admin/v1/media/requests",
        "/admin/v1/media/requests/{request_id}",
        "/admin/v1/media/worker",
        "/admin/v1/worker",
        "/admin/v1/maintenance/analyze",
        "/admin/v1/backups",
    ] {
        assert!(body["paths"].get(path).is_some(), "missing {path}");
    }
    assert!(
        body["components"]["schemas"]["ScanRequest"]["properties"]["mode"]["enum"]
            .as_array()
            .is_some_and(|modes| modes.iter().any(|mode| mode == "recovery"))
    );
    Ok(())
}

#[tokio::test]
async fn status_and_bounded_job_history_are_available_to_an_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, _) = app();
    let response = app
        .clone()
        .oneshot(
            Request::get("/admin/v1/status")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["data"]["build"]["schemaRevision"], "0053");

    let response = app
        .clone()
        .oneshot(
            Request::get("/admin/v1/jobs?limit=1")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::get("/admin/v1/jobs?limit=101")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn private_metrics_refreshes_bounded_operational_state()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, _) = app();
    let response = app
        .oneshot(
            Request::get("/metrics")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(to_bytes(response.into_body(), 32 * 1024).await?.to_vec())?;
    assert!(body.contains("component=\"worker\",state=\"ready\"} 1"));
    assert!(body.contains("component=\"upstream\",state=\"unknown\"} 1"));
    assert!(body.contains("scope=\"movies\"} 3"));
    Ok(())
}

#[tokio::test]
async fn state_changing_operations_require_idempotency_and_return_durable_jobs()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, job_id) = app();
    let request = || {
        Request::post("/admin/v1/scans")
            .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
            .header(header::CONTENT_TYPE, "application/json")
    };
    let response = app
        .clone()
        .oneshot(request().body(Body::from(
            r#"{"mode":"full_sweep","mediaTypes":["movie","tv"]}"#,
        ))?)
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            request()
                .header("idempotency-key", "scan-1")
                .body(Body::from(
                    r#"{"mode":"full_sweep","mediaTypes":["movie","tv"]}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["data"]["jobId"], job_id.as_uuid().to_string());

    let response = app
        .clone()
        .oneshot(
            request()
                .header("idempotency-key", "recovery-1")
                .body(Body::from(
                    r#"{"mode":"recovery","mediaTypes":["movie","tv"]}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/admin/v1/jobs/{}/cancel", job_id.as_uuid()))
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "cancel-1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::post(format!("/admin/v1/jobs/{}/retry", job_id.as_uuid()))
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "retry-1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    Ok(())
}

#[tokio::test]
async fn operation_payloads_and_keys_are_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let (router, _) = app();
    let response = router
        .oneshot(
            Request::post("/admin/v1/media/requests")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "x".repeat(129))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"items":[{"mediaType":"movie","tmdbId":42}]}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let (router, _) = app();
    let response = router
        .oneshot(
            Request::post("/admin/v1/scans")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "scan-1")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"mode":"full_sweep","mediaTypes":[]}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let (router, _) = app();
    let response = router
        .oneshot(
            Request::post("/admin/v1/media/requests")
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .header("idempotency-key", "media-request-oversized")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("x".repeat(16 * 1024 + 1)))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test]
async fn only_a_known_job_id_is_exposed() -> Result<(), Box<dyn std::error::Error>> {
    let (app, job_id) = app();
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/admin/v1/jobs/{}", job_id.as_uuid()))
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::get(format!("/admin/v1/jobs/{}", Uuid::now_v7()))
                .header("x-api-key", "a-test-key-that-is-long-enough-to-be-valid")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}
