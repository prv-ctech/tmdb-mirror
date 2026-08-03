use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderValue, Request, StatusCode, header},
};
use tmdb_api::{
    ApiState, ReadinessProbe, ReadinessReport, ShutdownError, build_admin_router,
    build_admin_router_with_auth, build_router, build_test_router, supervise_shutdown,
};
use tmdb_config::SecretString;
use tmdb_observability::Metrics;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Debug)]
struct FakeProbe {
    calls: AtomicUsize,
    result: Result<ReadinessReport, tmdb_api::ProbeError>,
}

#[derive(Debug)]
struct BlockingProbe {
    calls: AtomicUsize,
    dropped: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ReadinessProbe for BlockingProbe {
    async fn check(&self) -> Result<ReadinessReport, tmdb_api::ProbeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _guard = DropCounter(self.dropped.clone());
        std::future::pending::<Result<ReadinessReport, tmdb_api::ProbeError>>().await
    }
}

#[derive(Debug)]
struct MutableProbe {
    result: Mutex<Result<ReadinessReport, tmdb_api::ProbeError>>,
}

#[async_trait::async_trait]
impl ReadinessProbe for MutableProbe {
    async fn check(&self) -> Result<ReadinessReport, tmdb_api::ProbeError> {
        self.result
            .lock()
            .map_err(|_| tmdb_api::ProbeError::Query)?
            .clone()
    }
}

#[async_trait::async_trait]
impl ReadinessProbe for FakeProbe {
    async fn check(&self) -> Result<ReadinessReport, tmdb_api::ProbeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }
}

fn report() -> ReadinessReport {
    ReadinessReport {
        postgres_major: 18,
        schema_revision: "0027".to_owned(),
        extensions: vec![
            "pg_stat_statements".to_owned(),
            "pg_trgm".to_owned(),
            "unaccent".to_owned(),
        ],
    }
}

fn app(result: Result<ReadinessReport, tmdb_api::ProbeError>) -> (axum::Router, Arc<FakeProbe>) {
    let probe = Arc::new(FakeProbe {
        calls: AtomicUsize::new(0),
        result,
    });
    let state = ApiState::new(probe.clone(), Metrics::new("tmdb-api", "0.1.0", "test"));
    (build_router(state), probe)
}

#[tokio::test]
async fn liveness_is_independent_of_the_probe() -> Result<(), Box<dyn std::error::Error>> {
    let (app, probe) = app(Err(tmdb_api::ProbeError::Unavailable));
    let response = app
        .oneshot(Request::get("/health/live").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn readiness_returns_stable_json_on_success() -> Result<(), Box<dyn std::error::Error>> {
    let (app, probe) = app(Ok(report()));
    let response = app
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    let bytes = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["database"]["postgresMajor"], 18);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn readiness_failure_is_sanitized_problem_details() -> Result<(), Box<dyn std::error::Error>>
{
    let (app, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let response = app
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let bytes = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["status"], 503);
    assert_eq!(body["detail"], "A required dependency is unavailable.");
    Ok(())
}

#[tokio::test]
async fn readiness_timeout_is_bounded_and_drops_the_probe_future()
-> Result<(), Box<dyn std::error::Error>> {
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(BlockingProbe {
        calls: AtomicUsize::new(0),
        dropped: dropped.clone(),
    });
    let state = ApiState::new(probe.clone(), Metrics::new("tmdb-api", "0.1.0", "test"));
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        build_router(state).oneshot(Request::get("/health/ready").body(Body::empty())?),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn readiness_recovers_without_rebuilding_the_router() -> Result<(), Box<dyn std::error::Error>>
{
    let probe = Arc::new(MutableProbe {
        result: Mutex::new(Err(tmdb_api::ProbeError::Unavailable)),
    });
    let state = ApiState::new(probe.clone(), Metrics::new("tmdb-api", "0.1.0", "test"));
    let app = build_router(state);
    let response = app
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    *probe.result.lock().map_err(|_| "probe mutex poisoned")? = Ok(report());
    let response = app
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn unknown_routes_and_methods_are_problem_details() -> Result<(), Box<dyn std::error::Error>>
{
    let (app, _) = app(Ok(report()));
    let response = app
        .clone()
        .oneshot(Request::get("/missing").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let response = app
        .oneshot(Request::post("/health/live").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    assert!(response.headers().contains_key("allow"));
    Ok(())
}

#[tokio::test]
async fn request_id_is_accepted_only_in_canonical_lowercase_form()
-> Result<(), Box<dyn std::error::Error>> {
    let (router, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let valid = Uuid::now_v7().to_string();
    let request = Request::get("/health/live")
        .header("x-request-id", &valid)
        .body(Body::empty())?;
    let response = router.clone().oneshot(request).await?;
    assert_eq!(
        response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(valid.as_str())
    );

    let uppercase = valid.to_ascii_uppercase();
    let request = Request::get("/health/live")
        .header("x-request-id", &uppercase)
        .body(Body::empty())?;
    let response = router.oneshot(request).await?;
    let replacement = response
        .headers()
        .get("x-request-id")
        .ok_or("missing response id")?
        .to_str()?;
    assert_ne!(replacement, uppercase);
    assert_eq!(Uuid::parse_str(replacement)?.to_string(), replacement);

    let (router, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let response = router
        .oneshot(Request::get("/health/live").body(Body::empty())?)
        .await?;
    let generated = response
        .headers()
        .get("x-request-id")
        .ok_or("missing generated response id")?
        .to_str()?;
    assert_eq!(Uuid::parse_str(generated)?.to_string(), generated);

    let (router, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let malformed = Request::get("/health/live")
        .header("x-request-id", "not-a-uuid")
        .body(Body::empty())?;
    let response = router.oneshot(malformed).await?;
    let replacement = response
        .headers()
        .get("x-request-id")
        .ok_or("missing malformed replacement id")?
        .to_str()?;
    assert_eq!(Uuid::parse_str(replacement)?.to_string(), replacement);

    let (router, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let oversized = "x".repeat(4096);
    let oversized_request = Request::get("/health/live")
        .header("x-request-id", oversized)
        .body(Body::empty())?;
    let response = router.oneshot(oversized_request).await?;
    let replacement = response
        .headers()
        .get("x-request-id")
        .ok_or("missing oversized replacement id")?
        .to_str()?;
    assert_eq!(Uuid::parse_str(replacement)?.to_string(), replacement);
    Ok(())
}

#[tokio::test]
async fn readiness_problem_and_response_header_share_request_id()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, _) = app(Err(tmdb_api::ProbeError::Unavailable));
    let request_id = Uuid::now_v7().to_string();
    let request = Request::get("/health/ready")
        .header(header::AUTHORIZATION, "Bearer sentinel-secret")
        .header("x-request-id", &request_id)
        .body(Body::empty())?;
    let response = app.oneshot(request).await?;
    let response_id = response
        .headers()
        .get("x-request-id")
        .ok_or("missing response id")?
        .to_str()?
        .to_owned();
    let bytes = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["requestId"], response_id);
    assert!(!String::from_utf8_lossy(&bytes).contains("sentinel-secret"));
    Ok(())
}

#[tokio::test]
async fn metrics_are_private_and_expose_openmetrics() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let (public, _) = app(Ok(report()));
    let response = public
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let admin = build_admin_router(metrics);
    let response = admin
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "application/openmetrics-text; version=1.0.0; charset=utf-8"
    );
    let bytes = to_bytes(response.into_body(), 64 * 1024).await?;
    let body = String::from_utf8(bytes.to_vec())?;
    assert!(body.ends_with("# EOF\n"));
    assert!(body.contains("tmdb_build_info"));
    Ok(())
}

#[tokio::test]
async fn authenticated_admin_metrics_reject_missing_and_accept_bearer_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let admin =
        build_admin_router_with_auth(metrics, Some(SecretString::from("test-key".to_owned())));
    let response = admin
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
    assert!(response.headers().get("x-request-id").is_some());
    let response = admin
        .oneshot(
            Request::get("/metrics")
                .header(header::AUTHORIZATION, "Bearer test-key")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn metrics_use_the_matched_route_template() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let state = ApiState::new(
        Arc::new(FakeProbe {
            calls: AtomicUsize::new(0),
            result: Ok(report()),
        }),
        metrics.clone(),
    );
    let public = build_router(state);
    let response = public
        .oneshot(Request::get("/health/live").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let admin = build_admin_router(metrics);
    let response = admin
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    let bytes = to_bytes(response.into_body(), 128 * 1024).await?;
    let body = String::from_utf8(bytes.to_vec())?;
    assert!(body.contains("route=\"/health/live\""));
    assert!(body.contains("listener=\"public\""));
    Ok(())
}

#[tokio::test]
async fn panic_is_a_sanitized_problem_with_the_final_request_id()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let state = ApiState::new(
        Arc::new(FakeProbe {
            calls: AtomicUsize::new(0),
            result: Ok(report()),
        }),
        metrics,
    );
    let app = build_test_router(state, std::time::Duration::from_secs(1));
    let request_id = Uuid::now_v7().to_string();
    let response = app
        .oneshot(
            Request::get("/__test/panic")
                .header("x-request-id", &request_id)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(request_id.as_str())
    );
    let bytes = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["requestId"], request_id);
    assert!(!String::from_utf8_lossy(&bytes).contains("middleware-test-panic-sentinel"));
    Ok(())
}

#[tokio::test]
async fn total_timeout_returns_problem_details_and_aborts_the_handler()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let state = ApiState::new(
        Arc::new(FakeProbe {
            calls: AtomicUsize::new(0),
            result: Ok(report()),
        }),
        metrics,
    );
    let app = build_test_router(state, std::time::Duration::from_millis(20));
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        app.oneshot(Request::get("/__test/block").body(Body::empty())?),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let bytes = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["status"], 504);
    assert_eq!(body["detail"], "The request exceeded its deadline.");
    Ok(())
}

#[tokio::test]
async fn large_responses_use_zstd_only_when_requested() -> Result<(), Box<dyn std::error::Error>> {
    let state = ApiState::new(
        Arc::new(FakeProbe {
            calls: AtomicUsize::new(0),
            result: Ok(report()),
        }),
        Metrics::new("tmdb-api", "0.1.0", "test-revision"),
    );
    let app = build_test_router(state, std::time::Duration::from_secs(1));
    let response = app
        .clone()
        .oneshot(
            Request::get("/__test/large")
                .header("accept-encoding", "zstd")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(
        response
            .headers()
            .get("content-encoding")
            .map(HeaderValue::as_bytes),
        Some(&b"zstd"[..])
    );
    let compressed = to_bytes(response.into_body(), 128 * 1024).await?;
    let decoded = zstd::stream::decode_all(std::io::Cursor::new(compressed))?;
    assert_eq!(decoded, b"middleware-test-large-body".repeat(1024));

    let response = app
        .oneshot(Request::get("/__test/large").body(Body::empty())?)
        .await?;
    assert!(!response.headers().contains_key("content-encoding"));
    Ok(())
}

#[tokio::test]
async fn shutdown_cancellation_joins_both_listeners() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let public_cancellation = cancellation.clone();
    let admin_cancellation = cancellation.clone();
    let supervisor = tokio::spawn(supervise_shutdown(
        async move {
            public_cancellation.cancelled().await;
            Ok::<(), std::io::Error>(())
        },
        async move {
            admin_cancellation.cancelled().await;
            Ok::<(), std::io::Error>(())
        },
        cancellation.clone(),
        std::time::Duration::from_millis(100),
    ));
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert_eq!(supervisor.await?, Ok(()));
    Ok(())
}

#[tokio::test]
async fn shutdown_deadline_aborts_blocked_listeners() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug)]
    struct DropFlag(Arc<AtomicUsize>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let public_dropped = dropped.clone();
    let admin_dropped = dropped.clone();
    let cancellation = CancellationToken::new();
    let supervisor = tokio::spawn(supervise_shutdown(
        async move {
            let _guard = DropFlag(public_dropped);
            std::future::pending::<Result<(), std::io::Error>>().await
        },
        async move {
            let _guard = DropFlag(admin_dropped);
            std::future::pending::<Result<(), std::io::Error>>().await
        },
        cancellation.clone(),
        std::time::Duration::from_millis(10),
    ));
    cancellation.cancel();
    assert_eq!(supervisor.await?, Err(ShutdownError::DeadlineExceeded));
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn unexpected_listener_completion_is_a_failure() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let admin_cancellation = cancellation.clone();
    let result = supervise_shutdown(
        async { Ok::<(), std::io::Error>(()) },
        async move {
            admin_cancellation.cancelled().await;
            Ok::<(), std::io::Error>(())
        },
        cancellation,
        std::time::Duration::from_millis(100),
    )
    .await;
    assert_eq!(result, Err(ShutdownError::ListenerFailed));
    Ok(())
}
