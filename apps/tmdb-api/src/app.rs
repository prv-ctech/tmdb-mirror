use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Extension, MatchedPath, Request, State},
    http::{
        StatusCode,
        header::{self, HeaderName, HeaderValue},
    },
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use secrecy::{ExposeSecret, SecretString};
use tokio_util::sync::CancellationToken;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

use tmdb_db::{DbError, ReadinessReport};
use tmdb_observability::{
    HttpRequestLabels, HttpRequestLog, Listener, MethodClass, Metrics, StatusClass,
};

use crate::{admin_api, health, problem};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Correlation id carried through request extensions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestId(pub String);

/// Sanitized readiness failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProbeError {
    #[error("readiness dependency unavailable")]
    Unavailable,
    #[error("readiness dependency query failed")]
    Query,
    #[error("readiness dependency role is not authorized")]
    WrongRole,
    #[error("readiness migration state is invalid")]
    Migration,
    #[error("readiness invariant failed")]
    Invariant,
}

impl From<DbError> for ProbeError {
    fn from(error: DbError) -> Self {
        match error {
            DbError::Connection => Self::Unavailable,
            DbError::Query => Self::Query,
            DbError::WrongRole => Self::WrongRole,
            DbError::Migration => Self::Migration,
            DbError::Unready => Self::Invariant,
        }
    }
}

/// Object-safe asynchronous readiness probe boundary.
#[async_trait]
pub trait ReadinessProbe: Send + Sync + 'static {
    async fn check(&self) -> Result<ReadinessReport, ProbeError>;
}

/// Concrete `PostgreSQL` readiness probe that delegates all invariants to `tmdb-db`.
#[derive(Clone, Debug)]
pub struct DatabaseReadinessProbe {
    pool: sqlx::PgPool,
    database_owner: String,
}

impl DatabaseReadinessProbe {
    #[must_use]
    pub fn new(pool: sqlx::PgPool, database_owner: impl Into<String>) -> Self {
        Self {
            pool,
            database_owner: database_owner.into(),
        }
    }
}

#[async_trait]
impl ReadinessProbe for DatabaseReadinessProbe {
    async fn check(&self) -> Result<ReadinessReport, ProbeError> {
        tmdb_db::readiness(&self.pool, &self.database_owner)
            .await
            .map_err(ProbeError::from)
    }
}

/// Immutable API process state shared by health handlers and middleware.
#[derive(Clone)]
pub struct ApiState {
    pub(crate) probe: Arc<dyn ReadinessProbe>,
    pub(crate) metrics: Metrics,
}

impl std::fmt::Debug for ApiState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiState")
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

impl ApiState {
    #[must_use]
    pub fn new(probe: Arc<dyn ReadinessProbe>, metrics: Metrics) -> Self {
        Self { probe, metrics }
    }

    #[must_use]
    pub fn from_probe<P>(probe: P) -> Self
    where
        P: ReadinessProbe,
    {
        Self::new(
            Arc::new(probe),
            Metrics::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), "unknown"),
        )
    }
}

/// Build the public router. `/metrics` intentionally is not registered here.
pub fn build_router(state: ApiState) -> Router {
    build_router_with_timeout(state, REQUEST_TIMEOUT)
}

/// Build the public router with an explicit request deadline.
///
/// The production entry point uses [`REQUEST_TIMEOUT`].  The explicit variant
/// keeps deterministic timeout tests independent from wall-clock sleeps.
#[doc(hidden)]
pub fn build_router_with_timeout(state: ApiState, request_timeout: Duration) -> Router {
    build_router_inner(state, request_timeout, false)
}

fn build_router_inner(
    state: ApiState,
    request_timeout: Duration,
    include_test_routes: bool,
) -> Router {
    let request_id = HeaderName::from_static("x-request-id");
    let mut router = Router::new()
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready));
    if include_test_routes {
        router = router
            .route("/__test/panic", get(test_panic))
            .route("/__test/block", get(test_block))
            .route("/__test/large", get(test_large));
    }
    router
        .fallback(problem::not_found)
        .method_not_allowed_fallback(problem::method_not_allowed)
        .with_state(state.clone())
        .layer(CompressionLayer::new().zstd(true))
        .layer(middleware::from_fn(
            move |mut request: Request, next: Next| {
                request
                    .extensions_mut()
                    .insert(RequestDeadline(request_timeout));
                deadline_middleware(request, next)
            },
        ))
        .layer(CatchPanicLayer::custom(problem::panic_response))
        .layer(SetSensitiveResponseHeadersLayer::new([
            HeaderName::from_static("set-cookie"),
            HeaderName::from_static("www-authenticate"),
        ]))
        .layer(PropagateRequestIdLayer::new(request_id.clone()))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(request_span)
                .on_request(|_request: &Request<Body>, _span: &tracing::Span| {})
                .on_response(|_response: &Response, _latency: Duration, _span: &tracing::Span| {})
                .on_failure(|_classification, _latency, _span: &tracing::Span| {}),
        )
        .layer(middleware::from_fn_with_state(state, metrics_middleware))
        .layer(SetRequestIdLayer::new(request_id, MakeRequestUuid))
        .layer(SetSensitiveRequestHeadersLayer::new([
            header::AUTHORIZATION,
            header::COOKIE,
            HeaderName::from_static("proxy-authorization"),
            HeaderName::from_static("x-api-key"),
        ]))
        .layer(middleware::from_fn(normalize_middleware))
}

/// Builds an in-process-only router containing deterministic middleware
/// fixtures.  The production binary never calls this function, so these
/// routes cannot be reached from either deployed listener.
#[doc(hidden)]
pub fn build_test_router(state: ApiState, request_timeout: Duration) -> Router {
    build_router_inner(state, request_timeout, true)
}

#[allow(clippy::panic)]
async fn test_panic() -> &'static str {
    panic!("middleware-test-panic-sentinel")
}

async fn test_block() -> String {
    std::future::pending::<String>().await
}

async fn test_large() -> String {
    "middleware-test-large-body".repeat(1024)
}

/// Build the private metrics-only administrative router.
pub fn build_admin_router(metrics: Metrics) -> Router {
    build_admin_router_with_timeout(metrics, REQUEST_TIMEOUT)
}

/// Builds the administrative router and enables bearer/API-key authentication when configured.
/// Passing `None` preserves the unauthenticated in-process development/test behavior.
pub fn build_admin_router_with_auth(metrics: Metrics, api_key: Option<SecretString>) -> Router {
    build_admin_router_inner(metrics, api_key, None, REQUEST_TIMEOUT)
}

/// Builds the private administrative router with authenticated operational routes.
///
/// Operations remain unavailable unless this explicit constructor receives a durable store.
/// This keeps the metrics-only compatibility router usable by narrow tests and development
/// tools without accidentally exposing write handlers on another listener.
pub fn build_admin_router_with_operations_and_auth(
    metrics: Metrics,
    api_key: Option<SecretString>,
    operations: Arc<dyn admin_api::AdminApiStore>,
) -> Router {
    build_admin_router_inner(metrics, api_key, Some(operations), REQUEST_TIMEOUT)
}

/// Build the administrative router with an explicit request deadline.
#[doc(hidden)]
pub fn build_admin_router_with_timeout(metrics: Metrics, request_timeout: Duration) -> Router {
    build_admin_router_with_auth_and_timeout(metrics, None, request_timeout)
}

/// Builds the administrative router with an explicit timeout and optional API key.
#[doc(hidden)]
pub fn build_admin_router_with_auth_and_timeout(
    metrics: Metrics,
    api_key: Option<SecretString>,
    request_timeout: Duration,
) -> Router {
    build_admin_router_inner(metrics, api_key, None, request_timeout)
}

fn build_admin_router_inner(
    metrics: Metrics,
    api_key: Option<SecretString>,
    operations: Option<Arc<dyn admin_api::AdminApiStore>>,
    request_timeout: Duration,
) -> Router {
    let state = AdminState {
        metrics,
        operations,
    };
    let middleware_metrics = state.metrics.clone();
    let mut router = Router::<AdminState>::new().route("/metrics", get(metrics_handler));
    if state.operations.is_some() {
        router = admin_api::register_routes(router);
    }
    router
        .fallback(problem::not_found)
        .method_not_allowed_fallback(problem::method_not_allowed)
        .with_state(state.clone())
        // Administrative JSON is deliberately tiny: every write request has
        // a fixed shape and is persisted as a bounded durable job payload.
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(CompressionLayer::new().zstd(true))
        .layer(middleware::from_fn(
            move |mut request: Request, next: Next| {
                request
                    .extensions_mut()
                    .insert(RequestDeadline(request_timeout));
                deadline_middleware(request, next)
            },
        ))
        .layer(CatchPanicLayer::custom(problem::panic_response))
        .layer(SetSensitiveResponseHeadersLayer::new([
            HeaderName::from_static("set-cookie"),
            HeaderName::from_static("www-authenticate"),
        ]))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(request_span)
                .on_request(|_request: &Request<Body>, _span: &tracing::Span| {})
                .on_response(|_response: &Response, _latency: Duration, _span: &tracing::Span| {})
                .on_failure(|_classification, _latency, _span: &tracing::Span| {}),
        )
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(SetSensitiveRequestHeadersLayer::new([
            header::AUTHORIZATION,
            header::COOKIE,
            HeaderName::from_static("proxy-authorization"),
            HeaderName::from_static("x-api-key"),
        ]))
        .layer(middleware::from_fn(normalize_middleware))
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            admin_auth_middleware(request, next, api_key.clone())
        }))
        .layer(middleware::from_fn_with_state(
            middleware_metrics,
            admin_request_middleware,
        ))
}

async fn admin_auth_middleware(
    mut request: Request,
    next: Next,
    api_key: Option<SecretString>,
) -> Response {
    let request_id = normalize_request_id(&mut request);
    let Some(expected) = api_key else {
        return next.run(request).await;
    };
    let supplied = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });
    if supplied.is_some_and(|value| {
        constant_time_eq(value.as_bytes(), expected.expose_secret().as_bytes())
    }) {
        return next.run(request).await;
    }
    let response = problem::response(
        StatusCode::UNAUTHORIZED,
        "Unauthorized",
        "An API key is required.",
        &request_id.0,
    );
    let mut response = with_request_id(response, &request_id);
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = u8::from(left.len() != right.len());
    for index in 0..left.len().max(right.len()) {
        difference |= left.get(index).copied().unwrap_or_default()
            ^ right.get(index).copied().unwrap_or_default();
    }
    difference == 0
}

#[derive(Clone)]
pub(crate) struct AdminState {
    pub(crate) metrics: Metrics,
    pub(crate) operations: Option<Arc<dyn admin_api::AdminApiStore>>,
}

impl std::fmt::Debug for AdminState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminState")
            .field("metrics", &self.metrics)
            .field("operations_enabled", &self.operations.is_some())
            .finish()
    }
}

async fn metrics_handler(
    State(state): State<AdminState>,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id.map_or_else(String::new, |value| value.0.0);
    if let Some(operations) = &state.operations {
        match operations.status().await {
            Ok(status) => admin_api::record_status_metrics(&state.metrics, &status),
            Err(_) => {
                tracing::warn!(
                    event = "metrics_operational_snapshot_unavailable",
                    outcome = "admin_dependency_unavailable"
                );
            }
        }
    }
    match state.metrics.encode() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(Body::from(body))
            .unwrap_or_else(|_| Response::new(Body::empty())),
        Err(_) => problem::response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error",
            "Metrics could not be encoded.",
            &request_id,
        ),
    }
}

async fn admin_request_middleware(
    State(metrics): State<Metrics>,
    mut request: Request,
    next: Next,
) -> Response {
    let id = normalize_request_id(&mut request);
    let method = MethodClass::from_method(request.method().as_str());
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "<unmatched>".to_owned(), |path| path.as_str().to_owned());
    let started = Instant::now();
    let mut response = next.run(request).await;
    if response
        .extensions()
        .get::<problem::PanicResponse>()
        .is_some()
    {
        response = problem::response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error",
            "An unexpected server error occurred.",
            &id.0,
        );
    }
    let response = with_request_id(response, &id);
    let status = response.status();
    metrics.observe_http(
        &HttpRequestLabels::new(
            Listener::Admin,
            method,
            &route,
            StatusClass::from_status(status.as_u16()),
        ),
        started.elapsed(),
    );
    tmdb_observability::log_http_request(&HttpRequestLog {
        service: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        request_id: &id.0,
        listener: Listener::Admin,
        method,
        route: &route,
        status: status.as_u16(),
        duration: started.elapsed(),
        outcome: if status.is_success() {
            "success"
        } else {
            "error"
        },
    });
    response
}

async fn normalize_middleware(mut request: Request, next: Next) -> Response {
    normalize_request_id(&mut request);
    next.run(request).await
}

#[derive(Clone, Copy, Debug)]
struct RequestDeadline(Duration);

async fn metrics_middleware(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_else(|| RequestId(Uuid::now_v7().to_string()));
    let method = MethodClass::from_method(request.method().as_str());
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "<unmatched>".to_owned(), |path| path.as_str().to_owned());
    let started = Instant::now();
    let mut response = next.run(request).await;
    if response
        .extensions()
        .get::<problem::PanicResponse>()
        .is_some()
    {
        response = problem::response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error",
            "An unexpected server error occurred.",
            &id.0,
        );
    }
    let status = response.status();
    response.extensions_mut().insert(id.clone());
    response.headers_mut().insert(
        HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&id.0).unwrap_or_else(|_| HeaderValue::from_static("invalid")),
    );
    for name in [
        HeaderName::from_static("set-cookie"),
        HeaderName::from_static("www-authenticate"),
    ] {
        if let Some(value) = response.headers_mut().get_mut(&name) {
            value.set_sensitive(true);
        }
    }
    state.metrics.observe_http(
        &HttpRequestLabels::new(
            Listener::Public,
            method,
            &route,
            StatusClass::from_status(status.as_u16()),
        ),
        started.elapsed(),
    );
    tmdb_observability::log_http_request(&HttpRequestLog {
        service: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        request_id: &id.0,
        listener: Listener::Public,
        method,
        route: &route,
        status: status.as_u16(),
        duration: started.elapsed(),
        outcome: if status.is_success() {
            "success"
        } else {
            "error"
        },
    });
    response
}

async fn deadline_middleware(mut request: Request, next: Next) -> Response {
    let timeout = request
        .extensions()
        .get::<RequestDeadline>()
        .map_or(REQUEST_TIMEOUT, |deadline| deadline.0);
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map_or_else(|| Uuid::now_v7().to_string(), |id| id.0.clone());
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => problem::response(
            StatusCode::GATEWAY_TIMEOUT,
            "Gateway Timeout",
            "The request exceeded its deadline.",
            &request_id,
        ),
    }
}

fn request_span(request: &Request<Body>) -> tracing::Span {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map_or("", |id| id.0.as_str());
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("<unmatched>", MatchedPath::as_str);
    tracing::info_span!(
        "http_request",
        method = %MethodClass::from_method(request.method().as_str()).as_str(),
        route = %route,
        request_id = %request_id,
    )
}

fn normalize_request_id(request: &mut Request) -> RequestId {
    let candidate = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let value = candidate
        .filter(|text| Uuid::parse_str(text).is_ok_and(|uuid| uuid.to_string() == *text))
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    if let Ok(header_value) = HeaderValue::from_str(&value) {
        request.headers_mut().insert("x-request-id", header_value);
    }
    request.extensions_mut().insert(RequestId(value.clone()));
    RequestId(value)
}

fn with_request_id(mut response: Response, id: &RequestId) -> Response {
    if let Ok(value) = HeaderValue::from_str(&id.0) {
        response.headers_mut().insert("x-request-id", value);
    }
    response.extensions_mut().insert(id.clone());
    response
}

/// Run two listener futures with a cancellation token and bounded drain deadline.
///
/// # Errors
///
/// Returns [`ShutdownError::DeadlineExceeded`] when both listener tasks do not
/// finish before `drain_deadline`.
pub async fn supervise_shutdown<F1, F2>(
    public_server: F1,
    admin_server: F2,
    cancellation: CancellationToken,
    drain_deadline: Duration,
) -> Result<(), ShutdownError>
where
    F1: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    F2: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
{
    let mut public = tokio::spawn(public_server);
    let mut admin = tokio::spawn(admin_server);
    let mut early_completion = false;
    let mut public_result = None;
    let mut admin_result = None;

    tokio::select! {
        biased;
        () = cancellation.cancelled() => {}
        result = &mut public => {
            early_completion = true;
            public_result = Some(result);
            cancellation.cancel();
        }
        result = &mut admin => {
            early_completion = true;
            admin_result = Some(result);
            cancellation.cancel();
        }
    }

    let joined = tokio::time::timeout(drain_deadline, async {
        let public_result = if let Some(result) = public_result {
            result
        } else {
            (&mut public).await
        };
        let admin_result = if let Some(result) = admin_result {
            result
        } else {
            (&mut admin).await
        };
        (public_result, admin_result)
    })
    .await;
    match joined {
        Ok((Ok(Ok(())), Ok(Ok(())))) if !early_completion => Ok(()),
        Ok((Ok(Ok(())), Ok(Ok(())))) => Err(ShutdownError::ListenerFailed),
        Ok((_public, _admin)) => Err(ShutdownError::ListenerFailed),
        Err(_) => {
            // `timeout` dropped the join future but not the tasks. Abort and
            // explicitly await both handles so no listener is detached.
            public.abort();
            admin.abort();
            let _ = public.await;
            let _ = admin.await;
            Err(ShutdownError::DeadlineExceeded)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ShutdownError {
    #[error("listener drain deadline exceeded")]
    DeadlineExceeded,
    #[error("listener exited before shutdown completed")]
    ListenerFailed,
}

/// Wait for Ctrl-C on every platform and SIGTERM on Unix.
///
/// # Errors
///
/// Returns an error when the operating system cannot install a signal
/// listener or the signal stream terminates unexpectedly.
pub async fn shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            signal = terminate.recv() => signal.map_or_else(|| Err(std::io::Error::other("SIGTERM stream ended")), |()| Ok(())),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
