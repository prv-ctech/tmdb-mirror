use axum::{
    Json,
    body::Body,
    http::{HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// Marker attached to a response produced by panic conversion.  The outer
/// request middleware replaces the body with the normalized request id once
/// the panic has crossed the handler boundary.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PanicResponse;

/// RFC 9457-compatible, sanitized problem response.
#[derive(Clone, Debug, Serialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: &'static str,
    pub title: &'static str,
    pub status: u16,
    pub detail: &'static str,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

pub fn response(
    status: StatusCode,
    title: &'static str,
    detail: &'static str,
    request_id: &str,
) -> Response {
    let body = ProblemDetails {
        problem_type: "about:blank",
        title,
        status: status.as_u16(),
        detail,
        request_id: request_id.to_owned(),
    };
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    response
}

pub async fn not_found(request: Request<Body>) -> Response {
    let request_id = request
        .extensions()
        .get::<crate::RequestId>()
        .map_or_else(String::new, |value| value.0.clone());
    response(
        StatusCode::NOT_FOUND,
        "Not Found",
        "The requested resource was not found.",
        &request_id,
    )
}

pub async fn method_not_allowed(request: Request<Body>) -> Response {
    let request_id = request
        .extensions()
        .get::<crate::RequestId>()
        .map_or_else(String::new, |value| value.0.clone());
    response(
        StatusCode::METHOD_NOT_ALLOWED,
        "Method Not Allowed",
        "The requested method is not allowed.",
        &request_id,
    )
}

#[must_use]
pub fn panic_response(_: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let mut response = response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal Server Error",
        "An unexpected server error occurred.",
        "",
    );
    response.extensions_mut().insert(PanicResponse);
    response
}
