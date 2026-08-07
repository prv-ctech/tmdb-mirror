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

/// Bounded machine-readable outcome copied into the canonical request log.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ResponseOutcome(pub &'static str);

/// RFC 9457-compatible, sanitized problem response.
#[derive(Clone, Debug, Serialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: &'static str,
    pub title: &'static str,
    pub status: u16,
    pub detail: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<&'static str>,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

#[must_use]
pub fn response(
    status: StatusCode,
    title: &'static str,
    detail: &'static str,
    request_id: &str,
) -> Response {
    response_with_optional_code(status, title, detail, request_id, None)
}

#[must_use]
pub(crate) fn response_with_code(
    status: StatusCode,
    title: &'static str,
    detail: &'static str,
    request_id: &str,
    code: &'static str,
) -> Response {
    response_with_optional_code(status, title, detail, request_id, Some(code))
}

fn response_with_optional_code(
    status: StatusCode,
    title: &'static str,
    detail: &'static str,
    request_id: &str,
    code: Option<&'static str>,
) -> Response {
    let body = ProblemDetails {
        problem_type: "about:blank",
        title,
        status: status.as_u16(),
        detail,
        code,
        request_id: request_id.to_owned(),
    };
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    if let Some(code) = code {
        response.extensions_mut().insert(ResponseOutcome(code));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coded_problem_marks_the_canonical_log_outcome() {
        let response = response_with_code(
            StatusCode::CONFLICT,
            "Conflict",
            "Catalog maintenance is already active.",
            "request-id",
            "catalog_maintenance_active",
        );

        assert_eq!(
            response
                .extensions()
                .get::<ResponseOutcome>()
                .map(|outcome| outcome.0),
            Some("catalog_maintenance_active")
        );
    }
}
