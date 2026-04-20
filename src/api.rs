use crate::config::get_config;
use lambda_http::http::{HeaderValue, StatusCode};
use lambda_http::{Body, Error, Request, Response};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Debug, Serialize, Clone)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub data: T,
}

pub type AppResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn bad_request(message: impl std::fmt::Display) -> Self {
        Self {
            code: "bad_request".to_string(),
            message: message.to_string(),
            details: None,
        }
    }

    pub fn not_found(message: impl std::fmt::Display) -> Self {
        Self {
            code: "not_found".to_string(),
            message: message.to_string(),
            details: None,
        }
    }

    pub fn unauthorized(message: impl std::fmt::Display) -> Self {
        Self {
            code: "unauthorized".to_string(),
            message: message.to_string(),
            details: None,
        }
    }

    pub fn internal(message: impl std::fmt::Display) -> Self {
        Self {
            code: "internal_error".to_string(),
            message: message.to_string(),
            details: None,
        }
    }
}

pub fn ok<T: Serialize>(req: &Request, data: T) -> Result<Response<Body>, Error> {
    response(req, StatusCode::OK, &ApiResponse { data })
}

pub fn created<T: Serialize>(req: &Request, data: T) -> Result<Response<Body>, Error> {
    response(req, StatusCode::CREATED, &ApiResponse { data })
}

pub fn no_content(req: &Request) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(
        json!({
            "data": {
                "ok": true
            }
        })
        .to_string(),
    ));
    *response.status_mut() = StatusCode::OK;
    apply_headers(req, &mut response)?;
    Ok(response)
}

pub fn error_response(
    req: &Request,
    status: StatusCode,
    error: ApiError,
) -> Result<Response<Body>, Error> {
    response(req, status, &json!({ "error": error }))
}

pub fn options_response(req: &Request) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Empty);
    *response.status_mut() = StatusCode::NO_CONTENT;
    apply_headers(req, &mut response)?;
    Ok(response)
}

pub fn map_error(
    req: &Request,
    status: StatusCode,
    error: ApiError,
) -> Result<Response<Body>, Error> {
    error_response(req, status, error)
}

fn response<T: Serialize>(
    req: &Request,
    status: StatusCode,
    payload: &T,
) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(serde_json::to_string(payload)?));
    *response.status_mut() = status;
    apply_headers(req, &mut response)?;
    Ok(response)
}

fn apply_headers(req: &Request, response: &mut Response<Body>) -> Result<(), Error> {
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    let origin = req
        .headers()
        .get("origin")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let allowed_origin = get_config().resolve_origin(origin.as_deref());
    response.headers_mut().insert(
        "Access-Control-Allow-Origin",
        HeaderValue::from_str(&allowed_origin)?,
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Methods",
        HeaderValue::from_static("GET,POST,PUT,DELETE,OPTIONS"),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("Content-Type,Authorization"),
    );
    response
        .headers_mut()
        .insert("Vary", HeaderValue::from_static("Origin"));
    Ok(())
}
