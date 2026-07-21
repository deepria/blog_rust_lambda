use crate::config::get_config;
use lambda_http::http::header;
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
    pub fn status(&self) -> StatusCode {
        match self.code.as_str() {
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

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

    pub fn forbidden(message: impl std::fmt::Display) -> Self {
        Self {
            code: "forbidden".to_string(),
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

pub fn redirect(req: &Request, location: &str) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Empty);
    *response.status_mut() = StatusCode::FOUND;
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(location)?);
    apply_headers(req, &mut response)?;
    Ok(response)
}

pub fn ok_with_cookies<T: Serialize>(
    req: &Request,
    data: T,
    cookies: &[String],
) -> Result<Response<Body>, Error> {
    response_with_cookies(req, StatusCode::OK, &ApiResponse { data }, cookies)
}

pub fn redirect_with_cookies(
    req: &Request,
    location: &str,
    cookies: &[String],
) -> Result<Response<Body>, Error> {
    let mut response = redirect(req, location)?;
    for cookie in cookies {
        response
            .headers_mut()
            .append(header::SET_COOKIE, HeaderValue::from_str(cookie)?);
    }
    Ok(response)
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

pub fn error(req: &Request, error: ApiError) -> Result<Response<Body>, Error> {
    error_response(req, error.status(), error)
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

fn response_with_cookies<T: Serialize>(
    req: &Request,
    status: StatusCode,
    payload: &T,
    cookies: &[String],
) -> Result<Response<Body>, Error> {
    let mut response = response(req, status, payload)?;
    for cookie in cookies {
        response
            .headers_mut()
            .append(header::SET_COOKIE, HeaderValue::from_str(cookie)?);
    }
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
    response.headers_mut().insert(
        "Access-Control-Allow-Credentials",
        HeaderValue::from_static("true"),
    );
    response
        .headers_mut()
        .insert("Vary", HeaderValue::from_static("Origin"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ApiError;
    use lambda_http::http::StatusCode;

    #[test]
    fn maps_domain_error_codes_to_http_statuses() {
        assert_eq!(
            ApiError::unauthorized("no session").status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ApiError::forbidden("not allowed").status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            ApiError::not_found("missing").status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::internal("failed").status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ApiError::bad_request("invalid").status(),
            StatusCode::BAD_REQUEST
        );
    }
}
