use crate::api::{self, ApiError};
use crate::domain::todos::{self, TodoMeta};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct TodosRequest {
    items: Vec<Value>,
}

pub async fn handle_todos(req: Request) -> Result<Response<Body>, Error> {
    match *req.method() {
        lambda_http::http::Method::GET => match todos::get_todos().await {
            Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::POST => {
            let payload = parse_json_body::<TodosRequest>(&req).map_err(to_error)?;
            match todos::save_todos(payload.items).await {
                Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => api::map_error(
            &req,
            StatusCode::METHOD_NOT_ALLOWED,
            ApiError::bad_request("method not allowed"),
        ),
    }
}

pub async fn handle_meta(req: Request) -> Result<Response<Body>, Error> {
    match *req.method() {
        lambda_http::http::Method::GET => match todos::get_meta().await {
            Ok(meta) => api::ok(&req, meta),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::PUT => {
            let payload = parse_json_body::<TodoMeta>(&req).map_err(to_error)?;
            match todos::save_meta(payload).await {
                Ok(meta) => api::ok(&req, meta),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => api::map_error(
            &req,
            StatusCode::METHOD_NOT_ALLOWED,
            ApiError::bad_request("method not allowed"),
        ),
    }
}

fn parse_json_body<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, serde_json::Error> {
    match req.body() {
        Body::Text(body) => serde_json::from_str(body),
        Body::Binary(body) => serde_json::from_slice(body),
        _ => serde_json::from_str("null"),
    }
}

fn to_error(error: serde_json::Error) -> Error {
    error.into()
}
