use crate::api;
use crate::domain::auth;
use crate::domain::todos::{self, TodoItem, TodoMeta};
use crate::routes::request::{method_not_allowed, parse_json};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TodosRequest {
    items: Vec<TodoItem>,
}

pub async fn handle_todos(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match todos::get_todos(&user.id).await {
            Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::POST => {
            let payload = parse_json::<TodosRequest>(&req)?;
            match todos::save_todos(&user.id, payload.items).await {
                Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => method_not_allowed(&req),
    }
}

pub async fn handle_meta(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match todos::get_meta(&user.id).await {
            Ok(meta) => api::ok(&req, meta),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::PUT => {
            let payload = parse_json::<TodoMeta>(&req)?;
            match todos::save_meta(&user.id, payload).await {
                Ok(meta) => api::ok(&req, meta),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => method_not_allowed(&req),
    }
}
