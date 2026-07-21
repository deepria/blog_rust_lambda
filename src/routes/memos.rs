use crate::api;
use crate::domain::auth;
use crate::domain::memos::{self, UpsertMemoRequest};
use crate::routes::request::{method_not_allowed, parse_json};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle_collection(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match memos::list_memos(&user.id).await {
            Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::POST => {
            let payload = parse_json::<UpsertMemoRequest>(&req)?;
            match memos::create_memo(&user.id, payload).await {
                Ok(memo) => api::created(&req, memo),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => method_not_allowed(&req),
    }
}

pub async fn handle_item(req: Request, id: &str) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match memos::get_memo(&user.id, id).await {
            Ok(memo) => api::ok(&req, memo),
            Err(error) => {
                let status = if error.code == "not_found" {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::BAD_REQUEST
                };
                api::map_error(&req, status, error)
            }
        },
        lambda_http::http::Method::PUT => {
            let payload = parse_json::<UpsertMemoRequest>(&req)?;
            match memos::update_memo(&user.id, id, payload).await {
                Ok(memo) => api::ok(&req, memo),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        lambda_http::http::Method::DELETE => match memos::delete_memo(&user.id, id).await {
            Ok(_) => api::no_content(&req),
            Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
        },
        _ => method_not_allowed(&req),
    }
}
