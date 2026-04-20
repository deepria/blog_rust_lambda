use crate::api::{self, ApiError};
use crate::domain::memos::{self, UpsertMemoRequest};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle_collection(req: Request) -> Result<Response<Body>, Error> {
    match *req.method() {
        lambda_http::http::Method::GET => match memos::list_memos().await {
            Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::POST => {
            let payload = parse_json_body::<UpsertMemoRequest>(&req)?;
            match memos::create_memo(payload).await {
                Ok(memo) => api::created(&req, memo),
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

pub async fn handle_item(req: Request, id: &str) -> Result<Response<Body>, Error> {
    match *req.method() {
        lambda_http::http::Method::GET => match memos::get_memo(id).await {
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
            let payload = parse_json_body::<UpsertMemoRequest>(&req)?;
            match memos::update_memo(id, payload).await {
                Ok(memo) => api::ok(&req, memo),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        lambda_http::http::Method::DELETE => match memos::delete_memo(id).await {
            Ok(_) => api::no_content(&req),
            Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
        },
        _ => api::map_error(
            &req,
            StatusCode::METHOD_NOT_ALLOWED,
            ApiError::bad_request("method not allowed"),
        ),
    }
}

fn parse_json_body<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, Error> {
    match req.body() {
        Body::Text(body) => Ok(serde_json::from_str(body)?),
        Body::Binary(body) => Ok(serde_json::from_slice(body)?),
        _ => Err("invalid body".into()),
    }
}
