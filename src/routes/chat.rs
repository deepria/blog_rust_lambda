use crate::api;
use crate::domain::auth;
use crate::domain::chat::{self, ChatRequest};
use crate::routes::request::parse_json;
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle(req: Request) -> Result<Response<Body>, Error> {
    if let Err(error) = auth::require_user(&req).await {
        return crate::routes::auth::map_auth_error(&req, error);
    }
    let payload: ChatRequest = parse_json(&req)?;
    match chat::send_message(payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}
