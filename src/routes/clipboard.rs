use crate::api::{self, ApiError};
use crate::domain::auth;
use crate::domain::clipboard::{self, UpsertClipboardRequest};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match clipboard::get_clipboard(&user.id).await {
            Ok(item) => api::ok(&req, item),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::PUT | lambda_http::http::Method::POST => {
            let payload = parse_json_body::<UpsertClipboardRequest>(&req)?;
            match clipboard::save_clipboard(&user.id, payload).await {
                Ok(item) => api::ok(&req, item),
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

fn parse_json_body<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, Error> {
    match req.body() {
        Body::Text(body) => Ok(serde_json::from_str(body)?),
        Body::Binary(body) => Ok(serde_json::from_slice(body)?),
        _ => Err("invalid body".into()),
    }
}
