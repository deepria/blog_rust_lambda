use crate::api;
use crate::domain::auth;
use crate::domain::clipboard::{self, UpsertClipboardRequest};
use crate::routes::request::{method_not_allowed, parse_json};
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
            let payload = parse_json::<UpsertClipboardRequest>(&req)?;
            match clipboard::save_clipboard(&user.id, payload).await {
                Ok(item) => api::ok(&req, item),
                Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
            }
        }
        _ => method_not_allowed(&req),
    }
}
