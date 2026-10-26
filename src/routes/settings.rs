use crate::api::{self, ApiError};
use crate::domain::auth;
use crate::domain::settings::{self, UpdateSettingsRequest};
use crate::routes::request::{method_not_allowed, parse_json};
use lambda_http::http::Method;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let result = match *req.method() {
        Method::GET => settings::get_settings(&user.id).await,
        Method::PUT => {
            let payload = match parse_json::<UpdateSettingsRequest>(&req) {
                Ok(payload) => payload,
                Err(_) => {
                    return api::error(
                        &req,
                        ApiError::bad_request("dashboard_gallery_enabled must be a boolean"),
                    );
                }
            };
            settings::save_settings(&user.id, payload).await
        }
        _ => return method_not_allowed(&req),
    };
    match result {
        Ok(settings) => api::ok(&req, settings),
        Err(error) => api::error(&req, error),
    }
}
