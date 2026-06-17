use crate::api::{self, ApiError};
use crate::domain::auth;
use crate::domain::files::{
    self, AbortMultipartRequest, AccessRequest, CompleteMultipartRequest, FileOrganization,
    MultipartPartRequest, UploadRequest,
};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle_collection(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match files::list_files(&user.id).await {
            Ok(items) => api::ok(&req, serde_json::json!({ "items": items })),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        _ => api::map_error(
            &req,
            StatusCode::METHOD_NOT_ALLOWED,
            ApiError::bad_request("method not allowed"),
        ),
    }
}

pub async fn handle_presign_upload(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<UploadRequest>(&req)?;
    match files::create_upload(&user.id, payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_organization(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    match *req.method() {
        lambda_http::http::Method::GET => match files::get_organization(&user.id).await {
            Ok(data) => api::ok(&req, data),
            Err(error) => api::map_error(&req, StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        lambda_http::http::Method::PUT => {
            let payload = parse_json_body::<FileOrganization>(&req)?;
            match files::save_organization(&user.id, payload).await {
                Ok(data) => api::ok(&req, data),
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

pub async fn handle_multipart_initiate(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<UploadRequest>(&req)?;
    match files::initiate_multipart_upload(&user.id, payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_multipart_presign_part(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<MultipartPartRequest>(&req)?;
    match files::create_upload_part(&user.id, payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_multipart_complete(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<CompleteMultipartRequest>(&req)?;
    match files::finish_multipart_upload(&user.id, payload).await {
        Ok(_) => api::ok(&req, serde_json::json!({ "ok": true })),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_multipart_abort(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<AbortMultipartRequest>(&req)?;
    match files::cancel_multipart_upload(&user.id, payload).await {
        Ok(_) => api::ok(&req, serde_json::json!({ "ok": true })),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_presign_download(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<AccessRequest>(&req)?;
    match files::create_download(&user.id, payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => {
            let status = if error.code == "unauthorized" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_REQUEST
            };
            api::map_error(&req, status, error)
        }
    }
}

pub async fn handle_presign_delete(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let payload = parse_json_body::<AccessRequest>(&req)?;
    match files::create_delete(&user.id, payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => {
            let status = if error.code == "unauthorized" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_REQUEST
            };
            api::map_error(&req, status, error)
        }
    }
}

pub async fn handle_delete(req: Request, key: &str) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    let auth_key = req.uri().query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "authKey")
            .map(|(_, value)| value.to_string())
    });
    match files::delete_file(&user.id, key, auth_key.as_deref()).await {
        Ok(_) => api::no_content(&req),
        Err(error) => {
            let status = if error.code == "unauthorized" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_REQUEST
            };
            api::map_error(&req, status, error)
        }
    }
}

fn parse_json_body<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, Error> {
    match req.body() {
        Body::Text(body) => Ok(serde_json::from_str(body)?),
        Body::Binary(body) => Ok(serde_json::from_slice(body)?),
        _ => Err("invalid body".into()),
    }
}
