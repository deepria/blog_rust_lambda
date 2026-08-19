use crate::api;
use crate::domain::{auth, friends};
use crate::routes::request::{method_not_allowed, parse_json};
use lambda_http::http::Method;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle_overview(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    if *req.method() != Method::GET {
        return method_not_allowed(&req);
    }
    match friends::overview(&user.id).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::error(&req, error),
    }
}

pub async fn handle_requests(req: Request) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    if *req.method() != Method::POST {
        return method_not_allowed(&req);
    }
    let payload = parse_json::<friends::CreateFriendRequest>(&req)?;
    match friends::create_request(&user, &payload.email).await {
        Ok(data) => api::created(&req, data),
        Err(error) => api::error(&req, error),
    }
}

pub async fn handle_accept(req: Request, request_id: &str) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    if *req.method() != Method::POST {
        return method_not_allowed(&req);
    }
    match friends::accept_request(&user.id, request_id).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::error(&req, error),
    }
}

pub async fn handle_request(req: Request, request_id: &str) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    if *req.method() != Method::DELETE {
        return method_not_allowed(&req);
    }
    match friends::dismiss_request(&user.id, request_id).await {
        Ok(_) => api::no_content(&req),
        Err(error) => api::error(&req, error),
    }
}

pub async fn handle_friend(req: Request, friend_id: &str) -> Result<Response<Body>, Error> {
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return crate::routes::auth::map_auth_error(&req, error),
    };
    if *req.method() != Method::DELETE {
        return method_not_allowed(&req);
    }
    match friends::remove_friend(&user.id, friend_id).await {
        Ok(_) => api::no_content(&req),
        Err(error) => api::error(&req, error),
    }
}
