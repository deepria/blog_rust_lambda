use crate::api::{self, ApiError};
use crate::domain::auth::{self, AuthProvider};
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, Request, Response};

pub async fn handle_me(req: Request) -> Result<Response<Body>, Error> {
    match *req.method() {
        Method::GET => match auth::current_session(&req).await {
            Ok(session) => api::ok(&req, session),
            Err(error) => map_auth_error(&req, error),
        },
        _ => method_not_allowed(&req),
    }
}

pub async fn handle_login(req: Request, provider: &str) -> Result<Response<Body>, Error> {
    if *req.method() != Method::GET {
        return method_not_allowed(&req);
    }
    let provider = match AuthProvider::parse(provider) {
        Ok(provider) => provider,
        Err(error) => return api::map_error(&req, StatusCode::BAD_REQUEST, error),
    };
    let query = query_value(&req, "returnTo");
    match auth::oauth_authorize_url(provider, false, query) {
        Ok((url, state_cookie)) => api::redirect_with_cookies(&req, &url, &[state_cookie]),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_connect(req: Request, provider: &str) -> Result<Response<Body>, Error> {
    if *req.method() != Method::GET {
        return method_not_allowed(&req);
    }
    if let Err(error) = auth::require_user(&req).await {
        return map_auth_error(&req, error);
    }
    let provider = match AuthProvider::parse(provider) {
        Ok(provider) => provider,
        Err(error) => return api::map_error(&req, StatusCode::BAD_REQUEST, error),
    };
    let query = query_value(&req, "returnTo");
    match auth::oauth_authorize_url(provider, true, query) {
        Ok((url, state_cookie)) => api::redirect_with_cookies(&req, &url, &[state_cookie]),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

pub async fn handle_callback(req: Request, provider: &str) -> Result<Response<Body>, Error> {
    if *req.method() != Method::GET {
        return method_not_allowed(&req);
    }
    let provider = match AuthProvider::parse(provider) {
        Ok(provider) => provider,
        Err(error) => return api::map_error(&req, StatusCode::BAD_REQUEST, error),
    };
    let Some(code) = query_value(&req, "code") else {
        return api::map_error(
            &req,
            StatusCode::BAD_REQUEST,
            ApiError::bad_request("OAuth code is required"),
        );
    };
    let Some(state) = query_value(&req, "state") else {
        return api::map_error(
            &req,
            StatusCode::BAD_REQUEST,
            ApiError::bad_request("OAuth state is required"),
        );
    };
    match auth::handle_oauth_callback(&req, provider, code, state).await {
        Ok((_session, bundle, return_to)) => {
            let mut cookies = auth::session_cookies(&bundle);
            cookies.extend(
                auth::clear_cookies()
                    .into_iter()
                    .filter(|cookie| cookie.starts_with("dc_oauth_state=")),
            );
            api::redirect_with_cookies(&req, &return_to, &cookies)
        }
        Err(error) => map_auth_error(&req, error),
    }
}

pub async fn handle_refresh(req: Request) -> Result<Response<Body>, Error> {
    if *req.method() != Method::POST {
        return method_not_allowed(&req);
    }
    match auth::refresh_session(&req).await {
        Ok((session, bundle)) => {
            api::ok_with_cookies(&req, session, &auth::session_cookies(&bundle))
        }
        Err(error) => map_auth_error(&req, error),
    }
}

pub async fn handle_logout(req: Request) -> Result<Response<Body>, Error> {
    if *req.method() != Method::POST {
        return method_not_allowed(&req);
    }
    match auth::logout(&req).await {
        Ok(_) => api::ok_with_cookies(
            &req,
            serde_json::json!({ "ok": true }),
            &auth::clear_cookies(),
        ),
        Err(error) => map_auth_error(&req, error),
    }
}

pub async fn handle_disconnect(req: Request, provider: &str) -> Result<Response<Body>, Error> {
    if *req.method() != Method::DELETE {
        return method_not_allowed(&req);
    }
    let user = match auth::require_user(&req).await {
        Ok(user) => user,
        Err(error) => return map_auth_error(&req, error),
    };
    let provider = match AuthProvider::parse(provider) {
        Ok(provider) => provider,
        Err(error) => return api::map_error(&req, StatusCode::BAD_REQUEST, error),
    };
    match auth::disconnect_provider(&user.id, provider).await {
        Ok(session) => api::ok(&req, session),
        Err(error) => map_auth_error(&req, error),
    }
}

fn method_not_allowed(req: &Request) -> Result<Response<Body>, Error> {
    api::map_error(
        req,
        StatusCode::METHOD_NOT_ALLOWED,
        ApiError::bad_request("method not allowed"),
    )
}

pub fn map_auth_error(req: &Request, error: ApiError) -> Result<Response<Body>, Error> {
    let status = match error.code.as_str() {
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    };
    api::map_error(req, status, error)
}

fn query_value(req: &Request, key: &str) -> Option<String> {
    req.uri().query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.to_string())
    })
}
