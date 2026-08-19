use crate::api::{self, ApiError};
use crate::routes;
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn function_handler(req: Request) -> Result<Response<Body>, Error> {
    if req.method() == "OPTIONS" {
        return api::options_response(&req);
    }

    let path = req.uri().path().trim_end_matches('/').to_string();

    if path.is_empty() || path == "/" {
        return api::ok(&req, serde_json::json!({ "status": "ok" }));
    }

    match path.as_str() {
        "/health" => api::ok(&req, serde_json::json!({ "status": "ok" })),
        "/api/auth/me" => routes::auth::handle_me(req).await,
        "/api/auth/refresh" => routes::auth::handle_refresh(req).await,
        "/api/auth/logout" => routes::auth::handle_logout(req).await,
        "/api/auth/mobile/complete" => routes::auth::handle_mobile_complete(req).await,
        "/api/chat" if req.method() == "POST" => routes::chat::handle(req).await,
        "/api/clipboard" => routes::clipboard::handle(req).await,
        "/api/todos" => routes::todos::handle_todos(req).await,
        "/api/todo-meta" => routes::todos::handle_meta(req).await,
        "/api/memos" => routes::memos::handle_collection(req).await,
        "/api/files" => routes::files::handle_collection(req).await,
        "/api/files/shared-with-me" => routes::files::handle_shared_collection(req).await,
        "/api/files/organization" => routes::files::handle_organization(req).await,
        "/api/files/presign-upload" if req.method() == "POST" => {
            routes::files::handle_presign_upload(req).await
        }
        "/api/files/multipart/initiate" if req.method() == "POST" => {
            routes::files::handle_multipart_initiate(req).await
        }
        "/api/files/multipart/presign-part" if req.method() == "POST" => {
            routes::files::handle_multipart_presign_part(req).await
        }
        "/api/files/multipart/complete" if req.method() == "POST" => {
            routes::files::handle_multipart_complete(req).await
        }
        "/api/files/multipart/abort" if req.method() == "POST" => {
            routes::files::handle_multipart_abort(req).await
        }
        "/api/files/presign-download" if req.method() == "POST" => {
            routes::files::handle_presign_download(req).await
        }
        "/api/files/presign-delete" if req.method() == "POST" => {
            routes::files::handle_presign_delete(req).await
        }
        _ if path.starts_with("/api/memos/") => {
            let id = path.trim_start_matches("/api/memos/");
            routes::memos::handle_item(req, id).await
        }
        _ if path.starts_with("/api/auth/") && path.ends_with("/callback") => {
            let provider = path
                .trim_start_matches("/api/auth/")
                .trim_end_matches("/callback")
                .trim_matches('/');
            routes::auth::handle_callback(req, provider).await
        }
        _ if path.starts_with("/api/auth/") && path.ends_with("/connect") => {
            let provider = path
                .trim_start_matches("/api/auth/")
                .trim_end_matches("/connect")
                .trim_matches('/');
            routes::auth::handle_connect(req, provider).await
        }
        _ if path.starts_with("/api/auth/") && path.ends_with("/disconnect") => {
            let provider = path
                .trim_start_matches("/api/auth/")
                .trim_end_matches("/disconnect")
                .trim_matches('/');
            routes::auth::handle_disconnect(req, provider).await
        }
        _ if path.starts_with("/api/auth/") && path.ends_with("/mobile-login") => {
            let provider = path
                .trim_start_matches("/api/auth/")
                .trim_end_matches("/mobile-login")
                .trim_matches('/');
            routes::auth::handle_mobile_login(req, provider).await
        }
        _ if path.starts_with("/api/auth/") && path.ends_with("/login") => {
            let provider = path
                .trim_start_matches("/api/auth/")
                .trim_end_matches("/login")
                .trim_matches('/');
            routes::auth::handle_login(req, provider).await
        }
        _ if path.starts_with("/api/files/") && path.ends_with("/viewers") => {
            let key = path
                .trim_start_matches("/api/files/")
                .trim_end_matches("/viewers")
                .trim_matches('/');
            routes::files::handle_viewers(req, key).await
        }
        _ if path.starts_with("/api/files/") && path.contains("/viewers/") => {
            let remainder = path.trim_start_matches("/api/files/");
            let Some((key, viewer_id)) = remainder.split_once("/viewers/") else {
                return api::map_error(
                    &req,
                    StatusCode::BAD_REQUEST,
                    ApiError::bad_request("invalid viewer route"),
                );
            };
            routes::files::handle_viewer(req, key, viewer_id).await
        }
        _ if path.starts_with("/api/files/") && req.method() == "DELETE" => {
            let key = path.trim_start_matches("/api/files/");
            routes::files::handle_delete(req, key).await
        }
        _ => api::map_error(
            &req,
            StatusCode::NOT_FOUND,
            ApiError::not_found(format!(
                "route not found: {} {}",
                req.method(),
                req.uri().path()
            )),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::function_handler;
    use lambda_http::http::{Request as HttpRequest, StatusCode};
    use lambda_http::Body;

    #[tokio::test]
    async fn health_endpoint_returns_ok_without_infrastructure() {
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/health")
            .body(Body::Empty)
            .unwrap();
        let response = function_handler(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_route_returns_not_found_without_infrastructure() {
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/not-a-route")
            .body(Body::Empty)
            .unwrap();
        let response = function_handler(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
