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
        "/api/chat" if req.method() == "POST" => routes::chat::handle(req).await,
        "/api/clipboard" => routes::clipboard::handle(req).await,
        "/api/todos" => routes::todos::handle_todos(req).await,
        "/api/todo-meta" => routes::todos::handle_meta(req).await,
        "/api/memos" => routes::memos::handle_collection(req).await,
        "/api/files" => routes::files::handle_collection(req).await,
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
