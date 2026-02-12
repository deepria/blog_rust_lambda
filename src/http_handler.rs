use crate::dynamodb::{delete_item, get_item_value, put_item};
use crate::gemini::generate_content;
use crate::s3::{
    build_key, get_base_path, list_objects, presign_delete, presign_download, presign_upload,
};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct ChatPayload {
    message: String,
}

async fn handle_chat(req: Request) -> Result<Response<Body>, Error> {
    let body = req.body();
    let payload: ChatPayload = match body {
        Body::Text(s) => serde_json::from_str(s)?,
        Body::Binary(b) => serde_json::from_slice(b)?,
        _ => return text_response(400, "Invalid body".to_string()),
    };

    if payload.message.is_empty() {
        return text_response(400, "message is required".to_string());
    }

    match generate_content(&payload.message).await {
        Ok(reply) => json_response(200, json!({ "reply": reply })),
        Err(e) => {
            tracing::error!("gemini error: {:?}", e);
            text_response(500, format!("gemini error: {}", e))
        }
    }
}

fn add_cors_headers(response: &mut Response<Body>) {
    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", "*".parse().unwrap());
    response.headers_mut().insert(
        "Access-Control-Allow-Methods",
        "GET,POST,DELETE,OPTIONS".parse().unwrap(),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Headers",
        "Content-Type,Authorization".parse().unwrap(),
    );
}

#[derive(Debug, Deserialize)]
struct DynamodbPutItemPayload {
    part: String,
    idx: String,
    value: String,
}

fn text_response(status: u16, body: String) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(body));
    *response.status_mut() = status.try_into().unwrap_or_default();
    add_cors_headers(&mut response);
    Ok(response)
}

fn json_response(status: u16, value: serde_json::Value) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(value.to_string()));
    *response.status_mut() = status.try_into().unwrap_or_default();
    response.headers_mut().insert(
        "content-type",
        "application/json; charset=utf-8".parse()?,
    );
    add_cors_headers(&mut response);
    Ok(response)
}

fn query_param(req: &Request, key: &str) -> Option<String> {
    req.uri()
        .query()
        .and_then(|q| url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == key))
        .map(|(_, v)| v.to_string())
}

async fn handle_dynamodb(req: Request) -> Result<Response<Body>, Error> {
    let method = req.method().as_str();

    match method {
        "GET" => {
            let part = query_param(&req, "part").unwrap_or_default();
            let idx = query_param(&req, "idx").unwrap_or_default();

            if part.is_empty() {
                return text_response(400, "part is required".to_string());
            }
            if idx.is_empty() {
                return text_response(400, "idx is required".to_string());
            }

            match get_item_value(part, idx).await {
                Ok(Some(value)) => text_response(200, value),
                Ok(None) => text_response(200, "".to_string()),
                Err(e) => {
                    tracing::error!("dynamodb get error: {:?}", e);
                    text_response(500, "dynamodb error".to_string())
                }
            }
        }
        "POST" => {
            let body = req.body();
            let payload: DynamodbPutItemPayload = match body {
                Body::Text(s) => serde_json::from_str(s)?,
                Body::Binary(b) => serde_json::from_slice(b)?,
                Body::Empty => return text_response(400, "empty body".to_string()),
                _ => return text_response(400, "unsupported body type".to_string()),
            };

            if payload.part.is_empty() {
                return text_response(400, "part is required".to_string());
            }
            if payload.idx.is_empty() {
                return text_response(400, "idx is required".to_string());
            }

            if let Err(e) = put_item(payload.part, payload.idx, payload.value).await {
                tracing::error!("dynamodb put error: {:?}", e);
                return text_response(500, "dynamodb error".to_string());
            }

            text_response(200, "Success".to_string())
        }
        "DELETE" => {
            let part = query_param(&req, "part").unwrap_or_default();
            let idx = query_param(&req, "idx").unwrap_or_default();

            if part.is_empty() {
                return text_response(400, "part is required".to_string());
            }
            if idx.is_empty() {
                return text_response(400, "idx is required".to_string());
            }

            if let Err(e) = delete_item(part, idx).await {
                tracing::error!("dynamodb delete error: {:?}", e);
                return text_response(500, "dynamodb error".to_string());
            }

            text_response(200, "Success".to_string())
        }
        _ => text_response(405, "Method Not Allowed".to_string()),
    }
}

async fn handle_s3_list(
    req: Request,
) -> Result<Response<Body>, Error> {
    let part = query_param(&req, "part");
    let idx = query_param(&req, "idx");
    let path_param = query_param(&req, "path").unwrap_or_default();

    let base_path = get_base_path();
    // list logic: usually prefix is folder path
    // build_key(base, prefix=path_param, part, idx, filename="")
    let prefix = build_key(&base_path, &path_param, part.as_ref(), idx.as_ref(), "");

    match list_objects(prefix).await {
        Ok((folders, files)) => json_response(200, json!({ "folders": folders, "files": files })),
        Err(e) => {
            tracing::error!("s3 list error: {:?}", e);
            text_response(500, "s3 error".to_string())
        }
    }
}

async fn handle_s3_upload_url(
    req: Request,
) -> Result<Response<Body>, Error> {
    let part = query_param(&req, "part");
    let idx = query_param(&req, "idx");
    let path_param = query_param(&req, "path").unwrap_or_default();
    let filename = query_param(&req, "filename").unwrap_or_default();

    if filename.is_empty() {
        return text_response(400, "filename is required".to_string());
    }

    let content_type =
        query_param(&req, "contentType").unwrap_or("application/octet-stream".to_string());

    let base_path = get_base_path();
    let key = build_key(&base_path, &path_param, part.as_ref(), idx.as_ref(), &filename);

    match presign_upload(key, content_type).await {
        Ok(url) => text_response(200, url),
        Err(e) => {
            tracing::error!("s3 upload presign error: {:?}", e);
            text_response(500, "s3 error".to_string())
        }
    }
}

async fn handle_s3_download_url(
    req: Request,
) -> Result<Response<Body>, Error> {
    let part = query_param(&req, "part");
    let idx = query_param(&req, "idx");
    let path_param = query_param(&req, "path").unwrap_or_default();
    let filename = query_param(&req, "filename").unwrap_or_default();
    let original_name = query_param(&req, "originalName").unwrap_or_default();
    let final_name = if original_name.is_empty() {
        filename.clone()
    } else {
        original_name
    };

    if filename.is_empty() {
        return text_response(400, "filename is required".to_string());
    }

    let base_path = get_base_path();
    let key = build_key(&base_path, &path_param, part.as_ref(), idx.as_ref(), &filename);

    match presign_download(key, final_name).await {
        Ok(url) => text_response(200, url),
        Err(e) => {
            tracing::error!("s3 download presign error: {:?}", e);
            text_response(500, "s3 error".to_string())
        }
    }
}

async fn handle_s3_delete_url(
    req: Request,
) -> Result<Response<Body>, Error> {
    let part = query_param(&req, "part");
    let idx = query_param(&req, "idx");
    let path_param = query_param(&req, "path").unwrap_or_default();
    let filename = query_param(&req, "filename").unwrap_or_default();

    if filename.is_empty() {
        return text_response(400, "filename is required".to_string());
    }

    let base_path = get_base_path();
    let key = build_key(&base_path, &path_param, part.as_ref(), idx.as_ref(), &filename);

    match presign_delete(key).await {
        Ok(url) => text_response(200, url),
        Err(e) => {
            tracing::error!("s3 delete presign error: {:?}", e);
            text_response(500, "s3 error".to_string())
        }
    }
}

pub async fn function_handler(req: Request) -> Result<Response<Body>, Error> {
    if req.method() == "OPTIONS" {
        let mut response = Response::new(Body::Empty);
        *response.status_mut() = StatusCode::OK;
        add_cors_headers(&mut response);
        return Ok(response);
    }

    let path = req.uri().path().to_string();
    let method = req.method().as_str();

    if method == "GET" && path == "/helloWorld" {
        return text_response(200, "OK".to_string());
    }

    if path == "/dynamodb/item" {
        return handle_dynamodb(req).await;
    }

    if path == "/api/s3/list" && method == "GET" {
        return handle_s3_list(req).await;
    }

    if path == "/api/s3/upload-url" && method == "GET" {
        return handle_s3_upload_url(req).await;
    }

    if path == "/api/s3/download-url" && method == "GET" {
        return handle_s3_download_url(req).await;
    }

    if path == "/api/s3/delete-url" && method == "GET" {
        return handle_s3_delete_url(req).await;
    }

    if path == "/api/s3/delete-url" && method == "GET" {
        return handle_s3_delete_url(req).await;
    }

    if path == "/api/chat" && method == "POST" {
        return handle_chat(req).await;
    }

    text_response(404, format!("not found: {method} {path}"))
}
