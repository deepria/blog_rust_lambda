use crate::dynamodb::{delete_item, get_item_value, put_item};
use crate::s3::{list_objects, presign_delete, presign_download, presign_upload};
use lambda_http::{Body, Error, Request, Response};
use lambda_http::http::StatusCode;
use serde::Deserialize;
use serde_json::json;

fn add_cors_headers(response: &mut Response<Body>) {
    response.headers_mut().insert(
        "Access-Control-Allow-Origin",
        "*".parse().unwrap(),
    );
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

pub async fn function_handler(req: Request) -> Result<Response<Body>, Error> {
    if req.method() == "OPTIONS" {
        let mut response = Response::new(Body::Empty);
        *response.status_mut() = StatusCode::OK;
        add_cors_headers(&mut response);
        return Ok(response);
    }

    let path = req.uri().path().to_string();
    let method = req.method().as_str();

    let bucket = std::env::var("s3_bucket").expect("s3_bucket env missing");
    let base_path = std::env::var("s3_path").unwrap_or_default();

    // 1) health
    if method == "GET" && path == "/helloWorld" {
        return text_response(200, "OK".to_string());
    }

    // 2) dynamodb - attribute item
    if path == "/dynamodb/item" && method == "GET" {
        let part = query_param(&req, "part").unwrap_or_default();
        let idx = query_param(&req, "idx").unwrap_or_default();

        if part.is_empty() {
            return text_response(400, "part is required".to_string());
        }
        if idx.is_empty() {
            return text_response(400, "idx is required".to_string());
        }

        return match get_item_value(part, idx).await {
            Ok(Some(value)) => text_response(200, value),
            Ok(None) => text_response(200, "Value not found".to_string()),
            Err(e) => {
                tracing::error!("dynamodb get error: {:?}", e);
                text_response(500, "dynamodb error".to_string())
            }
        }
    }

    if path == "/dynamodb/item" && method == "POST" {
        let body = req.body();
        let payload: DynamodbPutItemPayload = match body {
            Body::Text(s) => serde_json::from_str(s)?,
            Body::Binary(b) => serde_json::from_slice(b)?,
            Body::Empty => {
                return text_response(400, "empty body".to_string());
            }
            _ => {
                return text_response(400, "unsupported body type".to_string());
            }
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

        return text_response(200, "Success".to_string());
    }

    if path == "/dynamodb/item" && method == "DELETE" {
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

        return text_response(200, "Success".to_string());
    }

    // 4) s3
    if path == "/api/s3/list" && method == "GET" {
        let part = query_param(&req, "part");
        let idx = query_param(&req, "idx");

        let prefix = if let (Some(part_val), Some(idx_val)) = (&part, &idx) {
            if !part_val.is_empty() && !idx_val.is_empty() {
                format!("{base_path}upload/{}/{}/", part_val, idx_val)
            } else {
                format!("{base_path}upload/")
            }
        } else {
            format!("{base_path}upload/")
        };

        return match list_objects(&bucket, prefix).await {
            Ok((folders, files)) => {
                json_response(200, json!({ "folders": folders, "files": files }))
            }
            Err(e) => {
                tracing::error!("s3 list error: {:?}", e);
                text_response(500, "s3 error".to_string())
            }
        };
    }

    if path == "/api/s3/upload-url" && method == "GET" {
        let part = query_param(&req, "part");
        let idx = query_param(&req, "idx");
        let filename = query_param(&req, "filename").unwrap_or_default();

        if filename.is_empty() {
            return text_response(400, "filename is required".to_string());
        }

        let content_type =
            query_param(&req, "contentType").unwrap_or("application/octet-stream".to_string());

        let key = if let (Some(part_val), Some(idx_val)) = (&part, &idx) {
            if !part_val.is_empty() && !idx_val.is_empty() {
                format!("{base_path}upload/{}/{}/{}", part_val, idx_val, filename)
            } else {
                format!("{base_path}upload/{}", filename)
            }
        } else {
            format!("{base_path}upload/{}", filename)
        };

        return match presign_upload(&bucket, key, content_type).await {
            Ok(url) => text_response(200, url),
            Err(e) => {
                tracing::error!("s3 upload presign error: {:?}", e);
                text_response(500, "s3 error".to_string())
            }
        };
    }

    if path == "/api/s3/download-url" && method == "GET" {
        let part = query_param(&req, "part");
        let idx = query_param(&req, "idx");
        let filename = query_param(&req, "filename").unwrap_or_default();

        if filename.is_empty() {
            return text_response(400, "filename is required".to_string());
        }

        let key = if let (Some(part_val), Some(idx_val)) = (&part, &idx) {
            if !part_val.is_empty() && !idx_val.is_empty() {
                format!("{base_path}{}/{}/{}", part_val, idx_val, filename)
            } else {
                format!("{base_path}{}", filename)
            }
        } else {
            format!("{base_path}{}", filename)
        };

        return match presign_download(&bucket, key).await {
            Ok(url) => text_response(200, url),
            Err(e) => {
                tracing::error!("s3 download presign error: {:?}", e);
                text_response(500, "s3 error".to_string())
            }
        };
    }

    if path == "/api/s3/delete-url" && method == "GET" {
        let part = query_param(&req, "part");
        let idx = query_param(&req, "idx");
        let filename = query_param(&req, "filename").unwrap_or_default();

        if filename.is_empty() {
            return text_response(400, "filename is required".to_string());
        }

        let key = if let (Some(part_val), Some(idx_val)) = (&part, &idx) {
            if !part_val.is_empty() && !idx_val.is_empty() {
                format!("{base_path}{}/{}/{}", part_val, idx_val, filename)
            } else {
                format!("{base_path}{}", filename)
            }
        } else {
            format!("{base_path}{}", filename)
        };

        return match presign_delete(&bucket, key).await {
            Ok(url) => text_response(200, url),
            Err(e) => {
                tracing::error!("s3 delete presign error: {:?}", e);
                text_response(500, "s3 error".to_string())
            }
        };
    }

    // not found
    text_response(404, format!("not found: {method} {path}"))
}
