use crate::dynamodb::{
    delete_item,
    get_item_value,
    put_item,
    scan_test_entities,
    query_test_entities_by_id,
    upsert_test_entity,
    delete_test_entity_by_id,
};
use crate::s3::{list_objects, presign_upload, presign_download, presign_delete};
use lambda_http::{Body, Error, Request, Response};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct DynamodbPutItemPayload {
    part: String,
    index: String,
    pk: String,
    value: String,
}

fn text_response(status: u16, body: String) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(body));
    *response.status_mut() = status.try_into().unwrap_or_default();
    Ok(response)
}

fn json_response(status: u16, value: serde_json::Value) -> Result<Response<Body>, Error> {
    let mut response = Response::new(Body::Text(value.to_string()));
    *response.status_mut() = status.try_into().unwrap_or_default();
    response.headers_mut().insert(
        "content-type",
        "application/json; charset=utf-8".parse().unwrap(),
    );
    Ok(response)
}

fn query_param(req: &Request, key: &str) -> Option<String> {
    req.uri()
        .query()
        .and_then(|q| url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == key))
        .map(|(_, v)| v.to_string())
}

pub async fn function_handler(req: Request) -> Result<Response<Body>, Error> {
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
        let index = query_param(&req, "index").unwrap_or_default();

        match get_item_value("deepria", part, index).await {
            Ok(Some(value)) => return text_response(200, value),
            Ok(None) => return text_response(200, "Value not found".to_string()),
            Err(e) => {
                tracing::error!("dynamodb get error: {:?}", e);
                return text_response(500, "dynamodb error".to_string());
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

        if let Err(e) = put_item(
            "deepria",
            payload.part,
            payload.index,
            payload.pk,
            payload.value,
        )
        .await
        {
            tracing::error!("dynamodb put error: {:?}", e);
            return text_response(500, "dynamodb error".to_string());
        }

        return text_response(200, "Success".to_string());
    }

    if path == "/dynamodb/item" && method == "DELETE" {
        let part = query_param(&req, "part").unwrap_or_default();
        let index = query_param(&req, "index").unwrap_or_default();

        if let Err(e) = delete_item("deepria", part, index).await {
            tracing::error!("dynamodb delete error: {:?}", e);
            return text_response(500, "dynamodb error".to_string());
        }

        return text_response(200, "Success".to_string());
    }

    // 3) dynamodb - entity style
    if path == "/dynamodb/list" && method == "GET" {
        match scan_test_entities("testTable").await {
            Ok(items) => return json_response(200, serde_json::to_value(items)?),
            Err(e) => {
                tracing::error!("dynamodb scan error: {:?}", e);
                return text_response(500, "dynamodb error".to_string());
            }
        }
    }

    if path.starts_with("/dynamodb/")
        && method == "GET"
        && path != "/dynamodb/item"
        && path != "/dynamodb/list"
    {
        let id = path.trim_start_matches("/dynamodb/").to_string();

        match query_test_entities_by_id("testTable", id).await {
            Ok(items) => return json_response(200, serde_json::to_value(items)?),
            Err(e) => {
                tracing::error!("dynamodb query error: {:?}", e);
                return text_response(500, "dynamodb error".to_string());
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct TestEntityPayload {
        id: String,
        value: Option<String>,
    }

    if path == "/dynamodb" && method == "POST" {
        let body = req.body();
        let payload: TestEntityPayload = match body {
            Body::Text(s) => serde_json::from_str(s)?,
            Body::Binary(b) => serde_json::from_slice(b)?,
            Body::Empty => {
                return text_response(400, "empty body".to_string());
            }
            _ => {
                return text_response(400, "unsupported body type".to_string());
            }
        };

        if let Err(e) = upsert_test_entity("testTable", payload.id, payload.value).await {
            tracing::error!("dynamodb upsert error: {:?}", e);
            return text_response(500, "dynamodb error".to_string());
        }

        return text_response(200, "Entity saved successfully.".to_string());
    }

    if path.starts_with("/dynamodb/") && method == "DELETE" && path != "/dynamodb/item" {
        let id = path.trim_start_matches("/dynamodb/").to_string();

        if let Err(e) = delete_test_entity_by_id("testTable", id.clone()).await {
            tracing::error!("dynamodb entity delete error: {:?}", e);
            return text_response(500, "dynamodb error".to_string());
        }

        return text_response(200, format!("Entity deleted successfully. ({id})"));
    }

    // 4) s3
    if path == "/api/s3/list" && method == "GET" {
        let prefix = query_param(&req, "prefix").unwrap_or_default();
        let full_prefix = format!("{base_path}{prefix}");

        match list_objects(&bucket, full_prefix).await {
            Ok((folders, files)) => {
                return json_response(200, json!({ "folders": folders, "files": files }));
            }
            Err(e) => {
                tracing::error!("s3 list error: {:?}", e);
                return text_response(500, "s3 error".to_string());
            }
        }
    }

    if path == "/api/s3/upload-url" && method == "GET" {
        let filename = query_param(&req, "filename").unwrap_or_default();
        let content_type = query_param(&req, "contentType").unwrap_or("application/octet-stream".to_string());
        let key = format!("{base_path}{filename}");

        match presign_upload(&bucket, key, content_type).await {
            Ok(url) => return text_response(200, url),
            Err(e) => {
                tracing::error!("s3 upload presign error: {:?}", e);
                return text_response(500, "s3 error".to_string());
            }
        }
    }

    if path == "/api/s3/download-url" && method == "GET" {
        let filename = query_param(&req, "filename").unwrap_or_default();
        let key = format!("{base_path}{filename}");

        match presign_download(&bucket, key).await {
            Ok(url) => return text_response(200, url),
            Err(e) => {
                tracing::error!("s3 download presign error: {:?}", e);
                return text_response(500, "s3 error".to_string());
            }
        }
    }

    if path == "/api/s3/delete-url" && method == "GET" {
        let filename = query_param(&req, "filename").unwrap_or_default();
        let key = format!("{base_path}{filename}");

        match presign_delete(&bucket, key).await {
            Ok(url) => return text_response(200, url),
            Err(e) => {
                tracing::error!("s3 delete presign error: {:?}", e);
                return text_response(500, "s3 error".to_string());
            }
        }
    }

    // not found
    text_response(404, format!("not found: {method} {path}"))
}
