use crate::api;
use crate::domain::chat::{self, ChatRequest};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

pub async fn handle(req: Request) -> Result<Response<Body>, Error> {
    let payload: ChatRequest = parse_json_body(&req)?;
    match chat::send_message(payload).await {
        Ok(data) => api::ok(&req, data),
        Err(error) => api::map_error(&req, StatusCode::BAD_REQUEST, error),
    }
}

fn parse_json_body<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, Error> {
    match req.body() {
        Body::Text(body) => Ok(serde_json::from_str(body)?),
        Body::Binary(body) => Ok(serde_json::from_slice(body)?),
        _ => Err("invalid body".into()),
    }
}
