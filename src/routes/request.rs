use crate::api::{self, ApiError};
use lambda_http::http::StatusCode;
use lambda_http::{Body, Error, Request, Response};

/// Deserialize a JSON request body consistently across every HTTP route.
pub fn parse_json<T: serde::de::DeserializeOwned>(req: &Request) -> Result<T, Error> {
    match req.body() {
        Body::Text(body) => Ok(serde_json::from_str(body)?),
        Body::Binary(body) => Ok(serde_json::from_slice(body)?),
        _ => Err("a JSON request body is required".into()),
    }
}

pub fn method_not_allowed(req: &Request) -> Result<Response<Body>, Error> {
    api::map_error(
        req,
        StatusCode::METHOD_NOT_ALLOWED,
        ApiError::bad_request("method not allowed"),
    )
}

pub fn query_value(req: &Request, key: &str) -> Option<String> {
    req.uri().query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.into_owned())
    })
}
