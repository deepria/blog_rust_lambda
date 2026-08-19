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

/// Decode the extra `%25` layer added when a storage key that already contains
/// percent escapes is embedded in a URL path. If an upstream proxy has already
/// removed that layer, preserve the key exactly as received.
pub fn decode_storage_key_segment(value: &str) -> String {
    if !value.to_ascii_lowercase().contains("%25") {
        return value.to_string();
    }
    let encoded = format!("key={value}");
    url::form_urlencoded::parse(encoded.as_bytes())
        .find(|(name, _)| name == "key")
        .map(|(_, value)| value.into_owned())
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::decode_storage_key_segment;

    #[test]
    fn removes_only_the_extra_path_encoding_layer() {
        assert_eq!(
            decode_storage_key_segment("file_eJxData%253D%253D"),
            "file_eJxData%3D%3D"
        );
    }

    #[test]
    fn preserves_keys_already_decoded_by_the_proxy() {
        assert_eq!(
            decode_storage_key_segment("file_eJxData%3D%3D"),
            "file_eJxData%3D%3D"
        );
    }
}
