use crate::api::{ApiError, AppResult};
use crate::dynamodb::put_value;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const CLIPBOARD_PART: &str = "clipboard";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Clipboard {
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpsertClipboardRequest {
    pub text: String,
}

pub async fn get_clipboard(user_id: &str) -> AppResult<Clipboard> {
    let raw = crate::dynamodb::get_value(CLIPBOARD_PART, &clipboard_idx(user_id))
        .await
        .map_err(ApiError::internal)?;
    parse_clipboard(raw.as_deref())
}

pub async fn save_clipboard(
    user_id: &str,
    payload: UpsertClipboardRequest,
) -> AppResult<Clipboard> {
    validate_text(&payload.text)?;

    let clipboard = Clipboard { text: payload.text };
    let raw = serde_json::to_string(&clipboard)
        .map_err(|e| ApiError::internal(format!("failed to serialize clipboard: {e}")))?;

    put_value(CLIPBOARD_PART, &clipboard_idx(user_id), raw)
        .await
        .map_err(ApiError::internal)?;

    Ok(clipboard)
}

fn clipboard_idx(user_id: &str) -> String {
    format!("user:{user_id}:latest")
}

fn validate_text(text: &str) -> AppResult<()> {
    if text.len() > 200_000 {
        return Err(ApiError::bad_request("clipboard text is too large"));
    }
    Ok(())
}

pub fn parse_clipboard(raw: Option<&str>) -> AppResult<Clipboard> {
    match raw {
        None | Some("") => Ok(Clipboard::default()),
        Some(raw) => {
            let value: Value = serde_json::from_str(raw)
                .map_err(|_| ApiError::bad_request("stored clipboard is not valid JSON"))?;
            normalize_clipboard(value)
        }
    }
}

fn normalize_clipboard(value: Value) -> AppResult<Clipboard> {
    if let Ok(item) = serde_json::from_value::<Clipboard>(value.clone()) {
        return Ok(item);
    }

    if let Some(data) = value.get("data").cloned() {
        if let Ok(item) = serde_json::from_value::<Clipboard>(data) {
            return Ok(item);
        }
    }

    if let Some(text) = value.as_str() {
        return Ok(Clipboard {
            text: text.to_string(),
        });
    }

    Err(ApiError::bad_request("stored clipboard payload is invalid"))
}

#[cfg(test)]
mod tests {
    use super::{parse_clipboard, Clipboard};

    #[test]
    fn returns_empty_clipboard_when_missing() {
        assert_eq!(parse_clipboard(None).unwrap(), Clipboard::default());
    }

    #[test]
    fn parses_wrapped_payload() {
        let clipboard = parse_clipboard(Some(r#"{"data":{"text":"hello"}}"#)).unwrap();
        assert_eq!(clipboard.text, "hello");
    }

    #[test]
    fn parses_plain_string_payload() {
        let clipboard = parse_clipboard(Some(r#""hello""#)).unwrap();
        assert_eq!(clipboard.text, "hello");
    }
}
