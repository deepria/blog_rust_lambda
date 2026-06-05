use crate::api::{ApiError, AppResult};
use crate::dynamodb::{delete_value, get_value, put_value};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const NOTE_PART: &str = "NOTE";
const NOTE_LIST_PART: &str = "NOTE_LIST";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memo {
    pub id: String,
    pub title: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoSummary {
    pub id: String,
    pub title: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpsertMemoRequest {
    pub title: String,
    pub content: String,
}

pub async fn list_memos(user_id: &str) -> AppResult<Vec<MemoSummary>> {
    let raw = get_value(NOTE_LIST_PART, &memo_list_idx(user_id))
        .await
        .map_err(ApiError::internal)?;
    Ok(parse_summary_list(raw.as_deref()))
}

pub async fn get_memo(user_id: &str, id: &str) -> AppResult<Memo> {
    validate_id(id)?;
    let raw = get_value(NOTE_PART, &memo_idx(user_id, id))
        .await
        .map_err(ApiError::internal)?;
    let raw = raw.ok_or_else(|| ApiError::not_found("memo not found"))?;
    parse_memo(&raw)
}

pub async fn create_memo(user_id: &str, payload: UpsertMemoRequest) -> AppResult<Memo> {
    let id = Uuid::new_v4().to_string();
    let now = now_iso();
    let memo = Memo {
        id: id.clone(),
        title: payload.title.trim().to_string(),
        content: payload.content,
        created_at: now.clone(),
        updated_at: now,
    };
    persist_memo(user_id, &memo).await?;
    Ok(memo)
}

pub async fn update_memo(user_id: &str, id: &str, payload: UpsertMemoRequest) -> AppResult<Memo> {
    validate_id(id)?;
    let mut current = get_memo(user_id, id).await?;
    current.title = payload.title.trim().to_string();
    current.content = payload.content;
    current.updated_at = now_iso();
    persist_memo(user_id, &current).await?;
    Ok(current)
}

pub async fn delete_memo(user_id: &str, id: &str) -> AppResult<()> {
    validate_id(id)?;
    delete_value(NOTE_PART, &memo_idx(user_id, id))
        .await
        .map_err(ApiError::internal)?;
    let summaries = list_memos(user_id).await?;
    let updated = summaries
        .into_iter()
        .filter(|item| item.id != id)
        .collect::<Vec<_>>();
    save_summary_list(user_id, &updated).await?;
    Ok(())
}

async fn persist_memo(user_id: &str, memo: &Memo) -> AppResult<()> {
    validate_payload(&memo.title, &memo.content)?;
    let raw = serde_json::to_string(memo)
        .map_err(|e| ApiError::internal(format!("failed to serialize memo: {e}")))?;
    put_value(NOTE_PART, &memo_idx(user_id, &memo.id), raw)
        .await
        .map_err(ApiError::internal)?;

    let mut summaries = list_memos(user_id).await?;
    let summary = MemoSummary {
        id: memo.id.clone(),
        title: memo.title.clone(),
        updated_at: memo.updated_at.clone(),
    };
    if let Some(existing) = summaries.iter_mut().find(|item| item.id == memo.id) {
        *existing = summary;
    } else {
        summaries.insert(0, summary);
    }
    save_summary_list(user_id, &summaries).await
}

async fn save_summary_list(user_id: &str, items: &[MemoSummary]) -> AppResult<()> {
    let raw = serde_json::to_string(&serde_json::json!({ "data": items }))
        .map_err(|e| ApiError::internal(format!("failed to serialize memo index: {e}")))?;
    put_value(NOTE_LIST_PART, &memo_list_idx(user_id), raw)
        .await
        .map_err(ApiError::internal)?;
    Ok(())
}

fn memo_idx(user_id: &str, id: &str) -> String {
    format!("user:{user_id}:memo:{id}")
}

fn memo_list_idx(user_id: &str) -> String {
    format!("user:{user_id}:INDEX")
}

fn parse_memo(raw: &str) -> AppResult<Memo> {
    let value = serde_json::from_str::<Value>(raw)
        .map_err(|_| ApiError::bad_request("memo payload is not valid JSON"))?;
    let mut value = unwrap_payload(value);

    copy_alias_field(&mut value, "created_at", &["createdAt"]);
    copy_alias_field(&mut value, "updated_at", &["updatedAt"]);
    if value.get("created_at").is_none() {
        copy_alias_field(&mut value, "created_at", &["updated_at", "updatedAt"]);
    }
    if value.get("updated_at").is_none() {
        copy_alias_field(&mut value, "updated_at", &["created_at", "createdAt"]);
    }
    stringify_field(&mut value, "created_at");
    stringify_field(&mut value, "updated_at");

    serde_json::from_value::<Memo>(value)
        .map_err(|_| ApiError::bad_request("memo payload is invalid"))
}

pub fn parse_summary_list(raw: Option<&str>) -> Vec<MemoSummary> {
    raw.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(parse_summary_value)
        .unwrap_or_default()
}

fn parse_summary_value(value: serde_json::Value) -> Option<Vec<MemoSummary>> {
    let value = unwrap_payload(value);

    if let Some(items) = value
        .get("data")
        .and_then(|items| normalize_summary_items(items.clone()))
    {
        return Some(items);
    }

    normalize_summary_items(value)
}

fn normalize_summary_items(value: serde_json::Value) -> Option<Vec<MemoSummary>> {
    let array = value.as_array()?.clone();
    Some(
        array
            .into_iter()
            .filter_map(summary_from_value)
            .collect::<Vec<_>>(),
    )
}

fn validate_id(id: &str) -> AppResult<()> {
    if id.trim().is_empty() {
        return Err(ApiError::bad_request("id is required"));
    }
    Ok(())
}

fn validate_payload(title: &str, content: &str) -> AppResult<()> {
    if title.trim().is_empty() {
        return Err(ApiError::bad_request("title is required"));
    }
    if content.len() > 500_000 {
        return Err(ApiError::bad_request("content is too large"));
    }
    Ok(())
}

fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    secs.to_string()
}

fn unwrap_payload(value: Value) -> Value {
    let mut current = value;
    for key in ["data", "value"] {
        let Some(next) = current.get(key).cloned() else {
            continue;
        };
        if matches!(next, Value::Object(_) | Value::Array(_)) {
            current = next;
        }
    }
    current
}

fn copy_alias_field(value: &mut Value, target: &str, aliases: &[&str]) {
    if value.get(target).is_some() {
        return;
    }
    for alias in aliases {
        if let Some(candidate) = value.get(*alias).cloned() {
            value[target] = candidate;
            return;
        }
    }
}

fn stringify_field(value: &mut Value, field: &str) {
    let Some(current) = value.get(field).cloned() else {
        return;
    };
    value[field] = match current {
        Value::String(text) => Value::String(text),
        Value::Number(number) => Value::String(number.to_string()),
        other => other,
    };
}

fn summary_from_value(value: Value) -> Option<MemoSummary> {
    let mut value = unwrap_payload(value);
    copy_alias_field(&mut value, "updated_at", &["updatedAt"]);
    stringify_field(&mut value, "updated_at");
    serde_json::from_value::<MemoSummary>(value).ok()
}

#[cfg(test)]
mod tests {
    use super::{parse_memo, parse_summary_list};

    #[test]
    fn parses_legacy_index_shape() {
        let items = parse_summary_list(Some(
            r#"{"data":[{"id":"1","title":"A","updated_at":"1"}]}"#,
        ));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "A");
    }

    #[test]
    fn parses_legacy_index_with_camel_case_timestamp() {
        let items = parse_summary_list(Some(
            r#"{"data":[{"id":"1","title":"A","updatedAt":"1710000000"}]}"#,
        ));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].updated_at, "1710000000");
    }

    #[test]
    fn parses_exact_index_payload_shape() {
        let items = parse_summary_list(Some(
            r#"{"data":[{"id":"37609a66-d670-4758-b34c-e4bd884974db","title":"tmp","updatedAt":"2026-04-01T00:29:43.080Z"}]}"#,
        ));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "37609a66-d670-4758-b34c-e4bd884974db");
        assert_eq!(items[0].title, "tmp");
    }

    #[test]
    fn parses_note_payload_with_camel_case_timestamps() {
        let memo = parse_memo(
            r#"{"id":"note-1","title":"Wrapped","content":"body","createdAt":"2026-02-09T06:42:42.850Z","updatedAt":"2026-02-09T06:42:42.850Z"}"#,
        )
        .expect("memo should parse");

        assert_eq!(memo.id, "note-1");
        assert_eq!(memo.title, "Wrapped");
        assert_eq!(memo.content, "body");
        assert_eq!(memo.updated_at, "2026-02-09T06:42:42.850Z");
    }

    #[test]
    fn parses_note_payload_without_created_at() {
        let memo = parse_memo(
            r#"{"id":"note-1","title":"Legacy","content":"body","updatedAt":"2026-03-24T04:12:27.828Z"}"#,
        )
        .expect("memo should parse");

        assert_eq!(memo.created_at, "2026-03-24T04:12:27.828Z");
        assert_eq!(memo.updated_at, "2026-03-24T04:12:27.828Z");
    }
}
