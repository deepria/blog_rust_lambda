use crate::api::{ApiError, AppResult};
use crate::dynamodb::{get_value, put_json, put_value};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TODO_PART: &str = "todo";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TodoMeta {
    pub groups: Vec<TodoOption>,
    pub priorities: Vec<TodoOption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoOption {
    pub key: i64,
    pub name: String,
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TodoId {
    Number(i64),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: TodoId,
    #[serde(default)]
    pub text: String,
    #[serde(default, rename = "groupKey")]
    pub group_key: Option<i64>,
    #[serde(default, rename = "priorityKey")]
    pub priority_key: Option<i64>,
    #[serde(default)]
    pub completed: bool,
}

pub async fn get_todos(user_id: &str) -> AppResult<Vec<TodoItem>> {
    let raw = get_value(TODO_PART, &todo_items_idx(user_id))
        .await
        .map_err(ApiError::internal)?;
    parse_todo_items(raw.as_deref())
}

pub async fn save_todos(user_id: &str, items: Vec<TodoItem>) -> AppResult<Vec<TodoItem>> {
    validate_items(&items)?;
    put_value(
        TODO_PART,
        &todo_items_idx(user_id),
        serde_json::to_string(&items)
            .map_err(|e| ApiError::internal(format!("failed to serialize todos: {e}")))?,
    )
    .await
    .map_err(ApiError::internal)?;
    Ok(items)
}

pub async fn get_meta(user_id: &str) -> AppResult<TodoMeta> {
    let legacy_raw = get_value(TODO_PART, &todo_meta_idx(user_id))
        .await
        .map_err(ApiError::internal)?;
    Ok(parse_meta(legacy_raw.as_deref()))
}

pub async fn save_meta(user_id: &str, mut meta: TodoMeta) -> AppResult<TodoMeta> {
    meta = normalize_meta(meta);
    put_json(TODO_PART, &todo_meta_idx(user_id), &meta)
        .await
        .map_err(ApiError::internal)?;
    Ok(meta)
}

fn todo_items_idx(user_id: &str) -> String {
    format!("user:{user_id}:todo")
}

fn todo_meta_idx(user_id: &str) -> String {
    format!("user:{user_id}:meta")
}

fn validate_items(items: &[TodoItem]) -> AppResult<()> {
    if items.iter().any(|item| item.text.chars().count() > 500) {
        return Err(ApiError::bad_request("todo text is too long"));
    }
    Ok(())
}

pub fn parse_todo_items(raw: Option<&str>) -> AppResult<Vec<TodoItem>> {
    match raw {
        None | Some("") => Ok(Vec::new()),
        Some(raw) => {
            let value: Value = serde_json::from_str(raw)
                .map_err(|_| ApiError::bad_request("stored todos are not valid JSON"))?;
            if let Some(items) = value.as_array() {
                return serde_json::from_value(Value::Array(items.clone()))
                    .map_err(|_| ApiError::bad_request("stored todos have an invalid shape"));
            }
            if let Some(items) = value.get("data").and_then(Value::as_array) {
                return serde_json::from_value(Value::Array(items.clone()))
                    .map_err(|_| ApiError::bad_request("stored todos have an invalid shape"));
            }
            Err(ApiError::bad_request("stored todos are not an array"))
        }
    }
}

pub fn parse_meta(raw: Option<&str>) -> TodoMeta {
    let meta = raw
        .and_then(|value| serde_json::from_str::<TodoMeta>(value).ok())
        .or_else(|| {
            raw.and_then(|value| serde_json::from_str::<Value>(value).ok())
                .and_then(value_to_meta)
        })
        .unwrap_or_default();
    normalize_meta(meta)
}

fn value_to_meta(value: Value) -> Option<TodoMeta> {
    serde_json::from_value::<TodoMeta>(value).ok()
}

fn normalize_meta(mut meta: TodoMeta) -> TodoMeta {
    if meta.groups.is_empty() {
        meta.groups.push(TodoOption {
            key: 1,
            name: "Default".to_string(),
            color: "#42b983".to_string(),
        });
    }
    if meta.priorities.is_empty() {
        meta.priorities.push(TodoOption {
            key: 1,
            name: "High".to_string(),
            color: "#ef4444".to_string(),
        });
    }
    meta
}

#[cfg(test)]
mod tests {
    use super::{parse_meta, parse_todo_items, TodoId};

    #[test]
    fn parses_legacy_todo_array() {
        let items = parse_todo_items(Some(r#"[{"id":1},{"id":2}]"#)).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, TodoId::Number(1));
    }

    #[test]
    fn fills_default_meta() {
        let meta = parse_meta(None);
        assert_eq!(meta.groups[0].name, "Default");
        assert_eq!(meta.priorities[0].name, "High");
    }
}
