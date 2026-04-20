use crate::api::{ApiError, AppResult};
use crate::dynamodb::{get_value, put_json, put_value};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TODO_PART: &str = "todo";
const TODO_ITEMS_IDX: &str = "todo";
const TODO_META_IDX: &str = "meta";

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

pub async fn get_todos() -> AppResult<Vec<Value>> {
    let raw = get_value(TODO_PART, TODO_ITEMS_IDX)
        .await
        .map_err(ApiError::internal)?;
    parse_todo_items(raw.as_deref())
}

pub async fn save_todos(items: Vec<Value>) -> AppResult<Vec<Value>> {
    validate_items(&items)?;
    put_value(
        TODO_PART,
        TODO_ITEMS_IDX,
        serde_json::to_string(&items)
            .map_err(|e| ApiError::internal(format!("failed to serialize todos: {e}")))?,
    )
    .await
    .map_err(ApiError::internal)?;
    Ok(items)
}

pub async fn get_meta() -> AppResult<TodoMeta> {
    let legacy_raw = get_value(TODO_PART, TODO_META_IDX)
        .await
        .map_err(ApiError::internal)?;
    Ok(parse_meta(legacy_raw.as_deref()))
}

pub async fn save_meta(mut meta: TodoMeta) -> AppResult<TodoMeta> {
    meta = normalize_meta(meta);
    put_json(TODO_PART, TODO_META_IDX, &meta)
        .await
        .map_err(ApiError::internal)?;
    Ok(meta)
}

fn validate_items(items: &[Value]) -> AppResult<()> {
    if items.iter().any(|item| !item.is_object()) {
        return Err(ApiError::bad_request("todos must be an array of objects"));
    }
    Ok(())
}

pub fn parse_todo_items(raw: Option<&str>) -> AppResult<Vec<Value>> {
    match raw {
        None | Some("") => Ok(Vec::new()),
        Some(raw) => {
            let value: Value = serde_json::from_str(raw)
                .map_err(|_| ApiError::bad_request("stored todos are not valid JSON"))?;
            if let Some(items) = value.as_array() {
                return Ok(items.clone());
            }
            if let Some(items) = value.get("data").and_then(Value::as_array) {
                return Ok(items.clone());
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
    use super::{parse_meta, parse_todo_items};

    #[test]
    fn parses_legacy_todo_array() {
        let items = parse_todo_items(Some(r#"[{"id":1},{"id":2}]"#)).unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn fills_default_meta() {
        let meta = parse_meta(None);
        assert_eq!(meta.groups[0].name, "Default");
        assert_eq!(meta.priorities[0].name, "High");
    }
}
