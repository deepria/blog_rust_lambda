use crate::config::get_config;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use std::collections::HashMap;
use tokio::sync::OnceCell;

static DYNAMODB_CLIENT: OnceCell<Client> = OnceCell::const_new();

async fn dynamodb_client() -> &'static Client {
    DYNAMODB_CLIENT
        .get_or_init(|| async {
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            Client::new(&config)
        })
        .await
}

pub async fn get_value(
    part: &str,
    idx: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("part".to_string(), AttributeValue::S(part.to_string()));
    key.insert("idx".to_string(), AttributeValue::S(idx.to_string()));

    let output = client
        .get_item()
        .table_name(&get_config().dynamodb_table)
        .set_key(Some(key))
        .send()
        .await?;

    let value = output
        .item
        .and_then(|item| item.get("value").cloned())
        .and_then(|attr| match attr {
            AttributeValue::S(value) => Some(value),
            _ => None,
        });

    Ok(value)
}

pub async fn get_json<T: serde::de::DeserializeOwned>(
    part: &str,
    idx: &str,
) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>> {
    let value = get_value(part, idx).await?;
    value
        .map(|raw| serde_json::from_str(&raw))
        .transpose()
        .map_err(|e| e.into())
}

pub async fn query_json_prefix<T: serde::de::DeserializeOwned>(
    part: &str,
    idx_prefix: &str,
) -> Result<Vec<T>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;
    let mut items = Vec::new();
    let mut start_key = None;

    loop {
        let output = client
            .query()
            .table_name(&get_config().dynamodb_table)
            .key_condition_expression("#part = :part AND begins_with(#idx, :prefix)")
            .expression_attribute_names("#part", "part")
            .expression_attribute_names("#idx", "idx")
            .expression_attribute_values(":part", AttributeValue::S(part.to_string()))
            .expression_attribute_values(":prefix", AttributeValue::S(idx_prefix.to_string()))
            .set_exclusive_start_key(start_key)
            .send()
            .await?;

        for item in output.items.unwrap_or_default() {
            let Some(AttributeValue::S(raw)) = item.get("value") else {
                continue;
            };
            items.push(serde_json::from_str(raw)?);
        }

        start_key = output.last_evaluated_key;
        if start_key.as_ref().is_none_or(HashMap::is_empty) {
            break;
        }
    }

    Ok(items)
}

pub async fn put_value(
    part: &str,
    idx: &str,
    value: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_value_with_ttl(part, idx, value, None).await
}

pub async fn put_value_with_ttl(
    part: &str,
    idx: &str,
    value: String,
    ttl: Option<i64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut item = HashMap::new();
    item.insert("part".to_string(), AttributeValue::S(part.to_string()));
    item.insert("idx".to_string(), AttributeValue::S(idx.to_string()));
    item.insert("value".to_string(), AttributeValue::S(value));
    if let Some(ttl) = ttl {
        item.insert("ttl".to_string(), AttributeValue::N(ttl.to_string()));
    }

    client
        .put_item()
        .table_name(&get_config().dynamodb_table)
        .set_item(Some(item))
        .send()
        .await?;

    Ok(())
}

pub async fn put_value_if_absent(
    part: &str,
    idx: &str,
    value: String,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut item = HashMap::new();
    item.insert("part".to_string(), AttributeValue::S(part.to_string()));
    item.insert("idx".to_string(), AttributeValue::S(idx.to_string()));
    item.insert("value".to_string(), AttributeValue::S(value));

    let result = client
        .put_item()
        .table_name(&get_config().dynamodb_table)
        .set_item(Some(item))
        .condition_expression("attribute_not_exists(#part) AND attribute_not_exists(#idx)")
        .expression_attribute_names("#part", "part")
        .expression_attribute_names("#idx", "idx")
        .send()
        .await;

    match result {
        Ok(_) => Ok(true),
        Err(error) => {
            let text = error.to_string();
            if text.contains("ConditionalCheckFailed") {
                Ok(false)
            } else {
                Err(error.into())
            }
        }
    }
}

pub async fn put_json_if_absent<T: serde::Serialize>(
    part: &str,
    idx: &str,
    value: &T,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    put_value_if_absent(part, idx, serde_json::to_string(value)?).await
}

pub async fn put_json<T: serde::Serialize>(
    part: &str,
    idx: &str,
    value: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_value(part, idx, serde_json::to_string(value)?).await
}

pub async fn put_json_with_ttl<T: serde::Serialize>(
    part: &str,
    idx: &str,
    value: &T,
    ttl: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_value_with_ttl(part, idx, serde_json::to_string(value)?, Some(ttl)).await
}

pub async fn delete_value(
    part: &str,
    idx: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("part".to_string(), AttributeValue::S(part.to_string()));
    key.insert("idx".to_string(), AttributeValue::S(idx.to_string()));

    client
        .delete_item()
        .table_name(&get_config().dynamodb_table)
        .set_key(Some(key))
        .send()
        .await?;

    Ok(())
}
