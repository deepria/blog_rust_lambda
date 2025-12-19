use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use std::collections::HashMap;

const TABLE_NAME: &str = "blog_deepria_master";

pub async fn dynamodb_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    Client::new(&config)
}

pub async fn get_item_value(
    part: String,
    idx: String,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let output = client
        .query()
        .table_name(TABLE_NAME)
        .key_condition_expression("part = :part AND idx = :idx")
        .expression_attribute_values(":part", AttributeValue::S(part))
        .expression_attribute_values(":idx", AttributeValue::S(idx))
        .send()
        .await?;

    let items_opt = output.items;
    let first_item = match items_opt.and_then(|mut items| items.pop()) {
        Some(item) => item,
        None => return Ok(None),
    };
    let value = match first_item.get("value") {
        Some(AttributeValue::S(s)) => Some(s.clone()),
        _ => None,
    };
    Ok(value)
}

pub async fn put_item(
    part: String,
    idx: String,
    value: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut item = HashMap::new();
    item.insert("part".to_string(), AttributeValue::S(part));
    item.insert("idx".to_string(), AttributeValue::S(idx));
    item.insert("value".to_string(), AttributeValue::S(value));

    client
        .put_item()
        .table_name(TABLE_NAME)
        .set_item(Some(item))
        .send()
        .await?;

    Ok(())
}

pub async fn delete_item(
    part: String,
    idx: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("part".to_string(), AttributeValue::S(part));
    key.insert("idx".to_string(), AttributeValue::S(idx));

    client
        .delete_item()
        .table_name(TABLE_NAME)
        .set_key(Some(key))
        .send()
        .await?;

    Ok(())
}
