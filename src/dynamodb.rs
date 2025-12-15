use aws_sdk_dynamodb::{types::AttributeValue, Client};
use aws_config::BehaviorVersion;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEntity {
    pub id: String,
    pub value: Option<String>,
}

pub async fn dynamodb_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    Client::new(&config)
}

pub async fn get_item_value(
    table_name: &str,
    part: String,
    index: String,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("part".to_string(), AttributeValue::S(part));
    key.insert("index".to_string(), AttributeValue::S(index));

    let output = client
        .get_item()
        .table_name(table_name)
        .set_key(Some(key))
        .send()
        .await?;

    let item = match output.item {
        Some(item) => item,
        None => return Ok(None),
    };

    let value = match item.get("value") {
        Some(AttributeValue::S(s)) => Some(s.clone()),
        _ => None,
    };

    Ok(value)
}
pub async fn put_item(
    table_name: &str,
    part: String,
    index: String,
    pk: String,
    value: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut item = HashMap::new();
    item.insert("part".to_string(), AttributeValue::S(part));
    item.insert("index".to_string(), AttributeValue::S(index));
    item.insert("pk".to_string(), AttributeValue::S(pk));
    item.insert("value".to_string(), AttributeValue::S(value));

    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item))
        .send()
        .await?;

    Ok(())
}

pub async fn delete_item(
    table_name: &str,
    part: String,
    index: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("part".to_string(), AttributeValue::S(part));
    key.insert("index".to_string(), AttributeValue::S(index));

    client
        .delete_item()
        .table_name(table_name)
        .set_key(Some(key))
        .send()
        .await?;

    Ok(())
}

pub async fn scan_test_entities(
    table_name: &str,
) -> Result<Vec<TestEntity>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let output = client
        .scan()
        .table_name(table_name)
        .send()
        .await?;

    let mut results = Vec::new();

    if let Some(items) = output.items {
        for item in items {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => continue,
            };

            let value = match item.get("value") {
                Some(AttributeValue::S(s)) => Some(s.clone()),
                _ => None,
            };

            results.push(TestEntity { id, value });
        }
    }

    Ok(results)
}

pub async fn query_test_entities_by_id(
    table_name: &str,
    id: String,
) -> Result<Vec<TestEntity>, Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let output = client
        .query()
        .table_name(table_name)
        .key_condition_expression("id = :id")
        .expression_attribute_values(":id", AttributeValue::S(id))
        .send()
        .await?;

    let mut results = Vec::new();

    if let Some(items) = output.items {
        for item in items {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => continue,
            };

            let value = match item.get("value") {
                Some(AttributeValue::S(s)) => Some(s.clone()),
                _ => None,
            };

            results.push(TestEntity { id, value });
        }
    }

    Ok(results)
}

pub async fn upsert_test_entity(
    table_name: &str,
    id: String,
    value: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S(id));

    if let Some(v) = value {
        item.insert("value".to_string(), AttributeValue::S(v));
    }

    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item))
        .send()
        .await?;

    Ok(())
}

pub async fn delete_test_entity_by_id(
    table_name: &str,
    id: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = dynamodb_client().await;

    let mut key = HashMap::new();
    key.insert("id".to_string(), AttributeValue::S(id));

    client
        .delete_item()
        .table_name(table_name)
        .set_key(Some(key))
        .send()
        .await?;

    Ok(())
}
