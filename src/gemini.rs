use crate::config::get_config;
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::sync::OnceLock;

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[derive(Serialize, Debug)]
struct InteractionRequest {
    model: String,
    input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_interaction_id: Option<String>,
}

#[derive(Debug)]
pub struct InteractionReply {
    pub reply: String,
    pub interaction_id: String,
}

fn init_credentials() {
    if env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok() {
        return;
    }

    if let Ok(json_content) = env::var("GOOGLE_CREDENTIALS_JSON") {
        let path = "/tmp/gcp_sa_key.json";
        let should_write = match fs::read_to_string(path) {
            Ok(existing) => existing != json_content,
            Err(_) => true,
        };
        if should_write {
            if let Err(e) = fs::write(path, &json_content) {
                tracing::error!("Failed to write GCP credentials to /tmp: {}", e);
                return;
            }
        }
        env::set_var("GOOGLE_APPLICATION_CREDENTIALS", path);
        tracing::info!("Initialized GOOGLE_APPLICATION_CREDENTIALS from JSON env var");
    }
}

pub async fn create_interaction(
    input: &str,
    previous_interaction_id: Option<&str>,
) -> Result<InteractionReply, String> {
    init_credentials();

    let authentication_manager = gcp_auth::provider()
        .await
        .map_err(|e| format!("Failed to create AuthenticationManager: {}", e))?;

    let scopes = &[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/generative-language",
    ];
    let token = authentication_manager
        .token(scopes)
        .await
        .map_err(|e| format!("Failed to get token: {}", e))?;

    let url = "https://generativelanguage.googleapis.com/v1beta/interactions";

    let request_body = InteractionRequest {
        model: get_config().gemini_model.clone(),
        input: input.to_string(),
        previous_interaction_id: previous_interaction_id.map(ToOwned::to_owned),
    };

    let client = HTTP_CLIENT.get_or_init(reqwest::Client::new);
    let mut request = client
        .post(url)
        .bearer_auth(token.as_str())
        .json(&request_body);

    if let Some(project_id) = &get_config().google_project_id {
        request = request.header("x-goog-user-project", project_id);
    }

    let res = request
        .send()
        .await
        .map_err(|e| format!("Failed to send request: {}", e))?;

    if !res.status().is_success() {
        let status = res.status();
        let text = res
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(format!("API Error {}: {}", status, text));
    }

    let interaction_res: Value = res
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    let interaction_id = interaction_res
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "Interaction response did not include an id".to_string())?;
    let reply = extract_text(&interaction_res).ok_or_else(|| {
        format!(
            "Interaction response did not include text output: {}",
            summarize_response(&interaction_res)
        )
    })?;

    Ok(InteractionReply {
        reply,
        interaction_id,
    })
}

fn extract_text(response: &Value) -> Option<String> {
    response
        .get("outputs")
        .and_then(Value::as_array)
        .and_then(|outputs| outputs.iter().rev().find_map(extract_text_from_output))
}

fn extract_text_from_output(output: &Value) -> Option<String> {
    let output_type = output
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("text")
        .to_ascii_lowercase();

    if output_type == "text" {
        if let Some(text) = output.get("text").and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return Some(text.to_string());
            }
        }
    }

    output
        .get("content")
        .and_then(Value::as_array)
        .and_then(|content| content.iter().rev().find_map(extract_text_from_output))
}

fn summarize_response(response: &Value) -> String {
    let status = response
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let output_types = response
        .get("outputs")
        .and_then(Value::as_array)
        .map(|outputs| {
            outputs
                .iter()
                .filter_map(|output| output.get("type").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|types| !types.is_empty())
        .unwrap_or_else(|| "none".to_string());

    format!("status={status}, output_types={output_types}")
}

#[cfg(test)]
mod tests {
    use super::{extract_text, summarize_response};
    use serde_json::json;

    #[test]
    fn extracts_text_from_response() {
        let response = json!({
            "id": "interaction-1",
            "outputs": [{ "type": "text", "text": "hello" }]
        });

        assert_eq!(extract_text(&response).as_deref(), Some("hello"));
    }

    #[test]
    fn extracts_last_text_output() {
        let response = json!({
            "id": "interaction-1",
            "outputs": [
                { "type": "text", "text": "first" },
                { "type": "image" },
                { "type": "text", "text": "last" }
            ]
        });

        assert_eq!(extract_text(&response).as_deref(), Some("last"));
    }

    #[test]
    fn extracts_uppercase_text_output() {
        let response = json!({
            "id": "interaction-1",
            "outputs": [{ "type": "TEXT", "text": "hello" }]
        });

        assert_eq!(extract_text(&response).as_deref(), Some("hello"));
    }

    #[test]
    fn extracts_nested_content_text_output() {
        let response = json!({
            "id": "interaction-1",
            "outputs": [
                {
                    "type": "message",
                    "content": [{ "type": "text", "text": "nested" }]
                }
            ]
        });

        assert_eq!(extract_text(&response).as_deref(), Some("nested"));
    }

    #[test]
    fn summarizes_response_without_text() {
        let response = json!({
            "status": "completed",
            "outputs": [{ "type": "function_call" }]
        });

        assert_eq!(
            summarize_response(&response),
            "status=completed, output_types=function_call"
        );
    }
}
