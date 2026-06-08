use crate::config::get_config;
use serde::{Deserialize, Serialize};
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

#[derive(Deserialize, Debug)]
struct InteractionResponse {
    id: Option<String>,
    outputs: Option<Vec<InteractionOutput>>,
}

#[derive(Deserialize, Debug)]
struct InteractionOutput {
    #[serde(rename = "type")]
    output_type: Option<String>,
    text: Option<String>,
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

    let interaction_res: InteractionResponse = res
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    let interaction_id = interaction_res
        .id
        .clone()
        .ok_or_else(|| "Interaction response did not include an id".to_string())?;
    let reply =
        extract_text(&interaction_res).unwrap_or_else(|| "No response generated".to_string());

    Ok(InteractionReply {
        reply,
        interaction_id,
    })
}

fn extract_text(response: &InteractionResponse) -> Option<String> {
    response
        .outputs
        .as_ref()
        .and_then(|outputs| {
            outputs
                .iter()
                .rev()
                .find(|output| output.output_type.as_deref().unwrap_or("text") == "text")
        })
        .and_then(|output| output.text.clone())
}

#[cfg(test)]
mod tests {
    use super::{extract_text, InteractionOutput, InteractionResponse};

    #[test]
    fn extracts_text_from_response() {
        let response = InteractionResponse {
            id: Some("interaction-1".to_string()),
            outputs: Some(vec![InteractionOutput {
                output_type: Some("text".to_string()),
                text: Some("hello".to_string()),
            }]),
        };

        assert_eq!(extract_text(&response).as_deref(), Some("hello"));
    }

    #[test]
    fn extracts_last_text_output() {
        let response = InteractionResponse {
            id: Some("interaction-1".to_string()),
            outputs: Some(vec![
                InteractionOutput {
                    output_type: Some("text".to_string()),
                    text: Some("first".to_string()),
                },
                InteractionOutput {
                    output_type: Some("image".to_string()),
                    text: None,
                },
                InteractionOutput {
                    output_type: Some("text".to_string()),
                    text: Some("last".to_string()),
                },
            ]),
        };

        assert_eq!(extract_text(&response).as_deref(), Some("last"));
    }
}
