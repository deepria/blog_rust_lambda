use crate::config::get_config;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::sync::OnceLock;

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[derive(Serialize, Debug)]
struct GeminiRequest {
    contents: Vec<Content>,
}

#[derive(Serialize, Debug)]
struct Content {
    parts: Vec<Part>,
}

#[derive(Serialize, Debug)]
struct Part {
    text: String,
}

#[derive(Deserialize, Debug)]
struct GeminiResponse {
    candidates: Option<Vec<Candidate>>,
}

#[derive(Deserialize, Debug)]
struct Candidate {
    content: Option<CandidateContent>,
}

#[derive(Deserialize, Debug)]
struct CandidateContent {
    parts: Option<Vec<CandidatePart>>,
}

#[derive(Deserialize, Debug)]
struct CandidatePart {
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

pub async fn generate_content(prompt: &str) -> Result<String, String> {
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

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
        get_config().gemini_model
    );

    let request_body = GeminiRequest {
        contents: vec![Content {
            parts: vec![Part {
                text: prompt.to_string(),
            }],
        }],
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

    let gemini_res: GeminiResponse = res
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    let text = extract_text(gemini_res).unwrap_or_else(|| "No response generated".to_string());

    Ok(text)
}

fn extract_text(response: GeminiResponse) -> Option<String> {
    response
        .candidates
        .and_then(|c| c.into_iter().next())
        .and_then(|c| c.content)
        .and_then(|c| c.parts)
        .and_then(|p| p.into_iter().next())
        .and_then(|p| p.text)
}

#[cfg(test)]
mod tests {
    use super::{extract_text, Candidate, CandidateContent, CandidatePart, GeminiResponse};

    #[test]
    fn extracts_text_from_response() {
        let response = GeminiResponse {
            candidates: Some(vec![Candidate {
                content: Some(CandidateContent {
                    parts: Some(vec![CandidatePart {
                        text: Some("hello".to_string()),
                    }]),
                }),
            }]),
        };

        assert_eq!(extract_text(response).as_deref(), Some("hello"));
    }
}
