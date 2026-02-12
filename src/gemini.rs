use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;

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
        // Write file if it doesn't exist
        if !Path::new(path).exists() {
            if let Err(e) = fs::write(path, &json_content) {
                tracing::error!("Failed to write GCP credentials to /tmp: {}", e);
                return;
            }
        }
        // Set the environment variable so gcp_auth can find it
        // SAFETY: This is safe as long as we don't have other threads reading/setting this env var concurrently during init.
        // In Lambda, this is typically fine as it's sequential per invocation or initialized at start.
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

    let url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent";

    let request_body = GeminiRequest {
        contents: vec![Content {
            parts: vec![Part {
                text: prompt.to_string(),
            }],
        }],
    };

    let client = reqwest::Client::new();
    let res = client
        .post(url)
        .bearer_auth(token.as_str())
        .header("x-goog-user-project", "deepria") // Optional, can be ignored or fetched if needed
        .json(&request_body)
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

    let text = gemini_res
        .candidates
        .and_then(|c| c.into_iter().next())
        .and_then(|c| c.content)
        .and_then(|c| c.parts)
        .and_then(|p| p.into_iter().next())
        .and_then(|p| p.text)
        .unwrap_or_else(|| "No response generated".to_string());

    Ok(text)
}
