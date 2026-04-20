use crate::api::{ApiError, AppResult};
use crate::gemini::generate_content;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ChatReply {
    pub reply: String,
}

pub async fn send_message(payload: ChatRequest) -> AppResult<ChatReply> {
    let message = payload.message.trim();
    if message.is_empty() {
        return Err(ApiError::bad_request("message is required"));
    }

    let reply = generate_content(message)
        .await
        .map_err(ApiError::internal)?;

    Ok(ChatReply { reply })
}
