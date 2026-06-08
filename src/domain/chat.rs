use crate::api::{ApiError, AppResult};
use crate::gemini::create_interaction;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub message: String,
    #[serde(rename = "previousInteractionId", alias = "previous_interaction_id")]
    pub previous_interaction_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatReply {
    pub reply: String,
    #[serde(rename = "interactionId")]
    pub interaction_id: String,
}

pub async fn send_message(payload: ChatRequest) -> AppResult<ChatReply> {
    let message = payload.message.trim();
    if message.is_empty() {
        return Err(ApiError::bad_request("message is required"));
    }

    let interaction = create_interaction(message, payload.previous_interaction_id.as_deref())
        .await
        .map_err(ApiError::internal)?;

    Ok(ChatReply {
        reply: interaction.reply,
        interaction_id: interaction.interaction_id,
    })
}
