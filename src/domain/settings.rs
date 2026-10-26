use crate::api::{ApiError, AppResult};
use crate::dynamodb::{get_json, put_json};
use serde::{Deserialize, Serialize};

const SETTINGS_PART: &str = "USER_SETTINGS";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settings {
    #[serde(default = "gallery_enabled_by_default")]
    pub dashboard_gallery_enabled: bool,
}

fn gallery_enabled_by_default() -> bool {
    true
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            dashboard_gallery_enabled: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSettingsRequest {
    pub dashboard_gallery_enabled: bool,
}

pub async fn get_settings(user_id: &str) -> AppResult<Settings> {
    Ok(get_json(SETTINGS_PART, &settings_idx(user_id))
        .await
        .map_err(ApiError::internal)?
        .unwrap_or_default())
}

pub async fn save_settings(user_id: &str, payload: UpdateSettingsRequest) -> AppResult<Settings> {
    let settings = Settings {
        dashboard_gallery_enabled: payload.dashboard_gallery_enabled,
    };
    put_json(SETTINGS_PART, &settings_idx(user_id), &settings)
        .await
        .map_err(ApiError::internal)?;
    Ok(settings)
}

fn settings_idx(user_id: &str) -> String {
    format!("user:{user_id}")
}

#[cfg(test)]
mod tests {
    use super::{settings_idx, Settings, UpdateSettingsRequest};

    #[test]
    fn existing_users_keep_gallery_enabled() {
        assert!(Settings::default().dashboard_gallery_enabled);
        let settings: Settings = serde_json::from_str("{}").unwrap();
        assert!(settings.dashboard_gallery_enabled);
    }

    #[test]
    fn false_is_preserved_when_reading_saved_settings() {
        let settings: Settings =
            serde_json::from_str(r#"{"dashboard_gallery_enabled":false}"#).unwrap();
        assert!(!settings.dashboard_gallery_enabled);
    }

    #[test]
    fn updates_require_an_explicit_boolean() {
        for raw in [
            "{}",
            r#"{"dashboard_gallery_enabled":null}"#,
            r#"{"dashboard_gallery_enabled":"false"}"#,
            r#"{"dashboard_gallery_enabled":0}"#,
            r#"{"dashboard_gallery_enabled":true,"user_id":"other-user"}"#,
        ] {
            assert!(serde_json::from_str::<UpdateSettingsRequest>(raw).is_err());
        }
        for enabled in [true, false] {
            let raw = format!(r#"{{"dashboard_gallery_enabled":{enabled}}}"#);
            let update: UpdateSettingsRequest = serde_json::from_str(&raw).unwrap();
            assert_eq!(update.dashboard_gallery_enabled, enabled);
        }
    }

    #[test]
    fn settings_keys_are_scoped_to_the_authenticated_user() {
        assert_ne!(settings_idx("user-a"), settings_idx("user-b"));
    }
}
