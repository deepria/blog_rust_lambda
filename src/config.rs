use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub dynamodb_table: String,
    pub s3_bucket: String,
    pub s3_base_path: String,
    pub allowed_origins: Vec<String>,
    pub google_project_id: Option<String>,
    pub gemini_model: String,
}

static CONFIG: OnceLock<EnvConfig> = OnceLock::new();

pub fn get_config() -> &'static EnvConfig {
    CONFIG.get_or_init(EnvConfig::from_env)
}

impl EnvConfig {
    fn from_env() -> Self {
        let allowed_origins = std::env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "*".to_string())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        Self {
            dynamodb_table: std::env::var("DYNAMODB_TABLE")
                .unwrap_or_else(|_| "blog_deepria_master".to_string()),
            s3_bucket: std::env::var("S3_BUCKET").unwrap_or_default(),
            s3_base_path: normalize_base_path(
                &std::env::var("S3_BASE_PATH").unwrap_or_else(|_| "upload".to_string()),
            ),
            allowed_origins,
            google_project_id: std::env::var("GOOGLE_PROJECT_ID").ok(),
            gemini_model: std::env::var("GEMINI_MODEL")
                .unwrap_or_else(|_| "gemini-3-flash-preview".to_string()),
        }
    }

    pub fn resolve_origin(&self, request_origin: Option<&str>) -> String {
        if self.allowed_origins.iter().any(|origin| origin == "*") {
            return request_origin.unwrap_or("*").to_string();
        }

        match request_origin {
            Some(origin) if self.allowed_origins.iter().any(|allowed| allowed == origin) => {
                origin.to_string()
            }
            _ => self
                .allowed_origins
                .first()
                .cloned()
                .unwrap_or_else(|| "http://localhost:5173".to_string()),
        }
    }
}

fn normalize_base_path(value: &str) -> String {
    value.trim().trim_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::normalize_base_path;

    #[test]
    fn normalizes_base_path() {
        assert_eq!(normalize_base_path("/upload/"), "upload");
        assert_eq!(normalize_base_path("upload"), "upload");
        assert_eq!(normalize_base_path("nested/path/"), "nested/path");
    }
}
