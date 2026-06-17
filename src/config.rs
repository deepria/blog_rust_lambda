use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub dynamodb_table: String,
    pub s3_bucket: String,
    pub s3_base_path: String,
    pub allowed_origins: Vec<String>,
    pub google_project_id: Option<String>,
    pub gemini_model: String,
    pub auth_base_url: String,
    pub auth_frontend_url: String,
    pub auth_allowed_frontend_urls: Vec<String>,
    pub auth_mobile_deep_link: String,
    pub auth_cookie_secure: bool,
    pub auth_cookie_same_site: String,
    pub auth_allowlist_enabled: bool,
    pub google_oauth_client_id: Option<String>,
    pub google_oauth_client_secret: Option<String>,
    pub github_oauth_client_id: Option<String>,
    pub github_oauth_client_secret: Option<String>,
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

        let auth_frontend_url = std::env::var("AUTH_FRONTEND_URL")
            .or_else(|_| std::env::var("FRONTEND_URL"))
            .unwrap_or_else(|_| "http://localhost:5173".to_string())
            .trim_end_matches('/')
            .to_string();

        let auth_allowed_frontend_urls = std::env::var("AUTH_ALLOWED_FRONTEND_URLS")
            .ok()
            .map(|value| parse_url_list(&value))
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![auth_frontend_url.clone()]);

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
            auth_base_url: std::env::var("AUTH_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string())
                .trim_end_matches('/')
                .to_string(),
            auth_frontend_url,
            auth_allowed_frontend_urls,
            auth_mobile_deep_link: std::env::var("AUTH_MOBILE_DEEP_LINK")
                .unwrap_or_else(|_| "deepria://auth/callback".to_string())
                .trim_end_matches('?')
                .to_string(),
            auth_cookie_secure: env_bool("AUTH_COOKIE_SECURE", true),
            auth_cookie_same_site: std::env::var("AUTH_COOKIE_SAMESITE")
                .ok()
                .and_then(|value| normalize_same_site(&value))
                .unwrap_or_else(|| "Lax".to_string()),
            auth_allowlist_enabled: env_bool("AUTH_ALLOWLIST_ENABLED", false),
            google_oauth_client_id: std::env::var("GOOGLE_OAUTH_CLIENT_ID").ok(),
            google_oauth_client_secret: std::env::var("GOOGLE_OAUTH_CLIENT_SECRET").ok(),
            github_oauth_client_id: std::env::var("GITHUB_OAUTH_CLIENT_ID").ok(),
            github_oauth_client_secret: std::env::var("GITHUB_OAUTH_CLIENT_SECRET").ok(),
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

fn parse_url_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_end_matches('/').to_string())
        .collect()
}

fn normalize_same_site(value: &str) -> Option<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "lax" => Some("Lax".to_string()),
        "strict" => Some("Strict".to_string()),
        "none" => Some("None".to_string()),
        _ => None,
    }
}

fn normalize_base_path(value: &str) -> String {
    value.trim().trim_matches('/').to_string()
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{normalize_base_path, normalize_same_site, parse_url_list};

    #[test]
    fn normalizes_base_path() {
        assert_eq!(normalize_base_path("/upload/"), "upload");
        assert_eq!(normalize_base_path("upload"), "upload");
        assert_eq!(normalize_base_path("nested/path/"), "nested/path");
    }

    #[test]
    fn parses_url_list() {
        assert_eq!(
            parse_url_list("https://deepria.cloud/, http://localhost:5173 "),
            vec!["https://deepria.cloud", "http://localhost:5173"]
        );
    }

    #[test]
    fn normalizes_same_site() {
        assert_eq!(normalize_same_site("none").as_deref(), Some("None"));
        assert_eq!(normalize_same_site("LAX").as_deref(), Some("Lax"));
        assert_eq!(normalize_same_site("invalid"), None);
    }
}
