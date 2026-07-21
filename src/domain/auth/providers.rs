use super::AuthProvider;
use crate::api::{ApiError, AppResult};
use crate::config::get_config;

pub(super) fn authorization_url(
    provider: &AuthProvider,
    nonce: &str,
    redirect_uri: &str,
) -> AppResult<String> {
    let config = get_config();
    let (client_id, configured) = match provider {
        AuthProvider::Google => (
            config.google_oauth_client_id.as_deref(),
            config.google_oauth_client_secret.is_some(),
        ),
        AuthProvider::Github => (
            config.github_oauth_client_id.as_deref(),
            config.github_oauth_client_secret.is_some(),
        ),
    };
    let client_id = client_id.filter(|_| configured).ok_or_else(|| {
        ApiError::bad_request(format!("{} OAuth is not configured", provider.as_str()))
    })?;
    let url = match provider {
        AuthProvider::Google => format!(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&access_type=offline&prompt=select_account",
            encode(client_id), encode(redirect_uri), encode("openid email profile"), encode(nonce)
        ),
        AuthProvider::Github => format!(
            "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope={}&state={}",
            encode(client_id), encode(redirect_uri), encode("read:user user:email"), encode(nonce)
        ),
    };
    Ok(url)
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
