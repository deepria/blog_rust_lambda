use crate::api::{ApiError, AppResult};
use crate::config::get_config;
use crate::dynamodb::{
    delete_value, get_json, get_value, put_json, put_json_if_absent, put_value_if_absent,
};
use chrono::{DateTime, Duration, Utc};
use lambda_http::Request;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub mod cookies;
mod providers;
mod sessions;

const USER_PART: &str = "USER";
const USER_EMAIL_PART: &str = "USER_EMAIL";
const OAUTH_ACCOUNT_PART: &str = "OAUTH_ACCOUNT";
const USER_OAUTH_PART: &str = "USER_OAUTH";
const SESSION_PART: &str = "SESSION";
const ALLOWED_EMAIL_PART: &str = "ALLOWED_EMAIL";
const MOBILE_LOGIN_CODE_PART: &str = "MOBILE_LOGIN_CODE";
const MOBILE_LOGIN_CODE_CLAIM_PART: &str = "MOBILE_LOGIN_CODE_CLAIM";
const ACCESS_COOKIE: &str = "dc_session";
const REFRESH_COOKIE: &str = "dc_refresh";
const OAUTH_STATE_COOKIE: &str = "dc_oauth_state";
const SESSION_DAYS: i64 = 30;
const REFRESH_DAYS: i64 = 90;
const STATE_MINUTES: i64 = 10;
const MOBILE_CODE_MINUTES: i64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthProvider {
    Google,
    Github,
}

impl AuthProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Google => "google",
            Self::Github => "github",
        }
    }

    pub fn parse(value: &str) -> AppResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "google" => Ok(Self::Google),
            "github" => Ok(Self::Github),
            _ => Err(ApiError::bad_request("unsupported OAuth provider")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    Owner,
    Admin,
    Member,
    Viewer,
}

impl UserRole {
    fn default_member() -> Self {
        Self::Member
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus {
    Active,
    Suspended,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub name: String,
    pub avatar_url: Option<String>,
    pub role: UserRole,
    pub status: UserStatus,
    pub created_at: String,
    pub updated_at: String,
    pub last_login_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthAccount {
    pub id: String,
    pub user_id: String,
    pub provider: AuthProvider,
    pub provider_account_id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub user_id: String,
    pub session_token: String,
    pub refresh_token: String,
    pub expires_at: String,
    pub refresh_expires_at: String,
    pub last_used_at: String,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowedEmail {
    pub id: String,
    pub email: String,
    pub role: UserRole,
    pub created_at: String,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthUser {
    pub id: String,
    pub email: String,
    pub name: String,
    pub avatar_url: Option<String>,
    pub role: UserRole,
    pub status: UserStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthSession {
    pub user: AuthUser,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CookieBundle {
    pub session_token: String,
    pub refresh_token: String,
}

#[derive(Debug, Clone)]
struct ProviderProfile {
    provider: AuthProvider,
    provider_account_id: String,
    email: Option<String>,
    email_verified: bool,
    name: String,
    avatar_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OAuthState {
    provider: String,
    nonce: String,
    link: bool,
    mobile: bool,
    return_to: String,
    exp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MobileLoginCode {
    id: String,
    user_id: String,
    expires_at: String,
    created_at: String,
    used_at: Option<String>,
    ip_address: Option<String>,
    user_agent: Option<String>,
}

pub enum OAuthCallbackResult {
    Web {
        bundle: CookieBundle,
        return_to: String,
    },
    Mobile {
        redirect_to: String,
    },
}

pub fn oauth_authorize_url(
    provider: AuthProvider,
    link: bool,
    return_to: Option<String>,
) -> AppResult<(String, String)> {
    oauth_authorize_url_with_mode(provider, link, false, return_to)
}

pub fn mobile_oauth_authorize_url(provider: AuthProvider) -> AppResult<(String, String)> {
    oauth_authorize_url_with_mode(provider, false, true, None)
}

fn oauth_authorize_url_with_mode(
    provider: AuthProvider,
    link: bool,
    mobile: bool,
    return_to: Option<String>,
) -> AppResult<(String, String)> {
    let state = OAuthState {
        provider: provider.as_str().to_string(),
        nonce: Uuid::new_v4().to_string(),
        link,
        mobile,
        return_to: sanitize_return_to(return_to),
        exp: (Utc::now() + Duration::minutes(STATE_MINUTES)).timestamp(),
    };
    let state_cookie = encode_state(&state)?;
    let redirect_uri = redirect_uri(&provider);
    let url = providers::authorization_url(&provider, &state.nonce, &redirect_uri)?;
    Ok((url, cookies::oauth_state_cookie(&state_cookie)))
}

pub async fn handle_oauth_callback(
    req: &Request,
    provider: AuthProvider,
    code: String,
    nonce: String,
) -> AppResult<OAuthCallbackResult> {
    let state = read_state(req)?;
    if state.provider != provider.as_str()
        || state.nonce != nonce
        || state.exp < Utc::now().timestamp()
    {
        return Err(ApiError::unauthorized("invalid OAuth state"));
    }
    let profile = fetch_provider_profile(&provider, &code).await?;

    let user = if state.link {
        let current = require_user(req).await?;
        link_provider_to_user(&current.id, profile).await?
    } else {
        login_or_create_user(profile).await?
    };
    if state.mobile {
        let code = create_mobile_login_code(req, &user.id).await?;
        return Ok(OAuthCallbackResult::Mobile {
            redirect_to: mobile_deep_link(&code),
        });
    }
    let bundle = sessions::create(req, &user.id)
        .await
        .map_err(ApiError::internal)?;
    Ok(OAuthCallbackResult::Web {
        bundle,
        return_to: state.return_to,
    })
}

pub async fn complete_mobile_login(
    req: &Request,
    code: &str,
) -> AppResult<(AuthSession, CookieBundle)> {
    let code = code.trim();
    if code.is_empty() {
        return Err(ApiError::bad_request("mobile login code is required"));
    }
    let code_hash = hash_token(code);
    let claim_id = Uuid::new_v4().to_string();
    let claimed = put_value_if_absent(MOBILE_LOGIN_CODE_CLAIM_PART, &code_hash, claim_id)
        .await
        .map_err(ApiError::internal)?;
    if !claimed {
        return Err(ApiError::unauthorized("mobile login code was already used"));
    }

    let mut login_code: MobileLoginCode = get_json(MOBILE_LOGIN_CODE_PART, &code_hash)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::unauthorized("mobile login code not found"))?;

    if login_code.used_at.is_some() || parse_time(&login_code.expires_at)? <= Utc::now() {
        return Err(ApiError::unauthorized("mobile login code expired"));
    }

    login_code.used_at = Some(now_iso());
    put_json(MOBILE_LOGIN_CODE_PART, &code_hash, &login_code)
        .await
        .map_err(ApiError::internal)?;

    let user = load_user(&login_code.user_id).await?;
    assert_active_user(&user)?;
    let bundle = sessions::create(req, &user.id)
        .await
        .map_err(ApiError::internal)?;
    let auth = build_auth_session(&user).await?;
    Ok((auth, bundle))
}

pub async fn current_session(req: &Request) -> AppResult<AuthSession> {
    let user = require_user(req).await?;
    build_auth_session(&user).await
}

pub async fn require_user(req: &Request) -> AppResult<User> {
    let token = cookies::cookie_value(req, ACCESS_COOKIE)
        .ok_or_else(|| ApiError::unauthorized("login required"))?;
    let hash = hash_token(&token);
    let mut session: Session = get_json(SESSION_PART, &hash)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::unauthorized("session not found"))?;
    if session.revoked_at.is_some() || parse_time(&session.expires_at)? <= Utc::now() {
        return Err(ApiError::unauthorized("session expired"));
    }
    session.last_used_at = now_iso();
    sessions::put_access(&session)
        .await
        .map_err(ApiError::internal)?;
    let user = load_user(&session.user_id).await?;
    assert_active_user(&user)?;
    Ok(user)
}

pub async fn find_active_user_by_email(email: &str) -> AppResult<Option<User>> {
    let email = normalize_email(email);
    if email.is_empty() {
        return Err(ApiError::bad_request("email is required"));
    }
    let Some(user_id) = get_value(USER_EMAIL_PART, &email)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    let user = load_user(&user_id).await?;
    Ok((user.status == UserStatus::Active).then_some(user))
}

pub async fn find_active_user_by_id(user_id: &str) -> AppResult<Option<User>> {
    match get_json::<User>(USER_PART, user_id)
        .await
        .map_err(ApiError::internal)?
    {
        Some(user) if user.status == UserStatus::Active => Ok(Some(user)),
        _ => Ok(None),
    }
}

pub async fn refresh_session(req: &Request) -> AppResult<(AuthSession, CookieBundle)> {
    let token = cookies::cookie_value(req, REFRESH_COOKIE)
        .ok_or_else(|| ApiError::unauthorized("refresh token missing"))?;
    let hash = hash_token(&token);
    let mut session: Session = get_json(SESSION_PART, &hash)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::unauthorized("refresh session not found"))?;
    if session.revoked_at.is_some() || parse_time(&session.refresh_expires_at)? <= Utc::now() {
        return Err(ApiError::unauthorized("refresh token expired"));
    }
    session.revoked_at = Some(now_iso());
    sessions::revoke(&hash, &session)
        .await
        .map_err(ApiError::internal)?;
    sessions::revoke(&session.session_token, &session)
        .await
        .map_err(ApiError::internal)?;
    let user = load_user(&session.user_id).await?;
    assert_active_user(&user)?;
    let bundle = sessions::create(req, &user.id)
        .await
        .map_err(ApiError::internal)?;
    let auth = build_auth_session(&user).await?;
    Ok((auth, bundle))
}

pub async fn logout(req: &Request) -> AppResult<()> {
    for name in [ACCESS_COOKIE, REFRESH_COOKIE] {
        if let Some(token) = cookies::cookie_value(req, name) {
            if let Some(mut session) = get_json::<Session>(SESSION_PART, &hash_token(&token))
                .await
                .map_err(ApiError::internal)?
            {
                session.revoked_at = Some(now_iso());
                sessions::revoke(&hash_token(&token), &session)
                    .await
                    .map_err(ApiError::internal)?;
                sessions::revoke(&session.session_token, &session)
                    .await
                    .map_err(ApiError::internal)?;
                sessions::revoke(&session.refresh_token, &session)
                    .await
                    .map_err(ApiError::internal)?;
            }
        }
    }
    Ok(())
}

pub async fn disconnect_provider(user_id: &str, provider: AuthProvider) -> AppResult<AuthSession> {
    let providers = list_provider_keys(user_id).await?;
    if providers.len() <= 1 {
        return Err(ApiError::bad_request(
            "at least one login provider must remain",
        ));
    }
    let key = user_provider_key(user_id, &provider);
    let provider_account_key = get_value(USER_OAUTH_PART, &key)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("provider is not connected"))?;
    delete_value(USER_OAUTH_PART, &key)
        .await
        .map_err(ApiError::internal)?;
    delete_value(OAUTH_ACCOUNT_PART, &provider_account_key)
        .await
        .map_err(ApiError::internal)?;
    let user = load_user(user_id).await?;
    build_auth_session(&user).await
}

async fn login_or_create_user(profile: ProviderProfile) -> AppResult<User> {
    if let Some(existing) =
        load_oauth_account(&profile.provider, &profile.provider_account_id).await?
    {
        let mut user = load_user(&existing.user_id).await?;
        assert_active_user(&user)?;
        user.last_login_at = Some(now_iso());
        user.updated_at = now_iso();
        put_json(USER_PART, &user.id, &user)
            .await
            .map_err(ApiError::internal)?;
        return Ok(user);
    }

    if !profile.email_verified && profile.email.is_none() {
        return Err(ApiError::forbidden(
            "OAuth provider did not return a verified email",
        ));
    }
    let email = profile
        .email
        .clone()
        .ok_or_else(|| ApiError::forbidden("verified email is required"))?;
    let email_key = normalize_email(&email);
    let allowed_role = check_allowlist(&email_key).await?;

    if profile.email_verified {
        if let Some(user_id) = get_value(USER_EMAIL_PART, &email_key)
            .await
            .map_err(ApiError::internal)?
        {
            let user = load_user(&user_id).await?;
            return link_provider_to_user(&user.id, profile).await;
        }
    }

    let now = now_iso();
    let user = User {
        id: Uuid::new_v4().to_string(),
        email,
        name: profile.name.clone(),
        avatar_url: profile.avatar_url.clone(),
        role: allowed_role.unwrap_or_else(UserRole::default_member),
        status: UserStatus::Active,
        created_at: now.clone(),
        updated_at: now.clone(),
        last_login_at: Some(now),
    };
    put_json(USER_PART, &user.id, &user)
        .await
        .map_err(ApiError::internal)?;
    if profile.email_verified {
        let claimed = put_value_if_absent(USER_EMAIL_PART, &email_key, user.id.clone())
            .await
            .map_err(ApiError::internal)?;
        if !claimed {
            return Err(ApiError::forbidden(
                "email is already linked to another account",
            ));
        }
    }
    link_provider_to_user(&user.id, profile).await
}

async fn link_provider_to_user(user_id: &str, profile: ProviderProfile) -> AppResult<User> {
    let account_key = oauth_key(&profile.provider, &profile.provider_account_id);
    if let Some(existing) =
        load_oauth_account(&profile.provider, &profile.provider_account_id).await?
    {
        if existing.user_id != user_id {
            return Err(ApiError::forbidden(
                "provider account is already linked to another user",
            ));
        }
        return load_user(user_id).await;
    }
    let now = now_iso();
    let account = OAuthAccount {
        id: Uuid::new_v4().to_string(),
        user_id: user_id.to_string(),
        provider: profile.provider.clone(),
        provider_account_id: profile.provider_account_id.clone(),
        email: profile.email.clone(),
        email_verified: profile.email_verified,
        access_token: None,
        refresh_token: None,
        expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let claimed = put_json_if_absent(OAUTH_ACCOUNT_PART, &account_key, &account)
        .await
        .map_err(ApiError::internal)?;
    if !claimed {
        return Err(ApiError::forbidden("provider account is already linked"));
    }
    put_value_if_absent(
        USER_OAUTH_PART,
        &user_provider_key(user_id, &profile.provider),
        account_key,
    )
    .await
    .map_err(ApiError::internal)?;
    load_user(user_id).await
}

async fn create_mobile_login_code(req: &Request, user_id: &str) -> AppResult<String> {
    let code = Uuid::new_v4().to_string();
    let now = Utc::now();
    let item = MobileLoginCode {
        id: Uuid::new_v4().to_string(),
        user_id: user_id.to_string(),
        expires_at: (now + Duration::minutes(MOBILE_CODE_MINUTES)).to_rfc3339(),
        created_at: now.to_rfc3339(),
        used_at: None,
        ip_address: header(req, "x-forwarded-for").or_else(|| header(req, "x-real-ip")),
        user_agent: header(req, "user-agent"),
    };
    let code_hash = hash_token(&code);
    let stored = put_json_if_absent(MOBILE_LOGIN_CODE_PART, &code_hash, &item)
        .await
        .map_err(ApiError::internal)?;
    if !stored {
        return Err(ApiError::internal("failed to allocate mobile login code"));
    }
    Ok(code)
}

async fn fetch_provider_profile(provider: &AuthProvider, code: &str) -> AppResult<ProviderProfile> {
    match provider {
        AuthProvider::Google => fetch_google_profile(code).await,
        AuthProvider::Github => fetch_github_profile(code).await,
    }
}

async fn fetch_google_profile(code: &str) -> AppResult<ProviderProfile> {
    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
    }
    #[derive(Deserialize)]
    struct GoogleUser {
        sub: String,
        email: Option<String>,
        email_verified: Option<bool>,
        name: Option<String>,
        picture: Option<String>,
    }

    let cfg = get_config();
    let token: TokenResponse = reqwest::Client::new()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            (
                "client_id",
                cfg.google_oauth_client_id.as_deref().unwrap_or_default(),
            ),
            (
                "client_secret",
                cfg.google_oauth_client_secret
                    .as_deref()
                    .unwrap_or_default(),
            ),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", &redirect_uri(&AuthProvider::Google)),
        ])
        .send()
        .await
        .map_err(ApiError::internal)?
        .error_for_status()
        .map_err(ApiError::internal)?
        .json()
        .await
        .map_err(ApiError::internal)?;

    let user: GoogleUser = reqwest::Client::new()
        .get("https://openidconnect.googleapis.com/v1/userinfo")
        .bearer_auth(token.access_token)
        .send()
        .await
        .map_err(ApiError::internal)?
        .error_for_status()
        .map_err(ApiError::internal)?
        .json()
        .await
        .map_err(ApiError::internal)?;

    Ok(ProviderProfile {
        provider: AuthProvider::Google,
        provider_account_id: user.sub,
        email: user.email.clone(),
        email_verified: user.email_verified.unwrap_or(false),
        name: user
            .name
            .or(user.email)
            .unwrap_or_else(|| "Google user".to_string()),
        avatar_url: user.picture,
    })
}

async fn fetch_github_profile(code: &str) -> AppResult<ProviderProfile> {
    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
    }
    #[derive(Deserialize)]
    struct GithubUser {
        id: u64,
        name: Option<String>,
        login: String,
        avatar_url: Option<String>,
        email: Option<String>,
    }
    #[derive(Deserialize)]
    struct GithubEmail {
        email: String,
        primary: bool,
        verified: bool,
    }

    let cfg = get_config();
    let token: TokenResponse = reqwest::Client::new()
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .form(&[
            (
                "client_id",
                cfg.github_oauth_client_id.as_deref().unwrap_or_default(),
            ),
            (
                "client_secret",
                cfg.github_oauth_client_secret
                    .as_deref()
                    .unwrap_or_default(),
            ),
            ("code", code),
            ("redirect_uri", &redirect_uri(&AuthProvider::Github)),
        ])
        .send()
        .await
        .map_err(ApiError::internal)?
        .error_for_status()
        .map_err(ApiError::internal)?
        .json()
        .await
        .map_err(ApiError::internal)?;
    let client = reqwest::Client::new();
    let user: GithubUser = client
        .get("https://api.github.com/user")
        .header("User-Agent", "deepria-cloud-auth")
        .bearer_auth(&token.access_token)
        .send()
        .await
        .map_err(ApiError::internal)?
        .error_for_status()
        .map_err(ApiError::internal)?
        .json()
        .await
        .map_err(ApiError::internal)?;
    let emails: Vec<GithubEmail> = client
        .get("https://api.github.com/user/emails")
        .header("User-Agent", "deepria-cloud-auth")
        .bearer_auth(&token.access_token)
        .send()
        .await
        .map_err(ApiError::internal)?
        .error_for_status()
        .map_err(ApiError::internal)?
        .json()
        .await
        .map_err(ApiError::internal)?;
    let verified = emails
        .into_iter()
        .find(|item| item.primary && item.verified);
    Ok(ProviderProfile {
        provider: AuthProvider::Github,
        provider_account_id: user.id.to_string(),
        email: verified
            .as_ref()
            .map(|item| item.email.clone())
            .or(user.email),
        email_verified: verified.is_some(),
        name: user.name.unwrap_or(user.login),
        avatar_url: user.avatar_url,
    })
}

async fn check_allowlist(email: &str) -> AppResult<Option<UserRole>> {
    if !get_config().auth_allowlist_enabled {
        return Ok(None);
    }
    let allowed: AllowedEmail = get_json(ALLOWED_EMAIL_PART, email)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::forbidden("this email is not allowed to access the service"))?;
    Ok(Some(allowed.role))
}

async fn build_auth_session(user: &User) -> AppResult<AuthSession> {
    Ok(AuthSession {
        user: AuthUser {
            id: user.id.clone(),
            email: user.email.clone(),
            name: user.name.clone(),
            avatar_url: user.avatar_url.clone(),
            role: user.role.clone(),
            status: user.status.clone(),
        },
        providers: list_provider_keys(&user.id)
            .await?
            .into_iter()
            .filter_map(|key| key.split(':').next().map(ToOwned::to_owned))
            .collect(),
    })
}

async fn list_provider_keys(user_id: &str) -> AppResult<Vec<String>> {
    let mut keys = Vec::new();
    for provider in [AuthProvider::Google, AuthProvider::Github] {
        if let Some(key) = get_value(USER_OAUTH_PART, &user_provider_key(user_id, &provider))
            .await
            .map_err(ApiError::internal)?
        {
            keys.push(key);
        }
    }
    Ok(keys)
}

async fn load_user(user_id: &str) -> AppResult<User> {
    get_json(USER_PART, user_id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("user not found"))
}

async fn load_oauth_account(
    provider: &AuthProvider,
    provider_account_id: &str,
) -> AppResult<Option<OAuthAccount>> {
    get_json(
        OAUTH_ACCOUNT_PART,
        &oauth_key(provider, provider_account_id),
    )
    .await
    .map_err(ApiError::internal)
}

fn assert_active_user(user: &User) -> AppResult<()> {
    match user.status {
        UserStatus::Active => Ok(()),
        UserStatus::Suspended => Err(ApiError::forbidden("account is suspended")),
        UserStatus::Deleted => Err(ApiError::forbidden("account is deleted")),
    }
}

fn read_state(req: &Request) -> AppResult<OAuthState> {
    let raw = cookies::cookie_value(req, OAUTH_STATE_COOKIE)
        .ok_or_else(|| ApiError::unauthorized("OAuth state cookie missing"))?;
    decode_state(&raw)
}

fn encode_state(state: &OAuthState) -> AppResult<String> {
    let payload = serde_json::to_string(state).map_err(ApiError::internal)?;
    let sig = sign(&payload);
    Ok(format!("{}.{}", STANDARD_NO_PAD.encode(payload), sig))
}

fn decode_state(value: &str) -> AppResult<OAuthState> {
    let (payload, sig) = value
        .split_once('.')
        .ok_or_else(|| ApiError::unauthorized("invalid OAuth state"))?;
    let raw = String::from_utf8(
        STANDARD_NO_PAD
            .decode(payload)
            .map_err(|_| ApiError::unauthorized("invalid OAuth state"))?,
    )
    .map_err(|_| ApiError::unauthorized("invalid OAuth state"))?;
    if sign(&raw).as_bytes().ct_eq(sig.as_bytes()).unwrap_u8() != 1 {
        return Err(ApiError::unauthorized("invalid OAuth state signature"));
    }
    serde_json::from_str(&raw).map_err(|_| ApiError::unauthorized("invalid OAuth state"))
}

fn sign(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        std::env::var("AUTH_SESSION_SECRET").unwrap_or_else(|_| "dev-only-change-me".to_string()),
    );
    hasher.update(":");
    hasher.update(value);
    hex::encode(hasher.finalize())
}

fn oauth_key(provider: &AuthProvider, provider_account_id: &str) -> String {
    format!("{}:{}", provider.as_str(), provider_account_id)
}

fn user_provider_key(user_id: &str, provider: &AuthProvider) -> String {
    format!("{}:{}", user_id, provider.as_str())
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

fn parse_time(value: &str) -> AppResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| ApiError::internal("invalid session timestamp"))
}

fn ttl_epoch(value: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    Ok(DateTime::parse_from_rfc3339(value)?.timestamp())
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn normalize_email(email: &str) -> String {
    email.trim().to_ascii_lowercase()
}

fn redirect_uri(provider: &AuthProvider) -> String {
    format!(
        "{}/api/auth/{}/callback",
        get_config().auth_base_url,
        provider.as_str()
    )
}

fn sanitize_return_to(value: Option<String>) -> String {
    let cfg = get_config();
    let default = cfg.auth_frontend_url.clone();
    value
        .filter(|candidate| {
            cfg.auth_allowed_frontend_urls.iter().any(|allowed| {
                candidate == allowed || candidate.starts_with(&format!("{allowed}/"))
            })
        })
        .unwrap_or(default)
}

fn mobile_deep_link(code: &str) -> String {
    let base = &get_config().auth_mobile_deep_link;
    let separator = if base.contains('?') { "&" } else { "?" };
    format!("{base}{separator}code={}", url_encode(code))
}

fn header(req: &Request, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn url_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

use base64::engine::general_purpose::URL_SAFE_NO_PAD as STANDARD_NO_PAD;
use base64::Engine;
