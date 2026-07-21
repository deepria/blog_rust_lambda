use super::{
    CookieBundle, ACCESS_COOKIE, OAUTH_STATE_COOKIE, REFRESH_COOKIE, REFRESH_DAYS, SESSION_DAYS,
    STATE_MINUTES,
};
use crate::config::get_config;
use lambda_http::Request;

pub fn session_cookies(bundle: &CookieBundle) -> Vec<String> {
    vec![
        auth_cookie(
            ACCESS_COOKIE,
            &bundle.session_token,
            SESSION_DAYS * 24 * 3600,
        ),
        auth_cookie(
            REFRESH_COOKIE,
            &bundle.refresh_token,
            REFRESH_DAYS * 24 * 3600,
        ),
    ]
}

pub fn clear_cookies() -> Vec<String> {
    [ACCESS_COOKIE, REFRESH_COOKIE, OAUTH_STATE_COOKIE]
        .iter()
        .map(|name| {
            format!(
                "{name}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax{}",
                secure_suffix()
            )
        })
        .collect()
}

pub(super) fn oauth_state_cookie(value: &str) -> String {
    auth_cookie(OAUTH_STATE_COOKIE, value, STATE_MINUTES * 60)
}

pub(super) fn cookie_value(req: &Request, name: &str) -> Option<String> {
    req.headers()
        .get_all("cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|header| header.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then(|| value.to_string()))
}

fn auth_cookie(name: &str, value: &str, max_age: i64) -> String {
    format!(
        "{name}={value}; Path=/; Max-Age={max_age}; HttpOnly; SameSite={}{}",
        get_config().auth_cookie_same_site,
        secure_suffix()
    )
}

fn secure_suffix() -> &'static str {
    if get_config().auth_cookie_secure {
        "; Secure"
    } else {
        ""
    }
}
