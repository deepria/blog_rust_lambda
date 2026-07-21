use super::{
    hash_token, header, ttl_epoch, CookieBundle, Session, REFRESH_DAYS, SESSION_DAYS, SESSION_PART,
};
use crate::dynamodb::put_json_with_ttl;
use chrono::{Duration, Utc};
use lambda_http::Request;
use uuid::Uuid;

pub(super) async fn create(
    req: &Request,
    user_id: &str,
) -> Result<CookieBundle, Box<dyn std::error::Error + Send + Sync>> {
    let session_token = Uuid::new_v4().to_string();
    let refresh_token = Uuid::new_v4().to_string();
    let now = Utc::now();
    let session = Session {
        id: Uuid::new_v4().to_string(),
        user_id: user_id.to_string(),
        session_token: hash_token(&session_token),
        refresh_token: hash_token(&refresh_token),
        expires_at: (now + Duration::days(SESSION_DAYS)).to_rfc3339(),
        refresh_expires_at: (now + Duration::days(REFRESH_DAYS)).to_rfc3339(),
        last_used_at: now.to_rfc3339(),
        ip_address: header(req, "x-forwarded-for").or_else(|| header(req, "x-real-ip")),
        user_agent: header(req, "user-agent"),
        created_at: now.to_rfc3339(),
        revoked_at: None,
    };
    put_access(&session).await?;
    put_refresh(&session).await?;
    Ok(CookieBundle {
        session_token,
        refresh_token,
    })
}

pub(super) async fn put_access(
    session: &Session,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_json_with_ttl(
        SESSION_PART,
        &session.session_token,
        session,
        ttl_epoch(&session.expires_at)?,
    )
    .await
}

async fn put_refresh(session: &Session) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_json_with_ttl(
        SESSION_PART,
        &session.refresh_token,
        session,
        ttl_epoch(&session.refresh_expires_at)?,
    )
    .await
}

pub(super) async fn revoke(
    idx: &str,
    session: &Session,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    put_json_with_ttl(SESSION_PART, idx, session, Utc::now().timestamp()).await
}
