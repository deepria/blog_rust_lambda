use super::{
    decode_legacy_string, file_meta_idx, file_org_idx, legacy_auth_index, FileMeta,
    FileOrganization, FILE_META_PART, FILE_ORG_PART, LEGACY_FILE_AUTH_PART,
};
use crate::api::{ApiError, AppResult};
use crate::dynamodb::{get_json, get_value};

pub(super) async fn load_organization(user_id: &str) -> AppResult<FileOrganization> {
    get_json(FILE_ORG_PART, &file_org_idx(user_id))
        .await
        .map_err(ApiError::internal)
        .map(|value| value.unwrap_or_default())
}

pub(super) async fn load_meta(user_id: &str, key: &str) -> AppResult<Option<FileMeta>> {
    get_json(FILE_META_PART, &file_meta_idx(user_id, key))
        .await
        .map_err(ApiError::internal)
}

pub(super) async fn load_legacy_auth(key: &str) -> AppResult<Option<String>> {
    let Some(raw) = get_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(key))
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    let value = decode_legacy_string(&raw).unwrap_or(raw);
    Ok((!value.is_empty()).then_some(value))
}
