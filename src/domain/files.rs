use self::storage_gateway::{
    abort_multipart_upload, build_key, complete_multipart_upload, create_multipart_upload,
    delete_object, list_objects, presign_delete, presign_download, presign_upload,
    presign_upload_part,
};
use crate::api::{ApiError, AppResult};
use crate::config::get_config;
use crate::dynamodb::{delete_value, put_json};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use flate2::read::ZlibDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use uuid::Uuid;

const FILE_META_PART: &str = "FILE_META";
const FILE_ORG_PART: &str = "FILE_ORG";
const LEGACY_FILE_AUTH_PART: &str = "file";
mod organization;
mod repository;
mod sharing;
mod storage_gateway;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileItem {
    pub key: String,
    pub display_name: String,
    pub has_password: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileViewer {
    pub viewer_id: String,
    pub viewer_email: String,
    pub viewer_name: String,
    pub shared_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedFileItem {
    pub key: String,
    pub display_name: String,
    pub has_password: bool,
    pub owner_id: String,
    pub owner_name: String,
    pub shared_at: String,
}

#[derive(Debug, Deserialize)]
pub struct ShareFileRequest {
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub viewer_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FileMeta {
    pub key: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDirectory {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub sort_order: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileOrganization {
    #[serde(default)]
    pub directories: Vec<FileDirectory>,
    #[serde(default)]
    pub file_locations: HashMap<String, Option<String>>,
}

#[derive(Debug, Deserialize)]
pub struct UploadRequest {
    pub filename: String,
    pub display_name: Option<String>,
    pub content_type: Option<String>,
    pub auth_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub key: String,
    pub upload_url: String,
    pub display_name: String,
}

#[derive(Debug, Deserialize)]
pub struct MultipartPartRequest {
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
}

#[derive(Debug, Deserialize)]
pub struct CompletedUploadPart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Deserialize)]
pub struct CompleteMultipartRequest {
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<CompletedUploadPart>,
}

#[derive(Debug, Deserialize)]
pub struct AbortMultipartRequest {
    pub key: String,
    pub upload_id: String,
}

#[derive(Debug, Serialize)]
pub struct MultipartUploadResponse {
    pub key: String,
    pub upload_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize)]
pub struct UploadPartResponse {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct AccessRequest {
    pub key: String,
    pub auth_key: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PresignedResponse {
    pub url: String,
}

pub async fn list_files(user_id: &str) -> AppResult<Vec<FileItem>> {
    let prefix = user_base_path(user_id);
    let (_folders, files) = list_objects(if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    })
    .await
    .map_err(ApiError::internal)?;

    let mut items = Vec::new();
    for key in files {
        let normalized = strip_base_prefix(&prefix, &key);
        if normalized.is_empty() {
            continue;
        }
        let meta = repository::load_meta(user_id, &normalized).await?;
        let display_name = meta
            .as_ref()
            .map(|item| item.display_name.clone())
            .filter(|value| !value.is_empty())
            .or_else(|| decode_display_name(&normalized))
            .unwrap_or_else(|| normalized.clone());
        let display_name = ensure_display_extension(&display_name, &normalized);

        let has_password = meta
            .as_ref()
            .and_then(|item| item.auth_hash.clone())
            .is_some()
            || repository::load_legacy_auth(&normalized).await?.is_some();

        items.push(FileItem {
            key: normalized,
            display_name,
            has_password,
        });
    }

    items.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
    });
    Ok(items)
}

pub async fn get_organization(user_id: &str) -> AppResult<FileOrganization> {
    repository::load_organization(user_id).await
}

pub async fn save_organization(
    user_id: &str,
    organization: FileOrganization,
) -> AppResult<FileOrganization> {
    let organization = organization::normalize(organization)?;
    put_json(FILE_ORG_PART, &file_org_idx(user_id), &organization)
        .await
        .map_err(ApiError::internal)?;
    Ok(organization)
}

pub async fn create_upload(user_id: &str, payload: UploadRequest) -> AppResult<UploadResponse> {
    let filename = payload.filename.trim();
    if filename.is_empty() {
        return Err(ApiError::bad_request("filename is required"));
    }
    if filename.len() > 255 {
        return Err(ApiError::bad_request("filename is too long"));
    }

    let display_name = upload_display_name(filename, payload.display_name.as_deref());
    let safe_key = generate_storage_name(filename);
    let key = build_key(&user_base_path(user_id), "", None, None, &safe_key);
    let content_type = payload
        .content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let upload_url = presign_upload(key.clone(), content_type)
        .await
        .map_err(ApiError::internal)?;

    let meta = FileMeta {
        key: safe_key.clone(),
        display_name: display_name.clone(),
        auth_hash: payload
            .auth_key
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(hash_auth_key),
        created_at: Some(now_ts()),
    };

    put_json(FILE_META_PART, &file_meta_idx(user_id, &safe_key), &meta)
        .await
        .map_err(ApiError::internal)?;

    Ok(UploadResponse {
        key: safe_key,
        upload_url,
        display_name,
    })
}

pub async fn initiate_multipart_upload(
    user_id: &str,
    payload: UploadRequest,
) -> AppResult<MultipartUploadResponse> {
    let filename = payload.filename.trim();
    if filename.is_empty() {
        return Err(ApiError::bad_request("filename is required"));
    }
    if filename.len() > 255 {
        return Err(ApiError::bad_request("filename is too long"));
    }

    let display_name = upload_display_name(filename, payload.display_name.as_deref());
    let safe_key = generate_storage_name(filename);
    let key = build_key(&user_base_path(user_id), "", None, None, &safe_key);
    let content_type = payload
        .content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let upload_id = create_multipart_upload(key, content_type)
        .await
        .map_err(ApiError::internal)?;

    let meta = FileMeta {
        key: safe_key.clone(),
        display_name: display_name.clone(),
        auth_hash: payload
            .auth_key
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(hash_auth_key),
        created_at: Some(now_ts()),
    };

    put_json(FILE_META_PART, &file_meta_idx(user_id, &safe_key), &meta)
        .await
        .map_err(ApiError::internal)?;

    Ok(MultipartUploadResponse {
        key: safe_key,
        upload_id,
        display_name,
    })
}

pub async fn create_upload_part(
    user_id: &str,
    payload: MultipartPartRequest,
) -> AppResult<UploadPartResponse> {
    let key = validate_key(&payload.key)?;
    let upload_id = validate_upload_id(&payload.upload_id)?;
    if payload.part_number < 1 || payload.part_number > 10_000 {
        return Err(ApiError::bad_request(
            "part_number must be between 1 and 10000",
        ));
    }

    let url = presign_upload_part(
        build_key(&user_base_path(user_id), "", None, None, &key),
        upload_id,
        payload.part_number,
    )
    .await
    .map_err(ApiError::internal)?;

    Ok(UploadPartResponse { url })
}

pub async fn finish_multipart_upload(
    user_id: &str,
    payload: CompleteMultipartRequest,
) -> AppResult<()> {
    let key = validate_key(&payload.key)?;
    let upload_id = validate_upload_id(&payload.upload_id)?;
    if payload.parts.is_empty() {
        return Err(ApiError::bad_request("parts are required"));
    }
    if payload.parts.len() > 10_000 {
        return Err(ApiError::bad_request("too many parts"));
    }

    let mut parts = payload
        .parts
        .into_iter()
        .map(|part| {
            if part.part_number < 1 || part.part_number > 10_000 {
                return Err(ApiError::bad_request(
                    "part_number must be between 1 and 10000",
                ));
            }
            let etag = part.etag.trim();
            if etag.is_empty() {
                return Err(ApiError::bad_request("etag is required"));
            }
            Ok((part.part_number, etag.to_string()))
        })
        .collect::<AppResult<Vec<_>>>()?;
    parts.sort_by_key(|(part_number, _)| *part_number);

    complete_multipart_upload(
        build_key(&user_base_path(user_id), "", None, None, &key),
        upload_id,
        parts,
    )
    .await
    .map_err(ApiError::internal)?;

    Ok(())
}

pub async fn cancel_multipart_upload(
    user_id: &str,
    payload: AbortMultipartRequest,
) -> AppResult<()> {
    let key = validate_key(&payload.key)?;
    let upload_id = validate_upload_id(&payload.upload_id)?;

    abort_multipart_upload(
        build_key(&user_base_path(user_id), "", None, None, &key),
        upload_id,
    )
    .await
    .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &file_meta_idx(user_id, &key))
        .await
        .map_err(ApiError::internal)?;

    Ok(())
}

pub async fn create_download(
    actor_id: &str,
    payload: AccessRequest,
) -> AppResult<PresignedResponse> {
    let key = validate_key(&payload.key)?;
    let owner_id = payload.owner_id.as_deref().unwrap_or(actor_id);
    validate_owner_id(owner_id)?;
    sharing::assert_can_read(actor_id, owner_id, &key).await?;
    assert_file_access(owner_id, &key, payload.auth_key.as_deref()).await?;
    let display_name = resolve_display_name(owner_id, &key).await?;
    let url = presign_download(
        build_key(&user_base_path(owner_id), "", None, None, &key),
        display_name,
    )
    .await
    .map_err(ApiError::internal)?;
    Ok(PresignedResponse { url })
}

pub async fn create_delete(user_id: &str, payload: AccessRequest) -> AppResult<PresignedResponse> {
    let key = validate_key(&payload.key)?;
    assert_file_access(user_id, &key, payload.auth_key.as_deref()).await?;
    let url = presign_delete(build_key(&user_base_path(user_id), "", None, None, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &file_meta_idx(user_id, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(&key))
        .await
        .map_err(ApiError::internal)?;
    remove_file_from_organization(user_id, &key).await?;
    sharing::remove_all_viewers(user_id, &key).await?;
    Ok(PresignedResponse { url })
}

pub async fn delete_file(user_id: &str, key: &str, auth_key: Option<&str>) -> AppResult<()> {
    let key = validate_key(key)?;
    assert_file_access(user_id, &key, auth_key).await?;
    delete_object(build_key(&user_base_path(user_id), "", None, None, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &file_meta_idx(user_id, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(&key))
        .await
        .map_err(ApiError::internal)?;
    remove_file_from_organization(user_id, &key).await?;
    sharing::remove_all_viewers(user_id, &key).await?;
    Ok(())
}

pub async fn list_file_viewers(owner_id: &str, key: &str) -> AppResult<Vec<FileViewer>> {
    let key = validate_key(key)?;
    ensure_file_exists(owner_id, &key).await?;
    sharing::list_viewers(owner_id, &key).await
}

pub async fn share_file(
    owner_id: &str,
    owner_name: &str,
    key: &str,
    payload: ShareFileRequest,
) -> AppResult<FileViewer> {
    let key = validate_key(key)?;
    ensure_file_exists(owner_id, &key).await?;
    let display_name = resolve_display_name(owner_id, &key).await?;
    let has_password = file_has_password(owner_id, &key).await?;
    sharing::add_viewer(
        owner_id,
        owner_name,
        &key,
        &display_name,
        has_password,
        payload.email.as_deref(),
        payload.viewer_id.as_deref(),
    )
    .await
}

pub async fn unshare_file(owner_id: &str, key: &str, viewer_id: &str) -> AppResult<()> {
    let key = validate_key(key)?;
    validate_owner_id(viewer_id)?;
    sharing::remove_viewer(owner_id, &key, viewer_id).await
}

pub async fn list_shared_files(viewer_id: &str) -> AppResult<Vec<SharedFileItem>> {
    sharing::list_shared_files(viewer_id).await
}

async fn remove_file_from_organization(user_id: &str, key: &str) -> AppResult<()> {
    let mut organization = repository::load_organization(user_id).await?;
    if organization.file_locations.remove(key).is_some() {
        save_organization(user_id, organization).await?;
    }
    Ok(())
}

async fn assert_file_access(user_id: &str, key: &str, auth_key: Option<&str>) -> AppResult<()> {
    let meta = repository::load_meta(user_id, key).await?;
    if let Some(meta) = meta {
        if let Some(expected) = meta.auth_hash {
            let candidate = auth_key.unwrap_or_default();
            if candidate.is_empty() || hash_auth_key(candidate) != expected {
                return Err(ApiError::unauthorized("invalid file password"));
            }
        }
        return Ok(());
    }

    if let Some(legacy) = repository::load_legacy_auth(key).await? {
        let candidate = auth_key.unwrap_or_default();
        if candidate.is_empty() || candidate != legacy {
            return Err(ApiError::unauthorized("invalid file password"));
        }
    }
    Ok(())
}

async fn ensure_file_exists(user_id: &str, key: &str) -> AppResult<()> {
    if repository::load_meta(user_id, key).await?.is_some() {
        return Ok(());
    }
    let exists = list_files(user_id)
        .await?
        .iter()
        .any(|item| item.key == key);
    if exists {
        Ok(())
    } else {
        Err(ApiError::not_found("file not found"))
    }
}

async fn file_has_password(user_id: &str, key: &str) -> AppResult<bool> {
    let meta_password = repository::load_meta(user_id, key)
        .await?
        .and_then(|meta| meta.auth_hash)
        .is_some();
    Ok(meta_password || repository::load_legacy_auth(key).await?.is_some())
}

async fn resolve_display_name(user_id: &str, key: &str) -> AppResult<String> {
    let meta = repository::load_meta(user_id, key).await?;
    let display_name = meta
        .map(|item| item.display_name)
        .filter(|value| !value.is_empty())
        .or_else(|| decode_display_name(key))
        .unwrap_or_else(|| key.to_string());
    Ok(ensure_display_extension(&display_name, key))
}

fn upload_display_name(filename: &str, display_name: Option<&str>) -> String {
    let candidate = display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(filename);
    ensure_display_extension(candidate, filename)
}

fn ensure_display_extension(display_name: &str, source_name: &str) -> String {
    let Some(ext) = file_extension(source_name) else {
        return display_name.to_string();
    };
    if display_name.to_lowercase().ends_with(&ext.to_lowercase()) {
        display_name.to_string()
    } else {
        format!("{display_name}{ext}")
    }
}

fn file_extension(filename: &str) -> Option<&str> {
    let name = filename.rsplit('/').next().unwrap_or(filename);
    let dot_idx = name.rfind('.')?;
    if dot_idx == 0 || dot_idx + 1 >= name.len() {
        return None;
    }
    Some(&name[dot_idx..])
}

fn validate_key(key: &str) -> AppResult<String> {
    let key = key.trim();
    if key.is_empty() {
        return Err(ApiError::bad_request("key is required"));
    }
    if key.contains("..") || key.contains('/') {
        return Err(ApiError::bad_request("invalid file key"));
    }
    Ok(key.to_string())
}

fn validate_owner_id(owner_id: &str) -> AppResult<()> {
    if owner_id.trim().is_empty()
        || owner_id.contains('/')
        || owner_id.contains("..")
        || owner_id.contains(':')
    {
        return Err(ApiError::bad_request("invalid owner_id"));
    }
    Ok(())
}

fn validate_upload_id(upload_id: &str) -> AppResult<String> {
    let value = upload_id.trim();
    if value.is_empty() {
        return Err(ApiError::bad_request("upload_id is required"));
    }
    Ok(value.to_string())
}

fn strip_base_prefix(base: &str, key: &str) -> String {
    if base.is_empty() {
        return key.trim_matches('/').to_string();
    }
    key.trim_start_matches(&format!("{base}/"))
        .trim_matches('/')
        .to_string()
}

fn user_base_path(user_id: &str) -> String {
    let base = get_config().s3_base_path.trim_matches('/');
    if base.is_empty() {
        format!("users/{user_id}")
    } else {
        format!("{base}/users/{user_id}")
    }
}

fn file_meta_idx(user_id: &str, key: &str) -> String {
    format!("user:{user_id}:file:{key}")
}

fn file_org_idx(user_id: &str) -> String {
    format!("user:{user_id}:organization")
}

fn generate_storage_name(filename: &str) -> String {
    let ext = filename
        .rsplit_once('.')
        .map(|(_, ext)| format!(".{}", sanitize_name(ext)))
        .unwrap_or_default();
    let stem = filename
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(filename);
    let safe_stem = sanitize_name(stem);
    format!("{}-{}{}", Uuid::new_v4(), safe_stem, ext)
}

fn sanitize_name(input: &str) -> String {
    let mut sanitized = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if sanitized.is_empty() {
        sanitized = "file".to_string();
    }
    sanitized
}

fn decode_display_name(key: &str) -> Option<String> {
    let encoded = key.split_once('_').map(|(_, value)| value).unwrap_or(key);
    decode_legacy_string(encoded)
}

fn decode_legacy_string(value: &str) -> Option<String> {
    let decoded = url::form_urlencoded::parse(value.as_bytes())
        .map(|(key, _)| key.to_string())
        .next()
        .unwrap_or_else(|| percent_decode(value));
    let bytes = STANDARD.decode(decoded).ok()?;
    let mut zlib = ZlibDecoder::new(bytes.as_slice());
    let mut output = Vec::new();
    zlib.read_to_end(&mut output).ok()?;
    String::from_utf8(output).ok()
}

fn percent_decode(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.as_bytes().iter().copied();
    while let Some(ch) = chars.next() {
        if ch == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(hi), Some(lo)) = (hi, lo) {
                let hex = [hi, lo];
                if let Ok(hex) = std::str::from_utf8(&hex) {
                    if let Ok(num) = u8::from_str_radix(hex, 16) {
                        output.push(num as char);
                        continue;
                    }
                }
            }
            output.push('%');
        } else {
            output.push(ch as char);
        }
    }
    output
}

fn hash_auth_key(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn legacy_auth_index(key: &str) -> String {
    format!("file:{key}")
}

fn now_ts() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_display_extension, hash_auth_key, sanitize_name, upload_display_name, validate_key,
    };

    #[test]
    fn rejects_nested_keys() {
        assert!(validate_key("../evil").is_err());
        assert!(validate_key("folder/file.txt").is_err());
    }

    #[test]
    fn hashes_auth_key() {
        assert_eq!(hash_auth_key("abc").len(), 64);
    }

    #[test]
    fn sanitizes_names() {
        assert_eq!(sanitize_name("my file"), "my-file");
    }

    #[test]
    fn appends_original_extension_to_custom_upload_name() {
        assert_eq!(
            upload_display_name("original.pdf", Some("보고서")),
            "보고서.pdf"
        );
    }

    #[test]
    fn keeps_custom_upload_name_when_it_already_has_extension() {
        assert_eq!(
            upload_display_name("original.pdf", Some("보고서.final.pdf")),
            "보고서.final.pdf"
        );
    }

    #[test]
    fn appends_original_extension_to_custom_name_with_other_dots() {
        assert_eq!(
            upload_display_name("original.pdf", Some("보고서.v1")),
            "보고서.v1.pdf"
        );
    }

    #[test]
    fn keeps_original_name_when_custom_upload_name_is_empty() {
        assert_eq!(upload_display_name("photo.png", Some("  ")), "photo.png");
    }

    #[test]
    fn keeps_extensionless_files_extensionless() {
        assert_eq!(
            upload_display_name("LICENSE", Some("license-copy")),
            "license-copy"
        );
    }

    #[test]
    fn restores_extension_for_existing_extensionless_display_names() {
        assert_eq!(
            ensure_display_extension("보고서", "uuid-original.pdf"),
            "보고서.pdf"
        );
    }
}
