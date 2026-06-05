use crate::api::{ApiError, AppResult};
use crate::config::get_config;
use crate::dynamodb::{delete_value, get_json, get_value, put_json};
use crate::s3::{
    abort_multipart_upload, build_key, complete_multipart_upload, create_multipart_upload,
    delete_object, list_objects, presign_delete, presign_download, presign_upload,
    presign_upload_part,
};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use flate2::read::ZlibDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;
use uuid::Uuid;

const FILE_META_PART: &str = "FILE_META";
const LEGACY_FILE_AUTH_PART: &str = "file";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileItem {
    pub key: String,
    pub display_name: String,
    pub has_password: bool,
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
}

#[derive(Debug, Serialize)]
pub struct PresignedResponse {
    pub url: String,
}

pub async fn list_files() -> AppResult<Vec<FileItem>> {
    let prefix = get_config().s3_base_path.clone();
    let (_folders, files) = list_objects(if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    })
    .await
    .map_err(ApiError::internal)?;

    let mut items = Vec::new();
    for key in files {
        let normalized = strip_base_prefix(&key);
        if normalized.is_empty() {
            continue;
        }
        let meta = load_meta(&normalized).await?;
        let display_name = meta
            .as_ref()
            .map(|item| item.display_name.clone())
            .filter(|value| !value.is_empty())
            .or_else(|| decode_display_name(&normalized))
            .unwrap_or_else(|| normalized.clone());

        let has_password = meta
            .as_ref()
            .and_then(|item| item.auth_hash.clone())
            .is_some()
            || load_legacy_auth(&normalized).await?.is_some();

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

pub async fn create_upload(payload: UploadRequest) -> AppResult<UploadResponse> {
    let filename = payload.filename.trim();
    if filename.is_empty() {
        return Err(ApiError::bad_request("filename is required"));
    }
    if filename.len() > 255 {
        return Err(ApiError::bad_request("filename is too long"));
    }

    let display_name = payload
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(filename)
        .to_string();
    let safe_key = generate_storage_name(filename);
    let key = build_key(&get_config().s3_base_path, "", None, None, &safe_key);
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

    put_json(FILE_META_PART, &safe_key, &meta)
        .await
        .map_err(ApiError::internal)?;

    Ok(UploadResponse {
        key: safe_key,
        upload_url,
        display_name,
    })
}

pub async fn initiate_multipart_upload(
    payload: UploadRequest,
) -> AppResult<MultipartUploadResponse> {
    let filename = payload.filename.trim();
    if filename.is_empty() {
        return Err(ApiError::bad_request("filename is required"));
    }
    if filename.len() > 255 {
        return Err(ApiError::bad_request("filename is too long"));
    }

    let display_name = payload
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(filename)
        .to_string();
    let safe_key = generate_storage_name(filename);
    let key = build_key(&get_config().s3_base_path, "", None, None, &safe_key);
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

    put_json(FILE_META_PART, &safe_key, &meta)
        .await
        .map_err(ApiError::internal)?;

    Ok(MultipartUploadResponse {
        key: safe_key,
        upload_id,
        display_name,
    })
}

pub async fn create_upload_part(payload: MultipartPartRequest) -> AppResult<UploadPartResponse> {
    let key = validate_key(&payload.key)?;
    let upload_id = validate_upload_id(&payload.upload_id)?;
    if payload.part_number < 1 || payload.part_number > 10_000 {
        return Err(ApiError::bad_request(
            "part_number must be between 1 and 10000",
        ));
    }

    let url = presign_upload_part(
        build_key(&get_config().s3_base_path, "", None, None, &key),
        upload_id,
        payload.part_number,
    )
    .await
    .map_err(ApiError::internal)?;

    Ok(UploadPartResponse { url })
}

pub async fn finish_multipart_upload(payload: CompleteMultipartRequest) -> AppResult<()> {
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
        build_key(&get_config().s3_base_path, "", None, None, &key),
        upload_id,
        parts,
    )
    .await
    .map_err(ApiError::internal)?;

    Ok(())
}

pub async fn cancel_multipart_upload(payload: AbortMultipartRequest) -> AppResult<()> {
    let key = validate_key(&payload.key)?;
    let upload_id = validate_upload_id(&payload.upload_id)?;

    abort_multipart_upload(
        build_key(&get_config().s3_base_path, "", None, None, &key),
        upload_id,
    )
    .await
    .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &key)
        .await
        .map_err(ApiError::internal)?;

    Ok(())
}

pub async fn create_download(payload: AccessRequest) -> AppResult<PresignedResponse> {
    let key = validate_key(&payload.key)?;
    assert_file_access(&key, payload.auth_key.as_deref()).await?;
    let display_name = resolve_display_name(&key).await?;
    let url = presign_download(
        build_key(&get_config().s3_base_path, "", None, None, &key),
        display_name,
    )
    .await
    .map_err(ApiError::internal)?;
    Ok(PresignedResponse { url })
}

pub async fn create_delete(payload: AccessRequest) -> AppResult<PresignedResponse> {
    let key = validate_key(&payload.key)?;
    assert_file_access(&key, payload.auth_key.as_deref()).await?;
    let url = presign_delete(build_key(&get_config().s3_base_path, "", None, None, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &key)
        .await
        .map_err(ApiError::internal)?;
    delete_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(&key))
        .await
        .map_err(ApiError::internal)?;
    Ok(PresignedResponse { url })
}

pub async fn delete_file(key: &str, auth_key: Option<&str>) -> AppResult<()> {
    let key = validate_key(key)?;
    assert_file_access(&key, auth_key).await?;
    delete_object(build_key(&get_config().s3_base_path, "", None, None, &key))
        .await
        .map_err(ApiError::internal)?;
    delete_value(FILE_META_PART, &key)
        .await
        .map_err(ApiError::internal)?;
    delete_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(&key))
        .await
        .map_err(ApiError::internal)?;
    Ok(())
}

async fn assert_file_access(key: &str, auth_key: Option<&str>) -> AppResult<()> {
    let meta = load_meta(key).await?;
    if let Some(meta) = meta {
        if let Some(expected) = meta.auth_hash {
            let candidate = auth_key.unwrap_or_default();
            if candidate.is_empty() || hash_auth_key(candidate) != expected {
                return Err(ApiError::unauthorized("invalid file password"));
            }
        }
        return Ok(());
    }

    if let Some(legacy) = load_legacy_auth(key).await? {
        let candidate = auth_key.unwrap_or_default();
        if candidate.is_empty() || candidate != legacy {
            return Err(ApiError::unauthorized("invalid file password"));
        }
    }
    Ok(())
}

async fn load_meta(key: &str) -> AppResult<Option<FileMeta>> {
    get_json(FILE_META_PART, key)
        .await
        .map_err(ApiError::internal)
}

async fn load_legacy_auth(key: &str) -> AppResult<Option<String>> {
    let Some(raw) = get_value(LEGACY_FILE_AUTH_PART, &legacy_auth_index(key))
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    let value = decode_legacy_string(&raw).unwrap_or(raw);
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

async fn resolve_display_name(key: &str) -> AppResult<String> {
    let meta = load_meta(key).await?;
    Ok(meta
        .map(|item| item.display_name)
        .filter(|value| !value.is_empty())
        .or_else(|| decode_display_name(key))
        .unwrap_or_else(|| key.to_string()))
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

fn validate_upload_id(upload_id: &str) -> AppResult<String> {
    let value = upload_id.trim();
    if value.is_empty() {
        return Err(ApiError::bad_request("upload_id is required"));
    }
    Ok(value.to_string())
}

fn strip_base_prefix(key: &str) -> String {
    let base = &get_config().s3_base_path;
    if base.is_empty() {
        return key.trim_matches('/').to_string();
    }
    key.trim_start_matches(&format!("{base}/"))
        .trim_matches('/')
        .to_string()
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
    use super::{hash_auth_key, sanitize_name, validate_key};

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
}
