use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use aws_sdk_s3::{presigning::PresigningConfig, Client};
use std::time::Duration;
use url::form_urlencoded;

async fn s3_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    Client::new(&config)
}



fn get_bucket() -> String {
    std::env::var("S3_BUCKET").expect("S3_BUCKET env missing")
}

pub async fn list_objects(
    prefix: String,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;
    let bucket = get_bucket();

    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix.clone())
        .delimiter("/")
        .send()
        .await?;

    let mut folders = Vec::new();
    let mut files = Vec::new();

    if let Some(common) = resp.common_prefixes {
        for p in common {
            if let Some(prefix) = p.prefix {
                folders.push(prefix);
            }
        }
    }

    if let Some(contents) = resp.contents {
        for obj in contents {
            if let Some(key) = obj.key {
                if key != prefix {
                    files.push(key);
                }
            }
        }
    }

    Ok((folders, files))
}

pub async fn presign_upload(
    key: String,
    content_type: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;
    let bucket = get_bucket();

    let presigned = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type(content_type)
        .storage_class(StorageClass::GlacierIr)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}

pub async fn presign_download(
    key: String,
    original_filename: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;
    let bucket = get_bucket();

    // [중요 수정] 띄어쓰기가 + 로 나오는 문제 해결
    // form_urlencoded는 공백을 '+'로 변환하지만, Content-Disposition 헤더 파일명은 '%20'이어야 함
    // 따라서 인코딩 후 '+'를 '%20'으로 치환해줍니다.
    let encoded_filename: String = form_urlencoded::byte_serialize(original_filename.as_bytes())
        .collect::<String>()
        .replace("+", "%20"); // <--- 이 부분이 핵심 수정 사항입니다.
    let content_disposition = format!("attachment; filename*=UTF-8''{}", encoded_filename);
    let presigned = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .response_content_disposition(content_disposition)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;
    Ok(presigned.uri().to_string())
}

pub async fn presign_delete(
    key: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;
    let bucket = get_bucket();

    let presigned = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}


pub fn get_base_path() -> String {
    let base = std::env::var("S3_BASE_PATH").unwrap_or_else(|_| "/upload".to_string());
    if base.ends_with('/') {
        base
    } else {
        format!("{}/", base)
    }
}

pub fn build_key(base_path: &str, prefix: &str, part: Option<&String>, idx: Option<&String>, filename: &str) -> String {
    let part_idx_path = match (part, idx) {
        (Some(p), Some(i)) if !p.is_empty() && !i.is_empty() => format!("{}/{}/", p, i),
        _ => String::new(),
    };

    let p_path = if !prefix.is_empty() {
        if prefix.ends_with('/') {
             format!("{}{}", prefix, part_idx_path)
        } else {
             format!("{}/{}", prefix, part_idx_path)
        }
    } else {
        part_idx_path
    };

    if filename.is_empty() {
        format!("{}{}", base_path, p_path)
    } else {
        format!("{}{}{}", base_path, p_path, filename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_build_key() {
        let base = "base/";
        
        // List case: prefix="upload/", filename=""
        // Note: now prefix handles slash automatically if missing
        assert_eq!(build_key(base, "upload", Some(&"p".to_string()), Some(&"i".to_string()), ""), "base/upload/p/i/");
        assert_eq!(build_key(base, "upload/", Some(&"p".to_string()), Some(&"i".to_string()), ""), "base/upload/p/i/");
        assert_eq!(build_key(base, "upload", None, None, ""), "base/upload/");
        
        // Upload case: prefix="upload"
         assert_eq!(build_key(base, "upload", Some(&"p".to_string()), Some(&"i".to_string()), "f.txt"), "base/upload/p/i/f.txt");
         assert_eq!(build_key(base, "upload", None, None, "f.txt"), "base/upload/f.txt");
         
        // Download/Delete case: prefix=""
        // If prefix is empty, no slash added from prefix logic
        assert_eq!(build_key(base, "", Some(&"p".to_string()), Some(&"i".to_string()), "f.txt"), "base/p/i/f.txt");
        assert_eq!(build_key(base, "", None, None, "f.txt"), "base/f.txt");
    }
}
