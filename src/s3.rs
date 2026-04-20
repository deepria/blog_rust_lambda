use crate::config::get_config;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{presigning::PresigningConfig, Client};
use std::time::Duration;
use tokio::sync::OnceCell;
use url::form_urlencoded;

static S3_CLIENT: OnceCell<Client> = OnceCell::const_new();

async fn s3_client() -> &'static Client {
    S3_CLIENT
        .get_or_init(|| async {
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            Client::new(&config)
        })
        .await
}

pub async fn list_objects(
    prefix: String,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

    let resp = client
        .list_objects_v2()
        .bucket(&get_config().s3_bucket)
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

    let presigned = client
        .put_object()
        .bucket(&get_config().s3_bucket)
        .key(key)
        .content_type(content_type)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}

pub async fn presign_download(
    key: String,
    original_filename: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

    let encoded_filename: String = form_urlencoded::byte_serialize(original_filename.as_bytes())
        .collect::<String>()
        .replace("+", "%20");
    let content_disposition = format!("attachment; filename*=UTF-8''{}", encoded_filename);
    let presigned = client
        .get_object()
        .bucket(&get_config().s3_bucket)
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

    let presigned = client
        .delete_object()
        .bucket(&get_config().s3_bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}

pub async fn delete_object(key: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;
    client
        .delete_object()
        .bucket(&get_config().s3_bucket)
        .key(key)
        .send()
        .await?;
    Ok(())
}

pub fn build_key(
    base_path: &str,
    prefix: &str,
    part: Option<&String>,
    idx: Option<&String>,
    filename: &str,
) -> String {
    let mut segments = Vec::new();
    if !base_path.trim().is_empty() {
        segments.push(base_path.trim_matches('/').to_string());
    }
    if !prefix.trim().is_empty() {
        segments.push(prefix.trim_matches('/').to_string());
    }
    if let (Some(part), Some(idx)) = (part, idx) {
        if !part.is_empty() && !idx.is_empty() {
            segments.push(part.trim_matches('/').to_string());
            segments.push(idx.trim_matches('/').to_string());
        }
    }
    if !filename.trim().is_empty() {
        segments.push(filename.trim_matches('/').to_string());
    }
    let mut joined = segments.join("/");
    if filename.is_empty() && !joined.is_empty() {
        joined.push('/');
    }
    joined
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_key() {
        let base = "base";
        assert_eq!(
            build_key(
                base,
                "upload",
                Some(&"p".to_string()),
                Some(&"i".to_string()),
                ""
            ),
            "base/upload/p/i/"
        );
        assert_eq!(build_key(base, "upload", None, None, ""), "base/upload/");
        assert_eq!(
            build_key(
                base,
                "upload",
                Some(&"p".to_string()),
                Some(&"i".to_string()),
                "f.txt"
            ),
            "base/upload/p/i/f.txt"
        );
        assert_eq!(
            build_key(base, "upload", None, None, "f.txt"),
            "base/upload/f.txt"
        );
        assert_eq!(
            build_key(
                base,
                "",
                Some(&"p".to_string()),
                Some(&"i".to_string()),
                "f.txt"
            ),
            "base/p/i/f.txt"
        );
        assert_eq!(build_key(base, "", None, None, "f.txt"), "base/f.txt");
    }
}
