use aws_config::BehaviorVersion;
use aws_sdk_s3::{presigning::PresigningConfig, Client};
use aws_sdk_s3::types::StorageClass;
use std::time::Duration;

async fn s3_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    Client::new(&config)
}

pub async fn list_objects(
    bucket: &str,
    prefix: String,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

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
    bucket: &str,
    key: String,
    content_type: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

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
    bucket: &str,
    key: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

    let presigned = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}

pub async fn presign_delete(
    bucket: &str,
    key: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = s3_client().await;

    let presigned = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(900))?)
        .await?;

    Ok(presigned.uri().to_string())
}
