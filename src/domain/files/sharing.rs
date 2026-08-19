use super::{repository, FileViewer, SharedFileItem};
use crate::api::{ApiError, AppResult};
use crate::domain::{auth, friends};
use crate::dynamodb::{delete_value, get_json, put_json, query_json_prefix};
use chrono::Utc;
use serde::{Deserialize, Serialize};

const FILE_VIEWER_PART: &str = "FILE_VIEWER";
const SHARED_FILE_PART: &str = "SHARED_FILE";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileShare {
    owner_id: String,
    owner_name: String,
    key: String,
    display_name: String,
    has_password: bool,
    viewer_id: String,
    viewer_email: String,
    viewer_name: String,
    shared_at: String,
}

pub(super) async fn assert_can_read(actor_id: &str, owner_id: &str, key: &str) -> AppResult<()> {
    if actor_id == owner_id {
        return Ok(());
    }
    let allowed = get_json::<FileShare>(FILE_VIEWER_PART, &viewer_idx(owner_id, key, actor_id))
        .await
        .map_err(ApiError::internal)?
        .is_some();
    if allowed {
        Ok(())
    } else {
        Err(ApiError::forbidden("file access denied"))
    }
}

pub(super) async fn add_viewer(
    owner_id: &str,
    owner_name: &str,
    key: &str,
    display_name: &str,
    has_password: bool,
    email: Option<&str>,
    viewer_id: Option<&str>,
) -> AppResult<FileViewer> {
    let viewer = if let Some(viewer_id) = viewer_id {
        if !friends::are_friends(owner_id, viewer_id).await? {
            return Err(ApiError::forbidden("files can only be shared with friends"));
        }
        auth::find_active_user_by_id(viewer_id)
            .await?
            .ok_or_else(|| ApiError::not_found("registered user not found"))?
    } else if let Some(email) = email {
        auth::find_active_user_by_email(email)
            .await?
            .ok_or_else(|| ApiError::not_found("registered user not found"))?
    } else {
        return Err(ApiError::bad_request("viewer_id is required"));
    };
    if viewer.id == owner_id {
        return Err(ApiError::bad_request("the file owner already has access"));
    }

    let share = FileShare {
        owner_id: owner_id.to_string(),
        owner_name: owner_name.to_string(),
        key: key.to_string(),
        display_name: display_name.to_string(),
        has_password,
        viewer_id: viewer.id.clone(),
        viewer_email: viewer.email.clone(),
        viewer_name: viewer.name.clone(),
        shared_at: Utc::now().to_rfc3339(),
    };
    put_json(
        FILE_VIEWER_PART,
        &viewer_idx(owner_id, key, &viewer.id),
        &share,
    )
    .await
    .map_err(ApiError::internal)?;
    if let Err(error) = put_json(
        SHARED_FILE_PART,
        &shared_idx(&viewer.id, owner_id, key),
        &share,
    )
    .await
    {
        let _ = delete_value(FILE_VIEWER_PART, &viewer_idx(owner_id, key, &viewer.id)).await;
        return Err(ApiError::internal(error));
    }
    Ok(to_viewer(&share))
}

pub(super) async fn list_viewers(owner_id: &str, key: &str) -> AppResult<Vec<FileViewer>> {
    let shares = query_json_prefix::<FileShare>(
        FILE_VIEWER_PART,
        &format!("owner:{owner_id}:file:{key}:viewer:"),
    )
    .await
    .map_err(ApiError::internal)?;
    let mut viewers = shares.iter().map(to_viewer).collect::<Vec<_>>();
    viewers.sort_by(|a, b| {
        a.viewer_name
            .to_lowercase()
            .cmp(&b.viewer_name.to_lowercase())
    });
    Ok(viewers)
}

pub(super) async fn remove_viewer(owner_id: &str, key: &str, viewer_id: &str) -> AppResult<()> {
    let idx = viewer_idx(owner_id, key, viewer_id);
    if get_json::<FileShare>(FILE_VIEWER_PART, &idx)
        .await
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(ApiError::not_found("file viewer not found"));
    }
    delete_value(FILE_VIEWER_PART, &idx)
        .await
        .map_err(ApiError::internal)?;
    delete_value(SHARED_FILE_PART, &shared_idx(viewer_id, owner_id, key))
        .await
        .map_err(ApiError::internal)?;
    Ok(())
}

pub(super) async fn remove_all_viewers(owner_id: &str, key: &str) -> AppResult<()> {
    let shares = query_json_prefix::<FileShare>(
        FILE_VIEWER_PART,
        &format!("owner:{owner_id}:file:{key}:viewer:"),
    )
    .await
    .map_err(ApiError::internal)?;
    for share in shares {
        delete_value(
            FILE_VIEWER_PART,
            &viewer_idx(owner_id, key, &share.viewer_id),
        )
        .await
        .map_err(ApiError::internal)?;
        delete_value(
            SHARED_FILE_PART,
            &shared_idx(&share.viewer_id, owner_id, key),
        )
        .await
        .map_err(ApiError::internal)?;
    }
    Ok(())
}

pub(super) async fn list_shared_files(viewer_id: &str) -> AppResult<Vec<SharedFileItem>> {
    let shares =
        query_json_prefix::<FileShare>(SHARED_FILE_PART, &format!("viewer:{viewer_id}:owner:"))
            .await
            .map_err(ApiError::internal)?;
    let mut items = Vec::new();
    for share in shares {
        // Owner-side metadata is the source of truth; stale reverse-index entries stay hidden.
        if repository::load_meta(&share.owner_id, &share.key)
            .await?
            .is_none()
        {
            continue;
        }
        items.push(SharedFileItem {
            key: share.key,
            display_name: share.display_name,
            has_password: share.has_password,
            owner_id: share.owner_id,
            owner_name: share.owner_name,
            shared_at: share.shared_at,
        });
    }
    items.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
    });
    Ok(items)
}

fn to_viewer(share: &FileShare) -> FileViewer {
    FileViewer {
        viewer_id: share.viewer_id.clone(),
        viewer_email: share.viewer_email.clone(),
        viewer_name: share.viewer_name.clone(),
        shared_at: share.shared_at.clone(),
    }
}

fn viewer_idx(owner_id: &str, key: &str, viewer_id: &str) -> String {
    format!("owner:{owner_id}:file:{key}:viewer:{viewer_id}")
}

fn shared_idx(viewer_id: &str, owner_id: &str, key: &str) -> String {
    format!("viewer:{viewer_id}:owner:{owner_id}:file:{key}")
}

#[cfg(test)]
mod tests {
    use super::{shared_idx, viewer_idx};

    #[test]
    fn builds_owner_and_viewer_lookup_keys() {
        assert_eq!(
            viewer_idx("owner-1", "file.txt", "viewer-1"),
            "owner:owner-1:file:file.txt:viewer:viewer-1"
        );
        assert_eq!(
            shared_idx("viewer-1", "owner-1", "file.txt"),
            "viewer:viewer-1:owner:owner-1:file:file.txt"
        );
    }
}
