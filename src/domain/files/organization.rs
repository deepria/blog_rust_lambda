use super::{validate_key, FileOrganization};
use crate::api::{ApiError, AppResult};
use std::collections::HashSet;

const MAX_DIRECTORIES: usize = 200;
const MAX_DIRECTORY_NAME_LEN: usize = 80;

pub(super) fn normalize(mut organization: FileOrganization) -> AppResult<FileOrganization> {
    if organization.directories.len() > MAX_DIRECTORIES {
        return Err(ApiError::bad_request("too many directories"));
    }

    let mut ids = HashSet::new();
    for directory in &mut organization.directories {
        directory.id = directory.id.trim().to_string();
        directory.name = directory.name.trim().to_string();
        directory.parent_id = directory
            .parent_id
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if directory.id.is_empty() || directory.id.len() > 80 {
            return Err(ApiError::bad_request("invalid directory id"));
        }
        if !directory
            .id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            return Err(ApiError::bad_request("invalid directory id"));
        }
        if directory.name.is_empty() {
            return Err(ApiError::bad_request("directory name is required"));
        }
        if directory.name.chars().count() > MAX_DIRECTORY_NAME_LEN {
            return Err(ApiError::bad_request("directory name is too long"));
        }
        if !ids.insert(directory.id.clone()) {
            return Err(ApiError::bad_request("duplicate directory id"));
        }
    }
    for directory in &organization.directories {
        if let Some(parent_id) = &directory.parent_id {
            if parent_id == &directory.id || !ids.contains(parent_id) {
                return Err(ApiError::bad_request("invalid parent directory"));
            }
        }
    }
    organization.file_locations.retain(|key, directory_id| {
        validate_key(key).is_ok() && directory_id.as_ref().is_none_or(|id| ids.contains(id))
    });
    Ok(organization)
}
