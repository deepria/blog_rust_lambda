//! Object-storage boundary for file use cases.
//!
//! `files` depends on this module rather than the S3 SDK adapter directly, so a
//! test double or a different provider can be introduced without changing file
//! authorization and organization logic.
pub(super) use crate::s3::{
    abort_multipart_upload, build_key, complete_multipart_upload, create_multipart_upload,
    delete_object, list_objects, presign_delete, presign_download, presign_upload,
    presign_upload_part,
};
