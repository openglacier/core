//! Bootstrap administrator creation isolated from daemon wiring.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::path::Path;
use crate::helpers::{insert_query, unix_time_millis};

use super::identity_file::{self, IdentityCredential, IdentityFileError};

#[derive(Debug)]
pub struct BootstrapAdmin {
    pub credential: IdentityCredential,
    pub created_at: u64,
}

impl BootstrapAdmin {
    pub fn generate() -> Result<Self, IdentityFileError> {
        Ok(Self {
            credential: IdentityCredential::generate()?,
            created_at: unix_time_millis(),
        })
    }

    #[must_use]
    pub fn registration_queries(&self) -> [String; 3] {
        let identity = &self.credential.identity_id;
        let device = &self.credential.device_id;
        let public = &self.credential.public_key;
        let created = self.created_at;
        [
            insert_query("_identities", serde_json::json!({"identityId": identity, "publicKey": public, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": created})),
            insert_query("_devices", serde_json::json!({"deviceId": device, "identityId": identity, "publicKey": public, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": created})),
            insert_query("_permissions", serde_json::json!({"identityId": identity, "action": "*", "resource": "*", "effect": "allow", "state": "active", "createdAt": created})),
        ]
    }

    pub fn stage( &self, path: &Path, password: &[u8], ) -> Result<std::path::PathBuf, IdentityFileError> {
        identity_file::stage(path, &self.credential, password)
    }

    pub fn commit(staged: &Path, path: &Path) -> Result<(), IdentityFileError> {
        identity_file::commit(staged, path)
    }
}
