//! Compile-time stub for builds without portable node identity support.
//!
//! The type surface is kept available because the daemon composition layer is
//! shared by all build profiles. Any attempt to perform identity cryptography in
//! a build without the `identity` feature fails explicitly.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    io,
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub struct IdentityCredential {
    pub identity_id: String,
    pub device_id: String,
    pub public_key: String,
}

impl fmt::Debug for IdentityCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdentityCredential")
            .field("identity_id", &self.identity_id)
            .field("device_id", &self.device_id)
            .field("public_key", &self.public_key)
            .finish()
    }
}

#[derive(Debug)]
pub enum IdentityFileError {
    FeatureDisabled,
    Io(io::Error),
}

impl Display for IdentityFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::FeatureDisabled => {
                f.write_str("portable identity support is not compiled into this og-core build")
            }
            Self::Io(error) => write!(f, "identity file I/O error: {error}"),
        }
    }
}

impl Error for IdentityFileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::FeatureDisabled => None,
        }
    }
}

impl From<io::Error> for IdentityFileError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl IdentityCredential {
    pub fn generate() -> Result<Self, IdentityFileError> {
        Err(IdentityFileError::FeatureDisabled)
    }
    pub fn renew(_identity_id: impl Into<String>) -> Result<Self, IdentityFileError> {
        Err(IdentityFileError::FeatureDisabled)
    }
    #[must_use]
    pub fn sign_base64(&self, _message: &[u8]) -> String {
        String::new()
    }
}

pub fn load(_path: &Path, _password: &[u8]) -> Result<IdentityCredential, IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn save(
    _path: &Path,
    _credential: &IdentityCredential,
    _password: &[u8],
) -> Result<(), IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn stage(
    _destination: &Path,
    _credential: &IdentityCredential,
    _password: &[u8],
) -> Result<PathBuf, IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn commit(_staged: &Path, _destination: &Path) -> Result<(), IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn copy_encrypted(
    _source: &Path,
    _destination: &Path,
    _password: &[u8],
) -> Result<(), IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn encrypt_bytes(
    _credential: &IdentityCredential,
    _password: &[u8],
) -> Result<Vec<u8>, IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}

pub fn decrypt_bytes(
    _bytes: &[u8],
    _password: &[u8],
) -> Result<IdentityCredential, IdentityFileError> {
    Err(IdentityFileError::FeatureDisabled)
}
