//! Encrypted portable identity credentials.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};

use crate::{
    helpers::{decode_base64, encode_base64},
    storage::UuidV7Generator,
};

const FILE_VERSION: u16 = 2;
const KDF: &str = "argon2id";
const AEAD: &str = "xchacha20-poly1305";
const SALT_BYTES: usize = 16;
const NONCE_BYTES: usize = 24;
const KEY_BYTES: usize = 32;
const MIN_PASSWORD_BYTES: usize = 12;
const AAD: &[u8] = b"og.identity.v2";
const ARGON_MEMORY_KIB: u32 = 64 * 1024;
const ARGON_ITERATIONS: u32 = 3;
const ARGON_LANES: u32 = 1;

#[derive(Clone)]
pub struct IdentityCredential {
    pub identity_id: String,
    pub device_id: String,
    pub public_key: String,
    seed: [u8; 32],
}

impl fmt::Debug for IdentityCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdentityCredential")
            .field("identity_id", &self.identity_id)
            .field("device_id", &self.device_id)
            .field("public_key", &self.public_key)
            .field("seed", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IdentityPayload {
    identity_id: String,
    device_id: String,
    private_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EncryptedIdentityFile {
    version: u16,
    kdf: String,
    aead: String,
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug)]
pub enum IdentityFileError {
    Io(io::Error),
    Json(serde_json::Error),
    InvalidFormat(&'static str),
    InvalidPassword,
    WeakPassword,
    Crypto,
}

impl Display for IdentityFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "identity file I/O error: {error}"),
            Self::Json(error) => write!(f, "invalid identity file: {error}"),
            Self::InvalidFormat(message) => write!(f, "invalid identity file: {message}"),
            Self::InvalidPassword => f.write_str("invalid identity password or corrupted file"),
            Self::WeakPassword => f.write_str("identity password must contain at least 12 bytes"),
            Self::Crypto => f.write_str("identity cryptography failed"),
        }
    }
}

impl Error for IdentityFileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for IdentityFileError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for IdentityFileError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl IdentityCredential {
    pub fn generate() -> Result<Self, IdentityFileError> {
        let ids = UuidV7Generator::new();
        Self::generate_for(ids.next_id().to_string(), ids.next_id().to_string())
    }

    pub fn renew(identity_id: impl Into<String>) -> Result<Self, IdentityFileError> {
        let ids = UuidV7Generator::new();
        Self::generate_for(identity_id.into(), ids.next_id().to_string())
    }

    fn generate_for(identity_id: String, device_id: String) -> Result<Self, IdentityFileError> {
        let mut seed = [0u8; 32];
        random_bytes(&mut seed)?;
        let signing = SigningKey::from_bytes(&seed);
        let mut spki = vec![
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        spki.extend_from_slice(&signing.verifying_key().to_bytes());
        Ok(Self {
            identity_id,
            device_id,
            public_key: encode_base64(&spki),
            seed,
        })
    }

    #[must_use]
    pub fn sign_base64(&self, message: &[u8]) -> String {
        encode_base64(&SigningKey::from_bytes(&self.seed).sign(message).to_bytes())
    }
}

pub fn load(path: &Path, password: &[u8]) -> Result<IdentityCredential, IdentityFileError> {
    let bytes = fs::read(path)?;
    decrypt_bytes(&bytes, password)
}

pub fn save(
    path: &Path,
    credential: &IdentityCredential,
    password: &[u8],
) -> Result<(), IdentityFileError> {
    let bytes = encrypt_bytes(credential, password)?;
    write_private(path, &bytes)
}

pub fn stage(
    destination: &Path,
    credential: &IdentityCredential,
    password: &[u8],
) -> Result<PathBuf, IdentityFileError> {
    let mut staged = destination.as_os_str().to_os_string();
    staged.push(".next");
    let staged = PathBuf::from(staged);
    save(&staged, credential, password)?;
    Ok(staged)
}

pub fn commit(staged: &Path, destination: &Path) -> Result<(), IdentityFileError> {
    fs::rename(staged, destination)?;
    Ok(())
}

pub fn copy_encrypted(
    source: &Path,
    destination: &Path,
    password: &[u8],
) -> Result<(), IdentityFileError> {
    let bytes = fs::read(source)?;
    // Export is password-gated: authenticate/decrypt the envelope before copying
    // the original ciphertext verbatim. The private key never leaves og-core.
    decrypt_bytes(&bytes, password)?;
    write_private(destination, &bytes)
}

pub fn encrypt_bytes(
    credential: &IdentityCredential,
    password: &[u8],
) -> Result<Vec<u8>, IdentityFileError> {
    if password.len() < MIN_PASSWORD_BYTES {
        return Err(IdentityFileError::WeakPassword);
    }
    let mut salt = [0u8; SALT_BYTES];
    let mut nonce = [0u8; NONCE_BYTES];
    random_bytes(&mut salt)?;
    random_bytes(&mut nonce)?;
    let key = derive_key(password, &salt)?;
    let plaintext = rmp_serde::to_vec_named(&IdentityPayload {
        identity_id: credential.identity_id.clone(),
        device_id: credential.device_id.clone(),
        private_key: encode_base64(&credential.seed),
    })
    .map_err(|_| IdentityFileError::Crypto)?;
    let ciphertext = XChaCha20Poly1305::new_from_slice(&key)
        .map_err(|_| IdentityFileError::Crypto)?
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: AAD,
            },
        )
        .map_err(|_| IdentityFileError::Crypto)?;
    Ok(serde_json::to_vec_pretty(&EncryptedIdentityFile {
        version: FILE_VERSION,
        kdf: KDF.to_owned(),
        aead: AEAD.to_owned(),
        salt: encode_base64(&salt),
        nonce: encode_base64(&nonce),
        ciphertext: encode_base64(&ciphertext),
    })?)
}

pub fn decrypt_bytes(
    bytes: &[u8],
    password: &[u8],
) -> Result<IdentityCredential, IdentityFileError> {
    let envelope: EncryptedIdentityFile = serde_json::from_slice(bytes)?;
    validate_envelope(&envelope)?;
    let salt = decode_fixed::<SALT_BYTES>(&envelope.salt, "invalid salt")?;
    let nonce = decode_fixed::<NONCE_BYTES>(&envelope.nonce, "invalid nonce")?;
    let ciphertext = decode_base64(&envelope.ciphertext)
        .map_err(|_| IdentityFileError::InvalidFormat("invalid ciphertext"))?;
    let key = derive_key(password, &salt)?;
    let plaintext = XChaCha20Poly1305::new_from_slice(&key)
        .map_err(|_| IdentityFileError::Crypto)?
        .decrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: AAD,
            },
        )
        .map_err(|_| IdentityFileError::InvalidPassword)?;
    let payload: IdentityPayload = rmp_serde::from_slice(&plaintext)
        .map_err(|_| IdentityFileError::InvalidFormat("invalid encrypted payload"))?;
    let seed = decode_fixed::<32>(&payload.private_key, "invalid private key")?;
    let signing = SigningKey::from_bytes(&seed);
    let mut spki = vec![
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    spki.extend_from_slice(&signing.verifying_key().to_bytes());
    Ok(IdentityCredential {
        identity_id: payload.identity_id,
        device_id: payload.device_id,
        public_key: encode_base64(&spki),
        seed,
    })
}

fn validate_envelope(file: &EncryptedIdentityFile) -> Result<(), IdentityFileError> {
    if file.version != FILE_VERSION {
        return Err(IdentityFileError::InvalidFormat("unsupported version"));
    }
    if file.kdf != KDF {
        return Err(IdentityFileError::InvalidFormat("unsupported KDF"));
    }
    if file.aead != AEAD {
        return Err(IdentityFileError::InvalidFormat("unsupported cipher"));
    }
    Ok(())
}

fn derive_key(password: &[u8], salt: &[u8]) -> Result<[u8; KEY_BYTES], IdentityFileError> {
    let params = Params::new(
        ARGON_MEMORY_KIB,
        ARGON_ITERATIONS,
        ARGON_LANES,
        Some(KEY_BYTES),
    )
    .map_err(|_| IdentityFileError::Crypto)?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; KEY_BYTES];
    argon
        .hash_password_into(password, salt, &mut key)
        .map_err(|_| IdentityFileError::Crypto)?;
    Ok(key)
}

fn decode_fixed<const N: usize>(
    value: &str,
    message: &'static str,
) -> Result<[u8; N], IdentityFileError> {
    decode_base64(value)
        .map_err(|_| IdentityFileError::InvalidFormat(message))?
        .try_into()
        .map_err(|_| IdentityFileError::InvalidFormat(message))
}

fn random_bytes(output: &mut [u8]) -> Result<(), IdentityFileError> {
    File::open("/dev/urandom")?.read_exact(output)?;
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), IdentityFileError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_identity_round_trips() {
        let identity = IdentityCredential::generate().unwrap();
        let bytes = encrypt_bytes(&identity, b"correct horse battery staple").unwrap();
        let decoded = decrypt_bytes(&bytes, b"correct horse battery staple").unwrap();
        assert_eq!(decoded.identity_id, identity.identity_id);
        assert_eq!(decoded.device_id, identity.device_id);
        assert_eq!(decoded.public_key, identity.public_key);
        assert_eq!(
            decoded.sign_base64(b"challenge"),
            identity.sign_base64(b"challenge")
        );
    }

    #[test]
    fn encrypted_export_requires_correct_password() {
        let identity = IdentityCredential::generate().unwrap();
        let unique = UuidV7Generator::new().next_id().to_string();
        let source = std::env::temp_dir().join(format!("og-{unique}.ogid"));
        let destination = std::env::temp_dir().join(format!("og-{unique}-copy.ogid"));
        save(&source, &identity, b"correct horse battery staple").unwrap();

        assert!(matches!(
            copy_encrypted(&source, &destination, b"wrong password"),
            Err(IdentityFileError::InvalidPassword)
        ));
        assert!(!destination.exists());

        copy_encrypted(&source, &destination, b"correct horse battery staple").unwrap();
        assert_eq!(fs::read(&source).unwrap(), fs::read(&destination).unwrap());

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }

    #[test]
    fn wrong_password_is_rejected() {
        let identity = IdentityCredential::generate().unwrap();
        let bytes = encrypt_bytes(&identity, b"good password").unwrap();
        assert!(matches!(
            decrypt_bytes(&bytes, b"bad password"),
            Err(IdentityFileError::InvalidPassword)
        ));
    }
}
