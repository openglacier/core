//! Compact per-connection authentication state.

use std::{fs::File, io::Read, time::Duration};

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::Serialize;

use crate::helpers::{decode_base64, encode_base64, u128_to_u64_saturating, unix_time_millis};

/// Default lifetime of one authentication challenge.
pub const DEFAULT_CHALLENGE_TTL: Duration = Duration::from_secs(30);

/// Principal attached to one daemon connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Principal {
    /// Compatibility principal used until authorization is enabled.
    Anonymous,
    /// Authenticated identity and device.
    Identity {
        identity_id: String,
        device_id: String,
    },
}

impl Default for Principal {
    fn default() -> Self {
        Self::Anonymous
    }
}

/// Public device material loaded from `_devices`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCredential {
    pub identity_id: String,
    pub device_id: String,
    pub public_key: String,
    pub algorithm: String,
    pub encoding: String,
    pub active: bool,
}

pub use crate::error::AuthError;

#[derive(Debug, Clone)]
struct PendingChallenge {
    id: String,
    identity_id: String,
    device_id: String,
    bytes: [u8; 32],
    expires_at: u64,
}

#[derive(Debug, Clone)]
struct PendingEnrollment {
    id: String,
    identity_id: String,
    identity_public_key: String,
    device_id: String,
    device_public_key: String,
    bytes: [u8; 32],
    expires_at: u64,
}

/// Enrollment data proven by one Ed25519 device signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentIdentity {
    pub identity_id: String,
    pub identity_public_key: String,
    pub device_id: String,
    pub device_public_key: String,
}

/// Authentication state owned by one socket connection.
#[derive(Debug, Default)]
pub struct ConnectionAuth {
    principal: Principal,
    pending: Option<PendingChallenge>,
    enrollment: Option<PendingEnrollment>,
}

impl ConnectionAuth {
    /// Creates trusted connection authentication state for a Gateway-delegated node channel.
    /// The Gateway may only use this on an already authenticated Hub session.
    #[must_use]
    pub fn from_principal(principal: Principal) -> Self {
        Self {
            principal,
            pending: None,
            enrollment: None,
        }
    }

    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Promotes the connection after a credential was verified by another core-owned mechanism.
    pub fn establish(&mut self, credential: &DeviceCredential) -> Result<Principal, AuthError> {
        if !credential.active {
            return Err(AuthError::DeviceMismatch);
        }
        if credential.algorithm != "ed25519" {
            return Err(AuthError::UnsupportedAlgorithm(
                credential.algorithm.clone(),
            ));
        }
        if credential.encoding != "spki-der" {
            return Err(AuthError::UnsupportedEncoding(credential.encoding.clone()));
        }
        let principal = Principal::Identity {
            identity_id: credential.identity_id.clone(),
            device_id: credential.device_id.clone(),
        };
        self.pending = None;
        self.enrollment = None;
        self.principal = principal.clone();
        Ok(principal)
    }

    #[must_use]
    pub fn pending_subject(&self) -> Option<(String, String)> {
        self.pending
            .as_ref()
            .map(|pending| (pending.identity_id.clone(), pending.device_id.clone()))
    }

    /// Starts one challenge for a known active device.
    pub fn begin(
        &mut self,
        credential: &DeviceCredential,
        ttl: Duration,
    ) -> Result<AuthChallenge, AuthError> {
        if !credential.active {
            return Err(AuthError::DeviceMismatch);
        }
        if credential.algorithm != "ed25519" {
            return Err(AuthError::UnsupportedAlgorithm(
                credential.algorithm.clone(),
            ));
        }
        if credential.encoding != "spki-der" {
            return Err(AuthError::UnsupportedEncoding(credential.encoding.clone()));
        }

        let mut bytes = [0u8; 32];
        File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut bytes))
            .map_err(|error| AuthError::Random(error.to_string()))?;
        let expires_at = unix_time_millis().saturating_add(u128_to_u64_saturating(ttl.as_millis()));
        let id = hex(&bytes[..16]);
        self.pending = Some(PendingChallenge {
            id: id.clone(),
            identity_id: credential.identity_id.clone(),
            device_id: credential.device_id.clone(),
            bytes,
            expires_at,
        });
        Ok(AuthChallenge {
            challenge_id: id,
            challenge: encode_base64(&bytes),
            expires_at,
        })
    }

    /// Starts a stateless enrollment challenge.
    pub fn begin_enrollment(
        &mut self,
        identity_id: String,
        identity_public_key: String,
        device_id: String,
        device_public_key: String,
        ttl: Duration,
    ) -> Result<AuthChallenge, AuthError> {
        // V1 uses one Ed25519 key pair for the identity and its first device.
        if identity_public_key != device_public_key {
            return Err(AuthError::DeviceMismatch);
        }
        let mut bytes = [0u8; 32];
        File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut bytes))
            .map_err(|error| AuthError::Random(error.to_string()))?;
        let expires_at = unix_time_millis().saturating_add(u128_to_u64_saturating(ttl.as_millis()));
        let id = hex(&bytes[..16]);
        self.enrollment = Some(PendingEnrollment {
            id: id.clone(),
            identity_id,
            identity_public_key,
            device_id,
            device_public_key,
            bytes,
            expires_at,
        });
        Ok(AuthChallenge {
            challenge_id: id,
            challenge: encode_base64(&bytes),
            expires_at,
        })
    }

    /// Completes enrollment by proving possession of the device private key.
    pub fn complete_enrollment(
        &mut self,
        challenge_id: &str,
        signature: &str,
    ) -> Result<EnrollmentIdentity, AuthError> {
        let pending = self
            .enrollment
            .take()
            .ok_or(AuthError::NoPendingChallenge)?;
        if pending.id != challenge_id {
            return Err(AuthError::ChallengeMismatch);
        }
        if unix_time_millis() > pending.expires_at {
            return Err(AuthError::ChallengeExpired);
        }
        verify_ed25519(&pending.device_public_key, signature, &pending.bytes)?;
        Ok(EnrollmentIdentity {
            identity_id: pending.identity_id,
            identity_public_key: pending.identity_public_key,
            device_id: pending.device_id,
            device_public_key: pending.device_public_key,
        })
    }

    /// Verifies the signature and promotes the connection principal.
    pub fn complete(
        &mut self,
        challenge_id: &str,
        signature: &str,
        credential: &DeviceCredential,
    ) -> Result<Principal, AuthError> {
        let pending = self.pending.take().ok_or(AuthError::NoPendingChallenge)?;
        if pending.id != challenge_id {
            return Err(AuthError::ChallengeMismatch);
        }
        if unix_time_millis() > pending.expires_at {
            return Err(AuthError::ChallengeExpired);
        }
        if pending.identity_id != credential.identity_id
            || pending.device_id != credential.device_id
            || !credential.active
        {
            return Err(AuthError::DeviceMismatch);
        }
        verify_ed25519(&credential.public_key, signature, &pending.bytes)?;
        let principal = Principal::Identity {
            identity_id: pending.identity_id,
            device_id: pending.device_id,
        };
        self.principal = principal.clone();
        Ok(principal)
    }
}

/// Challenge returned by `auth.begin`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthChallenge {
    pub challenge_id: String,
    pub challenge: String,
    #[serde(serialize_with = "crate::protocol::serialize_js_safe_u64")]
    pub expires_at: u64,
}

pub fn validate_ed25519_public_key(public_key: &str) -> Result<(), AuthError> {
    parse_ed25519_public_key(public_key).map(drop)
}

fn parse_ed25519_public_key(public_key: &str) -> Result<VerifyingKey, AuthError> {
    let der = decode_base64(public_key).map_err(|_| AuthError::InvalidBase64)?;
    const PREFIX: &[u8] = &[
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    let raw = der
        .strip_prefix(PREFIX)
        .ok_or(AuthError::InvalidPublicKey)?;
    let key_bytes: [u8; 32] = raw.try_into().map_err(|_| AuthError::InvalidPublicKey)?;
    VerifyingKey::from_bytes(&key_bytes).map_err(|_| AuthError::InvalidPublicKey)
}

fn verify_ed25519(public_key: &str, signature: &str, message: &[u8]) -> Result<(), AuthError> {
    let key = parse_ed25519_public_key(public_key)?;
    let signature_bytes = decode_base64(signature).map_err(|_| AuthError::InvalidBase64)?;
    let signature =
        Signature::from_slice(&signature_bytes).map_err(|_| AuthError::InvalidSignature)?;
    key.verify(message, &signature)
        .map_err(|_| AuthError::InvalidSignature)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trip_is_stable() {
        let input = b"openglacier authentication";
        assert_eq!(decode_base64(&encode_base64(input)).unwrap(), input);
    }

    #[test]
    fn challenge_is_bound_to_identity_and_device() {
        let credential = DeviceCredential {
            identity_id: "identity-a".to_owned(),
            device_id: "device-a".to_owned(),
            public_key: "unused".to_owned(),
            algorithm: "ed25519".to_owned(),
            encoding: "spki-der".to_owned(),
            active: true,
        };
        let mut auth = ConnectionAuth::default();
        let challenge = auth.begin(&credential, Duration::from_secs(1)).unwrap();
        assert!(!challenge.challenge_id.is_empty());
        assert_eq!(decode_base64(&challenge.challenge).unwrap().len(), 32);
        assert_eq!(auth.principal(), &Principal::Anonymous);
    }

    #[test]
    fn enrollment_challenge_keeps_stateless_subject_until_completion() {
        let mut auth = ConnectionAuth::default();

        let challenge = auth
            .begin_enrollment(
                "identity-a".to_owned(),
                "shared-public-key".to_owned(),
                "device-a".to_owned(),
                "shared-public-key".to_owned(),
                Duration::from_secs(1),
            )
            .unwrap();

        assert!(!challenge.challenge_id.is_empty());
        assert_eq!(auth.principal(), &Principal::Anonymous);
    }

    #[test]
    fn challenge_expiration_is_messagepack_js_number() {
        let challenge = AuthChallenge {
            challenge_id: "challenge-a".to_owned(),
            challenge: "payload".to_owned(),
            expires_at: 1_785_680_802_608,
        };

        let encoded = rmp_serde::to_vec_named(&challenge).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();

        assert_eq!(
            decoded.get("expiresAt").and_then(serde_json::Value::as_f64),
            Some(1_785_680_802_608.0),
        );
        assert!(decoded.get("expiresAt").unwrap().as_u64().is_none());
    }

    #[test]
    fn inactive_device_cannot_start_authentication() {
        let credential = DeviceCredential {
            identity_id: "identity-a".to_owned(),
            device_id: "device-a".to_owned(),
            public_key: "unused".to_owned(),
            algorithm: "ed25519".to_owned(),
            encoding: "spki-der".to_owned(),
            active: false,
        };
        let error = ConnectionAuth::default()
            .begin(&credential, Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error, AuthError::DeviceMismatch);
    }
}
