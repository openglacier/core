//! Backend-independent, time-ordered document identifiers.

use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::helpers::{u64_to_usize_saturating, unix_time_millis};

use super::{StorageError, StorageResult};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentId([u8; 16]);

impl DocumentId {
    pub const BYTE_LEN: usize = 16;

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Returns the canonical textual representation.
    ///
    /// This intentionally returns an owned `String`: the identifier is stored
    /// internally as 16 binary bytes, so no borrowed `&str` exists. The method
    /// is kept as a source-compatible bridge for callers that previously used
    /// `DocumentId::as_str()`. Hot storage and lookup paths must use `as_bytes()`.
    #[must_use]
    pub fn as_str(&self) -> String {
        self.to_string()
    }

    pub fn parse(value: impl AsRef<str>) -> StorageResult<Self> {
        let text = value.as_ref();
        let mut hex = [0u8; 32];
        let mut n = 0usize;
        for byte in text.bytes() {
            if byte == b'-' {
                continue;
            }
            if n == hex.len() || !byte.is_ascii_hexdigit() {
                return Err(StorageError::invalid_document_id(
                    text,
                    "expected a UUID v7",
                ));
            }
            hex[n] = byte;
            n += 1;
        }
        if n != 32 {
            return Err(StorageError::invalid_document_id(
                text,
                "expected 32 hexadecimal digits",
            ));
        }
        let mut bytes = [0u8; 16];
        for index in 0..16 {
            bytes[index] = (decode_hex(hex[index * 2]).unwrap() << 4)
                | decode_hex(hex[index * 2 + 1]).unwrap();
        }
        if bytes[6] >> 4 != 0x7 || bytes[8] >> 6 != 0b10 {
            return Err(StorageError::invalid_document_id(
                text,
                "identifier is not a UUID v7",
            ));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn timestamp_ms(self) -> u64 {
        self.0[..6]
            .iter()
            .fold(0u64, |value, byte| (value << 8) | u64::from(*byte))
    }

    /// Builds a deterministic, valid UUID v7 for engine-produced rows such as
    /// grouped or pivoted results. This is not used for persisted documents.
    #[must_use]
    pub(crate) fn synthetic(namespace: u64, ordinal: u64) -> Self {
        build_uuid_v7(
            0,
            ordinal & 0xffff,
            mix64(namespace ^ ordinal.rotate_left(17)),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_test_label(label: &str) -> Self {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in label.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self::synthetic(0x7465_7374, hash)
    }
}

fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

impl fmt::Debug for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, byte) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl TryFrom<&str> for DocumentId {
    type Error = StorageError;
    fn try_from(v: &str) -> Result<Self, Self::Error> {
        Self::parse(v)
    }
}
impl TryFrom<String> for DocumentId {
    type Error = StorageError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::parse(v)
    }
}

pub trait DocumentIdGenerator: Send + Sync {
    fn next_id(&self) -> DocumentId {
        self.reserve(1).next().expect("one id reserved")
    }
    fn reserve(&self, count: usize) -> IdReservation;
}

#[derive(Clone, Debug)]
pub struct UuidV7Generator {
    inner: Arc<GeneratorInner>,
}
#[derive(Debug)]
struct GeneratorInner {
    state: AtomicU64,
    node: u64,
}

impl Default for UuidV7Generator {
    fn default() -> Self {
        Self::new()
    }
}
impl UuidV7Generator {
    #[must_use]
    pub fn new() -> Self {
        let now = unix_time_millis();
        Self {
            inner: Arc::new(GeneratorInner {
                state: AtomicU64::new(now << 16),
                node: random_node(),
            }),
        }
    }

    #[must_use]
    pub fn next_id(&self) -> DocumentId {
        DocumentIdGenerator::next_id(self)
    }

    #[must_use]
    pub fn reserve(&self, count: usize) -> IdReservation {
        DocumentIdGenerator::reserve(self, count)
    }
}

impl DocumentIdGenerator for UuidV7Generator {
    fn reserve(&self, count: usize) -> IdReservation {
        assert!(count > 0, "an id reservation must not be empty");
        let count = u64::try_from(count).expect("reservation too large");
        let start = loop {
            let current = self.inner.state.load(Ordering::Relaxed);
            let current_ms = current >> 16;
            let current_seq = current & 0xffff;
            let real_ms = unix_time_millis();
            let mut start_ms = current_ms.max(real_ms);
            let mut start_seq = if start_ms == current_ms {
                current_seq
            } else {
                0
            };
            if start_seq + count > 0x1_0000 {
                start_ms += 1;
                start_seq = 0;
            }
            let start = (start_ms << 16) | start_seq;
            let next = start
                .checked_add(count)
                .expect("document id state overflow");
            if self
                .inner
                .state
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break start;
            }
        };
        IdReservation {
            cursor: start,
            remaining: count,
            node: self.inner.node,
        }
    }
}

#[derive(Debug)]
pub struct IdReservation {
    cursor: u64,
    remaining: u64,
    node: u64,
}
impl Iterator for IdReservation {
    type Item = DocumentId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let logical = self.cursor;
        self.cursor += 1;
        self.remaining -= 1;
        let timestamp = logical >> 16;
        let sequence = logical & 0xffff;
        Some(build_uuid_v7(timestamp, sequence, self.node))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = u64_to_usize_saturating(self.remaining);
        (n, Some(n))
    }
}
impl ExactSizeIterator for IdReservation {}

fn build_uuid_v7(timestamp: u64, sequence: u64, node: u64) -> DocumentId {
    let mut bytes = [0u8; 16];
    let ts = timestamp.to_be_bytes();
    bytes[..6].copy_from_slice(&ts[2..]);
    let rand_a = (sequence >> 4) as u16 & 0x0fff;
    bytes[6] = 0x70 | ((rand_a >> 8) as u8);
    bytes[7] = rand_a as u8;
    let rand_b = ((node & ((1u64 << 58) - 1)) << 4) | (sequence & 0x0f);
    let rb = rand_b.to_be_bytes();
    bytes[8..].copy_from_slice(&rb);
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    DocumentId(bytes)
}
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn random_node() -> u64 {
    let mut bytes = [0u8; 8];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| {
            use std::io::Read;
            f.read_exact(&mut bytes)
        })
        .is_ok()
    {
        return u64::from_le_bytes(bytes);
    }
    let pid = u64::from(std::process::id());
    unix_time_millis().rotate_left(17) ^ pid.rotate_left(31) ^ (&bytes as *const _ as usize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_ids_are_v7_unique_and_ordered() {
        let g = UuidV7Generator::new();
        let ids: Vec<_> = g.reserve(10_000).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
        assert!(ids
            .iter()
            .all(|id| DocumentId::parse(id.to_string()).unwrap() == *id));
    }
}
