#![cfg_attr(rustfmt, rustfmt_skip)]
//! The manifest: the durable list of live segments, their deleted rows and
//! per-field facts, at one generation. Replaced atomically (write, sync,
//! rename, sync directory); the rename is the commit point of a flush.

use std::{collections::BTreeMap, fs::File, io::Write, path::Path, sync::Arc};

use super::{
    codec::{checksum, corrupt, put_bytes, put_u64, put_u8, put_var, Reader},
    column::FACTS,
    segment::io_error,
    GLACIER_FORMAT_VERSION,
};
use crate::{model::FieldName, storage::{CollectionId, StorageError, StorageResult}};

const MAGIC: [u8; 8] = *b"OG6MANI1";
const VERSION: u8 = 1;
pub(super) const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";

/// Deleted rows of one segment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct DeadRows {
    words: Vec<u64>,
    count: u64,
}

impl DeadRows {
    #[must_use]
    pub(super) fn with_rows(rows: u64) -> Self { Self { words: vec![0; rows.div_ceil(64) as usize], count: 0 } }

    #[inline]
    #[must_use]
    pub(super) fn contains(&self, row: u64) -> bool {
        self.words.get((row / 64) as usize).is_some_and(|word| word & (1 << (row % 64)) != 0)
    }

    /// Marks a row; returns `false` when it was already dead.
    pub(super) fn insert(&mut self, row: u64) -> bool {
        let Some(word) = self.words.get_mut((row / 64) as usize) else { return false };
        let bit = 1 << (row % 64);
        if *word & bit != 0 {
            return false;
        }
        *word |= bit;
        self.count += 1;
        true
    }

    #[must_use]
    pub(super) const fn count(&self) -> u64 { self.count }

    /// Adds the rows of `other`, a bitmap of the same segment.
    pub(super) fn add(&mut self, other: &Self) {
        for (word, more) in self.words.iter_mut().zip(&other.words) {
            self.count += u64::from((more & !*word).count_ones());
            *word |= more;
        }
    }

    /// The bitmap words, little endian (for the history baseline).
    #[must_use]
    pub(super) fn to_bytes(&self) -> Vec<u8> { self.words.iter().flat_map(|word| word.to_le_bytes()).collect() }

    /// Reads [`Self::to_bytes`] for a segment of `rows` rows.
    #[must_use]
    pub(super) fn from_bytes(bytes: &[u8], rows: u64) -> Self {
        let mut words = bytes.chunks_exact(8).map(|chunk| u64::from_le_bytes(chunk.try_into().expect("8 bytes"))).collect::<Vec<_>>();
        words.resize(rows.div_ceil(64) as usize, 0);
        let count = words.iter().map(|word| u64::from(word.count_ones())).sum();
        Self { words, count }
    }
}

/// One segment entry of the manifest.
#[derive(Clone, Debug)]
pub(super) struct ManifestSegment {
    pub(super) number: u64,
    pub(super) dead: Option<Arc<DeadRows>>,
}

#[derive(Clone, Debug)]
pub(super) struct ManifestCollection {
    pub(super) name: CollectionId,
    pub(super) segments: Vec<ManifestSegment>,
    pub(super) facts: BTreeMap<FieldName, [u64; FACTS]>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Manifest {
    pub(super) generation: u64,
    pub(super) next_file: u64,
    pub(super) wal_sequence: u64,
    pub(super) collections: Vec<ManifestCollection>,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        put_u8(&mut body, VERSION);
        put_u64(&mut body, self.generation);
        put_u64(&mut body, self.next_file);
        put_u64(&mut body, self.wal_sequence);
        put_var(&mut body, self.collections.len() as u64);
        for collection in &self.collections {
            put_bytes(&mut body, collection.name.as_str().as_bytes());
            put_var(&mut body, collection.segments.len() as u64);
            for segment in &collection.segments {
                put_var(&mut body, segment.number);
                match segment.dead.as_deref().filter(|dead| dead.count > 0) {
                    None => put_u8(&mut body, 0),
                    Some(dead) => {
                        put_u8(&mut body, 1);
                        put_var(&mut body, dead.words.len() as u64);
                        // Sparse words: (gap, word) pairs.
                        let mut last = 0usize;
                        let nonzero = dead.words.iter().filter(|word| **word != 0).count();
                        put_var(&mut body, nonzero as u64);
                        for (index, word) in dead.words.iter().enumerate().filter(|(_, word)| **word != 0) {
                            put_var(&mut body, (index - last) as u64);
                            put_u64(&mut body, *word);
                            last = index;
                        }
                    }
                }
            }
            put_var(&mut body, collection.facts.len() as u64);
            for (field, counts) in &collection.facts {
                put_bytes(&mut body, field.as_str().as_bytes());
                for count in counts {
                    put_var(&mut body, *count);
                }
            }
        }
        let mut out = Vec::with_capacity(16 + body.len());
        out.extend_from_slice(&MAGIC);
        put_u64(&mut out, checksum(&body));
        out.extend_from_slice(&body);
        out
    }

    fn decode(bytes: &[u8]) -> StorageResult<Self> {
        if bytes.len() >= 8 && bytes[..8] != MAGIC {
            // `OG<n>MANI…`: a store of another Glacier format, said as such rather than as damage.
            if bytes[..2] == *b"OG" && bytes[3..7] == *b"MANI" && bytes[2].is_ascii_digit() && bytes[2] != MAGIC[2] {
                return Err(StorageError::backend(format!(
                    "Glacier store of format {}: this ogd reads format {GLACIER_FORMAT_VERSION} only",
                    char::from(bytes[2])
                )));
            }
        }
        if bytes.len() < 16 || bytes[..8] != MAGIC {
            return Err(corrupt("manifest magic"));
        }
        let expected = u64::from_le_bytes(bytes[8..16].try_into().map_err(|_| corrupt("manifest checksum"))?);
        let body = &bytes[16..];
        if checksum(body) != expected {
            return Err(corrupt("manifest checksum"));
        }
        let mut reader = Reader::new(body);
        let version = reader.u8()?;
        if version > VERSION {
            return Err(StorageError::backend(format!(
                "Glacier manifest version {version} was written by a newer ogd (this one reads up to {VERSION}): upgrade ogd or restore a backup"
            )));
        }
        if version != VERSION {
            return Err(corrupt("manifest version"));
        }
        let generation = reader.u64()?;
        let next_file = reader.u64()?;
        let wal_sequence = reader.u64()?;
        let count = reader.len()?;
        let mut collections = Vec::with_capacity(count.min(body.len()));
        for _ in 0..count {
            let name = CollectionId::parse(reader.str()?).map_err(|_| corrupt("manifest collection"))?;
            let segment_count = reader.len()?;
            let mut segments = Vec::with_capacity(segment_count.min(body.len()));
            for _ in 0..segment_count {
                let number = reader.var()?;
                let dead = match reader.u8()? {
                    0 => None,
                    1 => {
                        let len = reader.len()?;
                        if len > body.len() * 64 {
                            return Err(corrupt("dead rows length"));
                        }
                        let mut dead = DeadRows { words: vec![0; len], count: 0 };
                        let nonzero = reader.len()?;
                        let mut index = 0usize;
                        for _ in 0..nonzero {
                            index += reader.len()?;
                            let word = reader.u64()?;
                            *dead.words.get_mut(index).ok_or_else(|| corrupt("dead rows word"))? = word;
                            dead.count += u64::from(word.count_ones());
                        }
                        Some(Arc::new(dead))
                    }
                    _ => return Err(corrupt("dead rows mode")),
                };
                segments.push(ManifestSegment { number, dead });
            }
            let field_count = reader.len()?;
            let mut facts = BTreeMap::new();
            for _ in 0..field_count {
                let field = FieldName::from(reader.str()?);
                let mut counts = [0u64; FACTS];
                for count in &mut counts {
                    *count = reader.var()?;
                }
                facts.insert(field, counts);
            }
            collections.push(ManifestCollection { name, segments, facts });
        }
        Ok(Self { generation, next_file, wal_sequence, collections })
    }

    pub(super) fn load(dir: &Path) -> StorageResult<Option<Self>> {
        let path = dir.join(MANIFEST);
        match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error("read manifest", &path)(error)),
        }
    }

    pub(super) fn store(&self, dir: &Path) -> StorageResult<()> {
        let tmp = dir.join(MANIFEST_TMP);
        let mut file = File::create(&tmp).map_err(io_error("create manifest", &tmp))?;
        file.write_all(&self.encode()).and_then(|()| file.sync_all()).map_err(io_error("write manifest", &tmp))?;
        drop(file);
        let path = dir.join(MANIFEST);
        std::fs::rename(&tmp, &path).map_err(io_error("install manifest", &path))?;
        sync_dir(dir)
    }
}

pub(super) fn sync_dir(dir: &Path) -> StorageResult<()> {
    #[cfg(unix)]
    File::open(dir).and_then(|file| file.sync_all()).map_err(io_error("sync directory", dir))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn manifest_round_trips() { let dir = std::env::temp_dir().join(format!("og-g6-manifest-{}", std::process::id())); let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap(); let mut dead = DeadRows::with_rows(1000); assert!(dead.insert(3)); assert!(!dead.insert(3)); assert!(dead.insert(999)); let mut facts = BTreeMap::new(); facts.insert(FieldName::from("a"), [1, 2, 3, 4, 5, 6]); let manifest = Manifest { generation: 7, next_file: 9, wal_sequence: 4, collections: vec![ManifestCollection { name: CollectionId::parse("ana").unwrap(), segments: vec![ManifestSegment { number: 1, dead: Some(Arc::new(dead.clone())) }, ManifestSegment { number: 2, dead: None }], facts }] }; manifest.store(&dir).unwrap(); let loaded = Manifest::load(&dir).unwrap().unwrap(); assert_eq!((loaded.generation, loaded.next_file, loaded.wal_sequence), (7, 9, 4)); assert_eq!(loaded.collections[0].segments[0].dead.as_deref(), Some(&dead)); assert!(loaded.collections[0].segments[1].dead.is_none()); assert_eq!(loaded.collections[0].facts[&FieldName::from("a")], [1, 2, 3, 4, 5, 6]); std::fs::remove_dir_all(&dir).unwrap(); }

    #[test] fn another_or_a_newer_format_is_said_as_such_not_as_damage() {
        let encoded = Manifest { generation: 1, next_file: 1, wal_sequence: 1, collections: Vec::new() }.encode();
        let message = |bytes: &[u8]| { let Err(error) = Manifest::decode(bytes) else { panic!("refused") }; error.to_string() };
        let mut other = encoded.clone();
        other[2] = b'7';
        assert!(message(&other).contains("format 7: this ogd reads format 6 only"), "{}", message(&other));
        let mut newer = encoded.clone();
        newer[16] = VERSION + 1;
        let sum = checksum(&newer[16..]);
        newer[8..16].copy_from_slice(&sum.to_le_bytes());
        assert!(message(&newer).contains("written by a newer ogd"), "{}", message(&newer));
        let mut damaged = encoded;
        damaged[0] = b'X';
        assert!(message(&damaged).contains("corrupt data: manifest magic"));
        let dir = std::env::temp_dir().join(format!("og-g6-format5-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("data.og");
        std::fs::write(&store, b"OGGLACR\0rest of a format 5 store").unwrap();
        let Err(refused) = super::super::GlacierBackend::open(&store) else { panic!("refused") };
        let refused = refused.to_string();
        assert!(refused.contains("Glacier store of format 5 (ogd 1.x)"), "{refused}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
