#![cfg_attr(rustfmt, rustfmt_skip)]
//! Write-ahead log of the commits not yet folded into segments.
//!
//! ```text
//! record = payload_len u32 | checksum u64 | payload
//! payload = generation u64 | count var | entry*
//! entry   = op u8 | collection bytes | id [16] | version var | document (put only)
//! ```
//! A torn or corrupt tail record ends the replay; the file is cut there.
//! Records are only ever appended, so a crash can tear the last one alone: a
//! bad record followed by a valid later one is corruption, and the store
//! refuses to open rather than cut the valid commits behind it
//! ([`replay_store_log`]).

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{
    codec::{checksum, corrupt, decode_document, encode_document, put_bytes, put_u32, put_u64, put_u8, put_var, Reader},
    segment::io_error,
};
use crate::{model::Document, storage::{CollectionId, DocumentId, DocumentVersion, StorageResult}};

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const HEADER_BYTES: usize = 12;

/// One logged mutation.
#[derive(Clone, Debug)]
pub(super) enum LogEntry {
    Put { collection: CollectionId, id: DocumentId, version: DocumentVersion, document: Arc<Document> },
    Delete { collection: CollectionId, id: DocumentId },
}

#[must_use]
pub(super) fn wal_path(dir: &Path, sequence: u64) -> PathBuf { dir.join(wal_file_name(sequence)) }

#[must_use]
pub(super) fn wal_file_name(sequence: u64) -> String { format!("wal-{sequence:08}.log") }

/// Appends records and syncs them.
#[derive(Debug)]
pub(super) struct Wal {
    path: PathBuf,
    file: File,
    bytes: u64,
}

impl Wal {
    pub(super) fn open(path: &Path, valid_len: u64) -> StorageResult<Self> {
        let file = OpenOptions::new().create(true).read(true).write(true).truncate(false).open(path).map_err(io_error("open wal", path))?;
        file.set_len(valid_len).map_err(io_error("truncate wal", path))?;
        let mut wal = Self { path: path.to_owned(), file, bytes: valid_len };
        wal.seek_end()?;
        Ok(wal)
    }

    fn seek_end(&mut self) -> StorageResult<()> {
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(self.bytes)).map_err(io_error("seek wal", &self.path))?;
        Ok(())
    }

    #[must_use]
    pub(super) const fn bytes(&self) -> u64 { self.bytes }

    pub(super) fn append(&mut self, generation: u64, entries: &[LogEntry]) -> StorageResult<()> {
        let mut payload = Vec::with_capacity(64 + entries.len() * 128);
        put_u64(&mut payload, generation);
        put_var(&mut payload, entries.len() as u64);
        for entry in entries {
            match entry {
                LogEntry::Put { collection, id, version, document } => {
                    put_u8(&mut payload, OP_PUT);
                    put_bytes(&mut payload, collection.as_str().as_bytes());
                    payload.extend_from_slice(id.as_bytes());
                    put_var(&mut payload, version.get());
                    encode_document(&mut payload, document);
                }
                LogEntry::Delete { collection, id } => {
                    put_u8(&mut payload, OP_DELETE);
                    put_bytes(&mut payload, collection.as_str().as_bytes());
                    payload.extend_from_slice(id.as_bytes());
                    put_var(&mut payload, 0);
                }
            }
        }
        let mut record = Vec::with_capacity(HEADER_BYTES + payload.len());
        put_u32(&mut record, u32::try_from(payload.len()).map_err(|_| corrupt("wal record too large"))?);
        put_u64(&mut record, checksum(&payload));
        record.extend_from_slice(&payload);
        if let Err(error) = self.file.write_all(&record).and_then(|()| self.file.sync_data()) {
            // Drop the partial record so the log stays a valid prefix.
            let _ = self.file.set_len(self.bytes);
            let _ = self.seek_end();
            return Err(io_error("append wal", &self.path)(error));
        }
        self.bytes += record.len() as u64;
        Ok(())
    }
}

/// Reads every complete record; returns them with the valid byte length.
pub(super) fn replay(path: &Path) -> StorageResult<(Vec<(u64, Vec<LogEntry>)>, u64)> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => { file.read_to_end(&mut bytes).map_err(io_error("read wal", path))?; }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(error) => return Err(io_error("open wal", path)(error)),
    }
    let mut records = Vec::new();
    let mut at = 0usize;
    while bytes.len() - at >= HEADER_BYTES {
        let mut header = Reader::new(&bytes[at..at + HEADER_BYTES]);
        let len = header.u32()? as usize;
        let expected = header.u64()?;
        let Some(payload) = bytes.get(at + HEADER_BYTES..at + HEADER_BYTES + len) else { break };
        if checksum(payload) != expected {
            break;
        }
        let Ok(record) = decode_record(payload) else { break };
        records.push(record);
        at += HEADER_BYTES + len;
    }
    Ok((records, at as u64))
}

/// [`replay`] for the log a store opens on (commits after `base`): a bad
/// record with a valid later record behind it is an error, not a torn tail.
pub(super) fn replay_store_log(path: &Path, base: u64) -> StorageResult<(Vec<(u64, Vec<LogEntry>)>, u64)> {
    let (records, valid) = replay(path)?;
    if std::fs::metadata(path).map_or(true, |metadata| metadata.len() <= valid) {
        return Ok((records, valid));
    }
    let bytes = std::fs::read(path).map_err(io_error("read wal", path))?;
    let after = records.last().map_or(base, |record| record.0);
    if let Some((offset, generation)) = later_record(&bytes, valid as usize + 1, after) {
        return Err(corrupt(&format!(
            "wal {} is corrupt at byte {valid}: commit {generation} at byte {offset} is intact behind it (move the log aside to open the store without the commits after {after})",
            path.display()
        )));
    }
    Ok((records, valid))
}

/// The first complete, valid record at or after `from` whose generation
/// follows `after` (within what the log can hold), with its offset.
fn later_record(bytes: &[u8], from: usize, after: u64) -> Option<(usize, u64)> {
    let most = after.saturating_add(1 + (bytes.len() / HEADER_BYTES) as u64);
    for at in from..bytes.len().saturating_sub(HEADER_BYTES + 8) + 1 {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().ok()?) as usize;
        let Some(payload) = bytes.get(at + HEADER_BYTES..(at + HEADER_BYTES).checked_add(len)?) else { continue };
        if payload.len() < 8 {
            continue;
        }
        let generation = u64::from_le_bytes(payload[..8].try_into().ok()?);
        if generation <= after || generation > most {
            continue;
        }
        let expected = u64::from_le_bytes(bytes[at + 4..at + HEADER_BYTES].try_into().ok()?);
        if checksum(payload) == expected && decode_record(payload).is_ok() {
            return Some((at, generation));
        }
    }
    None
}

fn decode_record(payload: &[u8]) -> StorageResult<(u64, Vec<LogEntry>)> {
    let mut reader = Reader::new(payload);
    let generation = reader.u64()?;
    let count = reader.len()?;
    let mut entries = Vec::with_capacity(count.min(payload.len()));
    for _ in 0..count {
        let op = reader.u8()?;
        let collection = CollectionId::parse(reader.str()?).map_err(|_| corrupt("wal collection"))?;
        let id = DocumentId::from_bytes(reader.take(16)?.try_into().map_err(|_| corrupt("wal id"))?);
        let version = DocumentVersion::new(reader.var()?);
        entries.push(match op {
            OP_PUT => LogEntry::Put { collection, id, version, document: Arc::new(decode_document(&mut reader)?) },
            OP_DELETE => LogEntry::Delete { collection, id },
            _ => return Err(corrupt("wal op")),
        });
    }
    if reader.remaining() != 0 {
        return Err(corrupt("wal record trailing bytes"));
    }
    Ok((generation, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test] fn wal_replays_complete_records_and_cuts_a_torn_tail() { let dir = std::env::temp_dir().join(format!("og-g6-wal-{}", std::process::id())); let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap(); let path = wal_path(&dir, 1); let collection = CollectionId::parse("users").unwrap(); let mut document = Document::new(); document.insert("name", Value::string("Ada")); let mut wal = Wal::open(&path, 0).unwrap(); wal.append(1, &[LogEntry::Put { collection: collection.clone(), id: DocumentId::synthetic(1, 1), version: DocumentVersion::INITIAL, document: Arc::new(document) }]).unwrap(); wal.append(2, &[LogEntry::Delete { collection, id: DocumentId::synthetic(1, 1) }]).unwrap(); let full = wal.bytes(); drop(wal); let mut file = OpenOptions::new().append(true).open(&path).unwrap(); file.write_all(&[9, 0, 0, 0, 1, 2]).unwrap(); drop(file); let (records, valid) = replay(&path).unwrap(); assert_eq!(valid, full); assert_eq!(records.iter().map(|record| record.0).collect::<Vec<_>>(), vec![1, 2]); assert!(matches!(&records[0].1[0], LogEntry::Put { document, .. } if document.get("name") == Some(&Value::string("Ada")))); std::fs::remove_dir_all(&dir).unwrap(); }
}
