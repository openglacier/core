//! Streaming, backend-agnostic OG backup/restore format.

use crate::storage::{
    CollectionId, DocumentId, ScanOptions, StorageEngine, StorageError, StorageMutation,
    VersionPrecondition,
};
use crate::{helpers::document_to_json, query::vcollections, Document, Value};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::Arc,
};

const MAGIC: &[u8; 4] = b"OGB1";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const RESTORE_BATCH: usize = 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSummary {
    pub collections: u64,
    pub documents: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSource {
    pub instance_id: String,
    pub hostname: String,
    pub platform: String,
    pub arch: String,
    pub core_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupMetadata {
    pub created_at: u64,
    pub source: BackupSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupInfo {
    pub format: String,
    pub version: u16,
    pub created_at: u64,
    pub size_bytes: u64,
    pub collections: u64,
    pub documents: u64,
    pub source: BackupSource,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Header {
        version: u16,
        created_at: u64,
        source: BackupSource,
    },
    Collection {
        name: String,
        documents: u64,
    },
    Document {
        id: String,
        data: JsonValue,
    },
    End {
        collections: u64,
        documents: u64,
    },
}

pub use crate::error::BackupError;

pub fn create(
    storage: &dyn StorageEngine,
    path: &Path,
    metadata: BackupMetadata,
) -> Result<BackupSummary, BackupError> {
    let snapshot = storage.read()?;
    let collections = snapshot
        .collections()?
        .into_iter()
        .filter(|c| !is_virtual(c.as_str()))
        .collect::<Vec<_>>();
    let mut out = BufWriter::new(File::create(path)?);
    out.write_all(MAGIC)?;
    write_record(
        &mut out,
        &Record::Header {
            version: 1,
            created_at: metadata.created_at,
            source: metadata.source,
        },
    )?;
    let mut summary = BackupSummary::default();
    for collection in collections {
        let count = snapshot.count(&collection)?;
        write_record(
            &mut out,
            &Record::Collection {
                name: collection.to_string(),
                documents: count,
            },
        )?;
        summary.collections = summary.collections.saturating_add(1);
        let mut visitor = |stored: crate::storage::StoredDocument| {
            write_record(
                &mut out,
                &Record::Document {
                    id: stored.id().to_string(),
                    data: document_to_json(stored.document()),
                },
            )
            .map_err(|error| StorageError::backend(error.to_string()))?;
            summary.documents = summary.documents.saturating_add(1);
            Ok(true)
        };
        snapshot.scan_each(&collection, ScanOptions::default(), &mut visitor)?;
    }
    write_record(
        &mut out,
        &Record::End {
            collections: summary.collections,
            documents: summary.documents,
        },
    )?;
    out.flush()?;
    Ok(summary)
}

pub fn inspect(path: &Path) -> Result<BackupInfo, BackupError> {
    let size_bytes = std::fs::metadata(path)?.len();
    let mut reader = open_reader(path)?;
    let (version, created_at, source) = match read_record(&mut reader)? {
        Record::Header {
            version: 1,
            created_at,
            source,
        } => (1, created_at, source),
        Record::Header { version, .. } => {
            return Err(BackupError::Invalid(format!(
                "unsupported version {version}"
            )))
        }
        _ => return Err(BackupError::Invalid("missing header".into())),
    };
    let mut summary = BackupSummary::default();
    loop {
        match read_record(&mut reader)? {
            Record::Header { .. } => return Err(BackupError::Invalid("duplicate header".into())),
            Record::Collection { .. } => summary.collections += 1,
            Record::Document { .. } => summary.documents += 1,
            Record::End {
                collections,
                documents,
            } => {
                if summary.collections != collections || summary.documents != documents {
                    return Err(BackupError::Invalid(
                        "footer counters do not match stream".into(),
                    ));
                }
                return Ok(BackupInfo {
                    format: "ogb".to_owned(),
                    version,
                    created_at,
                    size_bytes,
                    collections,
                    documents,
                    source,
                });
            }
        }
    }
}

pub fn restore(
    storage: &dyn StorageEngine,
    path: &Path,
    replace: bool,
) -> Result<BackupSummary, BackupError> {
    if !replace {
        let read = storage.read()?;
        for collection in read.collections()? {
            if !is_virtual(collection.as_str()) && read.count(&collection)? != 0 {
                return Err(BackupError::Invalid(
                    "restore target is not empty; use replace=true".into(),
                ));
            }
        }
    } else {
        clear_persistent(storage)?;
    }
    let mut reader = open_reader(path)?;
    let mut current: Option<CollectionId> = None;
    let mut batch: Vec<StorageMutation> = Vec::with_capacity(RESTORE_BATCH);
    let mut summary = BackupSummary::default();
    loop {
        match read_record(&mut reader)? {
            Record::Header { version: 1, .. } => {}
            Record::Header { version, .. } => {
                return Err(BackupError::Invalid(format!(
                    "unsupported version {version}"
                )))
            }
            Record::Collection { name, .. } => {
                flush_batch(storage, current.as_ref(), &mut batch)?;
                current = Some(CollectionId::parse(name)?);
                summary.collections += 1;
            }
            Record::Document { id, data } => {
                let collection = current
                    .as_ref()
                    .ok_or_else(|| BackupError::Invalid("document before collection".into()))?;
                let document = json_to_document(data)?;
                batch.push(StorageMutation::insert(
                    DocumentId::parse(id)?,
                    Arc::new(document),
                ));
                summary.documents += 1;
                if batch.len() >= RESTORE_BATCH {
                    flush_batch(storage, Some(collection), &mut batch)?;
                }
            }
            Record::End {
                collections,
                documents,
            } => {
                flush_batch(storage, current.as_ref(), &mut batch)?;
                if summary.collections != collections || summary.documents != documents {
                    return Err(BackupError::Invalid(
                        "footer counters do not match stream".into(),
                    ));
                }
                return Ok(summary);
            }
        }
    }
}

fn clear_persistent(storage: &dyn StorageEngine) -> Result<(), BackupError> {
    loop {
        let read = storage.read()?;
        let collections = read
            .collections()?
            .into_iter()
            .filter(|c| !is_virtual(c.as_str()))
            .collect::<Vec<_>>();
        drop(read);
        let mut changed = false;
        for collection in collections {
            let read = storage.read()?;
            let docs = read.scan(
                &collection,
                ScanOptions::default().with_limit(RESTORE_BATCH),
            )?;
            drop(read);
            if docs.is_empty() {
                continue;
            }
            changed = true;
            let mut tx = storage.begin()?;
            for d in docs {
                tx.delete(&collection, d.id(), VersionPrecondition::Any)?;
            }
            tx.commit()?;
        }
        if !changed {
            return Ok(());
        }
    }
}
fn flush_batch(
    storage: &dyn StorageEngine,
    collection: Option<&CollectionId>,
    batch: &mut Vec<StorageMutation>,
) -> Result<(), BackupError> {
    if batch.is_empty() {
        return Ok(());
    }
    let c = collection.ok_or_else(|| BackupError::Invalid("missing collection".into()))?;
    storage.apply_batch_atomic_summary(c, std::mem::take(batch))?;
    Ok(())
}
fn is_virtual(name: &str) -> bool {
    vcollections::contains(name)
}
fn open_reader(path: &Path) -> Result<BufReader<File>, BackupError> {
    let mut r = BufReader::new(File::open(path)?);
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(BackupError::Invalid("bad magic".into()));
    }
    Ok(r)
}
fn write_record(w: &mut impl Write, record: &Record) -> Result<(), BackupError> {
    let bytes = rmp_serde::to_vec_named(record).map_err(BackupError::Encode)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(BackupError::Invalid("record too large".into()));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(&bytes)?;
    Ok(())
}
fn read_record(r: &mut impl Read) -> Result<Record, BackupError> {
    let mut h = [0u8; 4];
    r.read_exact(&mut h)?;
    let len = u32::from_be_bytes(h) as usize;
    if len == 0 || len > MAX_RECORD_BYTES {
        return Err(BackupError::Invalid(format!("invalid record length {len}")));
    }
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    rmp_serde::from_slice(&b).map_err(BackupError::Decode)
}
fn json_to_document(v: JsonValue) -> Result<Document, BackupError> {
    let JsonValue::Object(m) = v else {
        return Err(BackupError::Invalid(
            "document payload is not an object".into(),
        ));
    };
    let mut d = Document::new();
    for (k, v) in m {
        d.insert(k, json_to_value(v)?);
    }
    Ok(d)
}
fn json_to_value(v: JsonValue) -> Result<Value, BackupError> {
    Ok(match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(v) => Value::Bool(v),
        JsonValue::String(v) => Value::string(v),
        JsonValue::Array(v) => Value::array(
            v.into_iter()
                .map(json_to_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        JsonValue::Object(v) => Value::object(json_to_document(JsonValue::Object(v))?),
        JsonValue::Number(n) => {
            if let Some(v) = n.as_i64() {
                Value::signed(v)
            } else if let Some(v) = n.as_u64() {
                Value::unsigned(v)
            } else if let Some(v) = n.as_f64() {
                Value::float(v).map_err(|e| BackupError::Invalid(e.to_string()))?
            } else {
                return Err(BackupError::Invalid("unsupported number".into()));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    #[test]
    fn round_trip() {
        let storage = MemoryStorage::new();
        let c = CollectionId::parse("users").unwrap();
        let mut tx = storage.begin().unwrap();
        let mut d = Document::new();
        d.insert("name", Value::string("Alice"));
        tx.insert(&c, DocumentId::from_test_label("a"), Arc::new(d))
            .unwrap();
        tx.commit().unwrap();
        let p = std::env::temp_dir().join(format!("og-backup-{}.ogb", std::process::id()));
        let s = create(
            &storage,
            &p,
            BackupMetadata {
                created_at: 42,
                source: BackupSource {
                    instance_id: "instance-test".into(),
                    hostname: "test-host".into(),
                    platform: "test-os".into(),
                    arch: "test-arch".into(),
                    core_version: "0.1.0".into(),
                },
            },
        )
        .unwrap();
        assert_eq!(s.documents, 1);
        let info = inspect(&p).unwrap();
        assert_eq!(info.created_at, 42);
        assert_eq!(info.size_bytes, std::fs::metadata(&p).unwrap().len());
        assert_eq!(info.source.instance_id, "instance-test");
        let restored = MemoryStorage::new();
        restore(&restored, &p, false).unwrap();
        assert_eq!(restored.read().unwrap().count(&c).unwrap(), 1);
        let _ = std::fs::remove_file(p);
    }
}
