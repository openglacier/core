#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf, sync::Arc};

use super::{*, column::BLOCK};
use crate::{
    model::{FieldPath, Value},
    storage::{backend::memory::MemoryBackend, BlockCodes, BlockColumn, BlockGate, ProjectedBlock, ProjectedValueRef, ScanDirection, ScanOptions, StorageErrorKind, VersionPrecondition},
};

const TINY: Limits = Limits { flush_rows: 50, flush_wal_bytes: 1 << 30, direct_rows: 40, segment_rows: 64, small_segment_rows: 32, merge_small: 3 };

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("og-g6-{name}-{}-{:?}", std::process::id(), std::thread::current().id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

fn random_value(rng: &mut Rng, depth: u32) -> Value {
    match rng.below(if depth > 1 { 8 } else { 10 }) {
        0 => Value::Null,
        1 => Value::bool(rng.below(2) == 0),
        2 => Value::signed(rng.below(2000) as i64 - 1000),
        3 => Value::unsigned(rng.next()),
        4 => Value::float(rng.below(100_000) as f64 / 100.0).unwrap(),
        5 => Value::float(f64::from_bits(rng.next() >> 2)).unwrap_or(Value::Null),
        6 => Value::string(["FR", "DE", "", "é😀"][rng.below(4) as usize]),
        7 => Value::string(format!("s{}", rng.below(500))),
        8 => Value::array((0..rng.below(3)).map(|_| random_value(rng, depth + 1))),
        _ => {
            let mut inner = Document::new();
            inner.insert("x", random_value(rng, depth + 1));
            if rng.below(2) == 0 { inner.insert("y", Value::string("deep")); }
            Value::object(inner)
        }
    }
}

fn random_document(rng: &mut Rng) -> Arc<Document> {
    let mut document = Document::new();
    for name in ["a", "b", "c", "n"] {
        if rng.below(5) != 0 {
            document.insert(name, random_value(rng, 0));
        }
    }
    Arc::new(document)
}

fn paths() -> Vec<FieldPath> { ["b", "a", "n.x", "zz", "n", "n.y", "c.x.x", "a.x.y", "n.x.zz"].iter().map(|path| FieldPath::parse(path).unwrap()).collect() }

fn ref_value(value: &ProjectedValueRef<'_>) -> Value { value.to_value() }

/// Every read of `actual` must match `expected` (a memory backend).
fn assert_same(expected: &dyn StorageRead, actual: &dyn StorageRead, collections: &[CollectionId], rng: &mut Rng) {
    for collection in collections {
        let want = expected.scan(collection, ScanOptions::default()).unwrap();
        let got = actual.scan(collection, ScanOptions::default()).unwrap();
        assert_eq!(got.len(), want.len(), "{collection} scan length");
        for (got, want) in got.iter().zip(&want) {
            assert_eq!((got.id(), got.version(), got.document()), (want.id(), want.version(), want.document()));
        }
        let reverse = actual.scan(collection, ScanOptions::new().with_direction(ScanDirection::Reverse).with_limit(7)).unwrap();
        assert_eq!(reverse.iter().map(|stored| *stored.id()).collect::<Vec<_>>(), want.iter().rev().take(7).map(|stored| *stored.id()).collect::<Vec<_>>());
        assert_eq!(actual.count(collection).unwrap(), want.len() as u64);
        assert_eq!(actual.collection_exists(collection).unwrap(), !want.is_empty());
        for _ in 0..5 {
            if want.is_empty() { break; }
            let pick = &want[rng.below(want.len() as u64) as usize];
            let got = actual.get(collection, pick.id()).unwrap().unwrap();
            assert_eq!((got.version(), got.document()), (pick.version(), pick.document()));
        }
        assert!(actual.get(collection, &DocumentId::synthetic(99, rng.next())).unwrap().is_none());

        let fields = paths();
        let expected_rows = want.iter().map(|stored| (*stored.id(), fields.iter().map(|path| path.resolve_value(stored.document()).cloned()).collect::<Vec<_>>())).collect::<BTreeMap<_, _>>();
        let mut rows = BTreeMap::new();
        actual.scan_projected_row_refs_unordered_each(collection, ScanOptions::default(), &fields, &mut |id, _, values| {
            rows.insert(id, values.iter().map(|value| value.as_ref().map(ref_value)).collect::<Vec<_>>());
            Ok(true)
        }).unwrap();
        assert_eq!(rows, expected_rows, "{collection} row refs");
        let mut expected_values = expected_rows.values().cloned().collect::<Vec<_>>();
        let key = |row: &Vec<Option<Value>>| format!("{row:?}");
        expected_values.sort_by_key(key);

        for (limit, block_fields) in [(None, fields.clone()), (Some(3), fields.clone()), (None, fields[..2].to_vec())] {
            let mut blocks = BTreeMap::new();
            let options = limit.map_or(ScanOptions::default(), |limit| ScanOptions::new().with_limit(limit));
            let mut coded_cells = 0usize;
            let mut dictionaries = BTreeMap::<(u64, u32), Value>::new();
            let mut strings = std::collections::HashMap::<(u64, String), u32>::new();
            actual.scan_projected_blocks_each(collection, options, &block_fields, &|_| false, None, &mut |block| {
                assert!(block.columns.iter().all(|column| column.len() == block.ids.len()));
                for (row, (id, _)) in block.ids.iter().enumerate() {
                    blocks.insert(*id, block.columns.iter().map(|column| column.get(row).as_ref().map(ref_value)).collect::<Vec<_>>());
                }
                // A code always stands for the same string, under its dictionary.
                for (column, codes) in block.columns.iter().zip(block.codes) {
                    let Some(codes) = codes else { continue };
                    for (row, code) in codes.codes.iter().enumerate() {
                        match column.get(row) {
                            Some(ProjectedValueRef::String(text)) if *code != BlockCodes::NONE => {
                                let known = dictionaries.entry((codes.dictionary, *code)).or_insert_with(|| Value::string(text));
                                assert_eq!(known, &Value::string(text));
                                let code_of = strings.entry((codes.dictionary, text.to_owned())).or_insert(*code);
                                assert_eq!(code_of, code);
                                coded_cells += 1;
                            }
                            _ => assert_eq!(*code, BlockCodes::NONE),
                        }
                    }
                }
                Ok(true)
            }).unwrap();
            let _ = coded_cells;
            let expected_blocks = expected_rows.iter().map(|(id, row)| (*id, row[..block_fields.len()].to_vec())).collect::<BTreeMap<_, _>>();
            match limit {
                None => assert_eq!(blocks, expected_blocks, "{collection} blocks"),
                Some(limit) => { assert_eq!(blocks.len(), limit.min(want.len())); assert!(blocks.iter().all(|(id, row)| expected_blocks.get(id) == Some(row))); }
            }
        }
        assert!(actual.supports_projected_blocks());

        let accept = |values: &[Option<Value>]| values[0].as_ref().is_some_and(|value| matches!(value, Value::String(_) | Value::Null));
        let mut gated = BTreeMap::new();
        actual.scan_projected_row_values_gated_unordered_each(collection, ScanOptions::default(), &fields, 1, &mut |values| { assert!(values[1..].iter().all(Option::is_none)); Ok(accept(values)) }, &mut |id, _, values| { gated.insert(id, values.to_vec()); Ok(true) }).unwrap();
        assert_eq!(gated, expected_rows.iter().filter(|(_, row)| accept(row)).map(|(id, row)| (*id, row.clone())).collect::<BTreeMap<_, _>>(), "{collection} gated");
        let mut gated_documents = Vec::new();
        actual.scan_projected_gated_each(collection, ScanOptions::default(), &fields, &mut |values| Ok(accept(values)), &mut |stored| { gated_documents.push(*stored.id()); Ok(true) }).unwrap();
        assert_eq!(gated_documents, want.iter().filter(|stored| accept(&expected_rows[stored.id()])).map(|stored| *stored.id()).collect::<Vec<_>>());

        let mut ids = Vec::new();
        actual.visit_row_ids(collection, &mut |id, version| { ids.push((id, version)); Ok(true) }).unwrap();
        ids.sort();
        assert_eq!(ids, want.iter().map(|stored| (*stored.id(), stored.version())).collect::<Vec<_>>());

        let facts = actual.field_facts(collection).unwrap().unwrap();
        assert_eq!(facts.documents(), want.len() as u64);
        for name in ["a", "b", "c", "n", "zz"] {
            let present = want.iter().filter(|stored| stored.document().contains_key(name)).count();
            let mut kinds = want.iter().filter_map(|stored| stored.document().get(name)).map(Value::physical_kind).collect::<Vec<_>>();
            kinds.sort_by_key(|kind| *kind as u8);
            kinds.dedup();
            let expected_kinds = (present == want.len() && present > 0).then_some(kinds);
            assert_eq!(facts.kinds_when_always_present(name).map(<[_]>::to_vec), expected_kinds, "{collection} facts {name}");
        }
        // Bounds hold every visible value (they may be wider after deletions).
        let bounds = actual.field_bounds(collection).unwrap().unwrap();
        for stored in &want {
            for (name, value) in stored.document().iter() {
                let bounds = bounds.get(name.as_str()).unwrap_or_else(|| panic!("{collection}: no bounds for {name}"));
                let within = |range: Option<(f64, f64)>, value: f64| range.is_some_and(|(low, high)| low <= value && value <= high);
                match value {
                    Value::Number(crate::Number::Signed(value)) => assert!(bounds.signed.is_some_and(|(low, high)| low <= *value && *value <= high), "{name}: {value}"),
                    Value::Number(crate::Number::Unsigned(value)) => assert!(bounds.unsigned.is_some_and(|(low, high)| low <= *value && *value <= high), "{name}: {value}"),
                    Value::Number(crate::Number::Float(value)) => assert!(within(bounds.float, *value), "{name}: {value}"),
                    Value::String(value) => if let Some((low, high)) = &bounds.string { assert!(low <= value && value <= high, "{name}: {value}"); },
                    _ => {}
                }
            }
        }
        let mut top = actual.top_level_fields(collection).unwrap().unwrap();
        top.sort();
        let mut expected_top = want.iter().flat_map(|stored| stored.document().keys().map(|key| key.as_str().to_owned()).collect::<Vec<_>>()).collect::<Vec<_>>();
        expected_top.sort();
        expected_top.dedup();
        assert_eq!(top, expected_top);
    }
    let mut expected_collections = expected.collections().unwrap();
    expected_collections.retain(|collection| expected.count(collection).unwrap() > 0);
    assert_eq!(actual.collections().unwrap(), expected_collections);
}

#[test] fn random_operations_match_the_memory_backend() { for seed in [0x5EED_1234_ABCD_0001, 7, 0xDEAD_BEEF, 0x0123_4567_89AB_CDEF, 99] { random_run(seed); } }

fn random_run(seed: u64) {
    let dir = TempDir::new(&format!("random-{seed}"));
    let mut store = GlacierBackend::open_with(&dir.0, TINY).unwrap();
    let memory = MemoryBackend::new();
    let collections = [CollectionId::parse("ana").unwrap(), CollectionId::parse("other").unwrap()];
    let mut rng = Rng(seed);
    let mut ordinal = 0u64;
    for step in 0..400 {
        let collection = &collections[rng.below(2) as usize];
        let existing = memory.read().unwrap().scan(collection, ScanOptions::default()).unwrap();
        match rng.below(10) {
            0..=3 => {
                let count = if rng.below(6) == 0 { 40 + rng.below(30) } else { 1 + rng.below(8) };
                let mut mutations = Vec::new();
                for _ in 0..count {
                    ordinal += 1;
                    mutations.push(StorageMutation::insert(DocumentId::synthetic(7, ordinal), random_document(&mut rng)));
                }
                if !existing.is_empty() && rng.below(3) == 0 {
                    let pick = &existing[rng.below(existing.len() as u64) as usize];
                    mutations.push(StorageMutation::replace(*pick.id(), random_document(&mut rng), VersionPrecondition::Exact(pick.version())));
                }
                let want = memory.apply_batch_atomic(collection, mutations.clone()).map(|(stored, result)| (stored.len(), result));
                let got = store.apply_batch_atomic(collection, mutations).map(|(stored, result)| (stored.len(), result));
                assert_eq!(got.is_ok(), want.is_ok(), "step {step}");
                if let (Ok(got), Ok(want)) = (got, want) {
                    assert_eq!((got.0, got.1.inserted(), got.1.replaced()), (want.0, want.1.inserted(), want.1.replaced()));
                }
            }
            4..=5 if !existing.is_empty() => {
                let mut mutations = Vec::new();
                for _ in 0..1 + rng.below(5) {
                    let pick = &existing[rng.below(existing.len() as u64) as usize];
                    mutations.push(StorageMutation::replace(*pick.id(), random_document(&mut rng), VersionPrecondition::Any));
                }
                memory.apply_batch_atomic_summary(collection, mutations.clone()).unwrap();
                store.apply_batch_atomic_summary(collection, mutations).unwrap();
            }
            6..=8 if !existing.is_empty() => {
                let mut deletes = Vec::new();
                let bound = if rng.below(4) == 0 { 40 } else { 4 };
                for _ in 0..1 + rng.below(bound) {
                    deletes.push(*existing[rng.below(existing.len() as u64) as usize].id());
                }
                deletes.sort();
                deletes.dedup();
                ordinal += 1;
                let fresh = DocumentId::synthetic(7, ordinal);
                let fresh_document = random_document(&mut rng);
                for backend in [&store as &dyn crate::storage::StorageEngine, &memory as &dyn crate::storage::StorageEngine] {
                    let mut tx = backend.begin().unwrap();
                    for id in &deletes {
                        tx.delete(collection, id, VersionPrecondition::Any).unwrap();
                    }
                    tx.insert(collection, fresh, fresh_document.clone()).unwrap();
                    assert!(tx.get(collection, &fresh).unwrap().is_some());
                    tx.replace(collection, &fresh, random_document(&mut Rng(ordinal)), VersionPrecondition::Exact(DocumentVersion::INITIAL)).unwrap();
                    tx.commit().unwrap();
                }
            }
            _ => {
                if !existing.is_empty() {
                    let pick = *existing[0].id();
                    let duplicate = store.apply_batch_atomic_summary(collection, vec![StorageMutation::insert(pick, random_document(&mut rng))]);
                    assert!(matches!(duplicate.unwrap_err().kind(), StorageErrorKind::DocumentAlreadyExists { .. }));
                }
                if rng.below(3) == 0 {
                    store.checkpoint().unwrap();
                }
            }
        }
        if step % 25 == 24 {
            assert_same(&*memory.read().unwrap(), &*store.read().unwrap(), &collections, &mut rng);
            if rng.below(2) == 0 {
                drop(store);
                store = GlacierBackend::open_with(&dir.0, TINY).unwrap();
                assert_same(&*memory.read().unwrap(), &*store.read().unwrap(), &collections, &mut rng);
            }
        }
    }
    drop(store);
    let store = GlacierBackend::open_with(&dir.0, TINY).unwrap();
    assert_same(&*memory.read().unwrap(), &*store.read().unwrap(), &collections, &mut rng);
    let stats = store.stats().unwrap();
    assert!(stats.segments > 0, "{stats:?}");
}

#[test] fn the_tiny_run_exercises_every_write_path() { let dir = TempDir::new("paths"); let store = GlacierBackend::open_with(&dir.0, TINY).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let mut rng = Rng(42); let batch = |from: u64, count: u64, rng: &mut Rng| (from..from + count).map(|ordinal| StorageMutation::insert(DocumentId::synthetic(3, ordinal), random_document(rng))).collect::<Vec<_>>(); store.apply_batch_atomic_summary(&collection, batch(0, 45, &mut rng)).unwrap(); for start in 0..12 { store.apply_batch_atomic_summary(&collection, batch(1000 + start * 10, 10, &mut rng)).unwrap(); } let read = store.read().unwrap(); let all = read.scan(&collection, ScanOptions::default()).unwrap(); drop(read); let mut tx = store.begin().unwrap(); for stored in all.iter().take(40) { tx.delete(&collection, stored.id(), VersionPrecondition::Any).unwrap(); } tx.commit().unwrap(); store.checkpoint().unwrap(); let metrics = store.write_metrics().unwrap(); assert!(metrics.direct_commits >= 1 && metrics.wal_commits >= 12 && metrics.flushes >= 2 && metrics.compactions >= 1, "{metrics:?}"); assert_eq!(store.read().unwrap().count(&collection).unwrap(), 165 - 40); assert_eq!(store.stats().unwrap().delta_rows, 0); }

#[test] fn unflushed_commits_survive_a_reopen_and_a_torn_log_tail_is_ignored() { let dir = TempDir::new("recover"); let collection = CollectionId::parse("users").unwrap(); let id = DocumentId::synthetic(1, 1); { let store = GlacierBackend::open(&dir.0).unwrap(); let mut document = Document::new(); document.insert("name", Value::string("Ada")); store.apply_batch_atomic_summary(&collection, vec![StorageMutation::insert(id, Arc::new(document))]).unwrap(); let mut document = Document::new(); document.insert("name", Value::string("Grace")); store.apply_batch_atomic_summary(&collection, vec![StorageMutation::replace(id, Arc::new(document), VersionPrecondition::Any)]).unwrap(); assert!(store.stats().unwrap().wal_bytes > 0); } let log = std::fs::read_dir(&dir.0).unwrap().flatten().map(|entry| entry.path()).find(|path| path.extension().is_some_and(|ext| ext == "log")).unwrap(); let mut bytes = std::fs::read(&log).unwrap(); bytes.extend_from_slice(&[200, 0, 0, 0, 1, 2, 3]); std::fs::write(&log, bytes).unwrap(); let store = GlacierBackend::open(&dir.0).unwrap(); let stored = store.read().unwrap().get(&collection, &id).unwrap().unwrap(); assert_eq!(stored.version(), DocumentVersion::new(2)); assert_eq!(stored.document().get("name"), Some(&Value::string("Grace"))); assert_eq!(store.generation().unwrap(), 2); store.checkpoint().unwrap(); drop(store); let store = GlacierBackend::open(&dir.0).unwrap(); assert_eq!(store.read().unwrap().get(&collection, &id).unwrap().unwrap().version(), DocumentVersion::new(2)); assert_eq!(store.stats().unwrap().wal_bytes, 0); }

#[test] fn a_commit_after_another_commit_conflicts() { let dir = TempDir::new("conflict"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("c").unwrap(); let mut first = store.begin().unwrap(); first.insert(&collection, DocumentId::synthetic(1, 1), Arc::new(Document::new())).unwrap(); let mut second = store.begin().unwrap(); second.insert(&collection, DocumentId::synthetic(1, 2), Arc::new(Document::new())).unwrap(); first.commit().unwrap(); assert!(matches!(second.commit().unwrap_err().kind(), StorageErrorKind::TransactionConflict { .. })); }

#[test] fn import_keeps_versions_and_rejects_a_non_empty_target() { let dir = TempDir::new("import"); let store = GlacierBackend::open_with(&dir.0, TINY).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let mut ids = (0..150u64).map(|ordinal| DocumentId::synthetic(5, ordinal)).collect::<Vec<_>>(); ids.sort(); let rows = ids.iter().enumerate().map(|(index, id)| { let mut document = Document::new(); document.insert("i", Value::signed(index as i64)); StoredDocument::new(*id, DocumentVersion::new(1 + index as u64 % 4), Arc::new(document)) }).collect::<Vec<_>>(); let feed = |rows: &Vec<StorageResult<StoredDocument>>| { let rows = rows.iter().map(|row| row.as_ref().unwrap().clone()).collect::<Vec<_>>(); move |sink: &mut dyn FnMut(StoredDocument) -> StorageResult<()>| rows.iter().try_for_each(|row| sink(row.clone())) }; assert_eq!(store.import(&collection, &mut feed(&rows)).unwrap(), 150); assert!(store.import(&collection, &mut feed(&rows)).is_err()); drop(store); let store = GlacierBackend::open_with(&dir.0, TINY).unwrap(); let read = store.read().unwrap(); let scanned = read.scan(&collection, ScanOptions::default()).unwrap(); assert_eq!(scanned.len(), 150); assert!(scanned.iter().zip(&rows).all(|(got, want)| got.id() == want.as_ref().unwrap().id() && got.version() == want.as_ref().unwrap().version())); assert_eq!(store.stats().unwrap().segments, 3); }

#[test] fn a_corrupt_chunk_is_reported_not_returned() { let dir = TempDir::new("corrupt"); let collection = CollectionId::parse("ana").unwrap(); { let store = GlacierBackend::open(&dir.0).unwrap(); let rows = (0..100u64).map(|ordinal| { let mut document = Document::new(); document.insert("v", Value::string(format!("value-{ordinal}"))); StoredDocument::new(DocumentId::synthetic(0, ordinal), DocumentVersion::INITIAL, Arc::new(document)) }).collect::<Vec<_>>(); let mut sorted = rows.into_iter().map(Result::unwrap).collect::<Vec<_>>(); sorted.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| sorted.iter().try_for_each(|row| sink(row.clone()))).unwrap(); } let segment = std::fs::read_dir(&dir.0).unwrap().flatten().map(|entry| entry.path()).find(|path| path.extension().is_some_and(|ext| ext == "g6")).unwrap(); let mut bytes = std::fs::read(&segment).unwrap(); let at = bytes.windows(8).position(|window| window == b"value-42").unwrap(); bytes[at] ^= 1; std::fs::write(&segment, bytes).unwrap(); let store = GlacierBackend::open(&dir.0).unwrap(); assert!(store.read().unwrap().scan(&collection, ScanOptions::default()).is_err()); }

#[test] fn a_rolled_back_transaction_leaves_no_trace() { let dir = TempDir::new("rollback"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("c").unwrap(); let mut tx = store.begin().unwrap(); tx.insert(&collection, DocumentId::synthetic(1, 1), Arc::new(Document::new())).unwrap(); assert_eq!(tx.count(&collection).unwrap(), 1); tx.rollback().unwrap(); assert_eq!(store.read().unwrap().count(&collection).unwrap(), 0); assert_eq!(store.generation().unwrap(), 0); }

#[test] fn dictionary_strings_reach_blocks_with_their_codes() { let dir = TempDir::new("codes"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let mut rows = (0..3000u64).map(|ordinal| { let mut document = Document::new(); document.insert("p", Value::string(["01-2025", "02-2025", "03-2025"][ordinal as usize % 3])); if ordinal % 7 == 0 { document.insert("p", Value::Null); } StoredDocument::new(DocumentId::synthetic(4, ordinal), DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let (mut coded, mut seen) = (0usize, 0usize); store.read().unwrap().scan_projected_blocks_each(&collection, ScanOptions::default(), &[FieldPath::parse("p").unwrap()], &|_| false, None, &mut |block| { let codes = block.codes[0].expect("dictionary column"); for (row, code) in codes.codes.iter().enumerate() { seen += 1; match block.columns[0].get(row) { Some(ProjectedValueRef::String(_)) => { assert!(*code < 3); coded += 1; } _ => assert_eq!(*code, BlockCodes::NONE) } } Ok(true) }).unwrap(); assert_eq!((seen, coded), (3000, 3000 - 3000usize.div_ceil(7))); }

#[test] fn uniform_blocks_reach_the_engine_as_typed_slices() { let dir = TempDir::new("typed"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let document = |ordinal: u64| { let mut document = Document::new(); document.insert("f", Value::float(ordinal as f64 / 4.0 - 300.0).unwrap()); document.insert("i", Value::signed(ordinal as i64 * 7 - 9_000)); document.insert("u", Value::unsigned(u64::MAX - ordinal)); document.insert("d", Value::string(["01-2025", "02-2025", "03-2025"][(ordinal / 500) as usize % 3])); document.insert("t", Value::string(format!("text {ordinal}"))); /* Rare nulls: blocks without one are uniform, the others are not. */ document.insert("s", if ordinal % 1500 == 3 { Value::Null } else { Value::float(ordinal as f64 + 0.5).unwrap() }); StoredDocument::new(DocumentId::synthetic(5, ordinal), DocumentVersion::INITIAL, Arc::new(document)).unwrap() }; let mut rows = (0..5000u64).map(document).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let mut tx = store.begin().unwrap(); for stored in rows.iter().step_by(13) { tx.delete(&collection, stored.id(), VersionPrecondition::Any).unwrap(); } tx.commit().unwrap(); let fields = ["f", "i", "u", "d", "t", "s"].map(|name| FieldPath::parse(name).unwrap()); let read = store.read().unwrap(); let mut expected = BTreeMap::new(); read.scan_projected_row_refs_unordered_each(&collection, ScanOptions::default(), &fields, &mut |id, _, row| { expected.insert(id, row.iter().map(|value| value.as_ref().map(ref_value)).collect::<Vec<_>>()); Ok(true) }).unwrap(); let (mut actual, mut typed, mut values) = (BTreeMap::new(), [0usize; 6], 0usize); read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|_| false, None, &mut |block| { for (slot, column) in block.columns.iter().enumerate() { assert_eq!(column.len(), block.ids.len()); match column { BlockColumn::Values(_) => values += 1, _ => typed[slot] += 1 } if slot == 3 { assert!(block.codes[3].is_some_and(|codes| codes.codes.len() == block.ids.len())); } } for (row, (id, _)) in block.ids.iter().enumerate() { actual.insert(*id, block.columns.iter().map(|column| column.get(row).as_ref().map(ref_value)).collect::<Vec<_>>()); } Ok(true) }).unwrap(); assert_eq!(actual.len(), 5000 - 5000usize.div_ceil(13)); assert_eq!(actual, expected); assert!(typed.iter().all(|count| *count > 0) && values > 0, "{typed:?} {values}"); assert!(matches!(read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields[..1], &|_| false, None, &mut |block| { assert!(matches!(block.columns[0], BlockColumn::Floats(_))); Ok(true) }), Ok(()))); /* A field read twice shares one decoded slot, kept once per block. */ let twice = [fields[0].clone(), fields[1].clone(), fields[0].clone()]; let mut doubled = BTreeMap::new(); read.scan_projected_blocks_each(&collection, ScanOptions::default(), &twice, &|_| false, None, &mut |block| { for (row, (id, _)) in block.ids.iter().enumerate() { doubled.insert(*id, block.columns.iter().map(|column| column.get(row).as_ref().map(ref_value)).collect::<Vec<_>>()); } Ok(true) }).unwrap(); assert_eq!(doubled, expected.iter().map(|(id, row)| (*id, vec![row[0].clone(), row[1].clone(), row[0].clone()])).collect::<BTreeMap<_, _>>()); }

#[test] fn block_scans_without_locators_keep_rows_and_values() { let dir = TempDir::new("locators"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let document = |ordinal: u64| { let mut document = Document::new(); document.insert("i", Value::signed(ordinal as i64 * 3)); document.insert("d", Value::string(["a", "b"][ordinal as usize % 2])); Arc::new(document) }; let mut rows = (0..3000u64).map(|ordinal| StoredDocument::new(DocumentId::synthetic(5, ordinal), DocumentVersion::INITIAL, document(ordinal)).unwrap()).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); /* Dead rows in the segment, and rows in the delta. */ let mut tx = store.begin().unwrap(); for stored in rows.iter().step_by(7) { tx.delete(&collection, stored.id(), VersionPrecondition::Any).unwrap(); } for ordinal in 5000..5010 { tx.insert(&collection, DocumentId::synthetic(6, ordinal), document(ordinal)).unwrap(); } tx.commit().unwrap(); let fields = ["i", "d"].map(|name| FieldPath::parse(name).unwrap()); let read = store.read().unwrap(); let scan = |options: ScanOptions| { let mut seen = Vec::new(); read.scan_projected_blocks_each(&collection, options, &fields, &|_| false, None, &mut |block| { assert_eq!(block.ids.len(), if options.locators() { block.rows } else { 0 }); assert!(block.columns.iter().all(|column| column.len() == block.rows)); seen.extend((0..block.rows).map(|row| block.columns.iter().map(|column| column.get(row).as_ref().map(ref_value)).collect::<Vec<_>>())); Ok(true) }).unwrap(); seen }; let with = scan(ScanOptions::default()); assert_eq!(with.len(), 3000 - 3000usize.div_ceil(7) + 10); assert_eq!(scan(ScanOptions::default().without_locators()), with); assert_eq!(scan(ScanOptions::default().without_locators().with_limit(1500)), with[..1500]); }

#[test] fn pruned_block_scans_leave_out_only_groups_the_filters_drop() { use crate::query::{parse_expression, GroupPruning, PhysicalOperator}; let dir = TempDir::new("pruned"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let rows = segment::GROUP_ROWS as u64; /* Groups by period, in identifier order: "A", "B" (one row lacks it), */ /* "C" (one number). */ let mut ids = (0..rows * 2 + 3000).map(|ordinal| DocumentId::synthetic(5, ordinal)).collect::<Vec<_>>(); ids.sort(); let all = ids.iter().enumerate().map(|(position, id)| { let position = position as u64; let mut document = Document::new(); match (position / rows, position % rows) { (1, 7) => {} (2, 9) => { document.insert("p", Value::signed(3)); } (group, _) => { document.insert("p", Value::string(["A", "B", "C"][group as usize])); } } document.insert("x", Value::float(position as f64 + 0.5).unwrap()); StoredDocument::new(*id, DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); store.import(&collection, &mut |sink| all.iter().try_for_each(|row| sink(row.clone()))).unwrap(); /* Rows still in the delta are never left out. */ let mut tx = store.begin().unwrap(); let mut late = Document::new(); late.insert("p", Value::string("A")); tx.insert(&collection, DocumentId::synthetic(6, 1), Arc::new(late)).unwrap(); tx.commit().unwrap(); let fields = ["p", "x"].map(|name| FieldPath::parse(name).unwrap()); let read = store.read().unwrap(); let scan = |predicate: &str| { let operators = [PhysicalOperator::Filter { predicate: parse_expression(predicate).unwrap() }]; let pruning = GroupPruning::new(&operators, &fields).unwrap(); let mut periods = BTreeMap::<String, usize>::new(); read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|facts| pruning.skip(facts), None, &mut |block| { for row in 0..block.rows { let key = match block.columns[0].get(row) { Some(ProjectedValueRef::String(text)) => text.to_owned(), other => format!("{other:?}") }; *periods.entry(key).or_default() += 1; } Ok(true) }).unwrap(); periods }; let rows = rows as usize; /* "A" and the delta row stay; "B" keeps its row lacking `p`; "C" keeps */ /* its number. */ assert_eq!(scan("p == \"A\""), BTreeMap::from([("A".to_owned(), rows + 1), ("B".to_owned(), rows - 1), ("None".to_owned(), 1), ("C".to_owned(), 2999), ("Some(Signed(3))".to_owned(), 1)])); assert_eq!(scan("p == \"B\""), BTreeMap::from([("A".to_owned(), 1), ("B".to_owned(), rows - 1), ("None".to_owned(), 1), ("C".to_owned(), 2999), ("Some(Signed(3))".to_owned(), 1)])); /* A float bound leaves out the groups below it. */ assert_eq!(scan(&format!("x > {}", rows * 2)).values().sum::<usize>(), 3000 + 1); /* Unpruned scans read everything. */ let mut total = 0; read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|_| false, None, &mut |block| { total += block.rows; Ok(true) }).unwrap(); assert_eq!(total, rows * 2 + 3000 + 1); }

#[test] fn samples_draw_visible_rows_uniformly_by_position() { let dir = TempDir::new("sample"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let mut rows = (0..3000u64).map(|ordinal| { let mut document = Document::new(); document.insert("n", Value::unsigned(ordinal)); StoredDocument::new(DocumentId::synthetic(5, ordinal), DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); /* Dead rows, a replaced row (new version in the delta), new rows. */ let mut tx = store.begin().unwrap(); for stored in rows.iter().step_by(10) { tx.delete(&collection, stored.id(), VersionPrecondition::Any).unwrap(); } tx.replace(&collection, rows[1].id(), Arc::new(Document::new()), VersionPrecondition::Any).unwrap(); for ordinal in 0..20 { tx.insert(&collection, DocumentId::synthetic(6, ordinal), Arc::new(Document::new())).unwrap(); } tx.commit().unwrap(); let read = store.read().unwrap(); let mut visible = BTreeMap::new(); read.visit_row_ids(&collection, &mut |id, version| { visible.insert(id, version); Ok(true) }).unwrap(); assert_eq!(visible.len(), 3000 - 300 + 20); let mut hits = BTreeMap::<DocumentId, u32>::new(); for seed in 0..2000 { let (seen, drawn) = read.sample_row_ids(&collection, 30, seed).unwrap().expect("drawn by position"); assert_eq!(seen, visible.len() as u64); assert_eq!(drawn.len(), 30); assert_eq!(drawn.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>().len(), 30, "distinct"); for (id, version) in drawn { assert_eq!(visible.get(&id), Some(&version), "{id:?}"); *hits.entry(id).or_default() += 1; } } /* 60 000 draws over 2 720 rows: about 22 each. */ assert_eq!(hits.len(), visible.len()); assert!(hits.values().all(|hits| (5..=50).contains(hits)), "{:?}", hits.values().min().zip(hits.values().max())); assert!(read.sample_row_ids(&collection, visible.len(), 1).unwrap().is_none()); }

#[test] fn nested_leaves_read_as_their_own_columns() { use crate::query::{parse_expression, GroupPruning, PhysicalOperator}; let dir = TempDir::new("leaves"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let rows = segment::GROUP_ROWS as u64; let mut ids = (0..rows + 3000).map(|ordinal| DocumentId::synthetic(5, ordinal)).collect::<Vec<_>>(); ids.sort(); /* `o.k` counts up; `o.late` appears in the second group only; some rows */ /* hold `o` as a scalar, or `o.k` as an object. */ let all = ids.iter().enumerate().map(|(position, id)| { let position = position as u64; let mut document = Document::new(); let mut o = Document::new(); if position % 11 == 3 { o.insert("k", Value::object(Document::from_fields([("deep", Value::string("x"))]))); } else { o.insert("k", Value::signed(position as i64)); } o.insert("s", Value::string(["FR", "DE"][position as usize % 2])); if position >= rows { o.insert("late", Value::float(position as f64 / 2.0).unwrap()); } document.insert("o", if position % 13 == 5 { Value::signed(7) } else { Value::object(o) }); document.insert("m", Value::object(Document::from_fields([("n", Value::signed(position as i64))]))); StoredDocument::new(*id, DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); store.import(&collection, &mut |sink| all.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let state = store.state().unwrap(); let segment = &state.collections[&collection].segments[0].segment; let path = |text: &str| FieldPath::parse(text).unwrap(); for leaf in ["o.k", "o.s", "o.late", "o.k.deep"] { assert!(segment.leaf_index("o", &path(leaf).as_segments()[1..]).is_some(), "{leaf}"); } assert!(segment.leaf_index("o", &path("o.k.none").as_segments()[1..]).is_none()); let fields = ["o.k", "o.late", "o.s", "o", "o.k.deep", "o.none"].map(path); let read = store.read().unwrap(); let expected = all.iter().map(|stored| (*stored.id(), fields.iter().map(|field| field.resolve_value(stored.document()).cloned()).collect::<Vec<_>>())).collect::<BTreeMap<_, _>>(); let mut actual = BTreeMap::new(); read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|_| false, None, &mut |block| { for (row, (id, _)) in block.ids.iter().enumerate() { actual.insert(*id, block.columns.iter().map(|column| column.get(row).as_ref().map(ref_value)).collect::<Vec<_>>()); } Ok(true) }).unwrap(); assert_eq!(actual, expected); /* A leaf's statistics prune by its own values; mixed kinds or missing */ /* values keep the group. */ let scanned = |predicate: &str| { let fields = [path("m.n"), path("o.k")]; let operators = [PhysicalOperator::Filter { predicate: parse_expression(predicate).unwrap() }]; let pruning = GroupPruning::new(&operators, &fields).unwrap(); let mut total = 0; read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|facts| pruning.skip(facts), None, &mut |block| { total += block.rows; Ok(true) }).unwrap(); total }; assert_eq!(scanned(&format!("m.n > {}", rows + 10)), 3000); assert_eq!(scanned(&format!("m.n < {}", rows)), rows as usize); assert_eq!(scanned(&format!("o.k > {}", rows + 10)), rows as usize + 3000); }
#[test] fn number_codes_reach_blocks_on_request() { let dir = TempDir::new("number-codes"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let mut rows = (0..3000u64).map(|ordinal| { let mut document = Document::new(); /* Few values over a wide range: dictionary streams. */ document.insert("q", Value::signed(1_701_000_000 + 977 * (ordinal as i64 % 5))); document.insert("f", Value::float(1_701_046_290.0 + 977.0 * (ordinal % 4) as f64).unwrap()); StoredDocument::new(DocumentId::synthetic(4, ordinal), DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let fields = [FieldPath::parse("q").unwrap(), FieldPath::parse("f").unwrap()]; let read = store.read().unwrap(); read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|_| false, None, &mut |block| { assert!(block.codes.iter().all(Option::is_none)); Ok(true) }).unwrap(); let mut seen = 0usize; read.scan_projected_blocks_each(&collection, ScanOptions::default().with_number_codes(0b11), &fields, &|_| false, None, &mut |block| { for (column, codes) in block.columns.iter().zip(block.codes) { let codes = codes.expect("number dictionary codes"); let mut by_code = std::collections::HashMap::new(); for (row, code) in codes.codes.iter().enumerate() { let value = column.get(row).map(|value| value.to_value()); assert_eq!(by_code.entry(*code).or_insert_with(|| value.clone()), &value); seen += 1; } let values = by_code.values().map(|value| format!("{value:?}")).collect::<std::collections::HashSet<_>>(); assert_eq!(values.len(), by_code.len(), "different codes hold different values"); } Ok(true) }).unwrap(); assert_eq!(seen, 6000); }

#[test] fn gated_block_scans_visit_only_admitted_blocks() { let dir = TempDir::new("gated"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); /* One zone per block; `m` lacks one row of the third block. */ let mut rows = (0..5000u64).map(|ordinal| { let mut document = Document::new(); document.insert("z", Value::string(["a", "b", "c"][(ordinal as usize / BLOCK) % 3])); document.insert("x", Value::float(ordinal as f64 + 0.5).unwrap()); if ordinal != 2500 { document.insert("m", Value::signed(ordinal as i64)); } StoredDocument::new(DocumentId::synthetic(5, ordinal), DocumentVersion::INITIAL, Arc::new(document)).unwrap() }).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let fields = ["z", "x", "m"].map(|name| FieldPath::parse(name).unwrap()); let read = store.read().unwrap(); let zone = |block: &ProjectedBlock<'_, '_>, row: usize| match block.columns[0].get(row) { Some(ProjectedValueRef::String(text)) => text.to_owned(), other => panic!("{other:?}") }; let (mut seen, mut visited) = (Vec::new(), 0usize); let mut admit = |block: &ProjectedBlock<'_, '_>| { /* Only the gate's column, without locators. */ assert!(block.ids.is_empty() && block.columns[1].is_empty() && block.columns[2].is_empty() && block.columns[0].len() == block.rows); seen.push((zone(block, 0), block.complete)); Ok(zone(block, 0) != "b") }; read.scan_projected_blocks_each(&collection, ScanOptions::default(), &fields, &|_| false, Some(BlockGate { fields: vec![true, false, false], admit: &mut admit }), &mut |block| { assert!(block.columns.iter().all(|column| column.len() == block.rows) && block.ids.len() == block.rows); assert!((0..block.rows).all(|row| zone(block, row) != "b")); visited += block.rows; Ok(true) }).unwrap(); assert_eq!(visited, rows.iter().filter(|stored| stored.document().get("z") != Some(&Value::string("b"))).count()); assert_eq!(seen.iter().filter(|(_, complete)| !complete).count(), 1, "{seen:?}"); assert_eq!(seen.len(), 5000usize.div_ceil(BLOCK)); }

#[test] fn writes_check_the_version_of_staged_delta_and_segment_rows() { let dir = TempDir::new("versions"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("users").unwrap(); let id = DocumentId::synthetic(1, 1); let empty = || Arc::new(Document::new()); let exact = |version| VersionPrecondition::Exact(DocumentVersion::new(version)); let conflict = |error: StorageError| matches!(error.kind(), StorageErrorKind::VersionConflict { .. }); store.apply_batch_atomic_summary(&collection, vec![StorageMutation::insert(id, empty())]).unwrap(); store.checkpoint().unwrap(); let mut tx = store.begin().unwrap(); assert!(conflict(tx.delete(&collection, &id, exact(2)).unwrap_err())); assert_eq!(tx.replace(&collection, &id, empty(), exact(1)).unwrap().previous_version(), DocumentVersion::new(1)); assert!(conflict(tx.replace(&collection, &id, empty(), exact(1)).unwrap_err())); assert_eq!(tx.replace(&collection, &id, empty(), exact(2)).unwrap().stored().version(), DocumentVersion::new(3)); tx.commit().unwrap(); let mut tx = store.begin().unwrap(); assert!(conflict(tx.delete(&collection, &id, exact(2)).unwrap_err())); assert_eq!(tx.delete(&collection, &id, exact(3)).unwrap().deleted_version(), DocumentVersion::new(3)); assert!(tx.delete(&collection, &id, VersionPrecondition::Any).is_err()); assert!(tx.delete(&collection, &DocumentId::synthetic(1, 2), VersionPrecondition::Any).is_err()); }

#[test] fn a_transaction_past_its_memory_commits_what_it_spilled_as_one() { let dir = TempDir::new("spill"); let store = GlacierBackend::open_with(&dir.0, TINY).unwrap(); let memory = MemoryBackend::new(); let (collections, mut rng) = ([CollectionId::parse("ana").unwrap(), CollectionId::parse("new").unwrap()], Rng(11)); let segments = || std::fs::read_dir(&dir.0).unwrap().flatten().filter(|entry| segment_number(entry.file_name().to_str().unwrap()).is_some()).count(); for (from, count) in [(0, 90), (90, 5), (95, 5)] { let rows = (from..from + count).map(|ordinal| StorageMutation::insert(DocumentId::synthetic(3, ordinal), random_document(&mut rng))).collect::<Vec<_>>(); store.apply_batch_atomic_summary(&collections[0], rows.clone()).unwrap(); memory.apply_batch_atomic_summary(&collections[0], rows).unwrap(); } let existing = memory.read().unwrap().scan(&collections[0], ScanOptions::default()).unwrap(); let (mut tx, mut reference) = (store.begin().unwrap(), memory.begin().unwrap()); for (index, stored) in existing.iter().enumerate() { for tx in [&mut tx, &mut reference] { match index % 4 { 0 => { tx.delete(&collections[0], stored.id(), VersionPrecondition::Exact(stored.version())).unwrap(); } 1 => { tx.replace(&collections[0], stored.id(), Arc::new(Document::new()), VersionPrecondition::Exact(stored.version())).unwrap(); } _ => {} } } let document = random_document(&mut rng); for tx in [&mut tx, &mut reference] { tx.insert(&collections[1], DocumentId::synthetic(4, index as u64), document.clone()).unwrap(); } if index % 30 == 29 { assert!(tx.spill().unwrap()); } } tx.commit().unwrap(); reference.commit().unwrap(); assert_same(&*memory.read().unwrap(), &*store.read().unwrap(), &collections, &mut rng); assert_eq!(store.stats().unwrap().delta_rows, 0); drop(store); let store = GlacierBackend::open_with(&dir.0, TINY).unwrap(); assert_same(&*memory.read().unwrap(), &*store.read().unwrap(), &collections, &mut rng); let before = segments(); let mut dropped = store.begin().unwrap(); for ordinal in 0..50 { dropped.insert(&collections[1], DocumentId::synthetic(5, ordinal), Arc::new(Document::new())).unwrap(); } dropped.spill().unwrap(); assert!(segments() > before); drop(dropped); assert_eq!(segments(), before); let id = *store.read().unwrap().scan(&collections[1], ScanOptions::default()).unwrap()[0].id(); let mut twice = store.begin().unwrap(); twice.delete(&collections[1], &id, VersionPrecondition::Any).unwrap(); twice.spill().unwrap(); twice.delete(&collections[1], &id, VersionPrecondition::Any).unwrap(); assert!(matches!(twice.commit().unwrap_err().kind(), StorageErrorKind::TransactionConflict { .. })); let mut late = store.begin().unwrap(); late.insert(&collections[1], DocumentId::synthetic(6, 1), Arc::new(Document::new())).unwrap(); late.spill().unwrap(); store.apply_batch_atomic_summary(&collections[1], vec![StorageMutation::insert(DocumentId::synthetic(6, 2), Arc::new(Document::new()))]).unwrap(); assert!(matches!(late.commit().unwrap_err().kind(), StorageErrorKind::TransactionConflict { .. })); assert_eq!(segments(), before); }

#[test] fn usage_shares_group_bytes_by_live_rows_per_place() { let dir = TempDir::new("usage"); let store = GlacierBackend::open(&dir.0).unwrap(); let collection = CollectionId::parse("ana").unwrap(); let document = |ordinal: u64| { let mut document = Document::new(); document.insert("_place", Value::string(["p1", "p1", "p1", "p2"][ordinal as usize % 4])); document.insert("v", Value::unsigned(ordinal)); Arc::new(document) }; let mut rows = (0..4000u64).map(|ordinal| StoredDocument::new(DocumentId::synthetic(5, ordinal), DocumentVersion::INITIAL, document(ordinal)).unwrap()).collect::<Vec<_>>(); rows.sort_by_key(|stored| *stored.id()); store.import(&collection, &mut |sink| rows.iter().try_for_each(|row| sink(row.clone()))).unwrap(); let usage = store.usage_by("_place").unwrap(); let (p1, p2) = (usage["p1"], usage["p2"]); /* Three rows in four are in p1: three times p2, within rounding. */ assert!(p1.abs_diff(p2 * 3) <= 2, "{usage:?}"); /* The segment file also holds its footer. */ let file = store.stats().unwrap().segment_bytes; assert!(p1 + p2 <= file && p1 + p2 > file / 2, "{usage:?} of {file}"); /* Deleted rows leave the count (the cached result is computed again); delta rows count as JSON. */ let mut tx = store.begin().unwrap(); for stored in rows.iter().filter(|stored| stored.document().get("_place") == Some(&Value::string("p2"))) { tx.delete(&collection, stored.id(), VersionPrecondition::Any).unwrap(); } let mut late = Document::new(); late.insert("_place", Value::string("p3")); tx.insert(&collection, DocumentId::synthetic(6, 1), Arc::new(late)).unwrap(); tx.commit().unwrap(); let usage = store.usage_by("_place").unwrap(); assert_eq!(usage.get("p2"), None, "{usage:?}"); assert_eq!(usage["p1"], p1); assert_eq!(usage["p3"], 15); }
