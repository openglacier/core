//! Queries returning rows to the client, run on batches: leading filters on
//! batches then the kept rows whole by identifier, or every stage on batches
//! for a final `select`. Rows and errors must be those of the row path
//! (MemoryStorage reads no blocks); complete documents keep identifier
//! order, a final `select` the storage order of the row-at-a-time scan.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, PlannerPipeline},
    storage::{GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine, VersionPrecondition},
    Document, Value,
};

const ROWS: u32 = 3000;
const WIDE: u32 = 40_000;

fn row(index: u32) -> Document {
    let mut row = Document::new();
    row.insert("n", Value::from(i64::from(index)));
    row.insert("code", Value::from(format!("A-{:03}", index % 17)));
    row.insert("qty", Value::from(i64::from(index % 7)));
    row.insert("price", Value::float(f64::from(index % 13) * 1.5).unwrap());
    row.insert("period", Value::from(format!("{:02}-2025", 1 + index % 12)));
    // Integers, floats, text, null, missing, arrays: `mixed == 3` fails on text.
    match index % 6 {
        0 => {}
        1 => { row.insert("mixed", Value::null()); }
        2 => { row.insert("mixed", Value::from(i64::from(index % 4))); }
        3 => { row.insert("mixed", Value::float(f64::from(index % 4)).unwrap()); }
        4 => { row.insert("mixed", Value::array([Value::from("x")])); }
        _ => { row.insert("mixed", Value::from(format!("m{}", index % 5))); }
    }
    if index.is_multiple_of(5) {
        row.insert("note", Value::from(format!("promo{}", index % 3)));
    }
    row
}

fn id(prefix: u32, index: u32) -> DocumentId {
    DocumentId::parse(format!("01900000-0000-7000-{prefix:04x}-{:012x}", index + 1)).unwrap()
}

/// Two commits (Glacier: a checkpoint between them), deletions and
/// replacements after it, so rows come from segments and from the delta.
fn load(storage: Arc<dyn StorageEngine>, between: &dyn Fn()) -> Engine {
    let collection = CollectionId::parse("sales").unwrap();
    for range in [0..ROWS / 2, ROWS / 2..ROWS] {
        let mut transaction = storage.begin().unwrap();
        for index in range {
            transaction.insert(&collection, id(0x8000, index), Arc::new(row(index))).unwrap();
        }
        transaction.commit().unwrap();
        between();
    }
    let mut transaction = storage.begin().unwrap();
    for index in (0..ROWS).filter(|index| index % 97 == 0) {
        transaction.delete(&collection, &id(0x8000, index), VersionPrecondition::Any).unwrap();
    }
    for index in (1..ROWS).filter(|index| index % 89 == 0) {
        let mut replaced = row(index);
        replaced.insert("qty", Value::from(6));
        transaction.replace(&collection, &id(0x8000, index), Arc::new(replaced), VersionPrecondition::Any).unwrap();
    }
    transaction.commit().unwrap();
    let wide = CollectionId::parse("wide").unwrap();
    let mut transaction = storage.begin().unwrap();
    for index in 0..WIDE {
        let mut row = Document::new();
        row.insert("n", Value::from(i64::from(index)));
        row.insert("g", Value::from(format!("g{}", index % 3)));
        row.insert("v", Value::float(f64::from(index % 101) / 4.0).unwrap());
        transaction.insert(&wide, id(0x9000, index), Arc::new(row)).unwrap();
    }
    transaction.commit().unwrap();
    between();
    Engine::new(storage)
}

struct Engines {
    memory: Engine,
    stores: Vec<(&'static str, Engine)>,
    glacier: Arc<GlacierStorage>,
    directory: std::path::PathBuf,
}

impl Engines {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let directory = std::env::temp_dir().join(format!("og-row-outputs-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let memory = load(Arc::new(MemoryStorage::new()), &|| {});
        let storage = Arc::new(GlacierStorage::open(directory.join("rows.glacier")).unwrap());
        let checkpoints = Arc::clone(&storage);
        let glacier = load(Arc::clone(&storage) as Arc<dyn StorageEngine>, &move || checkpoints.checkpoint().unwrap());
        Self { memory, stores: vec![("glacier", glacier)], glacier: storage, directory }
    }

    /// Each query returns the rows of the row path, in identifier order
    /// (`ordered`) or as a set, or fails the same way.
    fn check(&self, ordered: bool, queries: &[&str]) {
        let mut failures = Vec::new();
        for text in queries {
            for (store, engine) in &self.stores {
                let expected = outcome(&self.memory, text, ordered);
                let actual = outcome(engine, text, ordered);
                if actual != expected {
                    let detail = match (&actual, &expected) {
                        (Ok(got), Ok(want)) => {
                            let at = got.iter().zip(want).position(|(got, want)| got != want).unwrap_or(got.len().min(want.len()));
                            format!("{} rows, expected {}; first difference at {at}:\n  got:      {:?}\n  expected: {:?}", got.len(), want.len(), got.get(at), want.get(at))
                        }
                        _ => format!("\n  got:      {:.400}\n  expected: {:.400}", format!("{actual:?}"), format!("{expected:?}")),
                    };
                    failures.push(format!("{store}: {text}: {detail}"));
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}

impl Drop for Engines {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Rows as the daemon streams them (identifier and document), or the error
/// without the value that raised it: the first failing row may differ
/// between identifier and storage order.
fn outcome(engine: &Engine, text: &str, ordered: bool) -> Result<Vec<String>, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    let pipeline = PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())?;
    let planned = engine.plan(&pipeline).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    let statistics = engine
        .stream_read_pipeline(planned.physical(), &mut |stored| {
            rows.push(format!("{} {}", stored.id(), og_core::helpers::value_to_json(&Value::object(stored.document().clone()))));
            Ok(())
        })
        .map_err(|error| error.to_string().split(", found").next().unwrap_or_default().replace("storage execution error: storage backend error: ", ""))?;
    assert!(statistics.is_some(), "{text}: not streamed");
    if !ordered {
        rows.sort();
    }
    Ok(rows)
}

#[test] fn complete_documents_keep_identifier_order() { Engines::new().check(true, &["on sales | where qty > 2", "on sales | where code == \"A-003\"", "on sales | where qty > 2 and price < 10 | derive t = price * qty", "on sales | where qty == 6 and note? != null", "on sales | where period == \"03-2025\" | where price > 3", "on sales | where price > 100", "on sales | where note != null", "on sales | where mixed == 3", "on sales | where qty > 5 | limit 3", "on sales | where qty > 5 | skip 4 | limit 3", "on sales | where qty > 1 | derive m = mixed + 1", "on wide | where v > 20", "on wide | where g == \"g1\" and v < 2"]); }

#[test] fn final_selects_run_on_batches() { Engines::new().check(false, &["on sales | where qty > 2 | select code, price", "on sales | where period == \"03-2025\" | select n, qty, note", "on sales | where qty > 2 | derive t = price * qty | select n, t", "on sales | where price > 100 | select n", "on sales | where mixed == 3 | select n", "on sales | where note != null | select n", "on sales | select n, mixed", "on wide | where g == \"g1\" | select n, v", "on wide | where v > 24 | select n"]); }

/// A final `select` keeps the order of the row-at-a-time projected scan it
/// replaces: segments, then the delta (replaced rows included).
#[test] fn final_selects_keep_the_storage_order() { let engines = Engines::new(); let read = engines.glacier.read().unwrap(); let fields = [og_core::FieldPath::parse("qty").unwrap(), og_core::FieldPath::parse("n").unwrap()]; let mut expected = Vec::new(); read.scan_projected_row_values_gated_unordered_each(&CollectionId::parse("sales").unwrap(), og_core::storage::ScanOptions::default(), &fields, 1, &mut |values| Ok(values[0].as_ref().and_then(|value| og_core::helpers::value_to_json(value).as_i64()).is_some_and(|qty| qty > 2)), &mut |id, _, values| { expected.push(format!("{id} {}", values[1].as_ref().and_then(|value| og_core::helpers::value_to_json(value).as_i64()).unwrap())); Ok(true) }).unwrap(); let (_, engine) = &engines.stores[0]; let text = "on sales | where qty > 2 | select n"; let ast = parse(text).unwrap(); let planned = engine.plan(&PlannerPipeline::from_ast(text, &ast).unwrap()).unwrap(); let mut actual = Vec::new(); engine.stream_read_pipeline(planned.physical(), &mut |stored| { actual.push(format!("{} {}", stored.id(), stored.document().get("n").and_then(|value| og_core::helpers::value_to_json(value).as_i64()).unwrap())); Ok(()) }).unwrap(); assert!(expected.len() > 1000); assert_eq!(actual, expected); }
