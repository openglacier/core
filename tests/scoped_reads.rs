//! A Place/App-scoped read runs with its scope written in its plan
//! ([`Engine::plan_scoped`]) and returns what the former scoped executor
//! returned, which filtered the stored rows by their scope fields (kept as a
//! digest): the same rows, and no error raised by a row of another scope.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, DocumentScope, PlannerPipeline},
    storage::{GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document, Value,
};

const ROWS: u32 = 4000;
const PLACE: &str = "place-a";
const APP: &str = "app-1";

/// The scope fields of a row: this scope, another instance, another Place,
/// and every kind of value a foreign or legacy row may hold.
fn scope_of(index: u32) -> (Option<Value>, Option<Value>) {
    match index % 10 {
        0..=3 => (Some(Value::from(PLACE)), Some(Value::from(APP))),
        4 => (Some(Value::from(PLACE)), Some(Value::from("app-2"))),
        5 => (Some(Value::from("place-b")), Some(Value::from(APP))),
        6 => (None, None),
        7 => (Some(Value::null()), Some(Value::from(APP))),
        8 => (Some(Value::from(i64::from(index))), Some(Value::from(APP))),
        _ => {
            let mut object = Document::new();
            object.insert("id", Value::from(PLACE));
            (Some(Value::object(object)), None)
        }
    }
}

fn row(index: u32) -> Document {
    let mut row = Document::new();
    let (place, app) = scope_of(index);
    if let Some(place) = place {
        row.insert("_place", place);
    }
    if let Some(app) = app {
        row.insert("_app_instance", app);
    }
    row.insert("n", Value::from(i64::from(index)));
    row.insert("code", Value::from(format!("C-{:02}", index % 23)));
    row.insert("price", Value::float(f64::from(index % 17) * 2.5).unwrap());
    row.insert("period", Value::from(format!("{:02}-2025", 1 + index % 12)));
    // Rows outside the scope hold a string `qty`: `qty > 3` fails on them.
    let in_scope = index % 10 <= 3;
    row.insert("qty", if in_scope { Value::from(i64::from(index % 7)) } else { Value::from("many") });
    if index.is_multiple_of(11) {
        row.insert("tags", Value::array([Value::from("x"), Value::from(i64::from(index % 3))]));
    }
    row
}

fn id(index: u32) -> DocumentId {
    DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).unwrap()
}

/// Two commits (Glacier: a checkpoint between them, so segments then
/// delta), a few deletions, and rows replaced after the checkpoint.
fn load(storage: Arc<dyn StorageEngine>, between: &dyn Fn()) -> Engine {
    let collection = CollectionId::parse("sales").unwrap();
    for range in [0..ROWS / 2, ROWS / 2..ROWS] {
        let mut transaction = storage.begin().unwrap();
        for index in range {
            transaction.insert(&collection, id(index), Arc::new(row(index))).unwrap();
        }
        transaction.commit().unwrap();
        between();
    }
    let mut transaction = storage.begin().unwrap();
    for index in (0..ROWS).filter(|index| index % 97 == 0) {
        transaction.delete(&collection, &id(index), og_core::storage::VersionPrecondition::Any).unwrap();
    }
    for index in (1..ROWS / 2).filter(|index| index % 89 == 0) {
        let mut replaced = row(index);
        replaced.insert("price", Value::float(999.0).unwrap());
        transaction.replace(&collection, &id(index), Arc::new(replaced), og_core::storage::VersionPrecondition::Any).unwrap();
    }
    transaction.commit().unwrap();
    Engine::new(storage)
}

struct Engines {
    engines: Vec<(&'static str, Engine)>,
    directory: std::path::PathBuf,
}

impl Engines {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let directory = std::env::temp_dir().join(format!("og-scoped-reads-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let memory = load(Arc::new(MemoryStorage::new()), &|| {});
        let storage = Arc::new(GlacierStorage::open(directory.join("scoped.glacier")).unwrap());
        let checkpoints = Arc::clone(&storage);
        let glacier = load(storage, &move || checkpoints.checkpoint().unwrap());
        Self { engines: vec![("memory", memory), ("glacier", glacier)], directory }
    }

    /// Each query, under each scope, on each store: a digest of the rows (or
    /// of the failure) the scoped executor returns, the same on every store.
    fn check(&self, queries: &[&str]) -> u64 {
        let digests = self.engines.iter().map(|(_, engine)| {
            let mut lines = Vec::new();
            for scope in [DocumentScope::new(PLACE, APP), DocumentScope::for_place(PLACE)] {
                for text in queries {
                    lines.push((*text).to_owned());
                    match comparable(text, scoped_executor(engine, text, &scope)) {
                        Ok(rows) => lines.extend(rows),
                        Err(_) => lines.push("error".to_owned()),
                    }
                }
            }
            digest(&lines)
        }).collect::<Vec<_>>();
        assert!(digests.windows(2).all(|pair| pair[0] == pair[1]), "{digests:x?}");
        digests[0]
    }
}

fn digest(rows: &[String]) -> u64 { rows.iter().flat_map(|row| row.bytes().chain(*b"\n")).fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)) }

impl Drop for Engines {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Rows in output order when the query orders them, sorted otherwise (the
/// former executor read by identifier, a stream in storage order; a
/// `distinct` kept the first-seen order, a stream the key order).
fn comparable(text: &str, outcome: Result<Vec<String>, String>) -> Result<Vec<String>, String> {
    outcome.map(|mut rows| {
        if !text.contains("sort") && !text.contains("group") && !text.contains("pivot") {
            rows.sort();
        }
        rows
    })
}

fn json(document: &Document) -> String {
    og_core::helpers::value_to_json(&Value::object(document.clone())).to_string()
}

fn scoped_executor(engine: &Engine, text: &str, scope: &DocumentScope) -> Result<Vec<String>, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    let pipeline = PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())?;
    let planned = engine.plan(&pipeline).map_err(|error| error.to_string())?;
    let output = engine.execute_physical_scoped(planned.physical(), scope).map_err(|error| error.to_string())?;
    Ok(output.rows().iter().map(|row| json(row.document())).collect())
}

#[test] fn scoped_reads_stream_the_rows_of_their_scope() { let engines = Engines::new(); assert_eq!(engines.check(&["on sales", "on sales | count", "on sales | limit 5", "on sales | skip 3 | limit 4", "on sales | where qty > 3", "on sales | where qty > 3 | count", "on sales | where qty > 3 | select code, price", "on sales | where price > 10 | derive total = price * qty | select n, total", "on sales | derive total = price * qty | where total > 20 | count", "on sales | select n, _place, _app_instance", "on sales | where _place == \"place-b\"", "on sales | where _id == \"01900000-0000-7000-8000-000000000002\"", "on sales | where _id == \"01900000-0000-7000-8000-000000000006\"", "on sales | sort price desc, n | limit 7", "on sales | sort qty | limit 3", "on sales | group code, price", "on sales | group | by period | sum price as s | count as c | avg qty as a | end", "on sales | group | by qty | count as c | end | sort c desc | limit 3", "on sales | distinct code", "on sales | distinct code | count", "on sales | select code | distinct", "on sales | pivot | rows code | columns period | values price | aggregate sum | end", "on sales | where tags != null | unwind tags | select n, tags", "on sales | where qty == 2 | limit 3 | lookup sales as o on o.code == code into same | select n, len(same) as k", "on sales as s | where qty == 1 | limit 2 | lookup sales as o | where o.code == s.code | into same | end | select n, len(same) as k", "on sales | where qty == 1 | limit 2 | lookup sales | where qty > 5 | into big | end | select n, len(big) as k", "on sales | where qty == 0 | union | on sales | where qty == 6 | end | count", "on sales | where qty == 0 | select n | union | on sales as u | where qty == 6 | select n | end", "on sales | where qty > 30 | count", "on sales | where price between 5 and 10 | count"]), 0x8361_69cf_1626_0ffc); }

#[test] fn scoped_writes_keep_the_scoped_executor() { let engines = Engines::new(); let (_, engine) = &engines.engines[0]; for text in ["on sales | where qty > 3 | delete", "on sales | insert {n: 1}", "on sales | where qty == 1 | into copy"] { let ast = parse(text).unwrap(); let pipeline = PlannerPipeline::from_ast(text, &ast).unwrap(); assert!(engine.plan_scoped(&pipeline, PLACE, Some(APP)).unwrap().is_none(), "{text}"); } }
