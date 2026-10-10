//! Keyed consumers on batches (`distinct`, `group`, `pivot`, Top-N): rows,
//! their order and errors must be exactly those of the document path
//! (MemoryStorage never reads batches). Glacier feeds batches row by row,
//! Glacier by blocks, with dictionary codes once rows reach segments.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, PlannerPipeline},
    storage::{GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine},
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
    // A key of mixed kinds: integers, the same values as floats, text,
    // null, missing, and a few arrays and objects.
    let mixed = match index % 9 {
        0 => None,
        1 => Some(Value::null()),
        2 => Some(Value::from(i64::from(index % 4))),
        3 => Some(Value::float(f64::from(index % 4)).unwrap()),
        4 => Some(Value::array([Value::from("x"), Value::from(i64::from(index % 2))])),
        5 => {
            let mut object = Document::new();
            object.insert("k", Value::from(i64::from(index % 3)));
            Some(Value::object(object))
        }
        _ => Some(Value::from(format!("m{}", index % 5))),
    };
    if let Some(mixed) = mixed {
        row.insert("mixed", mixed);
    }
    if index.is_multiple_of(5) {
        row.insert("note", Value::from(format!("promo{}", index % 3)));
    }
    if index.is_multiple_of(11) {
        row.insert("note2", Value::null());
    }
    row
}

fn id(index: u32) -> DocumentId {
    DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).unwrap()
}

/// Loads the rows in two commits (and, for Glacier, a checkpoint between
/// them: segments then delta), then deletes a few. (Replaced rows move in
/// Glacier's storage order, which `first`/`last` follow.)
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
    transaction.commit().unwrap();
    // Many distinct keys, for bounded groups that keep few of them.
    let wide = CollectionId::parse("wide").unwrap();
    let mut transaction = storage.begin().unwrap();
    for index in 0..WIDE {
        let mut row = Document::new();
        row.insert("n", Value::from(i64::from((index * 7919) % WIDE)));
        row.insert("g", Value::from(format!("g{}", index % 3)));
        row.insert("v", Value::float(f64::from(index % 101) / 4.0).unwrap());
        transaction.insert(&wide, DocumentId::parse(format!("01900000-0000-7000-9000-{:012x}", index + 1)).unwrap(), Arc::new(row)).unwrap();
    }
    transaction.commit().unwrap();
    between();
    Engine::new(storage)
}

struct Engines {
    memory: Engine,
    glacier: Engine,
    directory: std::path::PathBuf,
}

impl Engines {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let directory = std::env::temp_dir().join(format!("og-batch-keys-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let memory = load(Arc::new(MemoryStorage::new()), &|| {});
        let storage = Arc::new(GlacierStorage::open(directory.join("keys.glacier")).unwrap());
        let checkpoints = Arc::clone(&storage);
        let glacier = load(storage, &move || checkpoints.checkpoint().unwrap());
        Self { memory, glacier, directory }
    }

    /// Each query returns the rows (in order) or the error of the document
    /// path, on both stores.
    fn check(&self, queries: &[&str]) {
        for text in queries {
            let expected = outcome(&self.memory, text);
            assert!(expected.as_ref().map_or(true, |rows| !rows.is_empty()), "{text}: {expected:?}");
            assert_eq!(failure_kind(outcome(&self.glacier, text)), failure_kind(expected), "glacier: {text}");
        }
    }
}

impl Drop for Engines {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// An error without the value that raised it: an order-free consumer reads
/// rows in storage order, so the first failing row may differ from the
/// document path's (as `group` already does).
fn failure_kind(outcome: Result<Vec<String>, String>) -> Result<Vec<String>, String> {
    outcome.map_err(|error| error.split(", found").next().unwrap_or_default().to_owned())
}

/// Rows in output order, or the error text.
fn outcome(engine: &Engine, text: &str) -> Result<Vec<String>, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    let pipeline = PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())?;
    let planned = engine.plan(&pipeline).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    let statistics = if engine.supports_governed_blocking_streaming(planned.physical()) {
        engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(row.document().clone()); Ok(()) })
    } else {
        engine.stream_read_pipeline(planned.physical(), &mut |stored| { rows.push(stored.document().clone()); Ok(()) })
    }
    .map_err(|error| error.to_string())?;
    assert!(statistics.is_some(), "{text}: no streaming executor");
    Ok(rows.iter().map(|row| og_core::helpers::value_to_json(&Value::object(row.clone())).to_string()).collect())
}

#[test] fn distinct_on_batches_matches_the_document_path() { Engines::new().check(&["on sales | distinct code", "on sales | distinct code | count", "on sales | distinct mixed", "on sales | distinct mixed | count", "on sales | distinct note, code", "on sales | distinct note2, note", "on sales | distinct qty, mixed, period", "on sales | where qty > 2 | distinct code, qty", "on sales | derive k = qty % 3, p = substr(period, 0, 2) | distinct k, p", "on sales | derive k = code + 1 | distinct k", "on sales | select code | distinct", "on sales | select code | distinct | count", "on sales | select mixed, note | distinct", "on sales | select note | distinct", "on sales | where price > 3 | select period, qty | distinct", "on sales | select code, missing_field | distinct", "on sales | distinct code | limit 3", "on sales | distinct mixed | skip 2 | limit 3", "on sales | distinct note, code | limit 4", "on sales | distinct qty, mixed, period | limit 5", "on sales | where qty > 2 | distinct code, qty | skip 1 | limit 3"]); }

#[test] fn group_on_batches_matches_the_document_path() { Engines::new().check(&["on sales | group | by code | sum price as s | avg qty as a | min price as lo | max code as hi | first note as f | last note as l | count as c | end", "on sales | group | by note | count as c | count note as cn | last mixed as lm | first mixed as fm | end", "on sales | group | by mixed | count as c | sum qty as q | sum mixed as sm | avg mixed as am | min mixed as mn | max mixed as mx | end", "on sales | group | by qty, code | sum price as s | end", "on sales | group | by mixed, note2 | count as c | last code as l | end", "on sales | group code, qty", "on sales | group | by n | count as c | end | limit 5", "on sales | group | by price | sum qty as q | end | limit 4", "on sales | group | by mixed | count as c | end | limit 3", "on sales | group | by qty | count as c | end | limit 1", "on sales | derive d = qty - 3 | group d, code | limit 2", "on sales | derive f = price - 9.5 | group f | limit 1", "on sales | group mixed, price | limit 1", "on sales | group price, code | limit 3", "on sales | derive k = qty % 3 | group | by k | sum price as s | count as c | end", "on sales | where qty > 2 | group | by code | count as c | end", "on sales | derive k = if(qty > 3, qty, price) | group | by k | count as c | end", "on sales | derive k = qty + price | group | by k | min note as m | end", "on sales | group | by period | max price as m | min qty as q | end", "on sales | group | by code, period | min price as lo | max mixed as hi | min qty as q | end", "on sales | group | by qty, note | max price as hi | min mixed as lo | end", "on sales | derive k = code + 1 | group | by k | count as c | end", "on sales | group | by code | sum price as s | end | sort s desc | limit 3", "on wide | group | by n | count as c | sum v as s | end | limit 5", "on wide | group | by g, n | last v as l | end | limit 7", "on wide | group | by g | sum v as s | avg v as a | count as c | end", "on wide | group | by v | count as c | end", "on sales | group | by code | last note by n as ln | first price by qty as fp | last mixed by period as lm | first code by price where qty > 3 as fc | end", "on sales | group | by period | sum price where qty > 3 as s | count where note == null as nn | avg qty * price as a | min price - qty as lo | max code where mixed != null as hi | end", "on wide | group | by g | last n by v as top | first v by n where v > 10 as low | sum v where n % 2 == 0 as even | end | limit 2", "on wide | derive day = n * 86400000 * 3 | group | by g | min fiscal_year(day, 6) as fy | max fiscal_month(day, 3) as fm | max fiscal_end(day, 12) as fe | min fiscal_start(day, 1) as fs | max fiscal_year(day, \"2019-04-01\") as fo | end"]); }

#[test] fn pivot_on_batches_matches_the_document_path() { Engines::new().check(&["on sales | pivot | rows code | columns period | values price | aggregate sum | end", "on sales | pivot | rows code | columns period | values price, qty as q, price - qty as d | aggregate sum | end", "on sales | pivot | rows note | columns qty | values price | aggregate avg | end", "on sales | pivot | rows mixed | columns note | values code | aggregate max | end", "on sales | pivot | rows mixed, note2 | columns qty, note | values price | aggregate count | end", "on sales | pivot | rows period | columns mixed | values mixed | aggregate min | end", "on sales | pivot | rows code | columns note | values note | aggregate first | end", "on sales | pivot | rows code | columns note | values mixed | aggregate last | end", "on sales | where qty > 2 | derive k = qty % 3 | pivot | rows k | columns code | values price | aggregate sum | end", "on wide | pivot | rows g | columns n | values v | aggregate sum | end"]); }

#[test] fn top_n_on_batches_matches_the_document_path() { Engines::new().check(&["on sales | sort price | limit 5", "on sales | sort price desc | limit 7", "on sales | sort qty | limit 4", "on sales | sort qty desc, n | limit 6", "on sales | sort code desc | limit 5", "on sales | sort code, price desc | limit 9", "on sales | sort period, qty desc | skip 3 | limit 5", "on sales | sort note | limit 5", "on sales | sort note desc | limit 5", "on sales | sort mixed | limit 5", "on sales | where qty > 2 | sort price desc | limit 3", "on sales | sort price - qty desc | limit 5", "on sales | derive d = price * 2 | sort d, n desc | limit 5", "on wide | sort v desc | limit 8", "on wide | sort g desc, v | limit 8", "on wide | sort n | limit 3"]); }

#[test] fn window_on_batches_matches_the_document_path() { Engines::new().check(&["on sales | window | partition by code | order by n | lag price as p | lead qty 2 as q | cumsum price as c | first note as f | last mixed as l | row_number as r | end", "on wide | window | partition by g | order by v desc, n | cumsum v as c | row_number as r | end | where r <= 3", "on sales | where qty > 2 | window | order by price, n | lag code as prev | lead mixed as next | end | limit 20", "on sales | window | partition by qty, note | order by n desc | cumsum mixed as c | end | group | by qty | max c as m | end", "on sales | fill qty from 0 to 9 step 1 by code with price = 0, note = \"gap\"", "on wide | where n < 500 | fill n from 0 to 600 step 7 by g | group | by g | count as c | sum v as s | end", "on sales | fill mixed from 0 to 3 step 0.5 by note", "on sales as s | where n < 60 | lookup sales as r | where starts_with(s.code, r.code) and r.qty > 3 | select n | into hits | end", "on sales as s | where n < 20 | lookup sales as r | where starts_with(s.period, r.mixed) | select n | into hits | end"]); }
