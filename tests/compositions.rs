//! Several blocking stages in one query: the governed path chains them in
//! bounded memory, with and without spilling, and returns the rows the former
//! materialized executor returned (kept as a digest of each result).
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, PlannerPipeline},
    storage::{CollectionId, DocumentId, GlacierStorage, MemoryStorage, StorageEngine},
    Document, MemoryGovernor, Value,
};

const ROWS: u64 = 30_000;

fn storage() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    load(storage.as_ref(), &|| {});
    storage
}

/// The same rows on Glacier, a checkpoint after the first half: rows come
/// from segments, read as partial documents, and from the delta.
fn glacier() -> (Engine, std::path::PathBuf) {
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("og-compositions-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let storage = Arc::new(GlacierStorage::open(directory.join("compositions.glacier")).unwrap());
    load(storage.as_ref(), &|| storage.checkpoint().unwrap());
    (Engine::new(storage), directory)
}

fn load(storage: &dyn StorageEngine, between: &dyn Fn()) {
    let sales = CollectionId::parse("sales").unwrap();
    let articles = CollectionId::parse("articles").unwrap();
    let mut transaction = storage.begin().unwrap();
    for n in 0..ROWS {
        if n == ROWS / 2 {
            transaction.commit().unwrap();
            between();
            transaction = storage.begin().unwrap();
        }
        let mut document = Document::new();
        document.insert("n", n);
        document.insert("code", format!("A-{:05}", (n * 7919) % 9_000));
        document.insert("qty", n % 7);
        document.insert("period", format!("{:02}-2025", 1 + n % 12));
        if n % 5 == 0 {
            document.insert("note", format!("promo{}", n % 3));
        }
        let id = DocumentId::parse(format!("018bcfe5-6800-7000-8000-{n:012x}")).unwrap();
        transaction.insert(&sales, id, Arc::new(document)).unwrap();
    }
    for n in 0..40u64 {
        let mut document = Document::new();
        document.insert("code", format!("A-{:05}", n * 10));
        document.insert("label", format!("article {n}"));
        let id = DocumentId::parse(format!("018bcfe5-6800-7000-9000-{n:012x}")).unwrap();
        transaction.insert(&articles, id, Arc::new(document)).unwrap();
    }
    transaction.commit().unwrap();
}

fn plan(engine: &Engine, text: &str) -> og_core::query::PhysicalPlan {
    let ast = parse(text).unwrap();
    engine.plan(&PlannerPipeline::from_ast(text, &ast).unwrap()).unwrap().physical().clone()
}

fn render(document: &Document) -> String {
    let mut fields = document.iter().map(|(name, value)| format!("{name}={value:?}")).collect::<Vec<_>>();
    fields.sort();
    fields.join(",")
}

fn governed(engine: &Engine, text: &str) -> Result<Vec<String>, String> {
    let physical = plan(engine, text);
    let mut rows = Vec::new();
    engine
        .stream_governed_blocking_pipeline(&physical, &mut |row| {
            rows.push(render(row.document()));
            Ok(())
        })
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| panic!("{text}: a governed streaming shape"));
    Ok(rows)
}

fn digest(rows: &[String]) -> u64 { rows.iter().flat_map(|row| row.bytes().chain(*b"\n")).fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)) }

/// Queries whose rows come in a defined order, compared as they come.
const ORDERED: &[(&str, u64)] = &[
    ("on sales | sort qty desc, n | limit 500 | group code | sort count desc, code", 0xae6fa523c192b7b7),
    ("on sales | distinct code | sort code desc | limit 30", 0xe05c53dfb24a4599),
    ("on sales | distinct period | sort period", 0x75346dd63116fc0c),
    ("on sales | group code | where count > 3 | sort count desc, code", 0xa1e2532f8221b3c8),
    ("on sales | group code, qty | derive x = qty * 2 | sort x desc, code | limit 7", 0x7e22b8aa9a79d218),
    ("on sales | where qty == 3 | sort n desc | limit 40 | group period | sort period", 0x3ef2d2685a1de134),
    ("on sales | group period | sort period | distinct count", 0xbdda4aa57ac7ffb9),
    ("on sales | group code | sort count desc, code | limit 50 | sort code | first", 0xae1274fa0af80f1f),
    ("on sales | group code | select code, count | sort code desc | skip 5 | limit 5", 0x89e8fefd5e2bd427),
    ("on sales as s | where n < 400 | lookup articles as a | where a.code == s.code | into article | end | where n < 300 | sort n desc | limit 20", 0x279343b87cdebc29),
    ("on sales | where n < 50 | union | on articles | end | sort code, n", 0x3b59d27861cfeeea),
    ("on sales | where qty == 1 | sort n | distinct period", 0x0ea9a78331291a65),
    ("on sales | where qty == 2 | sort code, n | distinct code | sort n desc | limit 15", 0xbd89603a7330b78e),
    ("on sales | group code | group count | sort count", 0xc0860af20ad0af5c),
    ("on sales as s | group code | where count > 3 | lookup articles as a | where a.code == s.code | into article | end | sort code | limit 30", 0xbabd676b8efdf608),
    ("on sales | group period | union | on articles | where code < \"A-00100\" | end | sort period, code", 0x7d288a491e04434a),
    ("on sales | where qty == 5 | group period | sort count desc, period | group count | sort count", 0x937b54987cded8f6),
    ("on sales | group period, code, qty", 0xb8c9c02616ec3bcc),
    ("on sales | group qty, period", 0x17d8c0b742a53998),
    ("on sales | where qty > 3 | union | on sales | where qty < 2 | derive t = qty * 2 | end | group period, t | sort period, t", 0xeb5217813c903ebc),
    ("on sales | where n < 100 | union | on articles | where code < \"A-00050\" | end | group code | sort code", 0xab5dc3af10419642),
];

/// Queries whose final stage defines no order, compared as sets.
const UNORDERED: &[(&str, u64)] = &[
    ("on sales | sort n | limit 100 | group code", 0x01bada3001c9e95f),
    ("on sales | where qty == 4 | sort n desc | limit 300 | distinct code", 0xcea974d923511fa5),
    ("on sales | group code, period | group code", 0x13ed8371e6e28149),
    ("on sales | distinct code | count", 0xd6b1016033891f4a),
    ("on sales | distinct code, period, qty", 0x558b90c7285ba44d),
    ("on sales | group code | where count > 4 | count", 0xb654bd23eabd40d7),
    ("on sales | sort n | limit 2000 | pivot | rows period | columns qty | values n | aggregate sum | end", 0x9570478a90c77a53),
];

fn check(engine: &Engine) {
    for (list, ordered) in [(ORDERED, true), (UNORDERED, false)] {
        for (text, expected) in list {
            let mut rows = governed(engine, text).unwrap_or_else(|error| panic!("{text}: {error}"));
            if !ordered {
                rows.sort();
            }
            assert_eq!(digest(&rows), *expected, "{text}");
        }
    }
}

#[test] fn chained_blocking_stages_return_what_the_executor_returned() { check(&Engine::new(storage())); }

#[test] fn chained_blocking_stages_read_partial_documents_on_glacier() { let (engine, directory) = glacier(); check(&engine); drop(engine); let _ = std::fs::remove_dir_all(directory); }

#[test] fn chained_blocking_stages_spill_within_a_small_memory() { let limited = Engine::new(storage()).with_memory_governor(MemoryGovernor::with_limit(6 * 1024 * 1024)); check(&limited); assert_eq!(limited.memory_governor().reserved_bytes(), 0, "every stage returns its memory"); }

#[test] fn a_sample_after_a_blocking_stage_keeps_its_size() { let engine = Engine::new(storage()); let rows = governed(&engine, "on sales | group code | sample 10 | sort code").unwrap(); assert_eq!(rows.len(), 10); let mut sorted = rows.clone(); sorted.sort(); assert_eq!(rows, sorted); let rows = governed(&engine, "on sales | sort n | sample 5 | count").unwrap(); assert_eq!(rows, vec![format!("count={:?}", Value::from(5u64))]); }

#[test] fn later_sorts_and_groups_spill_to_runs() { use og_core::query::ExecutionStrategy::{ExternalGroup, ExternalSort}; let storage = storage(); let oracle = Engine::new(storage.clone()); let limited = Engine::new(storage).with_memory_governor(MemoryGovernor::with_limit(2 * 1024 * 1024)); for (text, strategy) in [("on sales | union | on articles | end | sort code, n", ExternalSort), ("on sales | union | on articles | end | group code, qty | sort code", ExternalGroup)] { let physical = plan(&limited, text); let mut rows = Vec::new(); let statistics = limited.stream_governed_blocking_pipeline(&physical, &mut |row| { rows.push(render(row.document())); Ok(()) }).unwrap().unwrap(); assert!(statistics.strategies().contains(strategy), "{text}: {:?}", statistics.strategies()); assert_eq!(rows, governed(&oracle, text).unwrap(), "{text}"); } }
