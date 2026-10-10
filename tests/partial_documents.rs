//! Partial documents: when a query's field demand is known, Glacier decodes
//! only the fields the stages read. Every query must return exactly what the
//! full-document path returns (MemoryStorage never takes the partial path).
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, ExecutionStrategy, PlannerPipeline},
    storage::{GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document, Value,
};

fn rows() -> Vec<Document> {
    (0..240_u32)
        .map(|index| {
            let mut profile = Document::new();
            profile.insert("country", Value::from(["FR", "DE", "IT"][index as usize % 3]));
            profile.insert("score", Value::from(i64::from(index % 10)));
            let mut row = Document::new();
            row.insert("n", Value::from(i64::from(index)));
            row.insert("code", Value::from(format!("A-{:03}", index % 17)));
            row.insert("qty", Value::from(i64::from(index % 7)));
            row.insert("price", Value::float(f64::from(index % 13) * 1.5).unwrap());
            row.insert("period", Value::from(format!("{:02}-2025", 1 + index % 12)));
            row.insert("profile", Value::object(profile));
            row.insert("tags", Value::array((0..index % 3).map(|tag| Value::from(format!("t{tag}")))));
            row.insert("payload", Value::from("x".repeat(64)));
            if index % 5 == 0 {
                row.insert("note", Value::from("promo"));
            }
            row
        })
        .collect()
}

fn engine(storage: Arc<dyn StorageEngine>) -> Engine {
    let collection = CollectionId::parse("sales").unwrap();
    let mut transaction = storage.begin().unwrap();
    for (index, row) in rows().into_iter().enumerate() {
        let id = DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).unwrap();
        transaction.insert(&collection, id, Arc::new(row)).unwrap();
    }
    transaction.commit().unwrap();
    Engine::new(storage)
}

fn glacier_engine() -> (Engine, std::path::PathBuf) {
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("og-partial-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(GlacierStorage::open(directory.join("partial.glacier")).unwrap());
    (engine(storage), directory)
}

/// Rows as JSON text, sorted: the partial path reads an unordered cursor.
fn run(engine: &Engine, text: &str) -> (Vec<String>, Vec<ExecutionStrategy>) {
    let (rows, statistics) = counted(engine, text);
    (rows, statistics.strategies().iter().collect())
}

fn counted(engine: &Engine, text: &str) -> (Vec<String>, og_core::query::ExecutionStatistics) {
    let ast = parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
    let pipeline = PlannerPipeline::from_ast(text, &ast).unwrap();
    let planned = engine.plan(&pipeline).unwrap_or_else(|error| panic!("{text}: {error}"));
    let mut rows = Vec::new();
    let statistics = if engine.supports_governed_blocking_streaming(planned.physical()) {
        engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(row.document().clone()); Ok(()) })
    } else {
        engine.stream_read_pipeline(planned.physical(), &mut |stored| { rows.push(stored.document().clone()); Ok(()) })
    }
    .unwrap_or_else(|error| panic!("{text}: {error}"))
    .unwrap_or_else(|| panic!("{text}: no bounded executor"));
    let mut rows = rows.iter().map(|row| og_core::helpers::value_to_json(&Value::object(row.clone())).to_string()).collect::<Vec<_>>();
    rows.sort();
    (rows, statistics)
}

#[test] fn partial_documents_return_the_same_rows_as_complete_documents() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); let queries = [/* Streaming, terminal count. */ "on sales | derive total = price * qty | where total > 20 | count", "on sales | where qty > 2 | derive country = profile.country, big = price > 10 | where country == \"FR\" and big | count", "on sales | derive promo = note? == \"promo\" | where promo | count", "on sales | derive id = _id | where len(id) == 36 | count", "on sales | where code != \"A-001\" | rename qty as q | where q > 3 | drop price | count", "on sales | where exists(tags) | unwind tags with index i | where i >= 0 | count", /* Blocking cores that ignore input order. */ "on sales | derive total = price * qty | group | by code | sum total as revenue | count as rows | end", "on sales | bucket price * qty by [0, 10, 50, 1000] default \"other\" as band", "on sales | pivot | rows code | columns period | values qty | aggregate sum | end", "on sales | where qty > 0 | sample 500 | count", "on sales | where qty > 3 | union | on sales | where qty < 2 | end | count", "on sales | derive country = profile.country | select country | distinct", "on sales | derive m = price - qty | select code, m | sort m desc, code | limit 5", /* Top-N ranked on partial documents; winners hydrated and replayed. */ "on sales | sort price * qty desc, n | limit 5", "on sales | where qty > 1 | derive total = price * qty | rename code as sku | sort total desc, n | first", "on sales | derive country = profile.country | drop payload | sort country, n | skip 3 | limit 4"]; for text in queries { let (expected, memory_strategies) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert!(!expected.is_empty(), "{text}"); assert_eq!(actual, expected, "{text}"); assert!(!memory_strategies.contains(&ExecutionStrategy::PartialDocuments), "{text}"); assert!(strategies.contains(&ExecutionStrategy::PartialDocuments), "{text}: {strategies:?}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn hydrated_top_n_rows_keep_every_field() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); let text = "on sales | derive total = price * qty | sort total desc, n | limit 3"; let (expected, _) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert_eq!(actual, expected); assert!(actual.iter().all(|row| row.contains("payload") && row.contains("\"total\"")), "{actual:?}"); assert!(strategies.contains(&ExecutionStrategy::PartialDocuments) && strategies.contains(&ExecutionStrategy::TopN), "{strategies:?}"); let _ = std::fs::remove_dir_all(directory); }

#[test] fn whole_row_outputs_keep_complete_documents() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* No field demand, or an order-dependent bound before the consumer: the */ /* ordered complete-document scan is kept. */ for text in ["on sales | derive total = price * qty | where total > 20 | limit 5", "on sales | where qty > 1 | limit 30 | count", "on sales | limit 100 | derive total = price * qty | group | by code | sum total as revenue | end", "on sales | limit 50 | sort price * qty desc, n | first"] { let (expected, _) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert_eq!(actual, expected, "{text}"); assert!(!strategies.contains(&ExecutionStrategy::PartialDocuments), "{text}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn sampled_rows_are_hydrated_complete() { let (glacier, directory) = glacier_engine(); let (rows, strategies) = run(&glacier, "on sales | where qty > 0 | derive total = price * qty | sample 7"); assert_eq!(rows.len(), 7); assert!(strategies.contains(&ExecutionStrategy::PartialDocuments), "{strategies:?}"); for row in &rows { let row: serde_json::Value = serde_json::from_str(row).unwrap(); let (price, qty, total) = (row["price"].as_f64().unwrap(), row["qty"].as_f64().unwrap(), row["total"].as_f64().unwrap()); assert!(row.get("payload").is_some() && qty > 0.0 && (price * qty - total).abs() < 1e-9, "{row}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn lookup_index_built_from_keys_joins_complete_rows() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* The Glacier index reads only the key; candidates are loaded complete. */ for text in ["on sales | where n < 20 | lookup sales on code == code into same | derive hits = len(same) | select n, hits, same", "on sales as s | where n < 10 | lookup sales as o | where o.qty == s.qty and o.n != s.n | first | into twin | end", "on sales as s | where n < 5 | derive sid = _id | lookup sales as o | where o._id == s.sid | into me | end"] { let (expected, _) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert_eq!(actual, expected, "{text}"); assert!(strategies.contains(&ExecutionStrategy::HashJoin), "{text}: {strategies:?}"); } let _ = std::fs::remove_dir_all(directory); }

/// Rows in output order, or error text.
fn ordered_outcome(engine: &Engine, text: &str) -> Result<Vec<String>, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    let pipeline = PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())?;
    let planned = engine.plan(&pipeline).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(row.document().clone()); Ok(()) }).map_err(|error| error.to_string())?;
    Ok(rows.iter().map(|row| og_core::helpers::value_to_json(&Value::object(row.clone())).to_string()).collect())
}

/// Rows or error text, rows sorted as [`run`] sorts them.
fn outcome(engine: &Engine, text: &str) -> Result<Vec<String>, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    let pipeline = PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())?;
    let planned = engine.plan(&pipeline).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(row.document().clone()); Ok(()) }).map_err(|error| error.to_string())?;
    let mut rows = rows.iter().map(|row| og_core::helpers::value_to_json(&Value::object(row.clone())).to_string()).collect::<Vec<_>>();
    rows.sort();
    Ok(rows)
}

#[test] fn bounded_lookup_semi_join_matches_the_full_index() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* The outer side is bounded: only joined rows that can match its keys */ /* are indexed. Missing keys, numbers against strings (an error) and */ /* errors must stay exactly as with every row indexed. */ for text in ["on sales | limit 3 | lookup sales on code == code into same | select n, same", "on sales as s | where n > 30 | limit 4 | lookup sales as o | where o.qty == s.qty and o.n != s.n | into twin | end", "on sales as s | skip 5 | limit 2 | lookup sales as o | where o.price == s.qty | into p | end", "on sales as s | limit 2 | derive k = note? | lookup sales as o | where o.note? == s.k | into same | end", "on sales as s | limit 2 | lookup sales as o | where o.code == s.qty | into mixed | end", "on sales as s | limit 2 | lookup sales as o | where exists(o.note) and o.note == s.n | into mixed | end"] { let expected = outcome(&memory, text); let actual = outcome(&glacier, text); assert_eq!(actual, expected, "{text}"); } let (_, strategies) = run(&glacier, "on sales | limit 3 | lookup sales on code == code into same | select n, same"); assert!(strategies.contains(&ExecutionStrategy::HashJoin), "{strategies:?}"); let _ = std::fs::remove_dir_all(directory); }

#[test] fn sample_drawn_before_total_stages_keeps_rows_and_errors() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* A sample larger than the collection keeps every row: the result is */ /* deterministic. Stages proven total (`code` is always a string) may run */ /* after the draw; a stage that may fail (`code + 1`) must still fail. */ for text in ["on sales | sample 500 | count", "on sales | sample 500", "on sales | derive c = code, promo = code == \"A-001\" | rename c as k | drop payload | sample 500", "on sales | where qty > 2 | derive c = code | sample 500 | count", "on sales | derive bad = code + 1 | sample 500 | count", "on sales | derive bad = code < 3 | sample 5 | count", "on sales | derive late = note == \"promo\" | sample 500"] { assert_eq!(outcome(&glacier, text), outcome(&memory, text), "{text}"); } assert!(outcome(&glacier, "on sales | derive bad = code + 1 | sample 5 | count").is_err()); let rows = outcome(&glacier, "on sales | derive c = code | sample 7").unwrap(); assert_eq!(rows.len(), 7); assert!(rows.iter().all(|row| row.contains("\"c\"") && row.contains("payload")), "{rows:?}"); let _ = std::fs::remove_dir_all(directory); }

#[test] fn batched_groups_match_the_document_path() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* Typed keys (text, numbers, missing, objects), several keys, integer */ /* against float keys of the same value, bounded groups and batched */ /* prefixes: rows, their order and errors match the document path. */ for text in ["on sales | group | by code | sum price as s | avg qty as a | min price as lo | max code as hi | first note as f | last note as l | count as c | end", "on sales | group | by note | count as c | end", "on sales | group | by profile | count as c | sum qty as q | end", "on sales | group | by qty, code | sum price as s | end", "on sales | group code, qty", "on sales | group | by n | count as c | end | limit 5", "on sales | group | by price | sum qty as q | end | limit 4", "on sales | derive k = qty % 3 | group | by k | sum price as s | count as c | end", "on sales | where qty > 2 | group | by code | count as c | end", "on sales | derive k = if(qty > 3, qty, price) | group | by k | count as c | end", "on sales | derive k = qty + price | group | by k | min note as m | end", "on sales | group | by period | max price as m | min qty as q | end", "on sales | derive k = code + 1 | group | by k | count as c | end"] { let expected = ordered_outcome(&memory, text); assert!(expected.as_ref().map_or(true, |rows| !rows.is_empty()), "{text}"); assert_eq!(ordered_outcome(&glacier, text), expected, "{text}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn shared_union_scan_counts_and_fails_as_two_scans() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); /* Both sides read `sales` once; a failing side fails as when each side */ /* scans in turn (the main side first). */ for text in ["on sales | where qty > 2 | union | on sales | where qty <= 2 | end | count", "on sales | derive t = price * qty | where t > 10 | union | on sales | where code == \"A-001\" | end | count", "on sales | union | on sales | end | count", "on sales | where qty > 2 | union | on sales | where code + 1 > 0 | end | count", "on sales | where code + 1 > 0 | union | on sales | where qty / 0 > 1 | end | count"] { assert_eq!(outcome(&glacier, text), outcome(&memory, text), "{text}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn row_local_stages_on_slots_match_documents() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); for text in ["on sales | derive qty = qty * 2, double = qty | where double > 6 | count", "on sales | where qty > 1 | select code, price * qty as total, total + 1 as next | where next > 10 | count", "on sales | rename note as tag | where tag? == \"promo\" | drop tag | count", "on sales | drop qty | derive q = qty? | where q == null | count", "on sales | derive total = price * qty | drop payload | group | by code | sum total as revenue | count as rows | end", "on sales | derive country = profile.country | select country, code | distinct", "on sales | where qty > 2 | derive total = price * qty | pivot | rows code | columns period | values total | aggregate sum | end", "on sales | pivot | rows code, qty | columns period | values note | aggregate count | end", "on sales | where qty > 2 | pivot | rows code | columns period | values qty, price * qty as total, profile.score as score | aggregate avg | end", /* `unwind` expands slot rows: arrays, empty arrays (`tags` of every */ /* third row), scalars, missing fields and `keep empty`. */ "on sales | derive parts = split(code, \"-\") | unwind parts with index i | select n, parts, i | where i >= 0 | distinct", "on sales | unwind tags with index i | derive k = concat(tags, \"/\", to_string(i)) | select n, tags, i, k | distinct", "on sales | unwind tags keep empty | select n, tags | distinct", "on sales | unwind qty with index i | where i == null | count", "on sales | unwind note with index i | select n, note, i | distinct", "on sales | unwind tags | unwind tags | count", "on sales | unwind tags with index i | pivot | rows code | columns i | values qty | aggregate sum | end", /* Top-N ranked on slots behind row-local stages, winners replayed. */ "on sales | derive total = price * qty | rename code as sku | sort total desc, n | limit 4", "on sales | where qty > 1 | select n, price * qty as total, code | sort total, n desc | limit 3"] { let (expected, _) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert!(!expected.is_empty(), "{text}"); assert_eq!(actual, expected, "{text}"); assert!(strategies.contains(&ExecutionStrategy::PartialDocuments), "{text}: {strategies:?}"); } /* Evaluation errors surface on slots exactly as on documents. */ for text in ["on sales | derive ratio = price / qty | count", "on sales | derive x = missing + 1 | count"] { let failure = |engine: &Engine| { let ast = parse(text).unwrap(); let pipeline = PlannerPipeline::from_ast(text, &ast).unwrap(); let planned = engine.plan(&pipeline).unwrap(); engine.stream_read_pipeline(planned.physical(), &mut |_| Ok(())).unwrap_err().to_string() }; assert_eq!(failure(&glacier), failure(&memory), "{text}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn nested_paths_read_as_columns_match_documents() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); for text in ["on sales | where profile.score > 3 | count", "on sales | derive s = profile.score * 2 | where s > 4 | count", "on sales | group profile.country, qty", "on sales | group | by profile.country | sum profile.score as total | count as n | end", "on sales | sort profile.score desc, n | limit 5", "on sales | distinct profile.country, code", "on sales | where profile.score > 5 | select n, profile.country", "on sales | where qty > 1 | select profile, profile.score, code", "on sales | derive total = price * qty | where profile.score > 2 | select code, total, profile.score", /* The top-level field redefined, renamed or dropped after the scan. */ "on sales | derive profile = {\"score\": 1} | where profile.score == 1 | count", "on sales | rename profile as p | where p.score > 3 | count", "on sales | drop profile | where profile.score? == null | count", "on sales | where profile.missing? == null | count"] { let (expected, _) = run(&memory, text); let (actual, _) = run(&glacier, text); assert!(!expected.is_empty(), "{text}"); assert_eq!(actual, expected, "{text}"); } let _ = std::fs::remove_dir_all(directory); }

#[test] fn sampled_rows_read_as_slots_come_back_complete() { let memory = engine(Arc::new(MemoryStorage::new())); let (glacier, directory) = glacier_engine(); let text = "on sales | where qty > 1 | derive total = price * qty | sample 7"; let (expected, _) = run(&memory, text); let (actual, strategies) = run(&glacier, text); assert_eq!(actual.len(), expected.len()); assert!(strategies.contains(&ExecutionStrategy::PartialDocuments), "{strategies:?}"); /* Every sampled row is a complete row that passed the prefix, with its derived field. */ let complete = run(&memory, "on sales | where qty > 1 | derive total = price * qty").0; assert!(actual.iter().all(|row| complete.contains(row) && row.contains("payload")), "{actual:?}"); let _ = std::fs::remove_dir_all(directory); }

#[test] fn whole_document_distinct_ranks_projected_values() { let collection = CollectionId::parse("flat").unwrap(); let load = |storage: Arc<dyn StorageEngine>| { let mut transaction = storage.begin().unwrap(); for index in 0..300_u32 { let mut row = Document::new(); /* Few distinct shapes: duplicates, absent fields and nulls. */ row.insert("b", Value::from(i64::from(index % 4))); if index % 3 != 0 { row.insert("a", Value::from(format!("k{}", index % 5))); }; if index % 7 == 0 { row.insert("Z", Value::null()); }; if index % 11 == 0 { row.insert("rare", Value::float(0.5).unwrap()); } let id = DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).unwrap(); transaction.insert(&collection, id, Arc::new(row)).unwrap(); } transaction.commit().unwrap(); /* The only rows holding `rare` are deleted: the field leaves the catalog. */ let mut transaction = storage.begin().unwrap(); for index in (0..300_u32).filter(|index| index % 11 == 0) { let id = DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).unwrap(); transaction.delete(&collection, &id, og_core::storage::VersionPrecondition::Any).unwrap(); } transaction.commit().unwrap(); Engine::new(storage) }; let memory = load(Arc::new(MemoryStorage::new())); let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-distinct-{}-{nonce}", std::process::id())); std::fs::create_dir_all(&directory).unwrap(); let glacier = load(Arc::new(GlacierStorage::open(directory.join("distinct.glacier")).unwrap())); for text in ["on flat | distinct | limit 7", "on flat | distinct | limit 100", "on flat | where b > 0 | distinct | limit 4", "on flat | distinct"] { let (expected, _) = run(&memory, text); let (actual, _) = run(&glacier, text); assert!(!expected.is_empty(), "{text}"); assert_eq!(actual, expected, "{text}"); } let _ = std::fs::remove_dir_all(directory); }
/// 20 000 rows in one commit go straight to Glacier segments (dictionary
/// codes, row groups); a second commit deletes and replaces some of them.
fn load_segments(storage: Arc<dyn StorageEngine>) -> Engine {
    let collection = CollectionId::parse("big").unwrap();
    let id = |index: u32| DocumentId::parse(format!("01900000-0000-7000-9000-{index:012x}")).unwrap();
    let row = |index: u32, replaced: bool| {
        let mut row = Document::new();
        row.insert("n", Value::from(i64::from(index)));
        row.insert("code", Value::from(format!("C-{:03}", index % 397)));
        row.insert("qty", Value::from(i64::from(index % 7)));
        row.insert("key", Value::from(format!("K{}", index % 50)));
        // Few values over a wide range: a number dictionary in Glacier.
        row.insert("client", Value::from(1_701_000_000 + 977 * i64::from(index % 37)));
        if index.is_multiple_of(3) { row.insert("note", Value::from(if replaced { "new" } else { "old" })); }
        // Clustered: whole blocks hold one zone.
        row.insert("zone", Value::from(format!("Z{:02}", index / 2000)));
        row
    };
    let mut transaction = storage.begin().unwrap();
    for index in 0..20_000 { transaction.insert(&collection, id(index), Arc::new(row(index, false))).unwrap(); }
    // Complete rows, one area per run of blocks: a whole-document distinct
    // knows their field count before decoding.
    let areas = CollectionId::parse("areas").unwrap();
    for index in 0..6_000_u32 { let mut row = Document::new(); row.insert("area", Value::from(format!("A{}", index / 2000))); row.insert("n", Value::from(i64::from(index))); transaction.insert(&areas, DocumentId::parse(format!("01900000-0000-7000-a000-{index:012x}")).unwrap(), Arc::new(row)).unwrap(); }
    transaction.commit().unwrap();
    let mut transaction = storage.begin().unwrap();
    for index in (5..20_000).step_by(1000) { transaction.delete(&collection, &id(index), og_core::storage::VersionPrecondition::Any).unwrap(); }
    for index in (7..20_000).step_by(997) { transaction.replace(&collection, &id(index), Arc::new(row(index, true)), og_core::storage::VersionPrecondition::Any).unwrap(); }
    transaction.commit().unwrap();
    Engine::new(storage)
}

#[test] fn reads_by_identifier_on_segments_match_memory() { let memory = load_segments(Arc::new(MemoryStorage::new())); let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-segments-{}-{nonce}", std::process::id())); std::fs::create_dir_all(&directory).unwrap(); let glacier = load_segments(Arc::new(GlacierStorage::open(directory.join("big.glacier")).unwrap())); let queries = [/* Bounded lookups: index keys read on batches, candidates read by identifier. */ "on big | where n < 5 | lookup big as o on o.code == code into same | select n, len(same) as k", "on big | where code == \"C-007\" | limit 3 | lookup big as o on o.code == code into same | select n, same", "on big as s | where n > 100 and n < 140 | lookup big as o | where o.key == s.key | into m | limit 7 | end | select n, m", "on big as s | where n == 7 | lookup big as o | where o.qty == s.qty and o.n != s.n | into twin | end", /* Rows kept by a filter, then read whole by identifier. */ "on big | where qty == 3 and n > 19000", "on big | where note? == \"new\"", /* Winners reloaded by identifier. */ "on big | sort n desc | limit 50", "on big | sort code desc, n | limit 300 | group code", /* Number keys and memoized functions on number codes. */ "on big | group client, qty as q", "on big | distinct client, qty", "on big | derive s = concat(to_string(client), \"-\", to_string(qty)) | group s", "on big | where client > 1701010000 | group client", /* Composite keys in key order. */ "on big | group key, client, qty", "on big | group qty, code", /* Bounded distinct: rows rejected once per dictionary code of the first field. */ "on big | distinct | limit 5", "on big | distinct code, n | limit 7", "on big | where qty > 2 | distinct code, qty | limit 3", "on big | select code, note | distinct | limit 4", "on big | distinct client, code | limit 6"]; for query in queries { assert_eq!(run(&memory, query).0, run(&glacier, query).0, "{query}"); } std::fs::remove_dir_all(&directory).unwrap(); }

#[test] fn block_gates_keep_rows_counts_and_errors() { let memory = load_segments(Arc::new(MemoryStorage::new())); let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-gates-{}-{nonce}", std::process::id())); std::fs::create_dir_all(&directory).unwrap(); let glacier = load_segments(Arc::new(GlacierStorage::open(directory.join("big.glacier")).unwrap())); /* Leading filters decide on their own columns first; blocks none of whose */ /* rows pass are not decoded further, their rows still scanned and filtered. */ /* A bounded distinct refuses blocks holding only rejected codes. */ for query in ["on big | where n > 19500 | select n, code", "on big | where n < 300 and qty == 2 | group code", "on big | where zone == \"Z03\" and qty > 4 | select n, zone", "on big | where code == \"C-007\" | select n, qty", "on big | where zone != \"Z05\" | group zone", "on big | distinct zone, n | limit 3", "on big | distinct zone, code | limit 5", "on areas | distinct | limit 3", "on areas | distinct | limit 2500", /* A top-N refuses blocks none of whose rows passes its bound. */ "on big | sort n | limit 5", "on big | sort zone desc, n | limit 7", "on areas | sort area, n desc | limit 3", "on big | sort qty, n | limit 4"] { let ((expected, expected_counts), (actual, counts)) = (counted(&memory, query), counted(&glacier, query)); assert_eq!(actual, expected, "{query}"); /* (Memory counts the rows a distinct drops as filtered, Glacier does not.) */ let filtered = |counts: og_core::query::ExecutionStatistics| if query.contains("distinct") { 0 } else { counts.filtered() }; assert_eq!((counts.scanned(), filtered(counts), counts.returned()), (expected_counts.scanned(), filtered(expected_counts), expected_counts.returned()), "{query}"); } /* An error in a block a filter alone would refuse surfaces as before. */ for query in ["on big | where n > 19000 and code + 1 > 0 | count", "on big | where code + 1 > 0 and n > 19000 | count"] { assert_eq!(outcome(&glacier, query), outcome(&memory, query), "{query}"); } std::fs::remove_dir_all(&directory).unwrap(); }

#[test] fn group_top_n_keeps_the_sort_and_its_ties() { let memory = load_segments(Arc::new(MemoryStorage::new())); let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-group-top-{}-{nonce}", std::process::id())); std::fs::create_dir_all(&directory).unwrap(); let glacier = load_segments(Arc::new(GlacierStorage::open(directory.join("big.glacier")).unwrap())); /* Only the groups the sort keeps are finished: rows, order, ties (in key */ /* order) and synthetic identifiers stay. */ for query in ["on big | group code, qty as q | sort q desc | limit 5", "on big | group code, qty as q | sort q, code desc | limit 7", "on big | group | by zone | sum qty as s | count as c | end | sort s desc | limit 3", "on big | group code | sort count desc | limit 4", "on big | group key, client | sort client desc | limit 6", "on big | group code, qty as q | sort q desc | limit 5 | derive id = _id", "on big | group code, qty as q | sort _id | limit 3", "on big | group code, note? as n | sort n | limit 3", /* One-key groups fold whole typed blocks. */ "on big | group zone, qty as q", "on big | group | by client | sum qty as s | avg n as a | count as c | count note as k | end", "on big | group code", "on areas | group | by area | avg n as a | sum n as s | end", "on big | group | by qty | first code as f | end", "on big | group | by zone | min n as lo | max code as hi | min note as m | avg note as a | end", "on big | group | by note | max client as m | count qty as c | end", "on big | group note, qty as q"] { assert_eq!(ordered_outcome(&glacier, query), ordered_outcome(&memory, query), "{query}"); } std::fs::remove_dir_all(&directory).unwrap(); }
