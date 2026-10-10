//! `group` and `group | sort` that spill: under a small memory limit, the
//! external paths return what the in-memory ones return without a limit.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{parse, ExecutionStrategy, PlannerPipeline},
    storage::{CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document, MemoryGovernor,
};

const ROWS: u64 = 120_000;

fn storage() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    let collection = CollectionId::parse("sales").unwrap();
    let mut transaction = storage.begin().unwrap();
    for n in 0..ROWS {
        let mut document = Document::new();
        document.insert("code", format!("article-{:06}", n % 40_000));
        document.insert("qty", n % 7);
        document.insert("n", n);
        let id = DocumentId::parse(format!("018bcfe5-6800-7000-8000-{n:012x}")).unwrap();
        transaction.insert(&collection, id, Arc::new(document)).unwrap();
    }
    transaction.commit().unwrap();
    storage
}

fn run(engine: &Engine, text: &str) -> (Vec<String>, bool) {
    let ast = parse(text).unwrap();
    let planned = engine.plan(&PlannerPipeline::from_ast(text, &ast).unwrap()).unwrap();
    let mut rows = Vec::new();
    let statistics = engine
        .stream_governed_blocking_pipeline(planned.physical(), &mut |row| {
            rows.push(format!("{:?}", row.document()));
            Ok(())
        })
        .unwrap()
        .expect("a governed streaming shape");
    (rows, statistics.strategies().contains(ExecutionStrategy::ExternalGroup))
}

#[test] fn spilled_groups_match_in_memory_groups() { let storage = storage(); let unlimited = Engine::new(storage.clone()); let limited = Engine::new(storage).with_memory_governor(MemoryGovernor::with_limit(8 * 1024 * 1024)); for text in ["on sales | group | by code | sum qty as q | count as c | end", "on sales | group | by code | sum qty as q | count as c | end | sort q desc, code", "on sales | group | by code | sum qty as q | count as c | end | sort q desc, code | limit 25", "on sales | where qty > 2 | group code, qty | sort code desc | limit 10", "on sales | group | by code | last n by qty as top | first n by qty where n % 3 == 0 as low | sum qty * 2 where n > 500 as q | end"] { let (expected, expected_external) = run(&unlimited, text); let (actual, external) = run(&limited, text); assert!(!expected_external, "{text}: the unlimited engine groups in memory"); assert!(external, "{text}: the limited engine spills"); assert_eq!(actual, expected, "{text}"); } }
#[test] fn window_sorts_through_spilled_runs_and_bounds_each_partition() { let storage = storage(); let unlimited = Engine::new(storage.clone()); let limited = Engine::new(storage).with_memory_governor(MemoryGovernor::with_limit(8 * 1024 * 1024)); let text = "on sales | window | partition by code | order by n | cumsum qty as c | lag n as p | end"; let rows = |engine: &Engine| { let ast = parse(text).unwrap(); let planned = engine.plan(&PlannerPipeline::from_ast(text, &ast).unwrap()).unwrap(); let mut rows = Vec::new(); let statistics = engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(format!("{:?}", row.document())); Ok(()) }).unwrap().expect("a governed streaming shape"); (rows, statistics.strategies().contains(ExecutionStrategy::ExternalSort)) }; let (expected, spilled) = rows(&unlimited); assert!(!spilled); assert_eq!(rows(&limited), (expected, true)); let whole = "on sales | window | order by n | cumsum qty as c | end"; let planned = limited.plan(&PlannerPipeline::from_ast(whole, &parse(whole).unwrap()).unwrap()).unwrap(); let error = limited.stream_governed_blocking_pipeline(planned.physical(), &mut |_| Ok(())).unwrap_err().to_string(); assert!(error.contains("a window partition exceeds the governed query working set"), "{error}"); }
#[test] fn fill_streams_behind_a_spilled_sort() { let storage = storage(); let unlimited = Engine::new(storage.clone()); let limited = Engine::new(storage).with_memory_governor(MemoryGovernor::with_limit(8 * 1024 * 1024)); let text = "on sales | fill qty from 0 to 9 step 1 by code with n = -1"; let rows = |engine: &Engine| { let planned = engine.plan(&PlannerPipeline::from_ast(text, &parse(text).unwrap()).unwrap()).unwrap(); let mut rows = Vec::new(); let statistics = engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(format!("{:?}", row.document())); Ok(()) }).unwrap().unwrap(); (rows, statistics.strategies().contains(ExecutionStrategy::ExternalSort)) }; let (expected, spilled) = rows(&unlimited); assert!(!spilled && expected.len() == 120_000 + 40_000 * 7); assert_eq!(rows(&limited), (expected, true)); }
