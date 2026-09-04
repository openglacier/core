//! End-to-end tests for the native `root` and `sample` stages.

use std::sync::Arc;

use og_core::{
    engine::Engine,
    query::{
        value_expression_runtime, PlannerPipeline, PlannerStage, QueryRuntimeMaterializationExt,
        ScanPlanLowerer, Span, StageName,
    },
    storage::{CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document, Value,
};

fn stage(name: &str, arguments: &str) -> PlannerStage {
    PlannerStage::new(
        StageName::parse(name).expect("valid stage name"),
        arguments,
        Span::new(0, arguments.len()),
    )
}

fn engine_with_rows(
    collection: &str,
    rows: impl IntoIterator<Item = (DocumentId, Document)>,
) -> Engine {
    let storage = Arc::new(MemoryStorage::new());
    let collection = CollectionId::parse(collection).unwrap();
    let mut transaction = storage.begin().unwrap();
    for (id, document) in rows {
        transaction
            .insert(&collection, id, Arc::new(document))
            .unwrap();
    }
    transaction.commit().unwrap();

    let runtime = Arc::new(
        value_expression_runtime()
            .unwrap()
            .with_default_materialization("test"),
    );
    Engine::new(storage, runtime, Arc::new(ScanPlanLowerer::new()))
}

#[test]
fn root_replaces_each_row_with_the_selected_object() {
    let mut payload = Document::new();
    payload.insert("name", "Alice");
    payload.insert("age", 42u64);

    let mut source = Document::new();
    source.insert("kind", "user");
    source.insert("payload", Value::object(payload));

    let engine = engine_with_rows(
        "events",
        [(
            DocumentId::parse("018bcfe5-6800-7000-8000-000000000101").unwrap(),
            source,
        )],
    );
    let query = PlannerPipeline::new("events", [stage("root", "payload")]);

    let result = engine.execute(&query).expect("root query succeeds");
    let rows = result.output().rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].document().get("name").and_then(Value::as_str),
        Some("Alice")
    );
    assert_eq!(rows[0].document().get("age"), Some(&Value::unsigned(42)));
    assert_eq!(rows[0].document().get("kind"), None);
    assert_eq!(rows[0].document().get("payload"), None);
}

#[test]
fn sample_returns_at_most_the_requested_number_of_distinct_rows() {
    let rows = (0..8u64).map(|index| {
        let id = DocumentId::parse(format!("018bcfe5-6800-7000-8000-{index:012x}"))
            .expect("valid deterministic id");
        let mut document = Document::new();
        document.insert("index", index);
        (id, document)
    });
    let engine = engine_with_rows("items", rows);
    let query = PlannerPipeline::new("items", [stage("sample", "3")]);

    let planned = engine.plan(&query).expect("sample plans");
    assert!(planned.physical().changes_cardinality());
    assert!(!planned.physical().is_memory_streaming());

    let result = engine.execute(&query).expect("sample query succeeds");
    let rows = result.output().rows();
    assert_eq!(rows.len(), 3);

    let mut indices = rows
        .iter()
        .map(|row| {
            row.document()
                .get("index")
                .and_then(Value::as_number)
                .and_then(|number| (*number).as_unsigned())
                .expect("sampled index")
        })
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();
    assert_eq!(indices.len(), 3);
    assert!(indices.iter().all(|index| *index < 8));
}

#[test]
fn sample_zero_returns_no_rows() {
    let mut document = Document::new();
    document.insert("value", 1u64);
    let engine = engine_with_rows(
        "items",
        [(
            DocumentId::parse("018bcfe5-6800-7000-8000-000000000201").unwrap(),
            document,
        )],
    );
    let query = PlannerPipeline::new("items", [stage("sample", "0")]);

    let result = engine.execute(&query).expect("sample zero succeeds");
    assert!(result.output().rows().is_empty());
}
