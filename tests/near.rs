//! End-to-end tests for the native `near` stage.

use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use og_core::{
    engine::Engine,
    query::{
        value_expression_runtime, PlannerPipeline, PlannerStage, QueryRuntimeMaterializationExt,
        ScanPlanLowerer, Span, StageName,
    },
    storage::{glacier::GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document, Value,
};

fn stage(name: &str, arguments: &str) -> PlannerStage {
    PlannerStage::new(
        StageName::parse(name).expect("valid stage name"),
        arguments,
        Span::new(0, arguments.len()),
    )
}

fn vector(values: &[f64]) -> Value {
    Value::array(
        values
            .iter()
            .map(|value| Value::float(*value).expect("finite test vector")),
    )
}

#[test]
fn near_composes_with_existing_sort_and_limit() {
    let storage = Arc::new(MemoryStorage::new());
    let collection = CollectionId::parse("memories").unwrap();
    let rows = [
        ("018bcfe5-6800-7000-8000-000000000001", "same", [1.0, 0.0]),
        ("018bcfe5-6800-7000-8000-000000000002", "close", [0.8, 0.2]),
        ("018bcfe5-6800-7000-8000-000000000003", "far", [0.0, 1.0]),
    ];

    let mut transaction = storage.begin().unwrap();
    for (id, label, embedding) in rows {
        let mut document = Document::new();
        document.insert("label", label);
        document.insert("embedding", vector(&embedding));
        transaction
            .insert(
                &collection,
                DocumentId::parse(id).unwrap(),
                Arc::new(document),
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let runtime = Arc::new(
        value_expression_runtime()
            .unwrap()
            .with_default_materialization("test"),
    );
    let engine = Engine::new(storage, runtime, Arc::new(ScanPlanLowerer::new()));
    let query = PlannerPipeline::new(
        "memories",
        [
            stage("near", "embedding, [1.0, 0.0]"),
            stage("sort", "_distance asc"),
            stage("limit", "2"),
        ],
    );

    let result = engine.execute(&query).expect("near query succeeds");
    let rows = result.output().rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].document().get("label").and_then(Value::as_str),
        Some("same")
    );
    assert_eq!(
        rows[1].document().get("label").and_then(Value::as_str),
        Some("close")
    );
    assert!(rows
        .iter()
        .all(|row| row.document().get("_distance").is_some()));
}

#[test]
fn glacier_near_top_n_projects_embeddings_and_hydrates_only_winners() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("og-near-glacier-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("store.glacier");
    let storage = Arc::new(GlacierStorage::open(&path).unwrap());
    let collection = CollectionId::parse("memories").unwrap();
    let rows = [
        ("018bcfe5-6800-7000-8000-000000000011", "same", [1.0, 0.0]),
        ("018bcfe5-6800-7000-8000-000000000012", "close", [0.8, 0.2]),
        ("018bcfe5-6800-7000-8000-000000000013", "far", [0.0, 1.0]),
    ];

    let mut transaction = storage.begin().unwrap();
    for (id, label, embedding) in rows {
        let mut document = Document::new();
        document.insert("label", label);
        document.insert("embedding", vector(&embedding));
        document.insert("payload", "x".repeat(4096));
        transaction
            .insert(
                &collection,
                DocumentId::parse(id).unwrap(),
                Arc::new(document),
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let runtime = Arc::new(
        value_expression_runtime()
            .unwrap()
            .with_default_materialization("test"),
    );
    let engine = Engine::new(storage.clone(), runtime, Arc::new(ScanPlanLowerer::new()));
    let query = PlannerPipeline::new(
        "memories",
        [
            stage("near", "embedding, [1.0, 0.0]"),
            stage("sort", "_distance asc"),
            stage("limit", "2"),
        ],
    );
    let planned = engine.plan(&query).unwrap();
    let before = storage.backend().read_metrics();
    let mut documents = Vec::new();
    let statistics = engine
        .stream_governed_blocking_pipeline(planned.physical(), &mut |row| {
            documents.push(row.shared_document());
            Ok(())
        })
        .unwrap()
        .expect("near Top-N should use governed blocking streaming");

    assert_eq!(documents.len(), 2);
    assert_eq!(
        documents[0].get("label").and_then(Value::as_str),
        Some("same")
    );
    assert_eq!(
        documents[1].get("label").and_then(Value::as_str),
        Some("close")
    );
    assert!(documents
        .iter()
        .all(|document| document.get("_distance").is_some()));
    assert!(statistics
        .strategies()
        .contains(og_core::query::ExecutionStrategy::TopN));

    let metrics = storage.backend().read_metrics();
    assert_eq!(metrics.projected_records - before.projected_records, 3);
    assert_eq!(
        metrics.borrowed_projected_materializations - before.borrowed_projected_materializations,
        3
    );
    assert_eq!(metrics.pointer_loads - before.pointer_loads, 2);
    assert_eq!(
        metrics.generic_scan_each_calls - before.generic_scan_each_calls,
        0
    );

    drop(engine);
    drop(storage);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn glacier_near_defers_embedding_decode_until_where_accepts_row() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "og-near-glacier-gated-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("store.glacier");
    let storage = Arc::new(GlacierStorage::open(&path).unwrap());
    let collection = CollectionId::parse("memories").unwrap();
    let rows = [
        ("018bcfe5-6800-7000-8000-000000000021", "keep", [1.0, 0.0]),
        ("018bcfe5-6800-7000-8000-000000000022", "skip", [0.8, 0.2]),
        ("018bcfe5-6800-7000-8000-000000000023", "skip", [0.0, 1.0]),
    ];

    let mut transaction = storage.begin().unwrap();
    for (id, kind, embedding) in rows {
        let mut document = Document::new();
        document.insert("kind", kind);
        document.insert("embedding", vector(&embedding));
        document.insert("payload", "x".repeat(4096));
        transaction
            .insert(
                &collection,
                DocumentId::parse(id).unwrap(),
                Arc::new(document),
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let runtime = Arc::new(
        value_expression_runtime()
            .unwrap()
            .with_default_materialization("test"),
    );
    let engine = Engine::new(storage.clone(), runtime, Arc::new(ScanPlanLowerer::new()));
    let query = PlannerPipeline::new(
        "memories",
        [
            stage("where", r#"kind == "keep""#),
            stage("near", "embedding, [1.0, 0.0]"),
            stage("sort", "_distance asc"),
            stage("limit", "1"),
        ],
    );
    let planned = engine.plan(&query).unwrap();
    let before = storage.backend().read_metrics();
    let mut documents = Vec::new();
    engine
        .stream_governed_blocking_pipeline(planned.physical(), &mut |row| {
            documents.push(row.shared_document());
            Ok(())
        })
        .unwrap()
        .expect("gated near Top-N should use governed blocking streaming");

    assert_eq!(documents.len(), 1);
    assert_eq!(
        documents[0].get("kind").and_then(Value::as_str),
        Some("keep")
    );

    let metrics = storage.backend().read_metrics();
    assert_eq!(metrics.projected_records - before.projected_records, 3);
    assert_eq!(metrics.decoded_fields - before.decoded_fields, 4);
    assert_eq!(
        metrics.projection_complex_values - before.projection_complex_values,
        1
    );
    assert_eq!(metrics.pointer_loads - before.pointer_loads, 1);

    drop(engine);
    drop(storage);
    fs::remove_dir_all(directory).unwrap();
}
