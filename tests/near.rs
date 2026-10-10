//! End-to-end tests for the native `near` stage.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use og_core::{
    engine::Engine,
    query::{PlannerPipeline, PlannerStage, Span, StageName},
    storage::{GlacierStorage, CollectionId, DocumentId, MemoryStorage, StorageEngine},
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

#[test] fn near_composes_with_existing_sort_and_limit() { let storage = Arc::new(MemoryStorage::new()); let collection = CollectionId::parse("memories").unwrap(); let rows = [("018bcfe5-6800-7000-8000-000000000001", "same", [1.0, 0.0]), ("018bcfe5-6800-7000-8000-000000000002", "close", [0.8, 0.2]), ("018bcfe5-6800-7000-8000-000000000003", "far", [0.0, 1.0])]; let mut transaction = storage.begin().unwrap(); for (id, label, embedding) in rows { let mut document = Document::new(); document.insert("label", label); document.insert("embedding", vector(&embedding)); transaction.insert(&collection, DocumentId::parse(id).unwrap(), Arc::new(document)).unwrap(); } transaction.commit().unwrap(); let engine = Engine::new(storage); let query = PlannerPipeline::new("memories", [stage("near", "embedding, [1.0, 0.0]"), stage("sort", "_distance asc"), stage("limit", "2")]); let result = engine.execute(&query).expect("near query succeeds"); let rows = result.output().rows(); assert_eq!(rows.len(), 2); assert_eq!(rows[0].document().get("label").and_then(Value::as_str), Some("same")); assert_eq!(rows[1].document().get("label").and_then(Value::as_str), Some("close")); assert!(rows.iter().all(|row| row.document().get("_distance").is_some())); }

#[test] fn glacier_near_top_n_projects_embeddings_and_hydrates_only_winners() { let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-near-glacier-{}-{nonce}", std::process::id())); fs::create_dir_all(&directory).unwrap(); let path = directory.join("store.glacier"); let storage = Arc::new(GlacierStorage::open(&path).unwrap()); let collection = CollectionId::parse("memories").unwrap(); let rows = [("018bcfe5-6800-7000-8000-000000000011", "same", [1.0, 0.0]), ("018bcfe5-6800-7000-8000-000000000012", "close", [0.8, 0.2]), ("018bcfe5-6800-7000-8000-000000000013", "far", [0.0, 1.0])]; let mut transaction = storage.begin().unwrap(); for (id, label, embedding) in rows { let mut document = Document::new(); document.insert("label", label); document.insert("embedding", vector(&embedding)); document.insert("payload", "x".repeat(4096)); transaction.insert(&collection, DocumentId::parse(id).unwrap(), Arc::new(document)).unwrap(); } transaction.commit().unwrap(); let engine = Engine::new(storage.clone()); let query = PlannerPipeline::new("memories", [stage("near", "embedding, [1.0, 0.0]"), stage("sort", "_distance asc"), stage("limit", "2")]); let planned = engine.plan(&query).unwrap(); let mut documents = Vec::new(); let statistics = engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { documents.push(row.shared_document()); Ok(()) }).unwrap().expect("near Top-N should use governed blocking streaming"); assert_eq!(documents.len(), 2); assert_eq!(documents[0].get("label").and_then(Value::as_str), Some("same")); assert_eq!(documents[1].get("label").and_then(Value::as_str), Some("close")); assert!(documents.iter().all(|document| document.get("_distance").is_some())); assert!(statistics.strategies().contains(og_core::query::ExecutionStrategy::TopN)); drop(engine); drop(storage); fs::remove_dir_all(directory).unwrap(); }

#[test] fn glacier_near_defers_embedding_decode_until_where_accepts_row() { let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(); let directory = std::env::temp_dir().join(format!("og-near-glacier-gated-{}-{nonce}", std::process::id())); fs::create_dir_all(&directory).unwrap(); let path = directory.join("store.glacier"); let storage = Arc::new(GlacierStorage::open(&path).unwrap()); let collection = CollectionId::parse("memories").unwrap(); let rows = [("018bcfe5-6800-7000-8000-000000000021", "keep", [1.0, 0.0]), ("018bcfe5-6800-7000-8000-000000000022", "skip", [0.8, 0.2]), ("018bcfe5-6800-7000-8000-000000000023", "skip", [0.0, 1.0])]; let mut transaction = storage.begin().unwrap(); for (id, kind, embedding) in rows { let mut document = Document::new(); document.insert("kind", kind); document.insert("embedding", vector(&embedding)); document.insert("payload", "x".repeat(4096)); transaction.insert(&collection, DocumentId::parse(id).unwrap(), Arc::new(document)).unwrap(); } transaction.commit().unwrap(); let engine = Engine::new(storage.clone()); let query = PlannerPipeline::new("memories", [stage("where", r#"kind == "keep""#), stage("near", "embedding, [1.0, 0.0]"), stage("sort", "_distance asc"), stage("limit", "1")]); let planned = engine.plan(&query).unwrap(); let mut documents = Vec::new(); engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { documents.push(row.shared_document()); Ok(()) }).unwrap().expect("gated near Top-N should use governed blocking streaming"); assert_eq!(documents.len(), 1); assert_eq!(documents[0].get("kind").and_then(Value::as_str), Some("keep")); drop(engine); drop(storage); fs::remove_dir_all(directory).unwrap(); }
