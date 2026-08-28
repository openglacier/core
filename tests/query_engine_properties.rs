//! Property-based integration tests for the query engine.
//!
//! Add the following development dependency before running this test target:
//!
//! ```toml
//! [dev-dependencies]
//! proptest = "1"
//! ```
//!
//! Run with:
//!
//! ```text
//! cargo test --test query_engine_properties
//! ```
//!
//! These properties exercise the public engine pipeline:
//!
//! ```text
//! PlannerPipeline
//!   → Planner
//!     → ScanPlanLowerer
//!       → Executor
//!         → MemoryStorage
//! ```
//!
//! The generated cases verify deterministic ordering, collection isolation,
//! filtering statistics, successful mutation semantics and transactional
//! rollback at arbitrary failure positions.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use og_core::{
    engine::{Engine, EngineErrorKind},
    query::{
        CustomOperatorResult, ExecutionError, PlannerPipeline, PlannerStage, QueryRuntime,
        QueryRuntimeBuilder, ScanPlanLowerer, Span, StageName,
    },
    storage::{CollectionId, DocumentId, MemoryStorage, StorageEngine},
    Document,
};
use proptest::prelude::*;

const MAX_DOCUMENTS: usize = 64;

fn stage(name: &str, arguments: &str) -> PlannerStage {
    PlannerStage::new(
        StageName::parse(name).expect("valid test stage name"),
        arguments,
        Span::new(0, arguments.len()),
    )
}

fn collection(name: &str) -> CollectionId {
    CollectionId::parse(name).expect("valid test collection")
}

fn document_id(label: &str) -> DocumentId {
    if let Ok(id) = DocumentId::parse(label) {
        return id;
    }

    // Deterministic UUID v7 fixture. The base-257 payload preserves lexical
    // ordering for the short ASCII labels used by these integration tests.
    assert!(label.len() <= 9, "test document labels must fit in 9 bytes");

    let mut ordinal = 0_u128;
    for index in 0..9 {
        ordinal *= 257;
        if let Some(byte) = label.as_bytes().get(index) {
            ordinal += u128::from(*byte) + 1;
        }
    }

    let rand_a = ((ordinal >> 62) & 0x0fff) as u16;
    let rand_b = (ordinal & ((1_u128 << 62) - 1)) as u64;
    let timestamp_ms = 1_700_000_000_000_u64;

    let value = ((timestamp_ms as u128) << 80)
        | (0x7_u128 << 76)
        | ((rand_a as u128) << 64)
        | (0b10_u128 << 62)
        | u128::from(rand_b);
    let hex = format!("{value:032x}");
    let text = format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    );

    DocumentId::parse(&text).expect("valid deterministic UUID v7 test document id")
}

fn canonical_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| document_id(&format!("doc-{index:03}")).to_string())
        .collect()
}

fn seed_ids(storage: &MemoryStorage, collection_name: &str, ids: impl IntoIterator<Item = String>) {
    let collection = collection(collection_name);
    let mut transaction = storage.begin().expect("begin seed transaction");

    for id in ids {
        transaction
            .insert(&collection, document_id(&id), Arc::new(Document::default()))
            .expect("insert seed document");
    }

    transaction.commit().expect("commit seed transaction");
}

fn default_runtime() -> Arc<QueryRuntime> {
    Arc::new(
        QueryRuntimeBuilder::new()
            .predicate(|_, _| Ok(true))
            .set(|_, document| Ok(Arc::new(document.clone())))
            .load(|_, document| Ok(Arc::new(document.clone())))
            .custom(|_, _, _, _| Ok(CustomOperatorResult::Keep))
            .build()
            .expect("valid default runtime"),
    )
}

fn engine(storage: Arc<MemoryStorage>, runtime: Arc<QueryRuntime>) -> Engine {
    Engine::new(storage, runtime, Arc::new(ScanPlanLowerer::new()))
}

macro_rules! result_ids {
    ($result:expr) => {{
        $result
            .output()
            .rows()
            .iter()
            .map(|row| row.id().to_string().to_owned())
            .collect::<Vec<_>>()
    }};
}

fn versions(storage: &MemoryStorage, collection_name: &str, ids: &[String]) -> Vec<u64> {
    let snapshot = storage.read().expect("read storage");
    let collection = collection(collection_name);

    ids.iter()
        .map(|id| {
            snapshot
                .get(&collection, &document_id(id))
                .expect("read document")
                .expect("document exists")
                .version()
                .get()
        })
        .collect()
}

fn insertion_order_strategy() -> impl Strategy<Value = (usize, Vec<usize>)> {
    (0usize..=MAX_DOCUMENTS).prop_flat_map(|count| {
        proptest::collection::vec(any::<u64>(), count).prop_map(move |priorities| {
            let mut order = (0..count).collect::<Vec<_>>();

            order.sort_by_key(|index| (priorities[*index], *index));
            (count, order)
        })
    })
}

fn selection_strategy() -> impl Strategy<Value = Vec<bool>> {
    proptest::collection::vec(any::<bool>(), 0..=MAX_DOCUMENTS)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        max_shrink_iters: 4_096,
        ..ProptestConfig::default()
    })]

    #[test]
    fn scan_order_is_independent_of_insertion_order(
        (count, insertion_order) in insertion_order_strategy(),
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let expected = canonical_ids(count);

        let inserted = insertion_order
            .into_iter()
            .map(|index| expected[index].clone())
            .collect::<Vec<_>>();

        seed_ids(&storage, "items", inserted);

        let engine = engine(Arc::clone(&storage), default_runtime());
        let result = engine
            .execute(&PlannerPipeline::new("items", []))
            .expect("scan succeeds");

        prop_assert_eq!(result_ids!(result), expected);
        prop_assert_eq!(
            result.output().statistics().scanned(),
            u64::try_from(count).expect("count fits in u64"),
        );
        prop_assert_eq!(
            result.output().statistics().returned(),
            u64::try_from(count).expect("count fits in u64"),
        );
        prop_assert_eq!(result.output().statistics().filtered(), 0);
        prop_assert_eq!(result.output().statistics().replaced(), 0);
        prop_assert!(!result.output().committed());
    }

    #[test]
    fn read_only_queries_are_repeatable(
        count in 0usize..=MAX_DOCUMENTS,
        repetitions in 1usize..=12,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let expected = canonical_ids(count);
        seed_ids(&storage, "items", expected.clone());

        let generation = storage
            .generation()
            .expect("read initial generation");
        let engine = engine(Arc::clone(&storage), default_runtime());

        for _ in 0..repetitions {
            let result = engine
                .execute(&PlannerPipeline::new("items", []))
                .expect("read succeeds");

            prop_assert_eq!(result_ids!(result), expected.clone());
            prop_assert!(!result.output().committed());
            prop_assert_eq!(
                storage.generation().expect("read generation"),
                generation,
            );
        }
    }

    #[test]
    fn filter_statistics_match_generated_selection(
        selection in selection_strategy(),
    ) {
        let count = selection.len();
        let storage = Arc::new(MemoryStorage::new());
        seed_ids(&storage, "items", canonical_ids(count));

        let predicate_index = Arc::new(AtomicUsize::new(0));
        let selection = Arc::new(selection);
        let predicate_selection = Arc::clone(&selection);
        let predicate_counter = Arc::clone(&predicate_index);

        let runtime = Arc::new(
            QueryRuntimeBuilder::new()
                .predicate(move |_, _| {
                    let index = predicate_counter
                        .fetch_add(1, Ordering::SeqCst);
                    Ok(predicate_selection[index])
                })
                .set(|_, document| Ok(Arc::new(document.clone())))
                .build()
                .expect("valid runtime"),
        );

        let engine = engine(storage, runtime);
        let result = engine
            .execute(&PlannerPipeline::new(
                "items",
                [stage("where", "selected")],
            ))
            .expect("filter succeeds");

        let kept = selection.iter().filter(|selected| **selected).count();
        let filtered = count - kept;
        let statistics = result.output().statistics();

        prop_assert_eq!(
            predicate_index.load(Ordering::SeqCst),
            count,
        );
        prop_assert_eq!(
            statistics.scanned(),
            u64::try_from(count).expect("count fits in u64"),
        );
        prop_assert_eq!(
            statistics.returned(),
            u64::try_from(kept).expect("kept count fits in u64"),
        );
        prop_assert_eq!(
            statistics.filtered(),
            u64::try_from(filtered).expect("filtered count fits in u64"),
        );
        prop_assert_eq!(
            statistics.scanned(),
            statistics.returned() + statistics.filtered(),
        );
        prop_assert_eq!(statistics.replaced(), 0);
        prop_assert!(!result.output().committed());
    }

    #[test]
    fn set_mutates_exactly_the_selected_rows(
        selection in selection_strategy(),
    ) {
        let count = selection.len();
        let expected_ids = canonical_ids(count);
        let storage = Arc::new(MemoryStorage::new());
        seed_ids(&storage, "items", expected_ids.clone());

        let predicate_index = Arc::new(AtomicUsize::new(0));
        let set_calls = Arc::new(AtomicUsize::new(0));

        let selection = Arc::new(selection);
        let predicate_selection = Arc::clone(&selection);
        let predicate_counter = Arc::clone(&predicate_index);
        let set_counter = Arc::clone(&set_calls);

        let runtime = Arc::new(
            QueryRuntimeBuilder::new()
                .predicate(move |_, _| {
                    let index = predicate_counter
                        .fetch_add(1, Ordering::SeqCst);
                    Ok(predicate_selection[index])
                })
                .set(move |_, document| {
                    set_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(document.clone()))
                })
                .build()
                .expect("valid runtime"),
        );

        let engine = engine(Arc::clone(&storage), runtime);
        let result = engine
            .execute(&PlannerPipeline::new(
                "items",
                [
                    stage("where", "selected"),
                    stage("set", "visited = true"),
                ],
            ))
            .expect("mutation succeeds");

        let selected = selection
            .iter()
            .enumerate()
            .filter_map(|(index, selected)| selected.then_some(index))
            .collect::<BTreeSet<_>>();

        prop_assert_eq!(
            set_calls.load(Ordering::SeqCst),
            selected.len(),
        );
        prop_assert_eq!(
            result.output().statistics().replaced(),
            u64::try_from(selected.len())
                .expect("selected count fits in u64"),
        );

        let stored_versions = versions(&storage, "items", &expected_ids);

        for (index, version) in stored_versions.into_iter().enumerate() {
            let expected_version =
                if selected.contains(&index) { 2 } else { 1 };

            prop_assert_eq!(version, expected_version);
        }

        prop_assert!(result.output().committed());
    }

    #[test]
    fn set_failure_rolls_back_at_every_generated_position(
        count in 1usize..=MAX_DOCUMENTS,
        failure_seed in any::<usize>(),
    ) {
        let failure_position = failure_seed % count;
        let expected_ids = canonical_ids(count);
        let storage = Arc::new(MemoryStorage::new());
        seed_ids(&storage, "items", expected_ids.clone());

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);

        let runtime = Arc::new(
            QueryRuntimeBuilder::new()
                .predicate(|_, _| Ok(true))
                .set(move |_, document| {
                    let index =
                        counter.fetch_add(1, Ordering::SeqCst);

                    if index == failure_position {
                        return Err(ExecutionError::evaluation(
                            format!("generated failure at {index}"),
                        ));
                    }

                    Ok(Arc::new(document.clone()))
                })
                .build()
                .expect("valid failing runtime"),
        );

        let engine = engine(Arc::clone(&storage), runtime);
        let error = engine
            .execute(&PlannerPipeline::new(
                "items",
                [stage("set", "visited = true")],
            ))
            .expect_err("mutation must fail");

        prop_assert!(matches!(
            error.kind(),
            EngineErrorKind::Execution(_),
        ));
        prop_assert_eq!(
            versions(&storage, "items", &expected_ids),
            vec![1; count],
        );
        prop_assert_eq!(
            storage.generation().expect("read generation"),
            1,
        );
    }

    #[test]
    fn mutations_are_isolated_to_the_target_collection(
        users_count in 0usize..=MAX_DOCUMENTS,
        orders_count in 0usize..=MAX_DOCUMENTS,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let users = canonical_ids(users_count);
        let orders = canonical_ids(orders_count);

        seed_ids(&storage, "users", users.clone());
        seed_ids(&storage, "orders", orders.clone());

        let engine = engine(Arc::clone(&storage), default_runtime());
        let result = engine
            .execute(&PlannerPipeline::new(
                "users",
                [stage("set", "visited = true")],
            ))
            .expect("users mutation succeeds");

        prop_assert_eq!(
            versions(&storage, "users", &users),
            vec![2; users_count],
        );
        prop_assert_eq!(
            versions(&storage, "orders", &orders),
            vec![1; orders_count],
        );
        prop_assert_eq!(
            result.output().statistics().replaced(),
            u64::try_from(users_count)
                .expect("users count fits in u64"),
        );
    }

    #[test]
    fn repeated_successful_mutations_increment_versions_once(
        count in 0usize..=MAX_DOCUMENTS,
        executions in 1usize..=8,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let expected_ids = canonical_ids(count);
        seed_ids(&storage, "items", expected_ids.clone());

        let engine = engine(Arc::clone(&storage), default_runtime());
        let query = PlannerPipeline::new(
            "items",
            [stage("set", "visited = true")],
        );

        for execution_index in 1..=executions {
            let result = engine.execute(&query).expect("set succeeds");

            prop_assert_eq!(
                result.output().statistics().replaced(),
                u64::try_from(count).expect("count fits in u64"),
            );

            let expected_version =
                1 + u64::try_from(execution_index)
                    .expect("execution count fits in u64");

            prop_assert_eq!(
                versions(&storage, "items", &expected_ids),
                vec![expected_version; count],
            );
        }
    }

    #[test]
    fn invalid_query_is_always_side_effect_free(
        count in 0usize..=MAX_DOCUMENTS,
        attempts in 1usize..=8,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let expected_ids = canonical_ids(count);
        seed_ids(&storage, "items", expected_ids.clone());

        let initial_generation =
            storage.generation().expect("read generation");
        let engine = engine(Arc::clone(&storage), default_runtime());
        let query = PlannerPipeline::new(
            "items",
            [stage("where", "")],
        );

        for _ in 0..attempts {
            let error =
                engine.execute(&query).expect_err("planning must fail");

            prop_assert!(matches!(
                error.kind(),
                EngineErrorKind::Planning(_),
            ));
            prop_assert_eq!(
                storage.generation().expect("read generation"),
                initial_generation,
            );
            prop_assert_eq!(
                versions(&storage, "items", &expected_ids),
                vec![1; count],
            );
        }
    }
}
