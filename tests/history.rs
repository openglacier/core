//! `on c as of …` and `on c history` on every backend, through the language.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{path::PathBuf, sync::Arc};

use og_core::{
    engine::Engine,
    query::{parse, DocumentScope, PlannerPipeline},
    storage::{
        history::{with_commit_author, with_history_floor, CommitAuthor},
        GlacierStorage, MemoryStorage, StorageEngine,
    },
    Document, Value,
};

fn pipeline(text: &str) -> PlannerPipeline {
    let ast = parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
    PlannerPipeline::from_ast(text, &ast).unwrap_or_else(|error| panic!("{text}: {error}"))
}

fn int(value: &Value) -> Option<u64> {
    match value.as_number()? {
        og_core::Number::Signed(value) => u64::try_from(*value).ok(),
        og_core::Number::Unsigned(value) => Some(*value),
        og_core::Number::Float(value) => (value.fract() == 0.0 && *value >= 0.0).then_some(*value as u64),
        _ => None,
    }
}

fn engine(storage: Arc<dyn StorageEngine>) -> Engine { Engine::new(storage) }

fn run(engine: &Engine, text: &str) -> Vec<Document> {
    let result = engine.execute(&pipeline(text)).unwrap_or_else(|error| panic!("{text}: {error}"));
    result.output().rows().iter().map(|row| row.document().clone()).collect()
}

/// A read the way `ogd` streams it, when it can.
fn stream(engine: &Engine, text: &str) -> Option<Vec<Document>> {
    let planned = engine.plan(&pipeline(text)).unwrap_or_else(|error| panic!("{text}: {error}"));
    let mut rows = Vec::new();
    let executed = if engine.supports_governed_blocking_streaming(planned.physical()) {
        engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| { rows.push(row.document().clone()); Ok(()) })
    } else {
        engine.stream_read_pipeline(planned.physical(), &mut |stored| { rows.push(stored.document().clone()); Ok(()) })
    };
    executed.unwrap_or_else(|error| panic!("{text}: {error}")).map(|_| rows)
}

fn error(engine: &Engine, text: &str) -> String {
    match engine.execute(&pipeline(text)) {
        Ok(_) => panic!("{text}: expected an error"),
        Err(error) => error.to_string(),
    }
}

/// `name:age` of each row, in name order.
fn people(rows: &[Document]) -> Vec<String> {
    let mut people = rows.iter().map(|row| format!("{}:{}", row.get("name").and_then(Value::as_str).unwrap_or("?"), row.get("age").and_then(int).map_or_else(|| "-".to_owned(), |age| age.to_string()))).collect::<Vec<_>>();
    people.sort();
    people
}

fn temp(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("og-history-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_file(&path);
    path
}

/// Writes a few generations, checks `as of` against the state seen at each
/// of them and `history` against the writes. `generation` reads the store's
/// current generation; `between` runs between steps (checkpoints, reopen).
fn scenario(engine: &Engine, generation: &dyn Fn() -> u64, between: &mut dyn FnMut(usize)) {
    let mut states = vec![(generation(), Vec::<String>::new())];
    let steps = [
        r#"on users | insert [{name: "Alice", age: 31, _place: "p1"}, {name: "Bob", age: 17, _place: "p2"}]"#,
        r#"on users | insert {name: "Carol", age: 45, _place: "p1"}"#,
        r#"on users | where name == "Bob" | set age = 18"#,
        r#"on users | where name == "Alice" | delete"#,
        r#"on users | insert {name: "Dave", age: 52, _place: "p2"}"#,
        r#"on users | where name == "Carol" | set age = 46"#,
    ];
    let author = CommitAuthor { identity: Arc::from("identity-ada"), device: Some(Arc::from("device-1")) };
    for (index, step) in steps.iter().enumerate() {
        with_commit_author(Some(author.clone()), || run(engine, step));
        between(index);
        states.push((generation(), people(&run(engine, "on users"))));
    }
    assert_eq!(states.last().unwrap().1, ["Bob:18", "Carol:46", "Dave:52"]);

    for (at, expected) in &states {
        let query = format!("on users as of {at}");
        assert_eq!(&people(&run(engine, &query)), expected, "{query}");
        if let Some(streamed) = stream(engine, &format!("{query} | sort name")) {
            assert_eq!(&people(&streamed), expected, "{query} (streamed)");
        }
        let counted = run(engine, &format!("{query} | count"));
        assert_eq!(counted[0].get("count").and_then(int), Some(expected.len() as u64), "{query} | count");
        let filtered = run(engine, &format!("{query} | where age >= 18 | sort age desc | limit 1"));
        let oldest = expected.iter().max_by_key(|person| person.split(':').nth(1).unwrap().parse::<i64>().unwrap()).cloned();
        assert_eq!(people(&filtered).first().cloned(), oldest, "{query} | top age");
    }

    // History: one row per write, in generation order.
    let history = run(engine, "on users history");
    let ops = history.iter().map(|row| format!("{}:{}", row.get("_op").and_then(Value::as_str).unwrap(), row.get("name").and_then(Value::as_str).unwrap_or("?"))).collect::<Vec<_>>();
    assert_eq!(ops, ["insert:Alice", "insert:Bob", "insert:Carol", "replace:Bob", "delete:Alice", "insert:Dave", "replace:Carol"]);
    let generations = history.iter().map(|row| row.get("_generation").and_then(int).unwrap()).collect::<Vec<_>>();
    assert!(generations.windows(2).all(|pair| pair[0] <= pair[1]));
    let bob = run(engine, r#"on users history | where name == "Bob" | sort _version"#);
    assert_eq!(bob.iter().map(|row| row.get("_version").and_then(int).unwrap()).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(bob[0].get("_doc_id"), bob[1].get("_doc_id"));
    let ids = engine.execute(&pipeline(r#"on users history | where name == "Bob""#)).unwrap().output().rows().iter().map(|row| *row.id()).collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert!(ids[0] < ids[1], "history rows have distinct, ordered identifiers");
    let deleted = run(engine, r#"on users history | where _op == "delete""#);
    assert_eq!(deleted[0].get("age").and_then(int), Some(31));
    assert_eq!(deleted[0].get("_version").and_then(int), Some(1));
    let counted = run(engine, "on users history | count");
    assert_eq!(counted[0].get("count").and_then(int), Some(7));
    let last = run(engine, "on users history | sort _generation desc, name | limit 1");
    assert_eq!(people(&last), ["Carol:46"]);

    // A Place scope applies to every version.
    let scoped = engine.execute_physical_scoped(engine.plan(&pipeline(&format!("on users as of {}", states[3].0))).unwrap().physical(), &DocumentScope::for_place("p1")).unwrap();
    assert_eq!(people(&scoped.rows().iter().map(|row| row.document().clone()).collect::<Vec<_>>()), ["Alice:31", "Carol:45"]);
    let scoped_history = engine.execute_physical_scoped(engine.plan(&pipeline("on users history")).unwrap().physical(), &DocumentScope::for_place("p2")).unwrap();
    assert_eq!(scoped_history.rows().len(), 3);

    // `as of` a later time is the current state; a terminal `into` restores a past state.
    assert_eq!(people(&run(engine, r#"on users as of "2999-01-01""#)), states.last().unwrap().1);
    run(engine, &format!("on users as of {} | into users_restored", states[3].0));
    assert_eq!(people(&run(engine, "on users_restored")), states[3].1);

    // Read-only, and not beyond the current generation.
    assert!(error(engine, &format!("on users as of {} | delete", states[3].0)).contains("read-only"));
    assert!(error(engine, "on users as of 999999").contains("after the current generation"));
}

fn check_authors(engine: &Engine) {
    let history = run(engine, "on users history | where _op == \"replace\"");
    assert!(history.iter().all(|row| row.get("_by") == Some(&Value::from("identity-ada")) && row.get("_by_device") == Some(&Value::from("device-1"))));
    assert!(history.iter().all(|row| row.get("_at").and_then(Value::as_str).is_some_and(|at| at.ends_with('Z'))));
}

#[test] fn memory_keeps_history_when_asked() { let storage = Arc::new(MemoryStorage::with_history()); let engine = engine(storage.clone()); let current = storage.clone(); scenario(&engine, &move || current.generation().unwrap(), &mut |_| {}); check_authors(&engine); let plain = self::engine(Arc::new(MemoryStorage::new())); assert!(error(&plain, "on users as of 0").contains("does not keep history")); }

#[test] fn glacier_keeps_history_across_flushes_and_reopens() { let dir = temp("glacier"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); storage.record_history().unwrap(); let engine = engine(storage.clone()); let current = storage.clone(); let checkpoints = storage.clone(); scenario(&engine, &move || current.generation().unwrap(), &mut |step| if step % 2 == 1 { checkpoints.checkpoint().unwrap() }); check_authors(&engine); drop(engine); drop(checkpoints); drop(storage); let reopened = Arc::new(GlacierStorage::open(&dir).unwrap()); assert!(reopened.keeps_history()); let engine = self::engine(reopened.clone()); assert_eq!(people(&run(&engine, "on users as of 1")), ["Alice:31", "Bob:17"]); assert_eq!(people(&run(&engine, "on users as of 4")), ["Bob:18", "Carol:45"]); assert_eq!(run(&engine, "on users history").len(), 7); check_authors(&engine); let _ = std::fs::remove_dir_all(&dir); }

#[test] fn glacier_history_starts_at_a_baseline_and_keeps_large_commits() { let dir = temp("glacier-base"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); let engine = engine(storage.clone()); run(&engine, r#"on items | insert [{k: 1}, {k: 2}]"#); run(&engine, r#"on items | where k == 1 | set k = 10"#); assert!(error(&engine, "on items as of 1").contains("does not keep history")); storage.record_history().unwrap(); let base = storage.generation().unwrap(); assert!(error(&engine, "on items as of 1").contains("keeps history from generation")); /* A commit of 20 000 puts skips the log: its rows are kept as a segment. */ let rows = (0..20_000).map(|index| format!("{{k: {}}}", 100 + index)).collect::<Vec<_>>().join(", "); run(&engine, &format!("on items | insert [{rows}]")); let large = storage.generation().unwrap(); run(&engine, r#"on items | where k == 2 | delete"#); run(&engine, r#"on items | where k >= 100 and k < 110 | set k = k + 100000"#); let count = |text: &str| run(&engine, &format!("{text} | count"))[0].get("count").and_then(int).unwrap(); assert_eq!(count(&format!("on items as of {base}")), 2); assert_eq!(count(&format!("on items as of {large}")), 20_002); assert_eq!(count(&format!("on items as of {large} | where k == 2")), 1); assert_eq!(count("on items | where k == 2"), 0); assert_eq!(count(&format!("on items as of {large} | where k >= 100000")), 0); assert_eq!(count("on items | where k >= 100000"), 10); let history = run(&engine, r#"on items history | where _op != "insert" | sort _generation, k"#); let ops = history.iter().map(|row| row.get("_op").and_then(Value::as_str).unwrap().to_owned()).collect::<Vec<_>>(); assert_eq!(ops.iter().filter(|op| *op == "baseline").count(), 2); assert_eq!(ops.iter().filter(|op| *op == "delete").count(), 1); assert_eq!(ops.iter().filter(|op| *op == "replace").count(), 10); assert_eq!(count("on items history"), 2 + 20_000 + 1 + 10); drop(engine); drop(storage); let reopened = Arc::new(GlacierStorage::open(&dir).unwrap()); let engine = self::engine(reopened); assert_eq!(run(&engine, &format!("on items as of {large} | count"))[0].get("count").and_then(int), Some(20_002)); let _ = std::fs::remove_dir_all(&dir); }

#[test] fn temporal_sources_parse_and_keep_aliases() { for text in [r#"on users as of "2026-09-01T00:00:00Z" | count"#, "on users as of 12 as u | where u.age > 3", "on users history as h | count", "on users as of | count"] { parse(text).unwrap_or_else(|error| panic!("{text}: {error}")); } assert!(parse(r#"on users as of where"#).is_err()); let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::with_history()); let engine = engine(storage); run(&engine, r#"on users | insert {name: "Zoe", age: 3}"#); assert_eq!(people(&run(&engine, "on users as of 1 as u | where u.age > 1")), ["Zoe:3"]); /* `as of` without a value stays an alias named `of`. */ assert_eq!(people(&run(&engine, "on users as of | where of.age > 1")), ["Zoe:3"]); let text = r#"on users as of "yesterday""#; let error = PlannerPipeline::from_ast(text, &parse(text).unwrap()).expect_err("not a date").to_string(); assert!(error.contains("ISO-8601"), "{error}"); }

#[test] fn glacier_writes_past_the_query_memory_in_one_transaction() { let dir = temp("glacier-spill"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); storage.record_history().unwrap(); let rows = (0..20_000).map(|index| format!("{{k: {index}, p: \"{}\"}}", "x".repeat(100))).collect::<Vec<_>>().join(", "); run(&engine(storage.clone()), &format!("on items | insert [{rows}]")); let base = storage.generation().unwrap(); let limited = engine(storage.clone()).with_memory_governor(og_core::MemoryGovernor::with_limit(1 << 20)); run(&limited, "on items | into archive"); run(&limited, "on items | where k % 2 == 0 | delete"); let plan = |text: &str| (limited.plan(&pipeline(text)).unwrap().physical().clone(), None); limited.execute_together(&[plan("on archive | where k < 1000 | delete"), plan("on items | derive k = k + 100000 | into archive")]).unwrap(); let count = |engine: &Engine, text: &str| run(engine, &format!("{text} | count"))[0].get("count").and_then(int).unwrap(); assert_eq!([count(&limited, "on items"), count(&limited, "on archive"), count(&limited, "on archive | where k >= 100000"), count(&limited, &format!("on items as of {base}")), count(&limited, "on items history | where _op == \"delete\"")], [10_000, 29_000, 10_000, 20_000, 10_000]); assert!(limited.execute_together(&[plan("on archive | delete"), plan("on _views | delete")]).is_err()); assert_eq!(count(&limited, "on archive"), 29_000); drop(limited); drop(storage); let engine = engine(Arc::new(GlacierStorage::open(&dir).unwrap())); assert_eq!([count(&engine, "on items"), count(&engine, "on archive"), count(&engine, &format!("on items as of {base}"))], [10_000, 29_000, 20_000]); let _ = std::fs::remove_dir_all(&dir); }
#[test] fn glacier_backup_journal_gives_diffs_and_keeps_only_what_they_need() { use og_core::backup::{self, BackupMetadata, BackupSource}; let (dir, backups) = (temp("glacier-journal"), temp("glacier-journal-backups")); std::fs::create_dir_all(&backups).unwrap(); let at = |name: &str| backups.join(format!("{name}.ogb")); let metadata = || BackupMetadata { created_at: 1, source: BackupSource { instance_id: "i".into(), hostname: "h".into(), platform: "p".into(), arch: "a".into(), core_version: "0".into() } }; let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); storage.record_journal().unwrap(); assert!(!storage.keeps_history()); let engine = engine(storage.clone()); let count = |engine: &Engine, text: &str| run(engine, &format!("{text} | count"))[0].get("count").and_then(int).unwrap(); run(&engine, r#"on items | insert [{k: 1}, {k: 2}, {k: 3}]"#); storage.checkpoint().unwrap(); backup::create(storage.as_ref(), &at("full"), metadata()).unwrap(); /* The full backup released what came before it. */ assert!(std::fs::read_dir(dir.join("hist")).unwrap().flatten().all(|entry| !entry.file_name().to_string_lossy().starts_with("w-"))); /* A commit of 20 000 puts skips the log: the journal keeps its segment. */ let rows = (0..20_000).map(|index| format!("{{k: {}}}", 100 + index)).collect::<Vec<_>>().join(", "); run(&engine, &format!("on items | insert [{rows}]")); run(&engine, r#"on items | where k == 2 | delete"#); storage.checkpoint().unwrap(); let one = backup::create_diff(storage.as_ref(), &at("d1"), metadata(), &at("full")).unwrap(); assert_eq!((one.documents, one.removed), (20_000, 1)); run(&engine, r#"on items | where k >= 100 and k < 110 | set k = k + 100000"#); run(&engine, r#"on other | insert {v: 1}"#); assert!(error(&engine, "on items as of 1").contains("does not keep history")); drop(engine); drop(storage); /* The journal survives a reopen. */ let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); assert!(storage.record_history().is_err()); let two = backup::create_diff(storage.as_ref(), &at("d2"), metadata(), &at("d1")).unwrap(); assert_eq!((two.collections, two.documents, two.removed), (2, 11, 0)); let source = self::engine(storage.clone()); let target_dir = temp("glacier-journal-target"); let target = Arc::new(GlacierStorage::open(&target_dir).unwrap()); backup::restore(target.as_ref(), &at("d2"), false).unwrap(); let restored = self::engine(target.clone()); for text in ["on items", "on items | where k >= 100000", "on items | where k == 2", "on other"] { assert_eq!(count(&restored, text), count(&source, text), "{text}"); } /* A diff made from the restored store must start from a full backup. */ assert!(backup::create_diff(target.as_ref(), &at("d3"), metadata(), &at("d2")).is_err()); let _ = [std::fs::remove_dir_all(&dir), std::fs::remove_dir_all(&backups), std::fs::remove_dir_all(&target_dir)]; }
#[test] fn glacier_history_survives_a_backup_with_history() { use og_core::backup::{self, BackupMetadata, BackupSource}; let (dir, target_dir, backups) = (temp("glacier-full"), temp("glacier-full-target"), temp("glacier-full-backups")); std::fs::create_dir_all(&backups).unwrap(); let at = backups.join("full.ogb"); let metadata = BackupMetadata { created_at: 1, source: BackupSource { instance_id: "i".into(), hostname: "h".into(), platform: "p".into(), arch: "a".into(), core_version: "0".into() } }; let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); let source = engine(storage.clone()); run(&source, r#"on items | insert [{k: 1}, {k: 2}]"#); run(&source, r#"on items | where k == 2 | set k = 20"#); storage.record_history().unwrap(); let base = storage.generation().unwrap(); let current = storage.clone(); let checkpoints = storage.clone(); scenario(&source, &move || current.generation().unwrap(), &mut |step| if step % 2 == 1 { checkpoints.checkpoint().unwrap() }); let rows = (0..20_000).map(|index| format!("{{k: {}}}", 100 + index)).collect::<Vec<_>>().join(", "); run(&source, &format!("on items | insert [{rows}]")); run(&source, r#"on scratch | insert [{v: 1}, {v: 2}]"#); run(&source, r#"on scratch | delete"#); run(&source, r#"on items | where k >= 100 and k < 105 | delete"#); let last = storage.generation().unwrap(); assert!(backup::create_with_history(&MemoryStorage::new(), &at, metadata.clone()).is_err()); let made = backup::create_with_history(storage.as_ref(), &at, metadata).unwrap(); assert!(made.events > 20_000); assert_eq!(backup::inspect(&at).unwrap().chain.unwrap().history, Some(base)); /* A live store gets the state; a new store, the history too. */ let state_only = Arc::new(MemoryStorage::new()); backup::restore(state_only.as_ref(), &at, false).unwrap(); assert_eq!(people(&run(&engine(state_only), "on users")), ["Bob:18", "Carol:46", "Dave:52"]); let target = Arc::new(GlacierStorage::open(&target_dir).unwrap()); backup::restore_history(target.as_ref(), &at).unwrap(); assert!(backup::restore_history(target.as_ref(), &at).is_err()); let compare = |restored: &Engine| { for generation in base..=last { for text in [format!("on users as of {generation}"), format!("on items as of {generation} | count"), format!("on scratch as of {generation} | count")] { assert_eq!(run(restored, &text), run(&source, &text), "{text}"); } } for text in ["on users history", "on items history | count", "on scratch history", "on items history | where _op == \"delete\" | count"] { assert_eq!(run(restored, text), run(&source, text), "{text}"); } check_authors(restored); }; compare(&engine(target.clone())); assert!(error(&engine(target.clone()), &format!("on users as of {}", base - 1)).contains("keeps history from generation")); drop(target); compare(&engine(Arc::new(GlacierStorage::open(&target_dir).unwrap()))); let _ = [std::fs::remove_dir_all(&dir), std::fs::remove_dir_all(&target_dir), std::fs::remove_dir_all(&backups)]; }

/// A floor on the history read (the retention of a Place's pool): `as of` before it is refused, `history` starts at it.
#[test] fn a_history_floor_refuses_an_earlier_as_of_and_leaves_earlier_versions_out() { let storage = Arc::new(MemoryStorage::with_history()); let engine = engine(storage.clone()); let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64; let pause = || std::thread::sleep(std::time::Duration::from_millis(5)); run(&engine, r#"on users | insert {name: "Alice", age: 31}"#); let first = storage.generation().unwrap(); pause(); run(&engine, r#"on users | insert {name: "Bob", age: 17}"#); let second = storage.generation().unwrap(); pause(); let floor = now(); pause(); run(&engine, r#"on users | where name == "Bob" | set age = 18"#); let ops = |rows: &[Document]| rows.iter().map(|row| format!("{}:{}", row.get("_op").and_then(Value::as_str).unwrap(), row.get("name").and_then(Value::as_str).unwrap_or("?"))).collect::<Vec<_>>(); let all = run(&engine, "on users history"); assert_eq!(ops(&all), ["insert:Alice", "insert:Bob", "replace:Bob"]); let early = all[0].get("_at").and_then(Value::as_str).unwrap().to_owned(); with_history_floor(Some(floor), || { assert_eq!(ops(&run(&engine, "on users history")), ["replace:Bob"]); assert_eq!(run(&engine, "on users history | count")[0].get("count").and_then(int), Some(1)); assert_eq!(people(&run(&engine, &format!("on users as of {second}"))), ["Alice:31", "Bob:17"], "the state the floor found is kept"); assert_eq!(people(&run(&engine, "on users")), ["Alice:31", "Bob:18"]); for query in [format!("on users as of {first}"), format!("on users as of \"{early}\"")] { assert!(error(&engine, &query).contains("earlier than the history this Place keeps"), "{query}"); } }); assert_eq!(people(&run(&engine, &format!("on users as of {first}"))), ["Alice:31"], "no floor outside"); }

/// A purge with a floor per Place: superseded versions older than the floor go, every state from the floor on stays.
#[test] fn a_purge_removes_versions_superseded_before_the_floor_of_their_place() { let dir = temp("glacier-purge"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); let engine = engine(storage.clone()); let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64; let pause = || std::thread::sleep(std::time::Duration::from_millis(5)); run(&engine, r#"on users | insert [{name: "Alice", age: 31, _place: "p1"}, {name: "Bob", age: 17, _place: "p2"}]"#); storage.record_history().unwrap(); let base = storage.generation().unwrap(); run(&engine, r#"on users | where name == "Alice" | set age = 32"#); run(&engine, r#"on users | where name == "Bob" | set age = 18"#); storage.checkpoint().unwrap(); /* A large commit is kept as a segment of rows: its rows are purged by a list beside it. */ let rows = (0..20_000).map(|index| format!("{{k: {index}, _place: \"p1\"}}")).collect::<Vec<_>>().join(", "); run(&engine, &format!("on items | insert [{rows}]")); run(&engine, "on items | where k < 5 | set k = -1"); let middle = storage.generation().unwrap(); pause(); let floor = now(); pause(); run(&engine, r#"on users | where name == "Alice" | set age = 33"#); run(&engine, r#"on users | where name == "Bob" | delete"#); run(&engine, r#"on users | insert {name: "Carol", age: 45, _place: "p1"}"#); let last = storage.generation().unwrap(); let versions = |name: &str| run(&engine, &format!(r#"on users history | where name == "{name}" | sort _generation"#)).iter().map(|row| row.get("age").and_then(int).unwrap()).collect::<Vec<_>>(); let states = |generations: &[u64]| generations.iter().map(|generation| (people(&run(&engine, &format!("on users as of {generation}"))), run(&engine, &format!("on items as of {generation} | where k < 0 | count"))[0].get("count").and_then(int))).collect::<Vec<_>>(); let (bob, kept) = (versions("Bob"), states(&[middle, middle + 1, last])); assert_eq!((versions("Alice"), people(&run(&engine, &format!("on users as of {base}")))), (vec![31, 32, 33], vec!["Alice:31".to_owned(), "Bob:17".to_owned()])); /* p1 from the floor, p2 kept whole: Alice at the baseline and the 5 rows replaced go. */ let purged = storage.purge_history(&|place| (place == Some("p1")).then_some(floor)).unwrap(); assert_eq!(purged.versions, 6, "{purged:?}"); assert_eq!((versions("Alice"), versions("Bob"), states(&[middle, middle + 1, last])), (vec![32, 33], bob.clone(), kept.clone())); assert_eq!(storage.purge_history(&|place| (place == Some("p1")).then_some(floor)).unwrap().versions, 0, "nothing left to purge"); /* Everything up to now: one version left per live document, none of a deleted one. */ let purged = storage.purge_history(&|_| Some(now())).unwrap(); assert_eq!((purged.versions, versions("Alice"), versions("Bob"), versions("Carol")), (4, vec![33], vec![], vec![45]), "{purged:?}"); assert_eq!(states(&[last]), kept[2..]); /* Below one row in four a segment keeps the list of its purged rows beside it; a Place that keeps nothing has it written again, the list gone. */ let lists = || std::fs::read_dir(dir.join("hist")).unwrap().flatten().filter(|entry| entry.file_name().to_string_lossy().ends_with(".dead")).count(); assert_eq!(lists(), 1, "the 5 purged rows of the large commit"); run(&engine, "on items | where k >= 10 and k < 13 | set k = -2"); let last = storage.generation().unwrap(); let before = states(&[last]); let purged = storage.purge_history(&|_| Some(i64::MAX)).unwrap(); assert_eq!((purged.versions, lists(), states(&[last])), (3, 0, before), "{purged:?}"); assert_eq!(run(&engine, "on items history | count")[0].get("count").and_then(int), Some(20_000)); drop(engine); drop(storage); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); let engine = self::engine(storage.clone()); assert_eq!(people(&run(&engine, &format!("on users as of {last}"))), ["Alice:33", "Carol:45"]); assert_eq!(run(&engine, r#"on users history | where name == "Alice""#).len(), 1); assert!(GlacierStorage::open(temp("glacier-purge-plain")).unwrap().purge_history(&|_| None).is_err(), "no history, no purge"); let _ = std::fs::remove_dir_all(dir); }

/// Every past state Glacier reads by itself (live segments masked, history segments, logs) matches the state the
/// memory store rebuilds, whatever wrote it: a baseline of segments, large commits written to segments, small
/// commits, deletes, checkpoints, a reopen.
#[test] fn glacier_reads_each_past_state_as_the_memory_store_does() { let dir = temp("glacier-native-as-of"); let glacier = Arc::new(GlacierStorage::open(&dir).unwrap()); let memory = Arc::new(MemoryStorage::with_history()); let rows = |from: u64, to: u64| (from..to).map(|k| format!("{{k: {k}, g: {}, s: \"v{}\"}}", k % 7, k % 3)).collect::<Vec<_>>().join(", "); let writes = [format!("on items | insert [{}]", rows(0, 20_000)), "on items | where k % 5 == 0 | set s = \"changed\"".to_owned(), "on items | where k < 300 | set g = g + 10".to_owned(), "on items | where k % 11 == 0 | delete".to_owned(), format!("on items | insert [{}]", rows(20_000, 38_000)), "on items | where k >= 19990 and k < 20010 | set s = \"edge\"".to_owned(), "on items | where g == 3 | set g = 30".to_owned(), "on items | where k % 13 == 1 | delete".to_owned()]; let (on_glacier, on_memory) = (engine(glacier.clone()), engine(memory.clone())); run(&on_glacier, &writes[0]); run(&on_memory, &writes[0]); glacier.checkpoint().unwrap(); glacier.record_history().unwrap(); let mut generations = vec![(glacier.generation().unwrap(), memory.generation().unwrap())]; for (step, write) in writes.iter().enumerate().skip(1) { run(&on_glacier, write); run(&on_memory, write); if step % 3 == 0 { glacier.checkpoint().unwrap(); } generations.push((glacier.generation().unwrap(), memory.generation().unwrap())); } let reads = ["| count", "| group g, s | sort g, s", "| where s == \"edge\" or k < 40 | sort k | select k, g, s", "| sort k desc | limit 5 | select k, g, s", "| where k == 19995 | select k, g, s, _version"]; let check = |on_glacier: &Engine, generations: &[(u64, u64)]| for (glacier_at, memory_at) in generations { for read in reads { let (left, right) = (run(on_glacier, &format!("on items as of {glacier_at} {read}")), run(&on_memory, &format!("on items as of {memory_at} {read}"))); let strip = |rows: Vec<Document>| rows.into_iter().map(|mut row| { row.remove("_id"); format!("{row:?}") }).collect::<Vec<_>>(); assert_eq!(strip(left), strip(right), "as of {glacier_at} {read}"); } }; check(&on_glacier, &generations); /* Read again, past states kept: a write to the collection must not serve them for its new states. */ let late = "on items | where k % 17 == 2 | set s = \"late\""; run(&on_glacier, late); run(&on_memory, late); generations.push((glacier.generation().unwrap(), memory.generation().unwrap())); check(&on_glacier, &generations); drop(on_glacier); drop(glacier); let reopened = Arc::new(GlacierStorage::open(&dir).unwrap()); check(&engine(reopened), &generations); let _ = std::fs::remove_dir_all(&dir); }

/// The rows a write returns move to disk past the query's memory with its pending writes: every one comes back, in
/// order, where they used to fail the write.
#[test] fn a_write_returns_rows_past_the_query_memory() { let dir = temp("glacier-returned"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); let rows = (0..20_000).map(|index| format!("{{k: {index}, p: \"{}\"}}", "x".repeat(100))).collect::<Vec<_>>().join(", "); run(&engine(storage.clone()), &format!("on items | insert [{rows}]")); let limited = engine(storage.clone()).with_memory_governor(og_core::MemoryGovernor::with_limit(1 << 20)); let returned = run(&limited, "on items | where k % 2 == 1 | set p = \"y\""); assert_eq!(returned.len(), 10_000); assert!(returned.iter().enumerate().all(|(index, row)| row.get("k").and_then(int) == Some(2 * index as u64 + 1) && row.get("p").and_then(Value::as_str) == Some("y"))); assert_eq!(run(&limited, "on items | where p == \"y\" | count")[0].get("count").and_then(int), Some(10_000)); let _ = std::fs::remove_dir_all(&dir); }
