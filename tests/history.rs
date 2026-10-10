//! `on c as of …` and `on c history` on every backend, through the language.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{path::PathBuf, sync::Arc};

use og_core::{
    engine::Engine,
    query::{parse, DocumentScope, PlannerPipeline},
    storage::{
        history::{with_commit_author, CommitAuthor},
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

/// Removes a Glacier store file and the files beside it (`<store>.*`).
fn remove(path: &std::path::Path) {
    let name = path.file_name().unwrap().to_string_lossy().into_owned();
    for entry in std::fs::read_dir(path.parent().unwrap()).into_iter().flatten().flatten() {
        if entry.file_name().to_string_lossy().strip_prefix(name.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('.')) { let _ = std::fs::remove_file(entry.path()); }
    }
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

#[test] fn glacier_keeps_history_across_flushes_and_reopens() { let dir = temp("glacier"); let storage = Arc::new(GlacierStorage::open(&dir).unwrap()); storage.record_history().unwrap(); let engine = engine(storage.clone()); let current = storage.clone(); let checkpoints = storage.clone(); scenario(&engine, &move || current.generation().unwrap(), &mut |step| if step % 2 == 1 { checkpoints.checkpoint().unwrap() }); check_authors(&engine); drop(engine); drop(checkpoints); drop(storage); let reopened = Arc::new(GlacierStorage::open(&dir).unwrap()); assert!(reopened.keeps_history()); let engine = self::engine(reopened.clone()); assert_eq!(people(&run(&engine, "on users as of 1")), ["Alice:31", "Bob:17"]); assert_eq!(people(&run(&engine, "on users as of 4")), ["Bob:18", "Carol:45"]); assert_eq!(run(&engine, "on users history").len(), 7); check_authors(&engine); remove(&dir); }

#[test] fn glacier_reads_history_from_its_store_file() { let path = temp("glacier-file"); let storage = Arc::new(GlacierStorage::open(&path).unwrap()); storage.record_history().unwrap(); let engine = engine(storage.clone()); let current = storage.clone(); let checkpoints = storage.clone(); scenario(&engine, &move || current.generation().unwrap(), &mut |step| if step == 2 { checkpoints.checkpoint().unwrap() }); check_authors(&engine); drop(engine); drop(checkpoints); drop(storage); /* After a reopen from a checkpoint, older states come from the frames. */ GlacierStorage::open(&path).unwrap().checkpoint().unwrap(); let engine = self::engine(Arc::new(GlacierStorage::open(&path).unwrap())); assert_eq!(people(&run(&engine, "on users as of 1")), ["Alice:31", "Bob:17"]); assert_eq!(people(&run(&engine, "on users as of 3")), ["Alice:31", "Bob:18", "Carol:45"]); assert_eq!(run(&engine, "on users history").len(), 7); check_authors(&engine); remove(&path); }

#[test] fn temporal_sources_parse_and_keep_aliases() { for text in [r#"on users as of "2026-09-01T00:00:00Z" | count"#, "on users as of 12 as u | where u.age > 3", "on users history as h | count", "on users as of | count"] { parse(text).unwrap_or_else(|error| panic!("{text}: {error}")); } assert!(parse(r#"on users as of where"#).is_err()); let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::with_history()); let engine = engine(storage); run(&engine, r#"on users | insert {name: "Zoe", age: 3}"#); assert_eq!(people(&run(&engine, "on users as of 1 as u | where u.age > 1")), ["Zoe:3"]); /* `as of` without a value stays an alias named `of`. */ assert_eq!(people(&run(&engine, "on users as of | where of.age > 1")), ["Zoe:3"]); let text = r#"on users as of "yesterday""#; let error = PlannerPipeline::from_ast(text, &parse(text).unwrap()).expect_err("not a date").to_string(); assert!(error.contains("ISO-8601"), "{error}"); }

#[test] fn glacier_runs_several_writes_in_one_transaction() { let path = temp("glacier-together"); let storage = Arc::new(GlacierStorage::open(&path).unwrap()); storage.record_history().unwrap(); let writer = engine(storage.clone()); let rows = (0..20_000).map(|index| format!("{{k: {index}, p: \"{}\"}}", "x".repeat(100))).collect::<Vec<_>>().join(", "); run(&writer, &format!("on items | insert [{rows}]")); let base = storage.generation().unwrap(); /* Glacier keeps a transaction in memory: past the budget, a write fails. */ assert!(engine(storage.clone()).with_memory_governor(og_core::MemoryGovernor::with_limit(1 << 20)).execute(&pipeline("on items | into archive")).is_err()); run(&writer, "on items | into archive"); run(&writer, "on items | where k % 2 == 0 | delete"); let plan = |text: &str| (writer.plan(&pipeline(text)).unwrap().physical().clone(), None); writer.execute_together(&[plan("on archive | where k < 1000 | delete"), plan("on items | derive k = k + 100000 | into archive")]).unwrap(); let count = |engine: &Engine, text: &str| run(engine, &format!("{text} | count"))[0].get("count").and_then(int).unwrap(); assert_eq!([count(&writer, "on items"), count(&writer, "on archive"), count(&writer, "on archive | where k >= 100000"), count(&writer, &format!("on items as of {base}")), count(&writer, "on items history | where _op == \"delete\"")], [10_000, 29_000, 10_000, 20_000, 10_000]); assert!(writer.execute_together(&[plan("on archive | delete"), plan("on _views | delete")]).is_err()); assert_eq!(count(&writer, "on archive"), 29_000); drop(writer); drop(storage); let engine = engine(Arc::new(GlacierStorage::open(&path).unwrap())); assert_eq!([count(&engine, "on items"), count(&engine, "on archive"), count(&engine, &format!("on items as of {base}"))], [10_000, 29_000, 20_000]); remove(&path); }
