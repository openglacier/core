//! Crash, corruption and recovery torture for Glacier.
//! Run: cargo test --release --test glacier_torture -- --nocapture --test-threads=1
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{collections::BTreeMap, io::{BufRead, BufReader, Write}, path::{Path, PathBuf}, process::{Command, Stdio}, sync::Arc};
use og_core::{storage::{CollectionId, DocumentId, GlacierStorage, ScanOptions, StorageEngine, StorageMutation}, Document, Value};

fn coll() -> CollectionId { CollectionId::parse("t").unwrap() }
fn dir(name: &str) -> PathBuf { let p = PathBuf::from(std::env::var("TORTURE_DIR").unwrap_or("/tmp".into())).join(format!("g6t-{name}-{}", std::process::id())); let _ = std::fs::remove_dir_all(&p); p }

/// Commit `tag` holds `n` docs with `c = tag`.
fn commit(s: &GlacierStorage, tag: u64, base: &mut u64, n: u64) -> Result<(), String> {
    let muts = (0..n).map(|i| { let mut d = Document::new(); d.insert("c", Value::from(tag as i64)); d.insert("k", Value::from(i as i64)); d.insert("s", Value::string(format!("v{}", i % 17))); *base += 1; StorageMutation::insert(DocumentId::from_bytes((((1u128) << 64) | u128::from(*base)).to_be_bytes()), Arc::new(d)) }).collect();
    s.apply_batch_atomic_summary(&coll(), muts).map(|_| ()).map_err(|e| e.to_string())
}
fn sizes(tag: u64) -> u64 { if tag.is_multiple_of(200) { 20_000 } else if tag.is_multiple_of(7) { 3_000 } else { 5 } }

/// tag -> row count
fn tags(s: &GlacierStorage) -> Result<BTreeMap<u64, u64>, String> {
    let r = s.read().map_err(|e| e.to_string())?;
    let mut m = BTreeMap::new();
    r.scan_each(&coll(), ScanOptions::default(), &mut |d| { let c = d.document().get("c").and_then(|v| v.as_number().map(|n| format!("{n:?}"))).unwrap_or_default(); let c: u64 = c.chars().filter(|x| x.is_ascii_digit()).collect::<String>().parse().unwrap_or(u64::MAX); *m.entry(c).or_default() += 1; Ok(true) }).map_err(|e| e.to_string())?;
    Ok(m)
}

/// A child process inherits every open `LOCK` until it `exec`s, so a store
/// another test drops meanwhile cannot be reopened: tests that spawn one take
/// this lock for writing, every other test for reading.
static FORK: std::sync::RwLock<()> = std::sync::RwLock::new(());
fn serial_read() -> std::sync::RwLockReadGuard<'static, ()> { FORK.read().unwrap_or_else(std::sync::PoisonError::into_inner) }
fn serial_write() -> std::sync::RwLockWriteGuard<'static, ()> { FORK.write().unwrap_or_else(std::sync::PoisonError::into_inner) }

#[test] fn child_writer() { let Ok(path) = std::env::var("G6_CHILD") else { return }; let s = GlacierStorage::open(&path).unwrap(); let mut base = 0u64; let mut tag = 0u64; if let Ok(t) = tags(&s) { tag = t.keys().filter(|k| **k != u64::MAX).max().copied().unwrap_or(0); base = s.read().unwrap().count(&coll()).unwrap_or(0) + 1_000_000 * tag; } let out = std::io::stdout(); loop { tag += 1; commit(&s, tag, &mut base, sizes(tag)).unwrap(); writeln!(out.lock(), "{tag}").unwrap(); out.lock().flush().unwrap(); if tag.is_multiple_of(50) { s.checkpoint().unwrap(); } } }

fn check(path: &Path, acked: u64) -> BTreeMap<u64, u64> {
    let s = GlacierStorage::open(path).unwrap_or_else(|e| panic!("REOPEN FAILED: {e}"));
    let t = tags(&s).unwrap_or_else(|e| panic!("SCAN FAILED: {e}"));
    for tag in 1..=acked { assert_eq!(t.get(&tag).copied(), Some(sizes(tag)), "acked commit {tag} lost or partial"); }
    for (tag, n) in &t { assert_eq!(*n, sizes(*tag), "commit {tag} partial ({n} rows)"); }
    let max = t.keys().max().copied().unwrap_or(0);
    for tag in 1..=max { assert!(t.contains_key(&tag), "hole at commit {tag} (max {max})"); }
    assert_eq!(s.read().unwrap().count(&coll()).unwrap(), t.values().sum::<u64>());
    drop(s);
    t
}

#[test] #[ignore = "slow (about 3 minutes)"] fn kill9_rounds() { let _serial = serial_write(); let path = dir("kill"); let exe = std::env::current_exe().unwrap(); let mut rng = 0x9E3779B97F4A7C15u64; let mut acked = 0u64; for round in 0..40 { let mut child = Command::new(&exe).args(["child_writer", "--exact", "--nocapture"]).env("G6_CHILD", &path).stdout(Stdio::piped()).spawn().unwrap(); rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; let lines = BufReader::new(child.stdout.take().unwrap()); let stop = 1 + rng % 400; let mut seen = 0u64; for l in lines.lines() { let Ok(l) = l else { break }; if let Ok(n) = l.trim().parse::<u64>() { acked = acked.max(n); seen += 1; if seen >= stop { break } } } std::thread::sleep(std::time::Duration::from_micros(rng % 3000)); child.kill().unwrap(); let _ = child.wait(); let t = check(&path, acked); eprintln!("round {round}: acked {acked}, recovered up to {}", t.keys().max().copied().unwrap_or(0)); acked = t.keys().filter(|k| **k != u64::MAX).max().copied().unwrap_or(0).min(acked).max(acked); /* continue from what is actually recovered */ acked = acked.min(t.keys().max().copied().unwrap_or(0)); } let _ = std::fs::remove_dir_all(&path); }

fn seeded(name: &str, commits: u64) -> PathBuf {
    let p = dir(name); let s = GlacierStorage::open(&p).unwrap(); let mut base = 0;
    for t in 1..=commits { commit(&s, t, &mut base, 5).unwrap(); }
    p
}
fn newest(p: &Path, prefix: &str, suffix: &str) -> PathBuf { let mut v: Vec<_> = std::fs::read_dir(p).unwrap().flatten().map(|e| e.path()).filter(|x| { let n = x.file_name().unwrap().to_str().unwrap(); n.starts_with(prefix) && n.ends_with(suffix) }).collect(); v.sort(); v.pop().unwrap() }
fn flip(file: &Path, at: usize) { let mut b = std::fs::read(file).unwrap(); let at = at.min(b.len() - 1); b[at] ^= 0xFF; std::fs::write(file, b).unwrap(); }

#[test] fn wal_corrupt_in_the_middle_is_not_silent() { let _serial = serial_read(); let p = seeded("walmid", 10); let wal = newest(&p, "wal-", ".log"); let len = std::fs::metadata(&wal).unwrap().len() as usize; flip(&wal, len / 3); /* record ~3 of 10: records 4..10 are intact behind it */ let error = GlacierStorage::open(&p).err().map(|e| e.to_string()); eprintln!("open: {error:?}"); assert!(error.is_some_and(|e| e.contains("corrupt")), "a corrupt record with valid commits behind it must not open"); assert_eq!(std::fs::metadata(&wal).unwrap().len() as usize, len, "the log must not be cut"); /* Moved aside, the store opens on its manifest alone. */ std::fs::rename(&wal, p.join("wal.corrupt")).unwrap(); assert!(GlacierStorage::open(&p).is_ok()); }
#[test] fn wal_torn_tail_still_opens() { let _serial = serial_read(); let p = seeded("waltail", 10); let wal = newest(&p, "wal-", ".log"); let len = std::fs::metadata(&wal).unwrap().len() as usize; flip(&wal, len - 3); /* the last record only */ let s = GlacierStorage::open(&p).unwrap(); assert_eq!(tags(&s).unwrap().len(), 9); }
#[test] fn manifest_missing_must_not_erase_segments() { let _serial = serial_read(); let p = seeded("nomani", 10); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } let segs = |p: &Path| std::fs::read_dir(p).unwrap().flatten().filter(|e| e.file_name().to_str().unwrap().ends_with(".g6")).count(); let before = segs(&p); std::fs::remove_file(p.join("MANIFEST")).unwrap(); let r = GlacierStorage::open(&p); eprintln!("segments before {before}, after open {}, open: {:?}", segs(&p), r.as_ref().err().map(|e| e.to_string())); assert!(r.is_err(), "a store whose MANIFEST vanished must not open as a new store"); assert!(segs(&p) >= before, "opening a store whose MANIFEST vanished deleted its segments"); }
#[test] fn manifest_missing_before_any_checkpoint_is_refused_too() { let _serial = serial_read(); /* Since the manifest comes first, a log without one is refused as well. */ let p = seeded("nomani0", 3); std::fs::remove_file(p.join("MANIFEST")).unwrap(); std::fs::rename(newest(&p, "wal-", ".log"), p.join("wal-00000002.log")).unwrap(); assert!(GlacierStorage::open(&p).is_err()); }
#[test] fn manifest_corrupt_refuses() { let _serial = serial_read(); let p = seeded("manicorrupt", 10); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } flip(&p.join("MANIFEST"), 30); let before = std::fs::read_dir(&p).unwrap().count(); let r = GlacierStorage::open(&p); eprintln!("open: {:?}; files before {before} after {}", r.as_ref().err().map(|e| e.to_string()), std::fs::read_dir(&p).unwrap().count()); assert!(r.is_err()); }
#[test] fn segment_bitrot_is_reported() { let _serial = serial_read(); let p = seeded("segrot", 3000); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } let seg = newest(&p, "0", ".g6"); let len = std::fs::metadata(&seg).unwrap().len() as usize; for (name, at) in [("chunk", len / 3), ("footer", len - 20), ("magic", 2), ("tail-magic", len - 2)] { let q = dir("segrot2"); copy_dir(&p, &q); let f = newest(&q, "0", ".g6"); flip(&f, at); match GlacierStorage::open(&q) { Err(e) => eprintln!("{name}: open error: {e}"), Ok(s) => match tags(&s) { Err(e) => eprintln!("{name}: scan error (lazy): {e}"), Ok(t) => eprintln!("{name}: FINDING silent, {} rows read", t.values().sum::<u64>()) } } let _ = std::fs::remove_dir_all(&q); } }
#[test] fn verify_finds_a_damaged_chunk_before_any_read() { let _serial = serial_read(); let p = seeded("verify", 3000); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } let clean = GlacierStorage::open(&p).unwrap().verify().unwrap(); assert!(clean.damaged.is_empty() && clean.chunks > 0, "{clean:?}"); let seg = newest(&p, "0", ".g6"); let len = std::fs::metadata(&seg).unwrap().len() as usize; flip(&seg, len / 3); let report = GlacierStorage::open(&p).unwrap().verify().unwrap(); eprintln!("{report:?}"); assert_eq!(report.damaged.len(), 1); }
#[test] fn segment_truncated_and_missing() { let _serial = serial_read(); let p = seeded("segtrunc", 3000); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } let q = dir("st2"); copy_dir(&p, &q); let f = newest(&q, "0", ".g6"); let len = std::fs::metadata(&f).unwrap().len(); std::fs::OpenOptions::new().write(true).open(&f).unwrap().set_len(len / 2).unwrap(); eprintln!("truncated: {:?}", GlacierStorage::open(&q).err().map(|e| e.to_string())); let q = dir("st3"); copy_dir(&p, &q); std::fs::remove_file(newest(&q, "0", ".g6")).unwrap(); eprintln!("missing: {:?}", GlacierStorage::open(&q).err().map(|e| e.to_string())); }
#[test] fn two_openers_on_one_store() { let _serial = serial_read(); let p = seeded("double", 3); let a = GlacierStorage::open(&p).unwrap(); let b = GlacierStorage::open(&p); eprintln!("second opener: {:?}", b.as_ref().err().map(|e| e.to_string())); assert!(b.is_err(), "a second handle on an open store must be refused"); drop(a); assert!(GlacierStorage::open(&p).is_ok(), "the lock goes with the handle"); }
#[test] fn second_process_is_refused() { let _serial = serial_write(); let p = seeded("double2", 3); let _held = GlacierStorage::open(&p).unwrap(); let exe = std::env::current_exe().unwrap(); let out = Command::new(&exe).args(["child_opener", "--exact", "--nocapture"]).env("G6_OPEN", &p).output().unwrap(); assert!(String::from_utf8_lossy(&out.stdout).contains("REFUSED"), "{}", String::from_utf8_lossy(&out.stdout)); }
#[test] fn child_opener() { let Ok(path) = std::env::var("G6_OPEN") else { return }; println!("{}", if GlacierStorage::open(&path).is_err() { "REFUSED" } else { "OPENED" }); }
#[test] fn torn_wal_every_cut_point() { let _serial = serial_read(); let p = seeded("tear", 6); let wal = newest(&p, "wal-", ".log"); let full = std::fs::read(&wal).unwrap(); let mut worst = 0; for cut in (0..full.len()).step_by(7) { let q = dir("tear2"); copy_dir(&p, &q); std::fs::write(newest(&q, "wal-", ".log"), &full[..cut]).unwrap(); let t = check_atomic(&q); worst = worst.max(t); let _ = std::fs::remove_dir_all(&q); } eprintln!("every cut point recovers a consistent prefix (max commits {worst})"); }
fn check_atomic(q: &Path) -> u64 { let s = GlacierStorage::open(q).unwrap_or_else(|e| panic!("open failed after torn log: {e}")); let t = tags(&s).unwrap(); for (k, n) in &t { assert_eq!(*n, 5, "tag {k}"); } let m = t.keys().max().copied().unwrap_or(0); assert_eq!(t.len() as u64, m); m }
fn copy_dir(a: &Path, b: &Path) { std::fs::create_dir_all(b).unwrap(); for e in std::fs::read_dir(a).unwrap().flatten() { if e.path().is_file() { std::fs::copy(e.path(), b.join(e.file_name())).unwrap(); } } }
#[test] fn leftovers_from_a_crashed_flush() { let _serial = serial_read(); let p = seeded("leftover", 10); std::fs::write(p.join("00000099.seg"), b"garbage").unwrap(); std::fs::write(p.join("MANIFEST.tmp"), b"half").unwrap(); std::fs::write(p.join("wal-00000500.log"), b"zz").unwrap(); let r = GlacierStorage::open(&p); eprintln!("open with leftovers: {:?}", r.as_ref().err().map(|e| e.to_string())); if let Ok(s) = r { eprintln!("tags {}", tags(&s).unwrap().len()); } eprintln!("files: {:?}", std::fs::read_dir(&p).unwrap().flatten().map(|e| e.file_name()).collect::<Vec<_>>()); }

#[test] fn segments_are_named_in_hex_and_legacy_names_still_open() { let _serial = serial_read(); let p = seeded("hexname", 10); { let s = GlacierStorage::open(&p).unwrap(); s.checkpoint().unwrap(); } let seg = newest(&p, "0", ".g6"); let name = seg.file_name().unwrap().to_str().unwrap().to_owned(); assert_eq!(name.len(), 16 + 3, "{name}"); let stem = name.trim_end_matches(".g6"); assert!(stem.chars().all(|c| c.is_ascii_hexdigit())); /* Stores written before `.g6` (hexadecimal `.seg`), and before the hexadecimal naming (decimal, 8 digits). */ let mut current = seg.clone(); for legacy in [p.join(format!("{stem}.seg")), p.join(format!("{:08}.seg", u64::from_str_radix(stem, 16).unwrap()))] { std::fs::rename(&current, &legacy).unwrap(); let s = GlacierStorage::open(&p).unwrap(); assert_eq!(tags(&s).unwrap().len(), 10); drop(s); assert!(legacy.exists(), "a referenced legacy segment must be kept: {legacy:?}"); current = legacy; } }
