#![cfg_attr(rustfmt, rustfmt_skip)]
#![cfg(feature = "native-import")]
//! `ogd data-worker`, the native data worker, as the Core starts it: a request on stdin, the answer on stdout, and a
//! failure that stops the worker alone (an error line and status 1, or its memory limit).
use std::io::Write;
use std::process::{Command, Stdio};

fn worker(request: &serde_json::Value, environment: &[(&str, &str)]) -> (Option<i32>, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ogd")).arg("data-worker").envs(environment.iter().copied()).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    child.stdin.take().unwrap().write_all(request.to_string().as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    (output.status.code(), String::from_utf8_lossy(&output.stdout).into_owned())
}

fn file(name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("og-native-import-{}-{name}", std::process::id())); std::fs::write(&path, bytes).unwrap(); path
}

#[test] fn the_worker_answers_an_import_batch_by_batch_then_its_summary() { let rows: String = (0..12_000).map(|index| format!("{:02}-2025;C{index};{index},5\n", index % 12 + 1)).collect(); let path = file("rows.csv", format!("Mois;Client;Montant\n{rows}").as_bytes()); let mapping = serde_json::json!({"fields": [{"source": "Mois", "target": "date", "transform": "month"}, {"source": "Montant", "target": "amount", "transform": "number"}]}); let (status, out) = worker(&serde_json::json!({"operation": "import", "file": path, "mapping": mapping, "batch": true}), &[]); let lines: Vec<serde_json::Value> = out.lines().map(|line| serde_json::from_str(line).unwrap()).collect(); assert_eq!((status, lines.len(), lines[0]["documents"].as_array().unwrap().len(), lines[2]["documents"][1999]["amount"].clone()), (Some(0), 4, 5000, serde_json::json!(11_999.5))); assert_eq!(lines[3], serde_json::json!({"status": "transformed", "rowCount": 12_000, "warnings": []})); std::fs::remove_file(path).unwrap(); }

// Past its memory limit the worker stops without a summary: the Core answers the import with worker.failed.
#[test] fn a_file_the_worker_cannot_read_is_an_error_and_its_limits_stop_it_alone() { let broken = file("broken.xlsx", b"PK\x03\x04 not a workbook"); let (status, out) = worker(&serde_json::json!({"operation": "analyze", "file": broken}), &[]); let error: serde_json::Value = serde_json::from_str(out.trim()).unwrap(); assert_eq!((status, error["status"].clone(), error["error"]["code"].clone()), (Some(1), serde_json::json!("error"), serde_json::json!("worker.failed"))); let long = file("long.csv", format!("a;b\n1;{}\n", "x".repeat(48 << 20)).as_bytes()); let (status, out) = worker(&serde_json::json!({"operation": "analyze", "file": long}), &[("OG_DATA_WORKER_MEMORY", "100000000")]); assert!(status != Some(0) && !out.contains("\"analyzed\""), "{status:?} {out}"); std::fs::remove_file(broken).unwrap(); std::fs::remove_file(long).unwrap(); }
