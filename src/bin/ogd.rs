#![cfg_attr(rustfmt, rustfmt_skip)]
// The name, description and home page of this daemon, in its version and help.
const NAME: &str = "ogd";
const DESCRIPTION: &str = "the openglacier Core daemon";
const HOMEPAGE: &str = "https://github.com/openglacier/core";
const CODENAME: &str = "antaa palaa";

fn help() -> String {
    let worker = if cfg!(feature = "native-import") { format!("\n       {NAME} data-worker                     the native data worker, started by the Core for data.import") } else { String::new() };
    format!(
        "{NAME} {version} ({CODENAME}): {DESCRIPTION}\n\n\
         usage: {NAME}                                 start the daemon, configured by its environment (OGD_*)\n       \
         {NAME} verify-glacier <store-directory>\n       \
         {NAME} restore-backup <backup.ogb> <new-store-directory>{worker}\n       \
         {NAME} --version                       print the version\n       \
         {NAME} --help                          print this help\n\n\
         Documentation: {HOMEPAGE}",
        version = env!("CARGO_PKG_VERSION"),
    )
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
extern "C" {
    fn malloc_trim(pad: usize) -> i32;
    fn mallopt(param: i32, value: i32) -> i32;
}
#[cfg(all(feature = "native-import", target_os = "linux"))]
extern "C" {
    fn setrlimit(resource: i32, limit: *const [u64; 2]) -> i32;
}

/// Bounds the data worker's process: its address space (`OG_DATA_WORKER_MEMORY`, bytes, default 4 GiB) and its
/// processor time (`OG_DATA_WORKER_CPU_SECONDS`, default one hour). Past them the worker alone stops, and the Core
/// answers the import with an error.
#[cfg(feature = "native-import")]
fn limit_data_worker() {
    #[cfg(target_os = "linux")]
    for (resource, variable, default) in [(9, "OG_DATA_WORKER_MEMORY", 4u64 << 30), (0, "OG_DATA_WORKER_CPU_SECONDS", 3600)] {
        let value = og_core::helpers::env_text(variable).and_then(|value| value.parse().ok()).unwrap_or(default);
        // SAFETY: setrlimit reads the two limits (soft, hard) given; 9 is RLIMIT_AS and 0 RLIMIT_CPU on Linux.
        unsafe {
            setrlimit(resource, &[value, value]);
        }
    }
}

fn main() -> std::process::ExitCode {
    // Options first: a mistyped one must not start a daemon.
    match std::env::args().nth(1).as_deref() {
        None | Some("verify-glacier" | "restore-backup") => {}
        Some("data-worker") if cfg!(feature = "native-import") => {}
        Some("--version" | "-V" | "-v" | "-version" | "version") => {
            println!("{NAME} {} ({CODENAME})", env!("CARGO_PKG_VERSION"));
            return std::process::ExitCode::SUCCESS;
        }
        Some("--help" | "-h" | "-help" | "-?" | "help") => {
            println!("{}", help());
            return std::process::ExitCode::SUCCESS;
        }
        Some(other) => {
            eprintln!("{NAME}: unknown argument {other:?}; see {NAME} --help");
            return std::process::ExitCode::from(2);
        }
    }
    // The native data worker, started by the Core for data.import (the default when compiled; OG_DATA_WORKER=python for the Python one): a process of its own.
    #[cfg(feature = "native-import")]
    if std::env::args().nth(1).as_deref() == Some("data-worker") {
        limit_data_worker();
        return std::process::ExitCode::from(u8::try_from(og_core::import::main()).unwrap_or(1));
    }
    // glibc gives each allocating thread an arena and keeps there what it
    // freed: the memory governor bounds the arenas to the query threads and,
    // under process pressure, returns their free memory to the system.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    og_core::memory::set_allocator(og_core::memory::Allocator {
        // SAFETY: malloc_trim only returns free heap pages to the system.
        release: || unsafe {
            malloc_trim(0);
        },
        // SAFETY: M_ARENA_MAX (-8) only bounds the arenas created from now on;
        // an explicit MALLOC_ARENA_MAX wins.
        limit_arenas: |arenas| {
            if std::env::var_os("MALLOC_ARENA_MAX").is_none() {
                unsafe {
                    mallopt(-8, i32::try_from(arenas).unwrap_or(i32::MAX));
                }
            }
        },
    });
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("verify-glacier") {
        let Some(path) = args.get(2) else {
            eprintln!("usage: {NAME} verify-glacier <store-directory>");
            return std::process::ExitCode::FAILURE;
        };
        // Opening checks the manifest and the log; verify, every segment chunk.
        let verified =
            og_core::storage::GlacierStorage::open(path).and_then(|store| store.verify());
        return match verified {
            Ok(report) => {
                eprintln!(
                    "generation {}: {} segments, {} chunks, {} bytes checked",
                    report.generation, report.segments, report.chunks, report.bytes
                );
                for damage in &report.damaged {
                    eprintln!("damaged: {damage}");
                }
                if report.damaged.is_empty() {
                    std::process::ExitCode::SUCCESS
                } else {
                    std::process::ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("openglacier: {error}");
                std::process::ExitCode::FAILURE
            }
        };
    }
    if args.get(1).map(String::as_str) == Some("restore-backup") {
        let (Some(backup), Some(store)) = (args.get(2), args.get(3)) else {
            eprintln!("usage: {NAME} restore-backup <backup.ogb> <new-store-directory>");
            return std::process::ExitCode::FAILURE;
        };
        // A backup with history restores its commits with their generations, times and authors; another, its state.
        let restored = og_core::storage::GlacierStorage::open(store).map_err(og_core::backup::BackupError::from).and_then(|storage| {
            match og_core::backup::inspect(std::path::Path::new(backup))?.chain.and_then(|chain| chain.history) {
                Some(_) => og_core::backup::restore_history(&storage, std::path::Path::new(backup)),
                None => og_core::backup::restore(&storage, std::path::Path::new(backup), false),
            }
        });
        return match restored {
            Ok(summary) => {
                eprintln!("{} collections, {} documents, {} removed, {} events", summary.collections, summary.documents, summary.removed, summary.events);
                std::process::ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("openglacier: {error}");
                std::process::ExitCode::FAILURE
            }
        };
    }
    match og_core::daemon::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("openglacier: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
