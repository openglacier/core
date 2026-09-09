use std::{
    env, fs,
    path::{Path, PathBuf},
};

const LLAMA_SYS_CRATE: &str = "llama-cpp-sys-2-0.1.156";

fn main() {
    println!("cargo:rerun-if-changed=src/chat_bridge.cpp");
    println!("cargo:rerun-if-env-changed=OGD_LLAMA_CPP_SYS_SOURCE");

    let sys_root = find_llama_sys_source().unwrap_or_else(|| {
        panic!(
            "unable to locate {LLAMA_SYS_CRATE} sources required by the native common/chat bridge; \
             set OGD_LLAMA_CPP_SYS_SOURCE to the llama-cpp-sys-2 0.1.156 crate directory"
        )
    });
    let llama = sys_root.join("llama.cpp");

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .file("src/chat_bridge.cpp")
        .include(&llama)
        .include(llama.join("common"))
        .include(llama.join("include"))
        .include(llama.join("ggml/include"))
        .include(llama.join("vendor"))
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-Wno-unused-function")
        .pic(true)
        .warnings(false);

    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        build.flag("/std:c++17");
    }

    build.compile("og_llama_common_chat_bridge");
}

fn find_llama_sys_source() -> Option<PathBuf> {
    if let Some(path) = env::var_os("OGD_LLAMA_CPP_SYS_SOURCE") {
        let path = PathBuf::from(path);
        if is_llama_sys_source(&path) {
            return Some(path);
        }
    }

    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR")?);
    for candidate in [
        manifest.join("..").join("vendor").join(LLAMA_SYS_CRATE),
        manifest.join("..").join("vendor").join("llama-cpp-sys-2"),
    ] {
        if is_llama_sys_source(&candidate) {
            return Some(candidate);
        }
    }

    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))?;

    find_named_dir(&cargo_home.join("registry/src"), LLAMA_SYS_CRATE, 2)
        .or_else(|| find_named_dir(&cargo_home.join("git/checkouts"), "llama-cpp-sys-2", 4))
}

fn find_named_dir(root: &Path, name: &str, depth: usize) -> Option<PathBuf> {
    if depth == 0 || !root.is_dir() {
        return None;
    }
    for entry in fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if entry.file_name() == name && is_llama_sys_source(&path) {
            return Some(path);
        }
        if let Some(found) = find_named_dir(&path, name, depth - 1) {
            return Some(found);
        }
    }
    None
}

fn is_llama_sys_source(path: &Path) -> bool {
    path.join("llama.cpp/common/chat.h").is_file()
        && path.join("llama.cpp/common/CMakeLists.txt").is_file()
}
