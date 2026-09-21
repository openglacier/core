fn main() -> std::process::ExitCode {
    match og_core::daemon::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("openglacier: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
