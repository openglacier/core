fn main() -> std::process::ExitCode {
    match og_core::cli::run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ogcli: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
