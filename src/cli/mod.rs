//! OpenGlacier command-line client runtime.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    env,
    error::Error,
    io::{self, BufRead, BufReader, BufWriter, IsTerminal, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{ atomic::{AtomicBool, Ordering}, mpsc, Arc, },
    thread,
    time::Duration,
};

use crate::{
    access::identity_file::{self, IdentityCredential},
    helpers::decode_base64,
    operation::{ operation_by_name, OperationRequest, TransportKind, AUTH_BEGIN, AUTH_COMPLETE, AUTH_ENROLL_BEGIN, AUTH_ENROLL_COMPLETE, QUERY_EXECUTE, },
    protocol::{
        decode_stream_response, encode_message, ensure_payload_size, MessageKind, RequestId,
        StreamResponse, LENGTH_PREFIX_BYTES, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
    },
};
use rustyline::{error::ReadlineError, history::DefaultHistory, CompletionType, Editor, ExternalPrinter};
use serde_json::{json, Map, Value};

mod complete;
mod import;
mod table;
#[cfg(feature = "cli-tui")]
mod screen;
#[cfg(feature = "cli-tui")]
mod tui;

pub type CliResult<T> = std::result::Result<T, Box<dyn Error>>;
type Result<T> = CliResult<T>;
const NAME: &str = "ogcli";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_ADDRESS: &str = "127.0.0.1:7878";

pub fn run() -> CliResult<ExitCode> {
    let (mut config, action) = Args::parse(env::args().skip(1))?;
    match action {
        Action::Help => print_help(),
        Action::Version => println!("{NAME} {VERSION}"),
        Action::Run(Some(line)) => return run_line(&mut config, line),
        Action::Run(None) => {
            #[cfg(feature = "cli-tui")]
            if config.tui {
                return tui::run(config);
            }
            return repl(config);
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn run_line(config: &mut Config, line: String) -> Result<ExitCode> {
    let mut client = Client::connect(config)?;
    let mut exit = ExitCode::SUCCESS;
    handle_line(config, &mut client, line.trim(), &mut exit)?;
    Ok(exit)
}

/// Where a line's output goes: the text, and whether it is an error or progress line.
type Emit<'a> = dyn FnMut(String, bool) -> Result<()> + 'a;

/// Client-side lines that change the session rather than reach ogd.
enum Local { Quit, Help, Reconnect, Format(Output) }

fn local_command(line: &str) -> Result<Option<Local>> {
    Ok(Some(match line {
        "exit" | "quit" | ".exit" | ".quit" => Local::Quit,
        "help" | ".help" => Local::Help,
        "reconnect" | ".reconnect" => Local::Reconnect,
        _ => match line.strip_prefix("format") {
            Some(mode) if mode.is_empty() || mode.starts_with(' ') => Local::Format(match mode.trim() {
                "table" => Output::Table, "pretty" => Output::Pretty, "compact" => Output::Compact, "quiet" => Output::Quiet,
                other => return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("format expects table, pretty, compact or quiet, found {other:?}")).into()),
            }),
            _ => return Ok(None),
        },
    }))
}

fn repl(mut config: Config) -> Result<ExitCode> {
    let mut exit = ExitCode::SUCCESS;

    if !io::stdin().is_terminal() {
        let mut client = Client::connect(&config)?;
        for line in io::stdin().lock().lines() {
            if handle_line(&mut config, &mut client, line?.trim(), &mut exit)? {
                break;
            }
        }
        return Ok(exit);
    }

    config.prepare_for_background()?;
    // Reconnect once after caching an interactive identity password so every
    // background request can authenticate without touching the input terminal.
    let client = Client::connect(&config)?;
    drop(client);

    println!("Connected to {}. Type help for commands; Tab completes.", config.address);
    println!("Interactive requests run concurrently; output is printed above the prompt.");

    let completer = Arc::new(complete::Completer::new(config.clone()));
    let mut editor: Editor<ReplHelper, DefaultHistory> = Editor::with_config(rustyline::Config::builder().completion_type(CompletionType::List).build())?;
    editor.set_helper(Some(ReplHelper(Arc::clone(&completer))));
    let mut external_printer = editor.create_external_printer()?;
    let (output_tx, output_rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        while let Ok(mut message) = output_rx.recv() {
            while let Ok(next) = output_rx.try_recv() {
                message.push('\n');
                message.push_str(&next);
            }
            if external_printer.print(message).is_err() {
                break;
            }
        }
    });

    let failed = Arc::new(AtomicBool::new(false));
    let mut next_job_id = 1_u64;
    let exit_code = |failed: &AtomicBool| if failed.load(Ordering::Relaxed) { ExitCode::FAILURE } else { ExitCode::SUCCESS };

    loop {
        match editor.readline("openglacier> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line);
                match local_command(line) {
                    Ok(Some(Local::Quit)) => return Ok(exit_code(&failed)),
                    Ok(Some(Local::Help)) => { print_repl_help(); continue; }
                    Ok(Some(Local::Format(output))) => { config.output = output; println!("format: {}", output.label()); continue; }
                    Ok(Some(Local::Reconnect)) => {
                        completer.invalidate();
                        match Client::connect(&config) {
                            Ok(_) => println!("Connection to {} verified.", config.address),
                            Err(error) => {
                                eprintln!("{NAME}: {error}");
                                failed.store(true, Ordering::Relaxed);
                            }
                        }
                        continue;
                    }
                    Err(error) => { eprintln!("{NAME}: {error}"); continue; }
                    Ok(None) => {}
                }

                let job_id = next_job_id;
                next_job_id = next_job_id.wrapping_add(1);
                spawn_background_line(
                    config.clone(),
                    line.to_owned(),
                    job_id,
                    output_tx.clone(),
                    Arc::clone(&failed),
                );
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => {
                println!();
                return Ok(exit_code(&failed));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Plugs the shared completer into rustyline; hints, highlighting and
/// validation keep their defaults.
struct ReplHelper(Arc<complete::Completer>);

impl rustyline::completion::Completer for ReplHelper {
    type Candidate = String;
    fn complete(&self, line: &str, pos: usize, _: &rustyline::Context<'_>) -> rustyline::Result<(usize, Vec<String>)> { Ok(self.0.complete(line, pos)) }
}
impl rustyline::hint::Hinter for ReplHelper { type Hint = String; }
impl rustyline::highlight::Highlighter for ReplHelper {}
impl rustyline::validate::Validator for ReplHelper {}
impl rustyline::Helper for ReplHelper {}

fn spawn_background_line( config: Config, line: String, job_id: u64, output: mpsc::Sender<String>, failed: Arc<AtomicBool>, ) {
    thread::spawn(move || {
        let _ = output.send(format!("[{job_id}] started"));
        let result = execute_background_line(&config, &line, |message| {
            // A table starts on its own line so the prefix does not shift its header.
            let separator = if message.contains('\n') { '\n' } else { ' ' };
            output
                .send(format!("[{job_id}]{separator}{message}"))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "REPL output closed").into())
        });
        match result {
            Ok(code) if code == ExitCode::SUCCESS => {
                let _ = output.send(format!("[{job_id}] done"));
            }
            Ok(_) => {
                failed.store(true, Ordering::Relaxed);
                let _ = output.send(format!("[{job_id}] failed"));
            }
            Err(error) => {
                failed.store(true, Ordering::Relaxed);
                let _ = output.send(format!("[{job_id}] {NAME}: {error}"));
            }
        }
    });
}

fn execute_background_line( config: &Config, line: &str, mut emit: impl FnMut(String) -> Result<()>, ) -> Result<ExitCode> {
    let mut client = Client::connect(config)?;
    execute_line(config, &mut client, line, false, &mut |message, _| emit(message))
}

/// Runs one line that reaches ogd: `import …`, `.OPERATION [JSON]` or a query.
fn execute_line(config: &Config, client: &mut Client, line: &str, collect: bool, emit: &mut Emit<'_>) -> Result<ExitCode> {
    if let Some(arguments) = line.strip_prefix("import").filter(|rest| rest.is_empty() || rest.starts_with(' ')) {
        let arguments = import::parse_args(arguments)?;
        let summary = import::run(
            &arguments,
            |query| client.query_rows(query, &Map::new(), |_| Ok(()))?.map_err(|message| io::Error::other(message).into()),
            |progress| emit(progress, true),
        )?;
        emit(render_json(&summary, if config.output == Output::Pretty { Output::Pretty } else { Output::Compact })?, false)?;
        return Ok(ExitCode::SUCCESS);
    }
    if let Some(operation) = line.strip_prefix('.') {
        let (op, data) = parse_operation(operation)?;
        if operation_by_name(op).is_some_and(|descriptor| descriptor.transport == TransportKind::MessageStream) {
            let output = if config.output == Output::Table { Output::Compact } else { config.output };
            return client.stream(op, data, output, false, emit);
        }
        let data = client.operation(op, data)?;
        let table = (config.output == Output::Table).then(|| table::render_value(&data)).flatten();
        emit(match table { Some(table) => table, None => render_json(&data, if config.output == Output::Table { Output::Pretty } else { config.output })? }, false)?;
        return Ok(ExitCode::SUCCESS);
    }
    client.stream(QUERY_EXECUTE, query_payload(line, &config.params), config.output, collect, emit)
}

/// One line of a one-shot or piped run: output goes to stdout through one
/// buffer, errors and progress to stderr. Returns whether to stop.
fn handle_line( config: &mut Config, client: &mut Client, line: &str, exit: &mut ExitCode, ) -> Result<bool> {
    if line.is_empty() {
        return Ok(false);
    }
    match local_command(line)? {
        Some(Local::Quit) => return Ok(true),
        Some(Local::Help) => { print_repl_help(); return Ok(false); }
        Some(Local::Reconnect) => { *client = Client::connect(config)?; return Ok(false); }
        Some(Local::Format(output)) => { config.output = output; return Ok(false); }
        None => {}
    }
    let mut stdout = BufWriter::with_capacity(1 << 16, io::stdout().lock());
    let mut emit = |message: String, error: bool| -> Result<()> {
        if error { eprintln!("{message}") } else { writeln!(stdout, "{message}")? }
        Ok(())
    };
    // An import is never replayed: a retry would write its first rows twice.
    let result = if line.starts_with("import ") {
        execute_line(config, client, line, true, &mut emit)
    } else {
        retry_closed(config, client, |client| execute_line(config, client, line, true, &mut emit))
    };
    stdout.flush()?;
    if result? != ExitCode::SUCCESS {
        *exit = ExitCode::FAILURE;
    }
    Ok(false)
}

fn retry_closed<T>(config: &Config, client: &mut Client, mut f: impl FnMut(&mut Client) -> Result<T>) -> Result<T> {
    match f(client) {
        Err(error) if is_closed(error.as_ref()) => {
            *client = Client::connect(config)?;
            f(client)
        }
        result => result,
    }
}

fn is_closed(error: &(dyn Error + 'static)) -> bool { error .downcast_ref::<io::Error>() .is_some_and(|error| error.kind() == io::ErrorKind::UnexpectedEof) }

fn parse_operation(input: &str) -> Result<(&str, Value)> {
    let input = input.trim();
    let split = input.find(char::is_whitespace).unwrap_or(input.len());
    let op = &input[..split];
    if op.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "missing operation name").into());
    }
    let payload = input[split..].trim();
    Ok((op, if payload.is_empty() { json!({}) } else { serde_json::from_str(payload)? }))
}

/// Responses are read through a buffer: a result streams many small frames.
struct Client { stream: BufReader<TcpStream>, next_id: u64, }

impl Client {
    fn connect(config: &Config) -> Result<Self> {
        let address = config
            .address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "address resolved to no endpoint"))?;
        let stream = TcpStream::connect_timeout(&address, config.connect_timeout)?;
        stream.set_read_timeout(config.read_timeout)?;
        stream.set_write_timeout(Some(config.write_timeout))?;
        stream.set_nodelay(true)?;
        let mut client = Self { stream: BufReader::with_capacity(1 << 16, stream), next_id: 1 };
        if let Some(path) = &config.identity {
            let identity = identity_file::load(path, &config.password()?)?;
            match client.authenticate(&identity) {
                // As og-hub does: an identity or device the node does not
                // know yet is enrolled, then authenticated.
                Err(error) if config.enroll && ["auth.device_not_found", "auth.identity_not_found", "authorization.invalid_principal"].iter().any(|code| error.to_string().starts_with(code)) => {
                    client.enroll(&identity)?;
                    client.authenticate(&identity)?;
                }
                result => result?,
            }
        }
        Ok(client)
    }

    fn send(&mut self, op: &str, data: Value) -> Result<u64> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let message = encode_message(
            &OperationRequest::new(id, op, data),
            MessageKind::Request,
            MAX_REQUEST_BYTES,
        )?;
        self.stream.get_mut().write_all(&message)?;
        Ok(id)
    }

    fn receive(&mut self, message: &mut Vec<u8>) -> Result<()> {
        loop {
            let mut header = [0; LENGTH_PREFIX_BYTES];
            self.stream.read_exact(&mut header)?;
            let len = u32::from_be_bytes(header) as usize;
            ensure_payload_size(MessageKind::Response, len, MAX_RESPONSE_BYTES)?;
            message.resize(len, 0);
            self.stream.read_exact(message)?;
            // Only the kind is read: the data is decoded once, by the caller.
            #[derive(serde::Deserialize)]
            struct Envelope { #[serde(default)] kind: String }
            if rmp_serde::from_slice::<Envelope>(message)?.kind != "event" {
                return Ok(());
            }
        }
    }

    fn operation(&mut self, op: &str, data: Value) -> Result<Value> {
        let expected = self.send(op, data)?;
        let mut message = Vec::new();
        self.receive(&mut message)?;
        let response: Value = rmp_serde::from_slice(&message)?;
        check_id(expected, response.get("id"))?;
        if let Some(error) = response.get("error") {
            let code = error.get("code").and_then(Value::as_str).unwrap_or("operation.error");
            let message = error.get("message").and_then(Value::as_str).unwrap_or("operation rejected");
            return Err(io::Error::other(format!("{code}: {message}")).into());
        }
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    }

    /// Sends `op` and hands each partial frame to `row` (an `Err` stops the
    /// stream): the statistics, or the server's error as `code: message`.
    fn stream_rows(&mut self, op: &str, data: Value, mut row: impl FnMut(Value) -> Result<()>) -> Result<std::result::Result<Value, String>> {
        let expected = self.send(op, data)?;
        let mut message = Vec::with_capacity(4096);
        loop {
            self.receive(&mut message)?;
            match decode_stream_response(&message)? {
                StreamResponse::Partial { id, data, .. } => { check_request_id(expected, &id)?; row(data)?; }
                StreamResponse::Complete { id, statistics, .. } => { check_request_id(expected, &id)?; return Ok(Ok(serde_json::to_value(statistics)?)); }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id.as_ref() { check_request_id(expected, id)?; }
                    return Ok(Err(match id { Some(id) => format!("request {id}: {}: {}", error.code, error.message), None => format!("{}: {}", error.code, error.message) }));
                }
            }
        }
    }

    fn query_rows(&mut self, query: &str, params: &Map<String, Value>, row: impl FnMut(Value) -> Result<()>) -> Result<std::result::Result<Value, String>> {
        if query.trim().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "query cannot be empty").into());
        }
        self.stream_rows(QUERY_EXECUTE, query_payload(query, params), row)
    }

    /// Runs a query or a streaming operation and writes its output to `emit`
    /// (`true` marks an error line). `collect` (one-shot and piped runs) gives
    /// one `{"documents", "statistics"}` value; otherwise rows go out as they
    /// come, then `{"complete", "statistics"}`. A table waits for the last row.
    fn stream(&mut self, op: &str, data: Value, output: Output, collect: bool, emit: &mut Emit<'_>) -> Result<ExitCode> {
        let started = std::time::Instant::now();
        let (mut documents, mut rows) = (Vec::new(), table::Rows::default());
        let table = output == Output::Table && op == QUERY_EXECUTE;
        let outcome = self.stream_rows(op, data, |data| {
            if table { rows.push(data); Ok(()) }
            else if collect && output != Output::Quiet { documents.push(data); Ok(()) }
            else { emit(serde_json::to_string(&data)?, false) }
        })?;
        match outcome {
            Ok(statistics) => {
                match output {
                    Output::Quiet => {}
                    _ if table => emit(rows.render(&statistics, started.elapsed().as_secs_f64() * 1e3), false)?,
                    _ if collect => emit(render_json(&json!({"documents": documents, "statistics": statistics}), output)?, false)?,
                    _ => emit(render_json(&json!({"complete": true, "statistics": statistics}), output)?, false)?,
                }
                Ok(ExitCode::SUCCESS)
            }
            Err(message) => { emit(format!("{NAME}: {message}"), true)?; Ok(ExitCode::FAILURE) }
        }
    }

    /// Enrolls `identity` with one device holding the same key, under the
    /// node's enrollment mode (`OGCLI_ENROLLMENT_TOKEN` for a token).
    fn enroll(&mut self, identity: &IdentityCredential) -> Result<()> {
        let mut begin = json!({
            "identityId": identity.identity_id,
            "identityPublicKey": identity.public_key,
            "deviceId": identity.device_id,
            "devicePublicKey": identity.public_key,
        });
        if let Ok(token) = env::var("OGCLI_ENROLLMENT_TOKEN") {
            begin["token"] = Value::String(token);
        }
        let challenge = self.operation(AUTH_ENROLL_BEGIN, begin)?;
        let (Some(challenge_id), Some(bytes)) = (challenge["challengeId"].as_str(), challenge["challenge"].as_str()) else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid enrollment response").into());
        };
        self.operation(
            AUTH_ENROLL_COMPLETE,
            json!({"challengeId": challenge_id, "signature": identity.sign_base64(&decode_base64(bytes)?)}),
        )?;
        Ok(())
    }

    fn authenticate(&mut self, identity: &IdentityCredential) -> Result<()> {
        let begin = self.operation(
            AUTH_BEGIN,
            json!({"identityId": identity.identity_id, "deviceId": identity.device_id}),
        )?;
        let challenge_id = begin["challengeId"]
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid auth response"))?;
        let challenge = begin["challenge"]
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid auth response"))?;
        self.operation(
            AUTH_COMPLETE,
            json!({
                "challengeId": challenge_id,
                "signature": identity.sign_base64(&decode_base64(challenge)?),
            }),
        )?;
        Ok(())
    }
}

fn check_id(expected: u64, id: Option<&Value>) -> Result<()> {
    let received = id
        .and_then(Value::as_u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "response has no numeric id"))?;
    if received == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("response id mismatch: expected {expected}, received {received}"),
        )
        .into())
    }
}

fn check_request_id(expected: u64, received: &RequestId) -> Result<()> {
    if *received == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("response id mismatch: expected {expected}, received {received}"),
        )
        .into())
    }
}

fn render_json(value: &Value, output: Output) -> Result<String> {
    Ok(match output {
        Output::Pretty | Output::Table => serde_json::to_string_pretty(value)?,
        Output::Compact | Output::Quiet => serde_json::to_string(value)?,
    })
}

#[derive(Clone)]
struct Config {
    address: String,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    write_timeout: Duration,
    output: Output,
    identity: Option<PathBuf>,
    /// Enrolls the identity (identity and device) when the node does not know it.
    enroll: bool,
    password_file: Option<PathBuf>,
    password: Option<String>,
    /// `$name` query parameters given with `--param name=value`.
    params: Map<String, Value>,
    #[cfg(feature = "cli-tui")]
    tui: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let mut identity = env::var("OGCLI_IDENTITY").ok().map(PathBuf::from);
        if identity.is_none() && Path::new("bootstrap/admin.ogid").is_file() {
            identity = Some("bootstrap/admin.ogid".into());
        }
        Ok(Self {
            address: env::var("OGD_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into()),
            connect_timeout: env_duration("OGCLI_CONNECT_TIMEOUT_MS", 5_000, false)?.unwrap(),
            read_timeout: env_duration("OGCLI_READ_TIMEOUT_MS", 0, true)?,
            write_timeout: env_duration("OGCLI_WRITE_TIMEOUT_MS", 30_000, false)?.unwrap(),
            output: Output::Pretty,
            identity,
            enroll: false,
            password_file: env::var("OGCLI_IDENTITY_PASSWORD_FILE").ok().map(PathBuf::from),
            password: env::var("OGCLI_IDENTITY_PASSWORD").ok(),
            params: Map::new(),
            #[cfg(feature = "cli-tui")]
            tui: false,
        })
    }

    fn prepare_for_background(&mut self) -> Result<()> {
        let needs_prompt = self.identity.is_some()
            && self.password_file.is_none()
            && self
                .password
                .as_deref()
                .is_none_or(|value| value.is_empty());
        if needs_prompt && io::stdin().is_terminal() {
            self.password = Some(rpassword::prompt_password("Identity password: ")?);
        }
        Ok(())
    }

    fn password(&self) -> Result<Vec<u8>> {
        if let Some(path) = &self.password_file {
            let value = std::fs::read(path)?;
            let value = value.strip_suffix(b"\n").unwrap_or(&value);
            let value = value.strip_suffix(b"\r").unwrap_or(value);
            if !value.is_empty() {
                return Ok(value.to_vec());
            }
        }
        if let Some(value) = self.password.as_deref().filter(|value| !value.is_empty()) {
            return Ok(value.as_bytes().to_vec());
        }
        if io::stdin().is_terminal() {
            return Ok(rpassword::prompt_password("Identity password: ")?.into_bytes());
        }
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "identity password required").into())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Output { Pretty, Compact, Quiet, Table, }

impl Output {
    const fn label(self) -> &'static str { match self { Self::Pretty => "pretty", Self::Compact => "compact", Self::Quiet => "quiet", Self::Table => "table" } }
}
enum Action { Help, Version, Run(Option<String>), }
struct Args;

impl Args {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<(Config, Action)> {
        let mut config = Config::from_env()?;
        let mut host = None;
        let mut port = None;
        let mut query = Vec::new();
        let mut args = arguments.into_iter();
        while let Some(arg) = args.next() {
            macro_rules! value {
                () => {
                    args.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing value for {arg}")))?
                };
            }
            match arg.as_str() {
                "-h" | "--help" => return Ok((config, Action::Help)),
                "-V" | "--version" => return Ok((config, Action::Version)),
                "--address" => config.address = value!(),
                "--host" => host = Some(value!()),
                "--port" => port = Some(value!().parse::<u16>()?),
                "--identity" => config.identity = Some(value!().into()),
                "--identity-password-file" => config.password_file = Some(value!().into()),
                "--identity-password" => config.password = Some(value!()),
                "--enroll" => config.enroll = true,
                "--connect-timeout-ms" => config.connect_timeout = duration(&value!(), false)?.unwrap(),
                "--read-timeout-ms" => config.read_timeout = duration(&value!(), true)?,
                "--write-timeout-ms" => config.write_timeout = duration(&value!(), false)?.unwrap(),
                #[cfg(feature = "cli-tui")]
                "--tui" => config.tui = true,
                #[cfg(not(feature = "cli-tui"))]
                "--tui" => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--tui requires building ogcli with the cli-tui feature",
                    )
                    .into())
                }
                "--param" => {
                    let assignment = value!();
                    let (name, raw) = assignment.split_once('=').ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("--param expects NAME=VALUE, found {assignment}"))
                    })?;
                    // JSON values (`18`, `true`, `["a"]`, `"x"`) keep their type; anything else is a string.
                    let value = serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()));
                    config.params.insert(name.to_owned(), value);
                }
                "--compact" => config.output = Output::Compact,
                "-q" | "--quiet" => config.output = Output::Quiet,
                "--table" => config.output = Output::Table,
                "--" => {
                    query.extend(args);
                    break;
                }
                _ if arg.starts_with('-') => {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("unknown option {arg}" )).into())
                }
                _ => query.push(arg),
            }
        }
        if host.is_some() || port.is_some() {
            config.address = address(&host.unwrap_or_else(|| "127.0.0.1".into()), port.unwrap_or(7878));
        }
        Ok((config, Action::Run((!query.is_empty()).then(|| query.join(" ")))))
    }
}

/// `query.execute` payload, with `params` only when some are bound.
fn query_payload(query: &str, params: &Map<String, Value>) -> Value {
    if params.is_empty() {
        json!({"query": query})
    } else {
        json!({"query": query, "params": params})
    }
}

fn duration(value: &str, zero_is_none: bool) -> Result<Option<Duration>> {
    let milliseconds = value.parse::<u64>()?;
    if milliseconds == 0 {
        if zero_is_none {
            return Ok(None);
        }
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "duration must be greater than zero").into());
    }
    Ok(Some(Duration::from_millis(milliseconds)))
}

fn env_duration(name: &str, default: u64, zero_is_none: bool) -> Result<Option<Duration>> {
    duration(&crate::helpers::env_var(name)?.unwrap_or_else(|| default.to_string()), zero_is_none)
}

fn address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn print_help() {
    println!(
        "{NAME} {VERSION}\n\n\
USAGE:\n    ogcli [OPTIONS] [QUERY...]\n\n\
OPTIONS:\n\
    --address ADDR            ogd address [default: {DEFAULT_ADDRESS}]\n\
    --host HOST               Host used with --port\n\
    --port PORT               Port used with --host [default: 7878]\n\
    --identity FILE           Encrypted identity used to authenticate\n\
    --identity-password-file FILE\n\
    --identity-password PASS\n\
    --enroll                  Enrolls an unknown identity (OGCLI_ENROLLMENT_TOKEN: token)\n\
    --connect-timeout-ms MS   [default: 5000]\n\
    --read-timeout-ms MS      0 waits indefinitely [default: 0]\n\
    --write-timeout-ms MS     [default: 30000]\n\
    --tui                     Interactive tabs/tiles UI (requires cli-tui)\n\
    --param NAME=VALUE        Binds $NAME in queries; VALUE is JSON or a plain string\n\
    --compact                 Compact JSON\n\
    --quiet, -q               Documents only\n\
    --table                   Results as a table (first {} rows)\n\
    --help, -h\n\
    --version, -V\n\n\
With no QUERY, starts the REPL. Use `.OPERATION [JSON]` for raw ogd operations,\n\
`import FILE [COLLECTION]` to load a CSV or TSV file.",
        table::MAX_ROWS
    );
}

fn print_repl_help() {
    println!(
        "REPL:\n\
    .OPERATION [JSON]  Execute an ogd operation\n\
    import FILE [COLLECTION] [--sep C] [--text]\n\
                       Load a CSV/TSV file (header = field names; the collection\n\
                       defaults to the file name). Cells are typed: integers, floats,\n\
                       true/false, empty = null, text; --text keeps every cell as text.\n\
                       Each request of about 7 MiB is committed on its own.\n\
    format table|pretty|compact|quiet\n\
                       Result display (also --table, --compact, -q)\n\
    Tab                Complete commands, stages, collections, fields and paths\n\
    help, .help        Show this help\n\
    reconnect, .reconnect\n\
                       Verify the daemon connection\n\
    quit, exit         Exit (also .quit, .exit)\n\n\
Interactive requests run concurrently on independent connections.\n\
Background output is prefixed with a local [job] id and printed above the prompt.\n\
Any other non-empty line is sent as a query."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn operation_payload_is_optional() { assert_eq!(parse_operation("ping").unwrap().1, json!({})); assert_eq!(parse_operation("x {\"a\":1}").unwrap().1, json!({"a": 1})); }
    #[test] fn formats_addresses() { assert_eq!(address("127.0.0.1", 7878), "127.0.0.1:7878"); assert_eq!(address("::1", 7878), "[::1]:7878"); }
    #[test] fn read_timeout_zero_is_none() { assert_eq!(duration("0", true).unwrap(), None); assert_eq!(duration("250", true).unwrap(), Some(Duration::from_millis(250))); }
}
