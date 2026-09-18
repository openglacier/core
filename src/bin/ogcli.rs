//! Thin shell for ogd daemon.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    env,
    error::Error,
    io::{self, BufRead, IsTerminal, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{ atomic::{AtomicBool, Ordering}, mpsc, Arc, },
    thread,
    time::Duration,
};

use og_core::{
    access::identity_file::{self, IdentityCredential},
    helpers::decode_base64,
    operation::{ operation_by_name, OperationRequest, TransportKind, AUTH_BEGIN, AUTH_COMPLETE, QUERY_EXECUTE, },
    protocol::{
        decode_stream_response, encode_message, ensure_payload_size, MessageKind, RequestId,
        StreamResponse, LENGTH_PREFIX_BYTES, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
    },
};
use rustyline::{error::ReadlineError, DefaultEditor, ExternalPrinter};
use serde_json::{json, Value};

#[cfg(feature = "cli-tui")]
#[path = "ogcli/tui.rs"]
mod tui;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const NAME: &str = "ogcli";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_ADDRESS: &str = "127.0.0.1:7878";

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{NAME}: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode> {
    let (config, action) = Args::parse(env::args().skip(1))?;
    match action {
        Action::Help => print_help(),
        Action::Version => println!("{NAME} {VERSION}"),
        Action::Run(Some(line)) => return run_line(&config, line),
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

fn run_line(config: &Config, line: String) -> Result<ExitCode> {
    let mut client = Client::connect(config)?;
    let mut exit = ExitCode::SUCCESS;
    handle_line(config, &mut client, line.trim(), false, &mut exit)?;
    Ok(exit)
}

fn repl(mut config: Config) -> Result<ExitCode> {
    let mut exit = ExitCode::SUCCESS;

    if !io::stdin().is_terminal() {
        let mut client = Client::connect(&config)?;
        for line in io::stdin().lock().lines() {
            if handle_line(&config, &mut client, line?.trim(), false, &mut exit)? {
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

    println!("Connected to {}. Type help for commands.", config.address);
    println!("Interactive requests run concurrently; output is printed above the prompt.");

    let mut editor = DefaultEditor::new()?;
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

    loop {
        match editor.readline("openglacier> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line);
                match line {
                    "exit" | "quit" | ".exit" | ".quit" => {
                        return Ok(if failed.load(Ordering::Relaxed) {
                            ExitCode::FAILURE
                        } else {
                            ExitCode::SUCCESS
                        });
                    }
                    "help" | ".help" => {
                        print_repl_help();
                        continue;
                    }
                    "reconnect" | ".reconnect" => {
                        match Client::connect(&config) {
                            Ok(_) => println!("Connection to {} verified.", config.address),
                            Err(error) => {
                                eprintln!("{NAME}: {error}");
                                failed.store(true, Ordering::Relaxed);
                            }
                        }
                        continue;
                    }
                    _ => {}
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
                return Ok(if failed.load(Ordering::Relaxed) {
                    ExitCode::FAILURE
                } else {
                    ExitCode::SUCCESS
                });
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn spawn_background_line( config: Config, line: String, job_id: u64, output: mpsc::Sender<String>, failed: Arc<AtomicBool>, ) {
    thread::spawn(move || {
        let _ = output.send(format!("[{job_id}] started"));
        let result = execute_background_line(&config, &line, |message| {
            output
                .send(format!("[{job_id}] {message}"))
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
    if let Some(operation) = line.strip_prefix('.') {
        let (op, data) = parse_operation(operation)?;
        if operation_by_name(op).is_some_and(|descriptor| descriptor.transport == TransportKind::MessageStream) {
            client.stream_operation_emit(op, data, config.output, &mut emit)
        } else {
            let data = client.operation(op, data)?;
            emit(render_json(&data, config.output)?)?;
            Ok(ExitCode::SUCCESS)
        }
    } else {
        client.query_stream_emit(line, config.output, &mut emit)
    }
}

fn handle_line( config: &Config, client: &mut Client, line: &str, interactive: bool, exit: &mut ExitCode, ) -> Result<bool> {
    if line.is_empty() {
        return Ok(false);
    }
    match line {
        "exit" | "quit" | ".exit" | ".quit" => return Ok(true),
        "help" | ".help" => {
            print_repl_help();
            return Ok(false);
        }
        "reconnect" | ".reconnect" => {
            *client = Client::connect(config)?;
            if interactive {
                println!("Reconnected to {}.", config.address);
            }
            return Ok(false);
        }
        _ => {}
    }

    let result = retry_closed(config, client, |client| {
        if let Some(operation) = line.strip_prefix('.') {
            let (op, data) = parse_operation(operation)?;
            if operation_by_name(op).is_some_and(|descriptor| descriptor.transport == TransportKind::MessageStream) {
                client.stream_operation(op, data, config.output)
            } else {
                let data = client.operation(op, data)?;
                print_json(&data, config.output)?;
                Ok(ExitCode::SUCCESS)
            }
        } else {
            client.query(line, config.output)
        }
    })?;
    if result != ExitCode::SUCCESS {
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

struct Client { stream: TcpStream, next_id: u64, }

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
        let mut client = Self { stream, next_id: 1 };
        if let Some(path) = &config.identity {
            let identity = identity_file::load(path, &config.password()?)?;
            client.authenticate(&identity)?;
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
        self.stream.write_all(&message)?;
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
            let envelope: Value = rmp_serde::from_slice(message)?;
            if envelope.get("kind").and_then(Value::as_str) != Some("event") {
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

    fn stream_operation(&mut self, op: &str, data: Value, output: Output) -> Result<ExitCode> {
        let expected = self.send(op, data)?;
        let mut message = Vec::with_capacity(4096);
        loop {
            self.receive(&mut message)?;
            match decode_stream_response(&message)? {
                StreamResponse::Partial { id, data, .. } => {
                    check_request_id(expected, &id)?;
                    print_json(&data, if output == Output::Pretty { Output::Compact } else { output })?;
                }
                StreamResponse::Complete { id, statistics, .. } => {
                    check_request_id(expected, &id)?;
                    if output != Output::Quiet {
                        print_json(&json!({"complete": true, "statistics": statistics}), output)?;
                    }
                    return Ok(ExitCode::SUCCESS);
                }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id.as_ref() {
                        check_request_id(expected, id)?;
                    }
                    match id {
                        Some(id) => eprintln!("{NAME}: request {id}: {}: {}", error.code, error.message),
                        None => eprintln!("{NAME}: {}: {}", error.code, error.message),
                    }
                    return Ok(ExitCode::FAILURE);
                }
            }
        }
    }

    fn stream_operation_emit( &mut self, op: &str, data: Value, output: Output, emit: &mut impl FnMut(String) -> Result<()>, ) -> Result<ExitCode> {
        let expected = self.send(op, data)?;
        let mut message = Vec::with_capacity(4096);
        loop {
            self.receive(&mut message)?;
            match decode_stream_response(&message)? {
                StreamResponse::Partial { id, data, .. } => {
                    check_request_id(expected, &id)?;
                    let partial_output = if output == Output::Pretty {
                        Output::Compact
                    } else {
                        output
                    };
                    emit(render_json(&data, partial_output)?)?;
                }
                StreamResponse::Complete { id, statistics, .. } => {
                    check_request_id(expected, &id)?;
                    if output != Output::Quiet {
                        emit(render_json(
                            &json!({"complete": true, "statistics": statistics}),
                            output,
                        )?)?;
                    }
                    return Ok(ExitCode::SUCCESS);
                }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id.as_ref() {
                        check_request_id(expected, id)?;
                    }
                    emit(match id {
                        Some(id) => format!(
                            "{NAME}: request {id}: {}: {}",
                            error.code, error.message
                        ),
                        None => format!("{NAME}: {}: {}", error.code, error.message),
                    })?;
                    return Ok(ExitCode::FAILURE);
                }
            }
        }
    }

    fn query_stream_emit( &mut self, query: &str, output: Output, emit: &mut impl FnMut(String) -> Result<()>, ) -> Result<ExitCode> {
        if query.trim().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "query cannot be empty").into());
        }
        let expected = self.send(QUERY_EXECUTE, json!({"query": query}))?;
        let mut message = Vec::with_capacity(4096);
        loop {
            self.receive(&mut message)?;
            match decode_stream_response(&message)? {
                StreamResponse::Partial { id, data, .. } => {
                    check_request_id(expected, &id)?;
                    let partial_output = if output == Output::Pretty {
                        Output::Compact
                    } else {
                        output
                    };
                    emit(render_json(&data, partial_output)?)?;
                }
                StreamResponse::Complete { id, statistics, .. } => {
                    check_request_id(expected, &id)?;
                    if output != Output::Quiet {
                        emit(render_json(
                            &json!({"complete": true, "statistics": statistics}),
                            output,
                        )?)?;
                    }
                    return Ok(ExitCode::SUCCESS);
                }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id.as_ref() {
                        check_request_id(expected, id)?;
                    }
                    emit(match id {
                        Some(id) => format!(
                            "{NAME}: request {id}: {}: {}",
                            error.code, error.message
                        ),
                        None => format!("{NAME}: {}: {}", error.code, error.message),
                    })?;
                    return Ok(ExitCode::FAILURE);
                }
            }
        }
    }

    fn query(&mut self, query: &str, output: Output) -> Result<ExitCode> {
        if query.trim().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "query cannot be empty").into());
        }
        let expected = self.send(QUERY_EXECUTE, json!({"query": query}))?;
        let mut documents = Vec::new();
        let mut message = Vec::with_capacity(4096);
        loop {
            self.receive(&mut message)?;
            match decode_stream_response(&message)? {
                StreamResponse::Partial { id, data, .. } => {
                    check_request_id(expected, &id)?;
                    if output == Output::Quiet {
                        print_json(&data, Output::Compact)?;
                    } else {
                        documents.push(data);
                    }
                }
                StreamResponse::Complete { id, statistics, .. } => {
                    check_request_id(expected, &id)?;
                    if output != Output::Quiet {
                        print_json(&json!({"documents": documents, "statistics": statistics}), output)?;
                    }
                    return Ok(ExitCode::SUCCESS);
                }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id.as_ref() {
                        check_request_id(expected, id)?;
                    }
                    match id {
                        Some(id) => eprintln!("{NAME}: request {id}: {}: {}", error.code, error.message),
                        None => eprintln!("{NAME}: {}: {}", error.code, error.message),
                    }
                    return Ok(ExitCode::FAILURE);
                }
            }
        }
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
        Output::Pretty => serde_json::to_string_pretty(value)?,
        Output::Compact | Output::Quiet => serde_json::to_string(value)?,
    })
}

fn print_json(value: &Value, output: Output) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match output {
        Output::Pretty => serde_json::to_writer_pretty(&mut stdout, value)?,
        Output::Compact | Output::Quiet => serde_json::to_writer(&mut stdout, value)?,
    }
    writeln!(stdout)?;
    Ok(())
}

#[derive(Clone)]
struct Config {
    address: String,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    write_timeout: Duration,
    output: Output,
    identity: Option<PathBuf>,
    password_file: Option<PathBuf>,
    password: Option<String>,
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
            password_file: env::var("OGCLI_IDENTITY_PASSWORD_FILE").ok().map(PathBuf::from),
            password: env::var("OGCLI_IDENTITY_PASSWORD").ok(),
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
enum Output { Pretty, Compact, Quiet, }
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
                "--compact" => config.output = Output::Compact,
                "-q" | "--quiet" => config.output = Output::Quiet,
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
    match env::var(name) {
        Ok(value) => duration(&value, zero_is_none),
        Err(env::VarError::NotPresent) => duration(&default.to_string(), zero_is_none),
        Err(error) => Err(error.into()),
    }
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
    --connect-timeout-ms MS   [default: 5000]\n\
    --read-timeout-ms MS      0 waits indefinitely [default: 0]\n\
    --write-timeout-ms MS     [default: 30000]\n\
    --tui                     Interactive tabs/tiles UI (requires cli-tui)\n\
    --compact                 Compact JSON\n\
    --quiet, -q               Documents only\n\
    --help, -h\n\
    --version, -V\n\n\
With no QUERY, starts the REPL. Use `.OPERATION [JSON]` for raw ogd operations."
    );
}

fn print_repl_help() {
    println!(
        "REPL:\n\
    .OPERATION [JSON]  Execute an ogd operation\n\
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
