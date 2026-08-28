//! OG command-line client entry point.

use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    io::{self, BufRead, BufReader, IsTerminal, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use og_core::access::identity_file::{self, IdentityCredential, IdentityFileError};
use og_core::helpers::{decode_base64, Base64DecodeError};
use og_core::operation::{
    OperationRequest, OperationResponse, AUTH_BEGIN, AUTH_COMPLETE, BACKUP_CREATE, BACKUP_INSPECT,
    BACKUP_RESTORE, COLLECTIONS_LIST, IDENTITY_RENEW, PING, STORAGE_STATS,
};
use og_core::protocol::{
    decode_stream_response, encode_request, ensure_payload_size, MessageKind, QueryRequest,
    RequestId, StreamResponse, LENGTH_PREFIX_BYTES, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
};
use rustyline::{error::ReadlineError, DefaultEditor};
use serde_json::Value;

const DEFAULT_ADDRESS: &str = "127.0.0.1:7878";
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_READ_TIMEOUT_MS: u64 = 0;
const DEFAULT_WRITE_TIMEOUT_MS: u64 = 30_000;
const CLIENT_NAME: &str = "ogcli";
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    match run() {
        Ok(exit_code) => exit_code,
        Err(error) => {
            eprintln!("{CLIENT_NAME}: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, CliError> {
    let command = Command::parse(env::args().skip(1))?;

    match command.action {
        Action::Help => {
            print_help();
            Ok(ExitCode::SUCCESS)
        }
        Action::Version => {
            println!("{CLIENT_NAME} {CLIENT_VERSION}");
            Ok(ExitCode::SUCCESS)
        }
        Action::Execute(query) => {
            let mut client = Client::connect(&command.configuration)?;
            if query.starts_with('.') {
                let mut exit = ExitCode::SUCCESS;
                handle_repl_line(
                    &command.configuration,
                    &mut client,
                    &query,
                    false,
                    &mut exit,
                )?;
                Ok(exit)
            } else {
                client.execute_and_render(&query, command.configuration.output)
            }
        }
        Action::IdentityGet(path) => {
            let source = command
                .configuration
                .identity_file
                .as_ref()
                .ok_or(CliError::IdentityRequired)?;
            let password = command.configuration.identity_password()?;
            identity_file::copy_encrypted(source, &path, &password)
                .map_err(CliError::IdentityCrypto)?;
            println!("{}", path.display());
            Ok(ExitCode::SUCCESS)
        }
        Action::IdentityAgent => run_identity_agent(&command.configuration),
        Action::IdentityIssue { identity_id, path } => {
            let password = command.configuration.new_identity_password()?;
            let identity =
                IdentityCredential::renew(identity_id).map_err(CliError::IdentityCrypto)?;
            identity_file::save(&path, &identity, &password).map_err(CliError::IdentityCrypto)?;
            println!(
                "{}",
                serde_json::json!({
                    "identityId": &identity.identity_id,
                    "deviceId": &identity.device_id,
                    "publicKey": &identity.public_key,
                    "path": path.display().to_string(),
                })
            );
            Ok(ExitCode::SUCCESS)
        }
        Action::IdentityRenew(path) => {
            let destination = path
                .or_else(|| command.configuration.identity_file.clone())
                .ok_or(CliError::IdentityRequired)?;
            let mut client = Client::connect(&command.configuration)?;
            client.renew_identity(&command.configuration, &destination)?;
            println!("{}", destination.display());
            Ok(ExitCode::SUCCESS)
        }
        Action::Repl => run_repl(&command.configuration),
    }
}

fn run_identity_agent(configuration: &Configuration) -> Result<ExitCode, CliError> {
    let path = configuration
        .identity_file
        .as_ref()
        .ok_or(CliError::IdentityRequired)?;
    let password = configuration.identity_password()?;
    let identity = identity_file::load(path, &password).map_err(CliError::IdentityCrypto)?;

    // Local signing protocol for non-Rust clients. stdout starts with public
    // metadata as one JSON line. Each subsequent non-empty stdin line must be
    // one base64 challenge; stdout answers with the base64 Ed25519 signature.
    // The .ogid password and private key never cross the daemon TCP protocol.
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    writeln!(
        output,
        "{}",
        serde_json::json!({
            "identityId": &identity.identity_id,
            "deviceId": &identity.device_id,
            "publicKey": &identity.public_key,
        })
    )
    .map_err(CliError::Output)?;
    output.flush().map_err(CliError::Output)?;
    let mut line = String::new();

    loop {
        line.clear();
        if input.read_line(&mut line).map_err(CliError::Stdin)? == 0 {
            return Ok(ExitCode::SUCCESS);
        }
        let challenge = line.trim();
        if challenge.is_empty() {
            continue;
        }
        let challenge = decode_base64(challenge).map_err(CliError::Base64)?;
        writeln!(output, "{}", identity.sign_base64(&challenge)).map_err(CliError::Output)?;
        output.flush().map_err(CliError::Output)?;
    }
}

fn run_repl(configuration: &Configuration) -> Result<ExitCode, CliError> {
    let mut client = Client::connect(configuration)?;

    if io::stdin().is_terminal() {
        run_interactive_repl(configuration, &mut client)
    } else {
        run_stream_repl(configuration, &mut client)
    }
}

fn run_interactive_repl(
    configuration: &Configuration,
    client: &mut Client,
) -> Result<ExitCode, CliError> {
    let mut editor = DefaultEditor::new().map_err(CliError::LineEditor)?;
    let mut final_exit = ExitCode::SUCCESS;

    println!(
        "Connected to {}. Type .help for commands.",
        configuration.address
    );

    loop {
        match editor.readline("openglacier> ") {
            Ok(line) => {
                let query = line.trim();
                if query.is_empty() {
                    continue;
                }

                // History is deliberately in-memory for now. Rustyline provides shell-style
                // Up/Down navigation and editing with Left/Right, Home/End and Ctrl-A/Ctrl-E.
                let _ = editor.add_history_entry(query);

                if handle_repl_line(configuration, client, query, true, &mut final_exit)? {
                    return Ok(final_exit);
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C cancels the current edit buffer without terminating the REPL.
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D exits like a regular shell.
                println!();
                return Ok(final_exit);
            }
            Err(error) => return Err(CliError::LineEditor(error)),
        }
    }
}

fn run_stream_repl(
    configuration: &Configuration,
    client: &mut Client,
) -> Result<ExitCode, CliError> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut line = String::new();
    let mut final_exit = ExitCode::SUCCESS;

    loop {
        line.clear();
        if input.read_line(&mut line).map_err(CliError::Stdin)? == 0 {
            return Ok(final_exit);
        }

        let query = line.trim();
        if query.is_empty() {
            continue;
        }

        if handle_repl_line(configuration, client, query, false, &mut final_exit)? {
            return Ok(final_exit);
        }
    }
}

fn handle_repl_line(
    configuration: &Configuration,
    client: &mut Client,
    query: &str,
    interactive: bool,
    final_exit: &mut ExitCode,
) -> Result<bool, CliError> {
    match query {
        ".exit" | ".quit" => return Ok(true),
        ".help" => {
            print_repl_help();
            return Ok(false);
        }
        ".reconnect" => {
            *client = Client::connect(configuration)?;
            if interactive {
                println!("Reconnected to {}.", configuration.address);
            }
            return Ok(false);
        }
        _ => {}
    }

    if query == ".collections" || query == ".collections stats" {
        let response = client.operation(
            COLLECTIONS_LIST,
            serde_json::json!({"stats": query.ends_with(" stats")}),
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }
    if query.starts_with(".collections ") {
        eprintln!("Usage: .collections [stats]");
        return Ok(false);
    }

    if query == ".storage" || query == ".storage stats" {
        let response = client.operation(STORAGE_STATS, serde_json::json!({}))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }
    /*if query.starts_with(".") {
        eprintln!("Usage: .[operation]");
        return Ok(false);
    }*/
    if query == ".ping" {
        let response = client.operation(PING, serde_json::json!({}))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }
    if query.starts_with(".storage ") {
        eprintln!("Usage: .storage stats");
        return Ok(false);
    }

    if query == ".backup" {
        eprintln!("Usage:\n  .backup create <name>\n  .backup inspect <name>");
        return Ok(false);
    }
    if let Some(name) = query.strip_prefix(".backup inspect ") {
        let name = name.trim();
        if name.is_empty() {
            eprintln!("Usage: .backup inspect <name>");
            return Ok(false);
        }
        let response = client.operation(BACKUP_INSPECT, serde_json::json!({"name": name}))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }
    if let Some(name) = query.strip_prefix(".backup create ") {
        let name = name.trim();
        if name.is_empty() {
            eprintln!("Usage: .backup create <name>");
            return Ok(false);
        }
        let response = client.operation(BACKUP_CREATE, serde_json::json!({"name": name}))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }
    // Backward-compatible shorthand: `.backup <name>` creates a backup.
    if let Some(name) = query.strip_prefix(".backup ") {
        let name = name.trim();
        if name.is_empty() {
            eprintln!("Usage:\n  .backup create <name>\n  .backup inspect <name>");
            return Ok(false);
        }
        let response = client.operation(BACKUP_CREATE, serde_json::json!({"name": name}))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }

    if query == ".restore" {
        eprintln!("Usage: .restore <name> [--replace]");
        return Ok(false);
    }
    if let Some(rest) = query.strip_prefix(".restore ") {
        let replace = rest.ends_with(" --replace");
        let name = rest.strip_suffix(" --replace").unwrap_or(rest).trim();
        if name.is_empty() {
            eprintln!("Usage: .restore <name> [--replace]");
            return Ok(false);
        }
        let response = client.operation(
            BACKUP_RESTORE,
            serde_json::json!({"name": name, "replace": replace}),
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.data).map_err(CliError::RenderJson)?
        );
        return Ok(false);
    }

    let exit = match client.execute_and_render(query, configuration.output) {
        Ok(exit) => exit,
        Err(CliError::ConnectionClosed) => {
            *client = Client::connect(configuration)?;
            client.execute_and_render(query, configuration.output)?
        }
        Err(error) => return Err(error),
    };
    if exit != ExitCode::SUCCESS {
        *final_exit = ExitCode::FAILURE;
    }

    Ok(false)
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    next_request_id: u64,
    identity: Option<IdentityCredential>,
}

impl Client {
    fn connect(configuration: &Configuration) -> Result<Self, CliError> {
        let address = resolve_address(&configuration.address, configuration.connect_timeout)?;

        let stream = TcpStream::connect_timeout(&address, configuration.connect_timeout).map_err(
            |source| CliError::Connect {
                address: configuration.address.clone(),
                source,
            },
        )?;

        stream
            .set_read_timeout(configuration.read_timeout)
            .map_err(CliError::ConfigureSocket)?;

        stream
            .set_write_timeout(Some(configuration.write_timeout))
            .map_err(CliError::ConfigureSocket)?;

        stream
            .set_nodelay(true)
            .map_err(CliError::ConfigureSocket)?;

        let writer = stream.try_clone().map_err(CliError::CloneSocket)?;

        let mut client = Self {
            reader: BufReader::new(stream),
            writer,
            next_request_id: 1,
            identity: None,
        };
        if let Some(path) = &configuration.identity_file {
            let password = configuration.identity_password()?;
            let identity =
                identity_file::load(path, &password).map_err(CliError::IdentityCrypto)?;
            client.authenticate(&identity)?;
            client.identity = Some(identity);
        }
        Ok(client)
    }

    fn authenticate(&mut self, identity: &IdentityCredential) -> Result<(), CliError> {
        let begin = self.operation(
            AUTH_BEGIN,
            serde_json::json!({
                "identityId": identity.identity_id, "deviceId": identity.device_id
            }),
        )?;
        let challenge_id = begin
            .data
            .get("challengeId")
            .and_then(Value::as_str)
            .ok_or(CliError::InvalidAuthResponse)?
            .to_owned();
        let challenge = begin
            .data
            .get("challenge")
            .and_then(Value::as_str)
            .ok_or(CliError::InvalidAuthResponse)?;
        let challenge = decode_base64(challenge).map_err(CliError::Base64)?;
        self.operation(
            AUTH_COMPLETE,
            serde_json::json!({
                "challengeId": challenge_id,
                "signature": identity.sign_base64(&challenge),
            }),
        )?;
        Ok(())
    }

    fn renew_identity(
        &mut self,
        configuration: &Configuration,
        destination: &Path,
    ) -> Result<(), CliError> {
        let current = self.identity.as_ref().ok_or(CliError::IdentityRequired)?;
        let renewed = IdentityCredential::renew(current.identity_id.clone())
            .map_err(CliError::IdentityCrypto)?;
        let password = configuration.new_identity_password()?;
        let staged = identity_file::stage(destination, &renewed, &password)
            .map_err(CliError::IdentityCrypto)?;
        let response = self.operation(
            IDENTITY_RENEW,
            serde_json::json!({ "deviceId": renewed.device_id, "publicKey": renewed.public_key }),
        );
        if let Err(error) = response {
            let _ = std::fs::remove_file(&staged);
            return Err(error);
        }
        identity_file::commit(&staged, destination).map_err(CliError::IdentityCrypto)?;
        self.identity = Some(renewed);
        Ok(())
    }

    fn operation(&mut self, op: &str, data: Value) -> Result<OperationResponse, CliError> {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        let request = OperationRequest::new(id, op, data);
        let encoded =
            og_core::protocol::encode_message(&request, MessageKind::Request, MAX_REQUEST_BYTES)
                .map_err(CliError::Protocol)?;
        self.writer.write_all(&encoded).map_err(CliError::Write)?;
        self.writer.flush().map_err(CliError::Write)?;
        let expected_id: RequestId = id.into();
        let mut message = Vec::new();
        loop {
            message.clear();
            if read_response_message(&mut self.reader, &mut message)? == 0 {
                return Err(CliError::ConnectionClosed);
            }

            // A connection is multiplexed: authenticated keepalive and subscribed
            // events can arrive between a request and its response. Skip events and
            // keep reading until the correlated response is received.
            let envelope: Value =
                rmp_serde::from_slice(&message).map_err(CliError::OperationDecode)?;
            if envelope.get("kind").and_then(Value::as_str) == Some("event") {
                continue;
            }

            let received_id: RequestId = envelope
                .get("id")
                .cloned()
                .ok_or(CliError::InvalidOperationResponseId)
                .and_then(|value| {
                    serde_json::from_value(value).map_err(CliError::OperationIdDecode)
                })?;
            if received_id != expected_id {
                return Err(CliError::MismatchedResponseId {
                    expected: expected_id,
                    received: received_id,
                });
            }

            if envelope.get("status").and_then(Value::as_str) == Some("error") {
                let error = envelope.get("error");
                let code = error
                    .and_then(|value| value.get("code"))
                    .and_then(Value::as_str)
                    .unwrap_or("operation.failed")
                    .to_owned();
                let message = error
                    .and_then(|value| value.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("operation rejected by daemon")
                    .to_owned();

                return Err(CliError::OperationRejected { code, message });
            }

            return rmp_serde::from_slice(&message).map_err(CliError::OperationDecode);
        }
    }

    fn execute_and_render(
        &mut self,
        query: &str,
        output: OutputMode,
    ) -> Result<ExitCode, CliError> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        let encoded =
            encode_request(&QueryRequest::new(request_id, query)).map_err(CliError::Protocol)?;
        self.writer.write_all(&encoded).map_err(CliError::Write)?;
        self.writer.flush().map_err(CliError::Write)?;

        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        let mut renderer = StreamingRenderer::new(&mut stdout, output);
        renderer.begin()?;
        let mut message = Vec::with_capacity(4096);

        loop {
            message.clear();
            if read_response_message(&mut self.reader, &mut message)? == 0 {
                return Err(CliError::ConnectionClosed);
            }

            let envelope: Value =
                rmp_serde::from_slice(&message).map_err(CliError::OperationDecode)?;
            if envelope.get("kind").and_then(Value::as_str) == Some("event") {
                continue;
            }

            match decode_stream_response(&message).map_err(CliError::Protocol)? {
                StreamResponse::Partial { id, data, .. } if id == request_id => {
                    renderer.document(&data)?;
                }
                StreamResponse::Complete { id, statistics, .. } if id == request_id => {
                    renderer.end(statistics.as_ref())?;
                    return Ok(ExitCode::SUCCESS);
                }
                StreamResponse::Error { id, error, .. } => {
                    if let Some(id) = id {
                        if id != request_id {
                            return Err(CliError::MismatchedResponseId {
                                expected: request_id.into(),
                                received: id,
                            });
                        }
                    }
                    renderer.abort()?;
                    match id {
                        Some(id) => eprintln!(
                            "{CLIENT_NAME}: request {id}: {}: {}",
                            error.code, error.message
                        ),
                        None => eprintln!("{CLIENT_NAME}: {}: {}", error.code, error.message),
                    }
                    return Ok(ExitCode::FAILURE);
                }
                StreamResponse::Partial { id, .. } | StreamResponse::Complete { id, .. } => {
                    return Err(CliError::MismatchedResponseId {
                        expected: request_id.into(),
                        received: id,
                    });
                }
            }
        }
    }
}

fn read_response_message(
    reader: &mut BufReader<TcpStream>,
    message: &mut Vec<u8>,
) -> Result<usize, CliError> {
    let mut header = [0_u8; LENGTH_PREFIX_BYTES];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            return Err(CliError::ReadTimeout)
        }
        Err(source) if source.kind() == io::ErrorKind::Interrupted => {
            return read_response_message(reader, message);
        }
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
        Err(source) => return Err(CliError::Read(source)),
    }

    let length = u32::from_be_bytes(header) as usize;
    ensure_payload_size(MessageKind::Response, length, MAX_RESPONSE_BYTES)
        .map_err(CliError::Protocol)?;
    message.resize(length, 0);
    reader.read_exact(message).map_err(CliError::Read)?;
    Ok(length)
}

fn resolve_address(address: &str, _timeout: Duration) -> Result<SocketAddr, CliError> {
    let mut addresses = address
        .to_socket_addrs()
        .map_err(|source| CliError::Resolve {
            address: address.to_owned(),
            source,
        })?;

    addresses.next().ok_or_else(|| CliError::NoAddress {
        address: address.to_owned(),
    })
}

struct StreamingRenderer<W: Write> {
    writer: W,
    output: OutputMode,
    begun: bool,
    document_count: u64,
}

impl<W: Write> StreamingRenderer<W> {
    fn new(writer: W, output: OutputMode) -> Self {
        Self {
            writer,
            output,
            begun: false,
            document_count: 0,
        }
    }

    fn begin(&mut self) -> Result<(), CliError> {
        if self.begun {
            return Ok(());
        }

        self.begun = true;
        match self.output {
            OutputMode::Pretty => self.writer.write_all(b"{\n  \"documents\": [")?,
            OutputMode::Compact => self.writer.write_all(b"{\"documents\":[")?,
            OutputMode::Quiet => {}
        }
        Ok(())
    }

    fn document(&mut self, document: &Value) -> Result<(), CliError> {
        self.begin()?;
        match self.output {
            OutputMode::Quiet => {
                serde_json::to_writer(&mut self.writer, document).map_err(CliError::RenderJson)?;
                self.writer.write_all(b"\n")?;
            }
            OutputMode::Compact => {
                if self.document_count != 0 {
                    self.writer.write_all(b",")?;
                }
                serde_json::to_writer(&mut self.writer, document).map_err(CliError::RenderJson)?;
            }
            OutputMode::Pretty => {
                if self.document_count == 0 {
                    self.writer.write_all(b"\n")?;
                } else {
                    self.writer.write_all(b",\n")?;
                }

                let rendered =
                    serde_json::to_string_pretty(document).map_err(CliError::RenderJson)?;
                for (index, line) in rendered.lines().enumerate() {
                    if index != 0 {
                        self.writer.write_all(b"\n")?;
                    }
                    self.writer.write_all(b"    ")?;
                    self.writer.write_all(line.as_bytes())?;
                }
            }
        }

        self.document_count = self.document_count.saturating_add(1);
        Ok(())
    }

    fn end(&mut self, statistics: Option<&Value>) -> Result<(), CliError> {
        self.begin()?;
        match self.output {
            OutputMode::Quiet => {}
            OutputMode::Compact => {
                self.writer.write_all(b"],\"statistics\":")?;
                match statistics {
                    Some(value) => serde_json::to_writer(&mut self.writer, value)
                        .map_err(CliError::RenderJson)?,
                    None => self.writer.write_all(b"null")?,
                }
                self.writer.write_all(b"}\n")?;
            }
            OutputMode::Pretty => {
                if self.document_count == 0 {
                    self.writer.write_all(b"],\n  \"statistics\": ")?;
                } else {
                    self.writer.write_all(b"\n  ],\n  \"statistics\": ")?;
                }

                let value = statistics.unwrap_or(&Value::Null);
                let rendered = serde_json::to_string_pretty(value).map_err(CliError::RenderJson)?;
                let mut lines = rendered.lines();
                if let Some(first) = lines.next() {
                    self.writer.write_all(first.as_bytes())?;
                }
                for line in lines {
                    self.writer.write_all(b"\n  ")?;
                    self.writer.write_all(line.as_bytes())?;
                }
                self.writer.write_all(b"\n}\n")?;
            }
        }

        self.writer.flush()?;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), CliError> {
        if !self.begun {
            return Ok(());
        }

        match self.output {
            OutputMode::Quiet => {}
            OutputMode::Compact => self.writer.write_all(b"]}\n")?,
            OutputMode::Pretty => {
                if self.document_count == 0 {
                    self.writer.write_all(b"]\n}\n")?;
                } else {
                    self.writer.write_all(b"\n  ]\n}\n")?;
                }
            }
        }

        self.writer.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Configuration {
    address: String,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    write_timeout: Duration,
    output: OutputMode,
    identity_file: Option<PathBuf>,
    identity_password_file: Option<PathBuf>,
    identity_password: Option<String>,
}

impl Configuration {
    fn identity_password(&self) -> Result<Vec<u8>, CliError> {
        if let Some(path) = &self.identity_password_file {
            let mut value = std::fs::read(path).map_err(|source| CliError::IdentityFile {
                path: path.clone(),
                source,
            })?;
            while value
                .last()
                .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
            {
                value.pop();
            }
            if !value.is_empty() {
                return Ok(value);
            }
        }
        if let Some(value) = self
            .identity_password
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            return Ok(value.as_bytes().to_vec());
        }
        if io::stdin().is_terminal() {
            return rpassword::prompt_password("Identity password: ")
                .map(|value| value.into_bytes())
                .map_err(CliError::PasswordPrompt);
        }
        Err(CliError::IdentityPasswordRequired)
    }

    fn new_identity_password(&self) -> Result<Vec<u8>, CliError> {
        if self.identity_password_file.is_some()
            || self.identity_password.is_some()
            || !io::stdin().is_terminal()
        {
            return self.identity_password();
        }
        let first = rpassword::prompt_password("New identity password: ")
            .map_err(CliError::PasswordPrompt)?;
        let second = rpassword::prompt_password("Confirm identity password: ")
            .map_err(CliError::PasswordPrompt)?;
        if first.is_empty() || first != second {
            return Err(CliError::IdentityPasswordMismatch);
        }
        Ok(first.into_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Pretty,
    Compact,
    Quiet,
}

#[derive(Debug)]
struct Command {
    configuration: Configuration,
    action: Action,
}

#[derive(Debug)]
enum Action {
    Execute(String),
    IdentityGet(PathBuf),
    IdentityAgent,
    IdentityIssue { identity_id: String, path: PathBuf },
    IdentityRenew(Option<PathBuf>),
    Repl,
    Help,
    Version,
}

impl Command {
    fn parse<I>(arguments: I) -> Result<Self, CliError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut address = env::var("OGD_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());

        let mut host: Option<String> = None;
        let mut port: Option<u16> = None;
        let mut connect_timeout =
            duration_from_environment("OGCLI_CONNECT_TIMEOUT_MS", DEFAULT_CONNECT_TIMEOUT_MS)?;
        let mut read_timeout =
            optional_duration_from_environment("OGCLI_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS)?;
        let mut write_timeout =
            duration_from_environment("OGCLI_WRITE_TIMEOUT_MS", DEFAULT_WRITE_TIMEOUT_MS)?;
        let mut output = OutputMode::Pretty;
        let mut identity_file = env::var("OGCLI_IDENTITY").ok().map(PathBuf::from);
        if identity_file.is_none() && Path::new("bootstrap/admin.ogid").is_file() {
            identity_file = Some(PathBuf::from("bootstrap/admin.ogid"));
        }
        let mut identity_password_file = env::var("OGCLI_IDENTITY_PASSWORD_FILE")
            .ok()
            .map(PathBuf::from);
        let mut identity_password = env::var("OGCLI_IDENTITY_PASSWORD").ok();
        let mut query_parts = Vec::new();
        let mut arguments = arguments.into_iter();

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--help" | "-h" => {
                    return Ok(Self {
                        configuration: Configuration {
                            address,
                            connect_timeout,
                            read_timeout,
                            write_timeout,
                            output,
                            identity_file: identity_file.clone(),
                            identity_password_file: identity_password_file.clone(),
                            identity_password: identity_password.clone(),
                        },
                        action: Action::Help,
                    });
                }
                "--version" | "-V" => {
                    return Ok(Self {
                        configuration: Configuration {
                            address,
                            connect_timeout,
                            read_timeout,
                            write_timeout,
                            output,
                            identity_file: identity_file.clone(),
                            identity_password_file: identity_password_file.clone(),
                            identity_password: identity_password.clone(),
                        },
                        action: Action::Version,
                    });
                }
                "--identity" => {
                    identity_file = Some(PathBuf::from(take_option_value(
                        "--identity",
                        &mut arguments,
                    )?));
                }
                "--identity-password-file" => {
                    identity_password_file = Some(PathBuf::from(take_option_value(
                        "--identity-password-file",
                        &mut arguments,
                    )?));
                }
                "--identity-password" => {
                    identity_password =
                        Some(take_option_value("--identity-password", &mut arguments)?);
                }
                "--address" => {
                    address = take_option_value("--address", &mut arguments)?;
                }
                "--host" => {
                    host = Some(take_option_value("--host", &mut arguments)?);
                }
                "--port" => {
                    let value = take_option_value("--port", &mut arguments)?;

                    port = Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| CliError::InvalidPort { value, source })?,
                    );
                }
                "--connect-timeout-ms" => {
                    connect_timeout = parse_duration_option(
                        "--connect-timeout-ms",
                        take_option_value("--connect-timeout-ms", &mut arguments)?,
                    )?;
                }
                "--read-timeout-ms" => {
                    read_timeout = parse_optional_duration_option(
                        "--read-timeout-ms",
                        take_option_value("--read-timeout-ms", &mut arguments)?,
                    )?;
                }
                "--write-timeout-ms" => {
                    write_timeout = parse_duration_option(
                        "--write-timeout-ms",
                        take_option_value("--write-timeout-ms", &mut arguments)?,
                    )?;
                }
                "--compact" => output = OutputMode::Compact,
                "--quiet" | "-q" => output = OutputMode::Quiet,
                "--" => {
                    query_parts.extend(arguments);
                    break;
                }
                _ if argument.starts_with('-') => {
                    return Err(CliError::UnknownOption(argument));
                }
                _ => query_parts.push(argument),
            }
        }

        if host.is_some() || port.is_some() {
            let selected_host = host.unwrap_or_else(|| "127.0.0.1".to_owned());
            let selected_port = port.unwrap_or(7878);
            address = format_address(&selected_host, selected_port);
        }

        let action = match query_parts.as_slice() {
            [] => Action::Repl,
            [identity, get, path] if identity == "identity" && get == "get" => {
                Action::IdentityGet(PathBuf::from(path))
            }
            [identity, agent] if identity == "identity" && agent == "agent" => {
                Action::IdentityAgent
            }
            [identity, issue, identity_id, path] if identity == "identity" && issue == "issue" => {
                Action::IdentityIssue {
                    identity_id: identity_id.clone(),
                    path: PathBuf::from(path),
                }
            }
            [identity, renew] if identity == "identity" && renew == "renew" => {
                Action::IdentityRenew(None)
            }
            [identity, renew, path] if identity == "identity" && renew == "renew" => {
                Action::IdentityRenew(Some(PathBuf::from(path)))
            }
            _ => Action::Execute(query_parts.join(" ")),
        };

        Ok(Self {
            configuration: Configuration {
                address,
                connect_timeout,
                read_timeout,
                write_timeout,
                output,
                identity_file,
                identity_password_file,
                identity_password,
            },
            action,
        })
    }
}

fn take_option_value<I>(option: &'static str, arguments: &mut I) -> Result<String, CliError>
where
    I: Iterator<Item = String>,
{
    arguments.next().ok_or(CliError::MissingOptionValue(option))
}

fn parse_duration_option(option: &'static str, value: String) -> Result<Duration, CliError> {
    let milliseconds = value
        .parse::<u64>()
        .map_err(|source| CliError::InvalidDuration {
            name: option,
            value: value.clone(),
            source,
        })?;

    if milliseconds == 0 {
        return Err(CliError::ZeroDuration(option));
    }

    Ok(Duration::from_millis(milliseconds))
}

fn parse_optional_duration_option(
    option: &'static str,
    value: String,
) -> Result<Option<Duration>, CliError> {
    let milliseconds = value
        .parse::<u64>()
        .map_err(|source| CliError::InvalidDuration {
            name: option,
            value: value.clone(),
            source,
        })?;

    Ok((milliseconds != 0).then(|| Duration::from_millis(milliseconds)))
}

fn optional_duration_from_environment(
    name: &'static str,
    default_milliseconds: u64,
) -> Result<Option<Duration>, CliError> {
    match env::var(name) {
        Ok(value) => parse_optional_duration_option(name, value),
        Err(env::VarError::NotPresent) => {
            Ok((default_milliseconds != 0).then(|| Duration::from_millis(default_milliseconds)))
        }
        Err(source) => Err(CliError::Environment { name, source }),
    }
}

fn duration_from_environment(
    name: &'static str,
    default_milliseconds: u64,
) -> Result<Duration, CliError> {
    match env::var(name) {
        Ok(value) => {
            let milliseconds =
                value
                    .parse::<u64>()
                    .map_err(|source| CliError::InvalidDuration {
                        name,
                        value: value.clone(),
                        source,
                    })?;

            if milliseconds == 0 {
                return Err(CliError::ZeroDuration(name));
            }

            Ok(Duration::from_millis(milliseconds))
        }
        Err(env::VarError::NotPresent) => Ok(Duration::from_millis(default_milliseconds)),
        Err(source) => Err(CliError::Environment { name, source }),
    }
}

fn format_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn print_help() {
    println!(
        "\
{CLIENT_NAME} {CLIENT_VERSION}

USAGE:
    ogcli [OPTIONS] [QUERY...]

MODES:
    QUERY...                  Execute one query and exit
    identity get FILE         Export the encrypted identity after password validation
    identity agent            Local .ogid signing agent over stdin/stdout
    identity issue ID FILE    Create a new encrypted device credential for identity ID
    identity renew [FILE]     Rotate identity/device keys safely
    no query                  Start the interactive REPL

OPTIONS:
    --address ADDR            Daemon address [default: {DEFAULT_ADDRESS}]
    --host HOST               Host used with --port
    --port PORT               Port used with --host [default: 7878]
    --connect-timeout-ms MS   Connection timeout [default: {DEFAULT_CONNECT_TIMEOUT_MS}]
    --read-timeout-ms MS      Read timeout; 0 waits indefinitely [default: 0]
    --write-timeout-ms MS     Write timeout [default: {DEFAULT_WRITE_TIMEOUT_MS}]
    --compact                 Print compact JSON
    --identity FILE          Encrypted identity credential
    --identity-password-file FILE
                              Read identity password from FILE
    --identity-password PASS  Password (prefer --identity-password-file)
    --quiet, -q               Print only returned documents
    --help, -h                Show this help
    --version, -V             Show version

ENVIRONMENT:
    OGD_ADDRESS
    OGCLI_CONNECT_TIMEOUT_MS
    OGCLI_READ_TIMEOUT_MS
    OGCLI_WRITE_TIMEOUT_MS
    OGCLI_IDENTITY
    OGCLI_IDENTITY_PASSWORD_FILE
    OGCLI_IDENTITY_PASSWORD

REPL COMMANDS:
    .help                     Show REPL commands
    .reconnect                Reconnect to the daemon
    .quit, .exit              Exit the REPL
"
    );
}

fn print_repl_help() {
    println!(
        "\
REPL commands:
    .help         Show this help
    .reconnect    Reconnect to the daemon
    .collections [stats]
    .storage stats
    .backup create NAME
    .backup inspect NAME
    .restore NAME [--replace]
    .quit         Exit
    .exit         Exit

Editing:
    Up / Down     Previous / next history entry
    Left / Right  Move within the current query
    Home / End    Move to start / end
    Ctrl-A / E    Move to start / end
    Ctrl-C        Cancel the current line
    Ctrl-D        Exit when the line is empty

Any other non-empty line is sent as a query."
    );
}

#[derive(Debug)]
enum CliError {
    IdentityFile {
        path: PathBuf,
        source: io::Error,
    },
    Base64(Base64DecodeError),
    IdentityCrypto(IdentityFileError),
    IdentityRequired,
    IdentityPasswordRequired,
    IdentityPasswordMismatch,
    PasswordPrompt(io::Error),
    OperationDecode(rmp_serde::decode::Error),
    OperationIdDecode(serde_json::Error),
    InvalidOperationResponseId,
    OperationRejected {
        code: String,
        message: String,
    },
    InvalidAuthResponse,
    Environment {
        name: &'static str,
        source: env::VarError,
    },
    MissingOptionValue(&'static str),
    UnknownOption(String),
    InvalidPort {
        value: String,
        source: std::num::ParseIntError,
    },
    InvalidDuration {
        name: &'static str,
        value: String,
        source: std::num::ParseIntError,
    },
    ZeroDuration(&'static str),
    Resolve {
        address: String,
        source: io::Error,
    },
    NoAddress {
        address: String,
    },
    Connect {
        address: String,
        source: io::Error,
    },
    ConfigureSocket(io::Error),
    CloneSocket(io::Error),
    Write(io::Error),
    Read(io::Error),
    ReadTimeout,
    Output(io::Error),
    RenderJson(serde_json::Error),
    LineEditor(ReadlineError),
    Stdin(io::Error),
    Protocol(og_core::protocol::ProtocolError),
    ConnectionClosed,
    MismatchedResponseId {
        expected: RequestId,
        received: RequestId,
    },
}

impl From<io::Error> for CliError {
    fn from(source: io::Error) -> Self {
        Self::Output(source)
    }
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityFile { path, source } => write!(formatter, "cannot read identity file {}: {source}", path.display()),
            Self::IdentityCrypto(source) => write!(formatter, "identity error: {source}"),
            Self::IdentityRequired => formatter.write_str("an identity file is required; use --identity FILE"),
            Self::IdentityPasswordRequired => formatter.write_str("identity password required; use a terminal, OGCLI_IDENTITY_PASSWORD_FILE, or OGCLI_IDENTITY_PASSWORD"),
            Self::IdentityPasswordMismatch => formatter.write_str("identity passwords do not match"),
            Self::PasswordPrompt(source) => write!(formatter, "cannot read identity password: {source}"),
            Self::Base64(source) => write!(formatter, "invalid base64 identity data: {source}"),
            Self::OperationDecode(source) => write!(formatter, "cannot decode protocol response: {source}"),
            Self::OperationIdDecode(source) => write!(formatter, "cannot decode response identifier: {source}"),
            Self::InvalidOperationResponseId => formatter.write_str("protocol response has no identifier"),
            Self::OperationRejected { code, message } => {
                write!(formatter, "{code}: {message}")
            }
            Self::InvalidAuthResponse => formatter.write_str("invalid authentication response"),
            Self::Environment { name, source } => {
                write!(formatter, "cannot read {name}: {source}")
            }
            Self::MissingOptionValue(option) => {
                write!(formatter, "missing value for {option}")
            }
            Self::UnknownOption(option) => {
                write!(formatter, "unknown option `{option}`")
            }
            Self::InvalidPort { value, source } => {
                write!(formatter, "invalid port `{value}`: {source}")
            }
            Self::InvalidDuration {
                name,
                value,
                source,
            } => write!(
                formatter,
                "{name} must be a positive integer in milliseconds; got `{value}`: {source}"
            ),
            Self::ZeroDuration(name) => {
                write!(formatter, "{name} must be greater than zero")
            }
            Self::Resolve { address, source } => {
                write!(formatter, "cannot resolve {address}: {source}")
            }
            Self::NoAddress { address } => {
                write!(formatter, "{address} resolved to no addresses")
            }
            Self::Connect { address, source } => {
                write!(formatter, "cannot connect to {address}: {source}")
            }
            Self::ConfigureSocket(source) => {
                write!(formatter, "cannot configure socket: {source}")
            }
            Self::CloneSocket(source) => {
                write!(formatter, "cannot clone socket: {source}")
            }
            Self::Write(source) => {
                write!(formatter, "cannot send request: {source}")
            }
            Self::Read(source) => {
                write!(formatter, "cannot read response: {source}")
            }
            Self::ReadTimeout => formatter.write_str(
                "timed out while waiting for the daemon response; use --read-timeout-ms 0 to wait indefinitely",
            ),
            Self::Output(source) => write!(formatter, "cannot write output: {source}"),
            Self::RenderJson(source) => write!(formatter, "cannot render JSON: {source}"),
            Self::LineEditor(source) => {
                write!(formatter, "cannot use interactive line editor: {source}")
            }
            Self::Stdin(source) => {
                write!(formatter, "cannot read standard input: {source}")
            }
            Self::Protocol(source) => {
                write!(formatter, "protocol error: {source}")
            }
            Self::ConnectionClosed => formatter.write_str("daemon closed the connection"),
            Self::MismatchedResponseId { expected, received } => write!(
                formatter,
                "response identifier mismatch: expected {expected}, received {received}"
            ),
        }
    }
}

impl Error for CliError {
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IdentityFile { source, .. } => Some(source),
            Self::IdentityCrypto(source) => Some(source),
            Self::PasswordPrompt(source) => Some(source),
            Self::Base64(source) => Some(source),
            Self::OperationDecode(source) => Some(source),
            Self::OperationIdDecode(source) => Some(source),
            Self::Environment { source, .. } => Some(source),
            Self::InvalidPort { source, .. } => Some(source),
            Self::InvalidDuration { source, .. } => Some(source),
            Self::Resolve { source, .. } => Some(source),
            Self::Connect { source, .. } => Some(source),
            Self::ConfigureSocket(source)
            | Self::CloneSocket(source)
            | Self::Write(source)
            | Self::Read(source)
            | Self::Output(source)
            | Self::Stdin(source) => Some(source),
            Self::LineEditor(source) => Some(source),
            Self::Protocol(source) => Some(source),
            Self::RenderJson(source) => Some(source),
            Self::OperationRejected { .. }
            | Self::InvalidOperationResponseId
            | Self::InvalidAuthResponse
            | Self::IdentityRequired
            | Self::IdentityPasswordRequired
            | Self::IdentityPasswordMismatch
            | Self::MissingOptionValue(_)
            | Self::UnknownOption(_)
            | Self::ZeroDuration(_)
            | Self::NoAddress { .. }
            | Self::ConnectionClosed
            | Self::ReadTimeout
            | Self::MismatchedResponseId { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_query() {
        let command = Command::parse([
            "from".to_owned(),
            "users".to_owned(),
            "|".to_owned(),
            "limit".to_owned(),
            "10".to_owned(),
        ])
        .expect("command parses");

        match command.action {
            Action::Execute(query) => {
                assert_eq!(query, "from users | limit 10");
            }
            _ => panic!("expected execute action"),
        }
    }

    #[test]
    fn parses_identity_get() {
        let command = Command::parse([
            "identity".to_owned(),
            "get".to_owned(),
            "me.ogid".to_owned(),
        ])
        .expect("command parses");
        assert!(
            matches!(command.action, Action::IdentityGet(ref path) if path == Path::new("me.ogid"))
        );
    }

    #[test]
    fn parses_identity_agent() {
        let command =
            Command::parse(["identity".to_owned(), "agent".to_owned()]).expect("command parses");
        assert!(matches!(command.action, Action::IdentityAgent));
    }

    #[test]
    fn parses_identity_renew() {
        let command = Command::parse([
            "identity".to_owned(),
            "renew".to_owned(),
            "next.ogid".to_owned(),
        ])
        .expect("command parses");
        assert!(
            matches!(command.action, Action::IdentityRenew(Some(ref path)) if path == Path::new("next.ogid"))
        );
    }

    #[test]
    fn parses_one_shot_repl_command() {
        let command =
            Command::parse([".storage".to_owned(), "stats".to_owned()]).expect("command parses");

        match command.action {
            Action::Execute(query) => assert_eq!(query, ".storage stats"),
            _ => panic!("expected execute action"),
        }
    }

    #[test]
    fn starts_repl_without_query() {
        let command = Command::parse(Vec::<String>::new()).expect("parses");

        assert!(matches!(command.action, Action::Repl));
    }

    #[test]
    fn parses_address_and_output_mode() {
        let command = Command::parse([
            "--address".to_owned(),
            "localhost:9000".to_owned(),
            "--compact".to_owned(),
            "from users".to_owned(),
        ])
        .expect("command parses");

        assert_eq!(command.configuration.address, "localhost:9000");
        assert_eq!(command.configuration.output, OutputMode::Compact);
    }

    #[test]
    fn host_and_port_override_address() {
        let command = Command::parse([
            "--address".to_owned(),
            "ignored:1".to_owned(),
            "--host".to_owned(),
            "::1".to_owned(),
            "--port".to_owned(),
            "9000".to_owned(),
        ])
        .expect("command parses");

        assert_eq!(command.configuration.address, "[::1]:9000");
    }

    #[test]
    fn rejects_unknown_option() {
        let error = Command::parse(["--wat".to_owned()]).expect_err("unknown option must fail");

        assert!(matches!(error, CliError::UnknownOption(_)));
    }

    #[test]
    fn zero_read_timeout_waits_indefinitely() {
        let command = Command::parse(["--read-timeout-ms".to_owned(), "0".to_owned()])
            .expect("zero read timeout is valid");

        assert_eq!(command.configuration.read_timeout, None);
    }

    #[test]
    fn positive_read_timeout_is_configured() {
        let command = Command::parse(["--read-timeout-ms".to_owned(), "250".to_owned()])
            .expect("positive read timeout is valid");

        assert_eq!(
            command.configuration.read_timeout,
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn formats_ipv4_address() {
        assert_eq!(format_address("127.0.0.1", 7878), "127.0.0.1:7878");
    }

    #[test]
    fn formats_ipv6_address() {
        assert_eq!(format_address("::1", 7878), "[::1]:7878");
    }

    #[test]
    fn compact_renderer_streams_valid_json_without_collecting_documents() {
        let mut output = Vec::new();
        let mut renderer = StreamingRenderer::new(&mut output, OutputMode::Compact);
        renderer.begin().expect("begin renders");
        renderer
            .document(&serde_json::json!({"id": 1}))
            .expect("first document renders");
        renderer
            .document(&serde_json::json!({"id": 2}))
            .expect("second document renders");
        renderer
            .end(Some(&serde_json::json!({"returned": 2})))
            .expect("end renders");

        let value: Value = serde_json::from_slice(&output).expect("valid streamed JSON");
        assert_eq!(value["documents"].as_array().map(Vec::len), Some(2));
        assert_eq!(value["statistics"]["returned"], 2);
    }

    #[test]
    fn quiet_renderer_emits_one_json_document_per_line() {
        let mut output = Vec::new();
        let mut renderer = StreamingRenderer::new(&mut output, OutputMode::Quiet);
        renderer
            .document(&serde_json::json!({"id": 1}))
            .expect("first document renders");
        renderer
            .document(&serde_json::json!({"id": 2}))
            .expect("second document renders");
        renderer.end(None).expect("end renders");

        let lines = std::str::from_utf8(&output)
            .expect("utf8 output")
            .lines()
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap()["id"], 1);
        assert_eq!(serde_json::from_str::<Value>(lines[1]).unwrap()["id"], 2);
    }

    #[test]
    fn pretty_renderer_streams_valid_json() {
        let mut output = Vec::new();
        let mut renderer = StreamingRenderer::new(&mut output, OutputMode::Pretty);
        renderer
            .document(&serde_json::json!({"nested": {"value": 1}}))
            .expect("document renders");
        renderer
            .end(Some(&serde_json::json!({"returned": 1})))
            .expect("end renders");

        let value: Value = serde_json::from_slice(&output).expect("valid pretty JSON");
        assert_eq!(value["documents"][0]["nested"]["value"], 1);
        assert_eq!(value["statistics"]["returned"], 1);
    }
}
