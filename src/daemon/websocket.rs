//! The websockets of the node channels (RFC 6455): the client a Core opens to its Gateway, plain (`ws://`) or TLS by
//! rustls (`wss://`, the webpki roots), and the server end the tests play the Gateway with. Text and binary messages,
//! fragmented or not, under the limits tungstenite had (a message of 64 MiB, a frame of 16 MiB); a ping is answered
//! and a close echoed on the next read, write or flush; no extension, no subprotocol.
//!
//! On a nonblocking stream, `read` gives `Error::Io(WouldBlock)` until a whole message is in, and `write`/`flush`
//! keep what the socket did not take, `Error::Io(WouldBlock)` telling it, for the next `write` or `flush`.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{fmt, io::{self, Read, Write}, net::TcpStream, sync::{Arc, OnceLock}};

use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use sha1::{Digest, Sha1};

use crate::helpers::encode_base64;

const MAX_MESSAGE: usize = 64 << 20;
const MAX_FRAME: usize = 16 << 20;
/// Bytes asked of the stream at a time.
const READ_CHUNK: usize = 64 * 1024;
/// An HTTP head longer than this is not a websocket handshake.
const MAX_HEAD: usize = 16 * 1024;
const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const CONTINUATION: u8 = 0x0;
const TEXT: u8 = 0x1;
const BINARY: u8 = 0x2;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;
const PONG: u8 = 0xA;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Message { Text(String), Binary(Vec<u8>), Ping(Vec<u8>), Pong(Vec<u8>), Close(Option<u16>) }

#[derive(Debug)]
pub(super) enum Error {
    Io(io::Error),
    /// The close handshake is over: nothing more comes.
    ConnectionClosed,
    /// A message written after the close.
    AlreadyClosed,
    Protocol(String),
    Handshake(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::ConnectionClosed => formatter.write_str("websocket connection closed"),
            Self::AlreadyClosed => formatter.write_str("websocket already closed"),
            Self::Protocol(message) => write!(formatter, "websocket protocol error: {message}"),
            Self::Handshake(message) => write!(formatter, "websocket handshake failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self { Self::Io(error) }
}

impl Error {
    pub(super) fn would_block(&self) -> bool { matches!(self, Self::Io(error) if error.kind() == io::ErrorKind::WouldBlock) }
}

/// The stream under a node websocket.
pub(super) enum NodeStream { Plain(TcpStream), Tls(Box<StreamOwned<ClientConnection, TcpStream>>) }

impl NodeStream {
    /// The TCP stream, under TLS or not: timeouts, blocking mode, poll.
    pub(super) fn tcp(&mut self) -> &mut TcpStream {
        match self { Self::Plain(stream) => stream, Self::Tls(stream) => stream.get_mut() }
    }
}

impl Read for NodeStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self { Self::Plain(stream) => stream.read(buffer), Self::Tls(stream) => stream.read(buffer) }
    }
}

impl Write for NodeStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self { Self::Plain(stream) => stream.write(buffer), Self::Tls(stream) => stream.write(buffer) }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self { Self::Plain(stream) => stream.flush(), Self::Tls(stream) => stream.flush() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role { Client, Server }

pub(super) struct WebSocket<S> {
    stream: S,
    role: Role,
    /// Bytes read and not parsed yet, from `start` on.
    input: Vec<u8>,
    start: usize,
    /// Frames written and not taken by the stream yet, from `written` on.
    output: Vec<u8>,
    written: usize,
    /// The opcode and the payload of a fragmented message so far.
    partial: Option<(u8, Vec<u8>)>,
    sent_close: bool,
    received_close: bool,
}

/// Opens a websocket to `url` (`ws://host[:port]/path` or `wss://…`, checked against the webpki roots).
pub(super) fn connect(url: &str) -> Result<WebSocket<NodeStream>, Error> { connect_with(url, tls_config()) }

/// [`connect`] with the TLS configuration of `tls` (the tests' own authority).
pub(super) fn connect_with(url: &str, tls: Arc<ClientConfig>) -> Result<WebSocket<NodeStream>, Error> {
    let (secure, rest) = match (url.strip_prefix("wss://"), url.strip_prefix("ws://")) {
        (Some(rest), _) => (true, rest),
        (None, Some(rest)) => (false, rest),
        _ => return Err(Error::Handshake(format!("{url:?} is not a ws:// or wss:// URL"))),
    };
    let (authority, path) = match rest.find(['/', '?']) {
        Some(index) if rest[index..].starts_with('?') => (&rest[..index], format!("/{}", &rest[index..])),
        Some(index) => (&rest[..index], rest[index..].to_owned()),
        None => (rest, "/".to_owned()),
    };
    let (host, port) = host_and_port(authority, if secure { 443 } else { 80 }).ok_or_else(|| Error::Handshake(format!("invalid host in {url:?}")))?;
    let tcp = TcpStream::connect((host, port))?;
    let stream = if secure {
        let name = ServerName::try_from(host.to_owned()).map_err(|error| Error::Handshake(format!("invalid TLS server name {host:?}: {error}")))?;
        let connection = ClientConnection::new(tls, name).map_err(|error| Error::Handshake(error.to_string()))?;
        NodeStream::Tls(Box::new(StreamOwned::new(connection, tcp)))
    } else {
        NodeStream::Plain(tcp)
    };
    client_handshake(stream, authority, &path)
}

/// `host[:port]` or `[v6][:port]`, the host without its brackets.
fn host_and_port(authority: &str, default: u16) -> Option<(&str, u16)> {
    let (host, port) = match authority.strip_prefix('[') {
        Some(rest) => { let (host, after) = rest.split_once(']')?; (host, after.strip_prefix(':')) }
        None => match authority.rsplit_once(':') { Some((host, port)) => (host, Some(port)), None => (authority, None) },
    };
    let port = match port { Some(port) => port.parse().ok()?, None => default };
    (!host.is_empty()).then_some((host, port))
}

fn tls_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    Arc::clone(CONFIG.get_or_init(|| {
        let roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        Arc::new(ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().expect("ring offers TLS 1.2 and 1.3")
            .with_root_certificates(roots).with_no_client_auth())
    }))
}

/// The `Sec-WebSocket-Accept` of a `Sec-WebSocket-Key`.
fn accept_key(key: &str) -> String {
    let mut hash = Sha1::new();
    hash.update(key.as_bytes());
    hash.update(ACCEPT_GUID.as_bytes());
    encode_base64(&hash.finalize())
}

fn client_handshake<S: Read + Write>(mut stream: S, authority: &str, path: &str) -> Result<WebSocket<S>, Error> {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|error| Error::Handshake(error.to_string()))?;
    let key = encode_base64(&nonce);
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let (head, rest) = read_head(&mut stream)?;
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or_default();
    if status.split_whitespace().nth(1) != Some("101") {
        return Err(Error::Handshake(format!("the server answered {:?}", status.trim())));
    }
    let headers: Vec<(&str, &str)> = lines.filter_map(|line| line.split_once(':')).map(|(name, value)| (name.trim(), value.trim())).collect();
    let header = |name: &str| headers.iter().find(|(key, _)| key.eq_ignore_ascii_case(name)).map(|(_, value)| *value);
    if !header("Upgrade").is_some_and(|value| value.eq_ignore_ascii_case("websocket")) || !header("Connection").is_some_and(|value| value.split(',').any(|token| token.trim().eq_ignore_ascii_case("upgrade"))) {
        return Err(Error::Handshake("the server did not upgrade the connection to a websocket".to_owned()));
    }
    if header("Sec-WebSocket-Accept") != Some(accept_key(&key).as_str()) {
        return Err(Error::Handshake("the server answered another Sec-WebSocket-Key".to_owned()));
    }
    let mut websocket = WebSocket::from_stream(stream, Role::Client);
    websocket.input = rest;
    Ok(websocket)
}

/// The server end of a handshake (the tests' Gateway).
#[cfg(test)]
pub(super) fn accept<S: Read + Write>(mut stream: S) -> Result<WebSocket<S>, Error> {
    let (head, rest) = read_head(&mut stream)?;
    let key = head.split("\r\n").filter_map(|line| line.split_once(':')).find(|(name, _)| name.trim().eq_ignore_ascii_case("Sec-WebSocket-Key")).map(|(_, value)| value.trim().to_owned())
        .ok_or_else(|| Error::Handshake("no Sec-WebSocket-Key".to_owned()))?;
    let response = format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n", accept_key(&key));
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    let mut websocket = WebSocket::from_stream(stream, Role::Server);
    websocket.input = rest;
    Ok(websocket)
}

/// The HTTP head up to its blank line, and the bytes read past it (the first frames).
fn read_head<S: Read>(stream: &mut S) -> Result<(String, Vec<u8>), Error> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let rest = buffer.split_off(end + 4);
            let head = String::from_utf8(buffer).map_err(|_| Error::Handshake("the HTTP head is not UTF-8".to_owned()))?;
            return Ok((head, rest));
        }
        if buffer.len() > MAX_HEAD {
            return Err(Error::Handshake("the HTTP head is too long".to_owned()));
        }
        match stream.read(&mut chunk)? {
            0 => return Err(Error::Handshake("the connection closed during the handshake".to_owned())),
            bytes => buffer.extend_from_slice(&chunk[..bytes]),
        }
    }
}

impl<S: Read + Write> WebSocket<S> {
    /// A websocket on a stream whose handshake is done.
    pub(super) fn from_stream(stream: S, role: Role) -> Self {
        Self { stream, role, input: Vec::new(), start: 0, output: Vec::new(), written: 0, partial: None, sent_close: false, received_close: false }
    }

    pub(super) fn get_ref(&self) -> &S { &self.stream }

    pub(super) fn get_mut(&mut self) -> &mut S { &mut self.stream }

    /// The next message; a ping is returned once answered (on the way out), a close once echoed.
    pub(super) fn read(&mut self) -> Result<Message, Error> {
        loop {
            // The pongs and the close echo go out first.
            match self.write_pending() { Err(error) if !error.would_block() => return Err(error), _ => {} }
            if self.received_close {
                return Err(Error::ConnectionClosed);
            }
            if let Some(message) = self.parse()? {
                return Ok(message);
            }
            self.fill()?;
        }
    }

    /// Queues `message` and writes out what the stream takes.
    pub(super) fn write(&mut self, message: Message) -> Result<(), Error> {
        if self.sent_close {
            return Err(Error::AlreadyClosed);
        }
        match message {
            Message::Text(text) => self.frame(TEXT, text.as_bytes()),
            Message::Binary(bytes) => self.frame(BINARY, &bytes),
            Message::Ping(bytes) => self.frame(PING, &bytes),
            Message::Pong(bytes) => self.frame(PONG, &bytes),
            Message::Close(code) => { let code = code.map(u16::to_be_bytes); self.frame(CLOSE, code.as_ref().map_or(&[][..], |code| &code[..])); self.sent_close = true; }
        }
        self.write_pending()
    }

    /// Writes `message` out, whole.
    pub(super) fn send(&mut self, message: Message) -> Result<(), Error> {
        self.write(message)?;
        self.flush()
    }

    pub(super) fn flush(&mut self) -> Result<(), Error> { self.write_pending() }

    /// Starts the close handshake (a close frame without code); the peer's echo ends it.
    pub(super) fn close(&mut self) -> Result<(), Error> {
        if !self.sent_close {
            self.frame(CLOSE, &[]);
            self.sent_close = true;
        }
        self.write_pending()
    }

    fn write_pending(&mut self) -> Result<(), Error> {
        while self.written < self.output.len() {
            match self.stream.write(&self.output[self.written..]) {
                Ok(0) => return Err(Error::Io(io::ErrorKind::WriteZero.into())),
                Ok(bytes) => self.written += bytes,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.output.clear();
        self.written = 0;
        // TLS keeps records the socket did not take yet.
        self.stream.flush()?;
        Ok(())
    }

    /// Appends one frame, masked when a client writes it.
    fn frame(&mut self, opcode: u8, payload: &[u8]) {
        let masked = self.role == Role::Client;
        self.output.push(0x80 | opcode);
        let mask_bit = if masked { 0x80 } else { 0 };
        match payload.len() {
            length @ 0..=125 => self.output.push(mask_bit | length as u8),
            length @ 126..=0xFFFF => { self.output.push(mask_bit | 126); self.output.extend_from_slice(&(length as u16).to_be_bytes()); }
            length => { self.output.push(mask_bit | 127); self.output.extend_from_slice(&(length as u64).to_be_bytes()); }
        }
        if !masked {
            self.output.extend_from_slice(payload);
            return;
        }
        let mut mask = [0u8; 4];
        // Without a random mask, a fixed one: masking only shields intermediaries, which TLS or a loopback do as well.
        let _ = getrandom::fill(&mut mask);
        self.output.extend_from_slice(&mask);
        let at = self.output.len();
        self.output.extend_from_slice(payload);
        apply_mask(&mut self.output[at..], mask);
    }

    /// Reads what the stream has into `input`.
    fn fill(&mut self) -> Result<(), Error> {
        if self.start > 0 && self.start * 2 >= self.input.len() {
            self.input.drain(..self.start);
            self.start = 0;
        }
        let length = self.input.len();
        self.input.resize(length + READ_CHUNK, 0);
        let read = loop {
            match self.stream.read(&mut self.input[length..]) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                other => break other,
            }
        };
        self.input.truncate(length + *read.as_ref().unwrap_or(&0));
        match read? {
            0 if self.sent_close => Err(Error::ConnectionClosed),
            0 => Err(Error::Protocol("connection reset without closing handshake".to_owned())),
            _ => Ok(()),
        }
    }

    /// The next whole message in `input`, if one is there.
    fn parse(&mut self) -> Result<Option<Message>, Error> {
        loop {
            let buffer = &self.input[self.start..];
            if buffer.len() < 2 {
                return Ok(None);
            }
            let (first, second) = (buffer[0], buffer[1]);
            if first & 0x70 != 0 {
                return Err(Error::Protocol("reserved bits set (no extension was agreed)".to_owned()));
            }
            let (fin, opcode, masked) = (first & 0x80 != 0, first & 0x0F, second & 0x80 != 0);
            if masked != (self.role == Role::Server) {
                return Err(Error::Protocol(if masked { "a server frame is masked" } else { "a client frame is not masked" }.to_owned()));
            }
            let (length, mut at) = match second & 0x7F {
                126 if buffer.len() >= 4 => (u64::from(u16::from_be_bytes([buffer[2], buffer[3]])), 4),
                127 if buffer.len() >= 10 => (u64::from_be_bytes(buffer[2..10].try_into().expect("eight bytes")), 10),
                126 | 127 => return Ok(None),
                length => (u64::from(length), 2),
            };
            if length > MAX_FRAME as u64 {
                return Err(Error::Protocol(format!("a frame of {length} bytes, over {MAX_FRAME}")));
            }
            let length = length as usize;
            let mask = if masked {
                let Some(mask) = buffer.get(at..at + 4) else { return Ok(None) };
                at += 4;
                Some([mask[0], mask[1], mask[2], mask[3]])
            } else {
                None
            };
            if buffer.len() < at + length {
                return Ok(None);
            }
            let mut payload = buffer[at..at + length].to_vec();
            self.start += at + length;
            if let Some(mask) = mask {
                apply_mask(&mut payload, mask);
            }
            if opcode >= CLOSE && (!fin || length > 125) {
                return Err(Error::Protocol("a control frame fragmented or over 125 bytes".to_owned()));
            }
            let (opcode, payload) = match opcode {
                TEXT | BINARY if self.partial.is_some() => return Err(Error::Protocol("a new message before the end of a fragmented one".to_owned())),
                TEXT | BINARY if !fin => { self.partial = Some((opcode, payload)); continue; }
                TEXT | BINARY => (opcode, payload),
                CONTINUATION => {
                    let Some((first, mut gathered)) = self.partial.take() else { return Err(Error::Protocol("a continuation frame outside a message".to_owned())) };
                    if gathered.len() + payload.len() > MAX_MESSAGE {
                        return Err(Error::Protocol(format!("a message over {MAX_MESSAGE} bytes")));
                    }
                    gathered.extend_from_slice(&payload);
                    if !fin { self.partial = Some((first, gathered)); continue; }
                    (first, gathered)
                }
                CLOSE => {
                    let code = (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]));
                    if !self.sent_close {
                        self.frame(CLOSE, &payload[..payload.len().min(2)]);
                        self.sent_close = true;
                    }
                    self.received_close = true;
                    // The echo leaves now, the caller may well drop the connection on this message; a peer gone already
                    // does not hide its close.
                    let _ = self.write_pending();
                    return Ok(Some(Message::Close(code)));
                }
                PING => { if !self.sent_close { self.frame(PONG, &payload); } return Ok(Some(Message::Ping(payload))); }
                PONG => return Ok(Some(Message::Pong(payload))),
                other => return Err(Error::Protocol(format!("unknown opcode {other:#x}"))),
            };
            return Ok(Some(if opcode == TEXT {
                Message::Text(String::from_utf8(payload).map_err(|_| Error::Protocol("a text message that is not UTF-8".to_owned()))?)
            } else {
                Message::Binary(payload)
            }));
        }
    }
}

fn apply_mask(bytes: &mut [u8], mask: [u8; 4]) {
    let word = u32::from_ne_bytes(mask);
    let mut chunks = bytes.chunks_exact_mut(4);
    for chunk in &mut chunks {
        let value = u32::from_ne_bytes(chunk.try_into().expect("four bytes")) ^ word;
        chunk.copy_from_slice(&value.to_ne_bytes());
    }
    for (index, byte) in chunks.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[index];
    }
}

/// A certificate authority and a server certificate of `localhost` and 127.0.0.1 (P-256, valid until 2126), for the
/// `wss://` of the tests.
#[cfg(test)]
pub(super) mod test_tls {
    use super::*;
    use rustls::{pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer}, ServerConfig, ServerConnection};

    const AUTHORITY: &str = "-----BEGIN CERTIFICATE-----
MIIBmjCCAUGgAwIBAgIUY3xH4e3BV3Utn3hMPCPGdNNxZSkwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPb2ctY29yZSB0ZXN0IENBMCAXDTI2MTAzMDA5MjYzN1oYDzIx
MjYxMDA2MDkyNjM3WjAaMRgwFgYDVQQDDA9vZy1jb3JlIHRlc3QgQ0EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAARU31mpnfUsb2N0D0W+pIyyQAja6A2jpQmJ/O/5
rQn1H8Er30xt0F6Tz506ijGek4rH+ObKyDsJACMGYR8cCAgSo2MwYTAdBgNVHQ4E
FgQUdEaxxeDYGcifYphgHpkFWSkeFJkwHwYDVR0jBBgwFoAUdEaxxeDYGcifYphg
HpkFWSkeFJkwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAgQwCgYIKoZI
zj0EAwIDRwAwRAIgOu9CsXsQDRIb/rtrdRNVRpsY6TDnUrGA8uOHGJatDl4CIEbe
sgofmS6EVahr4d89b3uU9v43M7GF66jy792U7MwS
-----END CERTIFICATE-----";
    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----
MIIBxDCCAWugAwIBAgIUeeGYqkFJTdpj1CZwY/PczmK3CU0wCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPb2ctY29yZSB0ZXN0IENBMCAXDTI2MTAzMDA5MjYzN1oYDzIx
MjYxMDA2MDkyNjM3WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAAQ1y0TVnER4Fm3m0flD3ZYt0bq9v4zlxaORxQQFiuxBM9CK
CiveMnWZDTdI3f8ExHtlfXgSYdFHXXwbYgH25UeDo4GSMIGPMAwGA1UdEwEB/wQC
MAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBoGA1UdEQQT
MBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUpibuWbhIROG9RaYJN8AfKdz2
vV4wHwYDVR0jBBgwFoAUdEaxxeDYGcifYphgHpkFWSkeFJkwCgYIKoZIzj0EAwID
RwAwRAIgM30EIiEh9XMj67En7VblH1hsi3sVQVXwEx63/DrI9cUCIDtgEyupNcGT
KSybIlD7KwDwLnOjRY1jW2Ykpsf1YWEh
-----END CERTIFICATE-----";
    const KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgvTGr5ofAmqSGt/qV
f9GEyL4IFzrVfPdb1L11/BGl4VChRANCAAQ1y0TVnER4Fm3m0flD3ZYt0bq9v4zl
xaORxQQFiuxBM9CKCiveMnWZDTdI3f8ExHtlfXgSYdFHXXwbYgH25UeD
-----END PRIVATE KEY-----";

    /// A client trusting the test authority alone.
    pub(in crate::daemon) fn client() -> Arc<ClientConfig> { let mut roots = RootCertStore::empty(); roots.add(CertificateDer::from_pem_slice(AUTHORITY.as_bytes()).unwrap()).unwrap(); Arc::new(ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider())).with_safe_default_protocol_versions().unwrap().with_root_certificates(roots).with_no_client_auth()) }

    /// The server end of a `wss://` connection.
    pub(in crate::daemon) fn accept(stream: TcpStream) -> WebSocket<StreamOwned<ServerConnection, TcpStream>> { let config = ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider())).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(vec![CertificateDer::from_pem_slice(CERTIFICATE.as_bytes()).unwrap()], PrivateKeyDer::from_pem_slice(KEY.as_bytes()).unwrap()).unwrap(); super::accept(StreamOwned::new(ServerConnection::new(Arc::new(config)).unwrap(), stream)).unwrap() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::{Shutdown, TcpListener}, thread, time::{Duration, Instant}};

    /// A client and the server end of the same connection.
    fn pair() -> (WebSocket<NodeStream>, WebSocket<TcpStream>) { let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let url = format!("ws://{}/node?x=1", listener.local_addr().unwrap()); let server = thread::spawn(move || accept(listener.accept().unwrap().0).unwrap()); let client = connect(&url).unwrap(); (client, server.join().unwrap()) }

    /// One frame as a server writes it (unmasked, under 126 bytes).
    fn raw(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> { let mut frame = vec![if fin { 0x80 } else { 0 } | opcode, payload.len() as u8]; frame.extend_from_slice(payload); frame }

    #[test] fn the_accept_key_is_the_one_of_the_rfc() { assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="); }
    #[test] fn hosts_are_read_with_or_without_port_and_brackets() { assert_eq!(host_and_port("gw.example:8443", 443), Some(("gw.example", 8443))); assert_eq!(host_and_port("gw.example", 443), Some(("gw.example", 443))); assert_eq!(host_and_port("[::1]:9", 80), Some(("::1", 9))); assert_eq!(host_and_port("[::1]", 80), Some(("::1", 80))); assert_eq!((host_and_port("", 80), host_and_port("h:port", 80)), (None, None)); assert!(matches!(connect("http://h/"), Err(Error::Handshake(_)))); }
    #[test] fn messages_cross_both_ways_whatever_their_length() { let (mut client, mut server) = pair(); let echo = thread::spawn(move || { while let Ok(message) = server.read() { if server.send(message).is_err() { break; } } }); for length in [0, 1, 125, 126, 65_535, 65_536, 3 * 1024 * 1024] { let bytes = (0..length).map(|index| (index % 251) as u8).collect::<Vec<_>>(); client.send(Message::Binary(bytes.clone())).unwrap(); assert_eq!(client.read().unwrap(), Message::Binary(bytes), "{length}"); } client.send(Message::Text("héllo".into())).unwrap(); assert_eq!(client.read().unwrap(), Message::Text("héllo".into())); client.close().unwrap(); assert_eq!(client.read().unwrap(), Message::Close(None)); echo.join().unwrap(); }
    #[test] fn a_ping_is_answered_and_a_fragmented_message_gathered_around_it() { let (mut client, mut server) = pair(); let bytes = [raw(false, TEXT, b"Hel"), raw(true, PING, b"p"), raw(false, CONTINUATION, b"l"), raw(true, CONTINUATION, b"o")].concat(); server.get_mut().write_all(&bytes).unwrap(); assert_eq!(client.read().unwrap(), Message::Ping(b"p".to_vec())); assert_eq!(client.read().unwrap(), Message::Text("Hello".into())); assert_eq!(server.read().unwrap(), Message::Pong(b"p".to_vec())); }
    #[test] fn the_close_handshake_ends_both_sides() { let (mut client, mut server) = pair(); client.close().unwrap(); assert!(matches!(client.write(Message::Text("late".into())), Err(Error::AlreadyClosed))); assert_eq!(server.read().unwrap(), Message::Close(None)); assert!(matches!(server.read(), Err(Error::ConnectionClosed))); assert_eq!(client.read().unwrap(), Message::Close(None)); assert!(matches!(client.read(), Err(Error::ConnectionClosed))); server.get_mut().shutdown(Shutdown::Both).unwrap(); }
    #[test] fn a_peer_breaking_the_protocol_is_refused() { let (mut client, mut server) = pair(); server.get_mut().write_all(&[0x82, 0x81, 1, 2, 3, 4, 5]).unwrap(); assert!(matches!(client.read(), Err(Error::Protocol(message)) if message.contains("masked"))); let (mut client, mut server) = pair(); server.get_mut().write_all(&[&[0x82, 127][..], &(MAX_FRAME as u64 + 1).to_be_bytes()].concat()).unwrap(); assert!(matches!(client.read(), Err(Error::Protocol(message)) if message.contains("frame of"))); let (mut client, mut server) = pair(); server.get_mut().write_all(&raw(true, CONTINUATION, b"x")).unwrap(); assert!(matches!(client.read(), Err(Error::Protocol(_)))); let (client, mut server) = pair(); let NodeStream::Plain(mut stream) = client.stream else { unreachable!() }; stream.write_all(&raw(true, BINARY, b"x")).unwrap(); assert!(matches!(server.read(), Err(Error::Protocol(message)) if message.contains("not masked"))); let (client, mut server) = pair(); drop(client); assert!(matches!(server.read(), Err(Error::Protocol(message)) if message.contains("without closing"))); }
    #[test] fn a_nonblocking_read_waits_for_a_whole_message() { let (mut client, mut server) = pair(); client.get_mut().tcp().set_nonblocking(true).unwrap(); assert!(client.read().unwrap_err().would_block()); let frame = raw(true, BINARY, b"abc"); server.get_mut().write_all(&frame[..3]).unwrap(); thread::sleep(Duration::from_millis(20)); assert!(client.read().unwrap_err().would_block()); server.get_mut().write_all(&frame[3..]).unwrap(); let started = Instant::now(); let message = loop { match client.read() { Err(error) if error.would_block() && started.elapsed() < Duration::from_secs(5) => thread::sleep(Duration::from_millis(1)), other => break other.unwrap() } }; assert_eq!(message, Message::Binary(b"abc".to_vec())); }
    #[test] fn wss_carries_messages_both_ways_and_refuses_an_unknown_authority() { let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let port = listener.local_addr().unwrap().port(); let server = thread::spawn(move || { let mut server = test_tls::accept(listener.accept().unwrap().0); while let Ok(message) = server.read() { if server.send(message).is_err() { break; } } }); let mut client = connect_with(&format!("wss://localhost:{port}/node"), test_tls::client()).unwrap(); assert!(matches!(client.get_ref(), NodeStream::Tls(_))); for length in [0, 125, 65_536, 3 * 1024 * 1024] { let bytes = (0..length).map(|index| (index % 251) as u8).collect::<Vec<_>>(); client.send(Message::Binary(bytes.clone())).unwrap(); assert_eq!(client.read().unwrap(), Message::Binary(bytes), "{length}"); } client.close().unwrap(); assert_eq!(client.read().unwrap(), Message::Close(None)); server.join().unwrap(); let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let port = listener.local_addr().unwrap().port(); let server = thread::spawn(move || { let _ = listener.accept(); }); assert!(matches!(connect(&format!("wss://127.0.0.1:{port}/node")), Err(Error::Io(_) | Error::Handshake(_)))); server.join().unwrap(); }
}
