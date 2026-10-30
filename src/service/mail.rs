//! Mail submission to a remote SMTP server (`mailsend` feature).
//!
//! The node hosting the `mail` capability holds the server settings; the Gateway picks that
//! node from the Place providers, else from the Fabric (system) default.
//! `OGD_MAIL_URL` is `smtps://host[:465]` (implicit TLS), `smtp://host[:587]` (STARTTLS required)
//! or `smtp+plain://host[:25]` (no TLS, local relays only); `OGD_MAIL_FROM` is the sender mailbox,
//! `OGD_MAIL_USER`/`OGD_MAIL_PASSWORD` enable AUTH (PLAIN, else LOGIN, as the server offers) and `OGD_MAIL_TIMEOUT_SECS` bounds each step.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::helpers::{encode_base64, hex};
use crate::operation::{mailbox_address, MailSendInput};
use crate::query::dates::civil_from_days;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Security { Implicit, StartTls, Plain }

#[derive(Debug)]
pub struct MailError { code: &'static str, message: String }

impl MailError {
    #[must_use]
    pub const fn code(&self) -> &'static str { self.code }
}

impl fmt::Display for MailError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str(&self.message) }
}

impl std::error::Error for MailError {}

fn fail(code: &'static str, message: impl Into<String>) -> MailError { MailError { code, message: message.into() } }

fn io_fail(error: &io::Error) -> MailError { fail("mail.connection", format!("SMTP connection failed: {error}")) }

pub struct MailService {
    security: Security,
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
    from: String,
    timeout: Duration,
    tls: Arc<ClientConfig>,
}

impl fmt::Debug for MailService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("MailService").field("host", &self.host).field("port", &self.port).field("security", &self.security).finish_non_exhaustive()
    }
}

impl MailService {
    pub fn from_environment() -> Result<Self, MailError> {
        let var = crate::helpers::env_text;
        let url = var("OGD_MAIL_URL").ok_or_else(|| fail("mail.config", "OGD_MAIL_URL is required: smtps://host[:465], smtp://host[:587] or smtp+plain://host[:25]"))?;
        let from = var("OGD_MAIL_FROM").ok_or_else(|| fail("mail.config", "OGD_MAIL_FROM is required"))?;
        let timeout = var("OGD_MAIL_TIMEOUT_SECS").map_or(Ok(30), |value| value.parse::<u64>().ok().filter(|secs| *secs > 0).ok_or_else(|| fail("mail.config", "OGD_MAIL_TIMEOUT_SECS must be a positive integer")))?;
        let credentials = var("OGD_MAIL_USER").map(|user| (user, std::env::var("OGD_MAIL_PASSWORD").unwrap_or_default()));
        Self::new(&url, credentials, from, Duration::from_secs(timeout))
    }

    fn new(url: &str, credentials: Option<(String, String)>, from: String, timeout: Duration) -> Result<Self, MailError> {
        let (scheme, authority) = url.split_once("://").ok_or_else(|| fail("mail.config", format!("invalid OGD_MAIL_URL {url:?}")))?;
        let (security, default_port) = match scheme.to_ascii_lowercase().as_str() {
            "smtps" => (Security::Implicit, 465),
            "smtp" => (Security::StartTls, 587),
            "smtp+plain" => (Security::Plain, 25),
            other => return Err(fail("mail.config", format!("unsupported mail scheme {other:?}"))),
        };
        let authority = authority.trim_end_matches('/');
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host, port.parse().map_err(|_| fail("mail.config", format!("invalid port in OGD_MAIL_URL {url:?}")))?),
            None => (authority, default_port),
        };
        if host.is_empty() { return Err(fail("mail.config", format!("missing host in OGD_MAIL_URL {url:?}"))); }
        if mailbox_address(&from).is_none() { return Err(fail("mail.config", format!("invalid OGD_MAIL_FROM {from:?}"))); }
        if credentials.is_some() && security == Security::Plain { return Err(fail("mail.config", "OGD_MAIL_USER needs smtps:// or smtp:// (STARTTLS): credentials never travel in clear")); }
        let roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let tls = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().map_err(|error| fail("mail.config", error.to_string()))?
            .with_root_certificates(roots).with_no_client_auth();
        Ok(Self { security, host: host.trim_matches(['[', ']']).to_owned(), port, credentials, from, timeout, tls: Arc::new(tls) })
    }

    /// Submits one message and returns its `Message-ID`.
    pub fn send(&self, input: &MailSendInput) -> Result<String, MailError> {
        let domain = mailbox_address(&self.from).and_then(|address| address.rsplit_once('@')).map_or("localhost", |(_, domain)| domain);
        let mut token = [0_u8; 16];
        getrandom::fill(&mut token).map_err(|error| fail("mail.internal", error.to_string()))?;
        let message_id = format!("<{}@{domain}>", hex(&token));
        let message = self.message(input, &message_id, &hex(&token[..8]), SystemTime::now());

        let mut session = Session::new(self.connect()?);
        if self.security == Security::Implicit { session = session.secure(self)?; }
        session.reply(b'2', "mail.connection")?;
        let mut capabilities = session.command(&format!("EHLO {domain}"), b'2', "mail.connection")?;
        if self.security == Security::StartTls {
            if !capabilities.lines().any(|line| line.get(4..).is_some_and(|extension| extension.trim().eq_ignore_ascii_case("STARTTLS"))) {
                return Err(fail("mail.tls", format!("{} does not offer STARTTLS", self.host)));
            }
            session.command("STARTTLS", b'2', "mail.tls")?;
            session = session.secure(self)?;
            capabilities = session.command(&format!("EHLO {domain}"), b'2', "mail.connection")?;
        }
        if let Some((user, password)) = &self.credentials {
            let mechanisms = auth_mechanisms(&capabilities);
            if mechanisms.iter().any(|mechanism| mechanism == "PLAIN") {
                session.command(&format!("AUTH PLAIN {}", encode_base64(format!("\0{user}\0{password}").as_bytes())), b'2', "mail.auth")?;
            } else if mechanisms.iter().any(|mechanism| mechanism == "LOGIN") {
                session.command("AUTH LOGIN", b'3', "mail.auth")?;
                session.command(&encode_base64(user.as_bytes()), b'3', "mail.auth")?;
                session.command(&encode_base64(password.as_bytes()), b'2', "mail.auth")?;
            } else if mechanisms.is_empty() {
                return Err(fail("mail.auth", format!("{} offers no authentication", self.host)));
            } else {
                return Err(fail("mail.auth", format!("{} offers AUTH {} only; PLAIN and LOGIN are supported", self.host, mechanisms.join(" "))));
            }
        }
        session.command(&format!("MAIL FROM:<{}>", mailbox_address(&self.from).unwrap_or_default()), b'2', "mail.rejected")?;
        for mailbox in input.to.iter().chain(&input.cc).chain(&input.bcc) {
            session.command(&format!("RCPT TO:<{}>", mailbox_address(mailbox).unwrap_or_default()), b'2', "mail.rejected")?;
        }
        session.command("DATA", b'3', "mail.rejected")?;
        // Bodies are base64 and headers start with a name: no line can begin with a dot.
        session.command(&format!("{message}."), b'2', "mail.rejected")?;
        let _ = session.command("QUIT", b'2', "mail.connection");
        Ok(message_id)
    }

    fn connect(&self) -> Result<TcpStream, MailError> {
        let address = (self.host.as_str(), self.port).to_socket_addrs().map_err(|error| io_fail(&error))?.next()
            .ok_or_else(|| fail("mail.connection", format!("{} does not resolve", self.host)))?;
        let stream = TcpStream::connect_timeout(&address, self.timeout).map_err(|error| io_fail(&error))?;
        stream.set_read_timeout(Some(self.timeout)).and_then(|()| stream.set_write_timeout(Some(self.timeout))).map_err(|error| io_fail(&error))?;
        Ok(stream)
    }

    fn message(&self, input: &MailSendInput, message_id: &str, boundary: &str, now: SystemTime) -> String {
        let mut message = format!("Date: {}\r\nFrom: {}\r\nMessage-ID: {message_id}\r\nMIME-Version: 1.0\r\nSubject: {}\r\n", rfc5322_date(now), encode_mailbox(&self.from), encode_header(&input.subject));
        for (name, mailboxes) in [("To", input.to.as_slice()), ("Cc", input.cc.as_slice())].into_iter().chain(input.reply_to.as_ref().map(|reply_to| ("Reply-To", std::slice::from_ref(reply_to)))) {
            if !mailboxes.is_empty() { message += &format!("{name}: {}\r\n", mailboxes.iter().map(|mailbox| encode_mailbox(mailbox)).collect::<Vec<_>>().join(",\r\n ")); }
        }
        match &input.html {
            None => push_part(&mut message, "text/plain", &input.text),
            Some(html) => {
                message += &format!("Content-Type: multipart/alternative; boundary=\"og-{boundary}\"\r\n\r\n");
                for (kind, body) in [("text/plain", input.text.as_str()), ("text/html", html)] {
                    message += &format!("--og-{boundary}\r\n");
                    push_part(&mut message, kind, body);
                }
                message += &format!("--og-{boundary}--\r\n");
            }
        }
        message
    }
}

fn push_part(message: &mut String, kind: &str, body: &str) {
    *message += &format!("Content-Type: {kind}; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n");
    for line in encode_base64(body.as_bytes()).as_bytes().chunks(76) {
        *message += std::str::from_utf8(line).expect("base64 is ASCII");
        *message += "\r\n";
    }
}

/// RFC 2047 encoded words of at most 45 bytes of UTF-8 each (60 base64 characters).
fn encode_header(value: &str) -> String {
    if value.is_ascii() { return value.to_owned(); }
    let mut words = Vec::new();
    let mut start = 0;
    for (index, character) in value.char_indices() {
        if index + character.len_utf8() - start > 45 { words.push(&value[start..index]); start = index; }
    }
    words.push(&value[start..]);
    words.iter().map(|word| format!("=?UTF-8?B?{}?=", encode_base64(word.as_bytes()))).collect::<Vec<_>>().join("\r\n ")
}

fn encode_mailbox(mailbox: &str) -> String {
    match mailbox.rsplit_once('<') {
        Some((name, address)) if !name.trim().is_empty() => format!("{} <{address}", encode_header(name.trim())),
        _ => mailbox.to_owned(),
    }
}

fn rfc5322_date(now: SystemTime) -> String {
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    let seconds = now.duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs()) as i64;
    let (days, time) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!("{}, {day} {} {year} {:02}:{:02}:{:02} +0000", WEEKDAYS[days.rem_euclid(7) as usize], MONTHS[month as usize - 1], time / 3600, time / 60 % 60, time % 60)
}

enum Stream { Plain(TcpStream), Tls(Box<StreamOwned<ClientConnection, TcpStream>>) }

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self { Self::Plain(stream) => stream.read(buffer), Self::Tls(stream) => stream.read(buffer) }
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self { Self::Plain(stream) => stream.write(buffer), Self::Tls(stream) => stream.write(buffer) }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self { Self::Plain(stream) => stream.flush(), Self::Tls(stream) => stream.flush() }
    }
}

/// The mechanisms of the `AUTH` extension in an EHLO reply (`250-AUTH LOGIN XOAUTH2`, or the
/// older `250-AUTH=LOGIN`), upper-cased.
fn auth_mechanisms(capabilities: &str) -> Vec<String> {
    capabilities.lines()
        .filter_map(|line| line.get(4..))
        .filter_map(|extension| extension.get(..5).filter(|keyword| keyword.eq_ignore_ascii_case("AUTH ") || keyword.eq_ignore_ascii_case("AUTH=")).map(|_| &extension[5..]))
        .flat_map(str::split_whitespace)
        .map(str::to_ascii_uppercase)
        .collect()
}

struct Session { stream: BufReader<Stream> }

impl Session {
    fn new(stream: TcpStream) -> Self { Self { stream: BufReader::new(Stream::Plain(stream)) } }

    fn secure(self, service: &MailService) -> Result<Self, MailError> {
        let Stream::Plain(stream) = self.stream.into_inner() else { return Err(fail("mail.tls", "TLS is already active")) };
        let name = ServerName::try_from(service.host.clone()).map_err(|error| fail("mail.config", format!("invalid TLS server name: {error}")))?;
        let connection = ClientConnection::new(Arc::clone(&service.tls), name).map_err(|error| fail("mail.tls", error.to_string()))?;
        Ok(Self { stream: BufReader::new(Stream::Tls(Box::new(StreamOwned::new(connection, stream)))) })
    }

    /// Reads one (possibly multiline) reply and checks its class digit.
    fn reply(&mut self, expected: u8, code: &'static str) -> Result<String, MailError> {
        let mut reply = String::new();
        loop {
            let mut line = String::new();
            if (&mut self.stream).take(4096).read_line(&mut line).map_err(|error| io_fail(&error))? == 0 {
                return Err(fail("mail.connection", "the SMTP server closed the connection"));
            }
            reply += &line;
            if line.as_bytes().get(3) != Some(&b'-') { break; }
            if reply.len() > 65_536 { return Err(fail("mail.connection", "SMTP reply too long")); }
        }
        if reply.as_bytes().first() == Some(&expected) { Ok(reply) } else { Err(fail(code, format!("SMTP server replied: {}", reply.trim_end()))) }
    }

    fn command(&mut self, line: &str, expected: u8, code: &'static str) -> Result<String, MailError> {
        let stream = self.stream.get_mut();
        stream.write_all(line.as_bytes()).and_then(|()| stream.write_all(b"\r\n")).and_then(|()| stream.flush()).map_err(|error| io_fail(&error))?;
        self.reply(expected, code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::place::RequestedExecutionContext;
    use std::net::TcpListener;

    fn input(html: Option<&str>) -> MailSendInput { MailSendInput { context: RequestedExecutionContext { place_id: "p".into(), app_instance_id: None, as_profile: None }, to: vec!["Zoé <zoe@example.test>".into()], cc: vec![], bcc: vec!["audit@example.test".into()], reply_to: None, subject: "Relevé été".into(), text: "Bonjour".into(), html: html.map(Into::into) } }

    /// Plays a minimal SMTP server on one connection and returns the client lines.
    fn fake_server(replies: &'static [&'static str]) -> (u16, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (port, std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut lines = Vec::new();
            stream.write_all(b"220 fake ESMTP\r\n").unwrap();
            let mut in_data = false;
            for reply in replies {
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap() == 0 { return lines; }
                    let line = line.trim_end().to_owned();
                    if in_data && line != "." { lines.push(format!("data:{line}")); continue; }
                    in_data = line == "DATA";
                    lines.push(line);
                    break;
                }
                stream.write_all(reply.as_bytes()).unwrap();
            }
            lines
        }))
    }

    #[test] fn sends_one_message_through_a_plain_relay() { let (port, server) = fake_server(&["250-fake\r\n250 8BITMIME\r\n", "250 ok\r\n", "250 ok\r\n", "250 ok\r\n", "354 go\r\n", "250 queued\r\n", "221 bye\r\n"]); let service = MailService::new(&format!("smtp+plain://127.0.0.1:{port}"), None, "OG <noreply@og.test>".into(), Duration::from_secs(5)).unwrap(); let id = service.send(&input(None)).unwrap(); let lines = server.join().unwrap(); assert!(id.ends_with("@og.test>")); assert_eq!(&lines[..5], ["EHLO og.test", "MAIL FROM:<noreply@og.test>", "RCPT TO:<zoe@example.test>", "RCPT TO:<audit@example.test>", "DATA"]); assert_eq!(lines.last().unwrap(), "QUIT"); assert!(lines.contains(&"data:Subject: =?UTF-8?B?UmVsZXbDqSDDqXTDqQ==?=".to_owned())); assert!(lines.contains(&format!("data:{}", encode_base64("Bonjour".as_bytes())))); assert!(!lines.iter().any(|line| line.starts_with("data:") && line.contains("audit@"))); }
    #[test] fn server_rejections_keep_their_code() { let (port, server) = fake_server(&["250 fake\r\n", "550 sender refused\r\n", "221 bye\r\n"]); let service = MailService::new(&format!("smtp+plain://127.0.0.1:{port}"), None, "noreply@og.test".into(), Duration::from_secs(5)).unwrap(); let error = service.send(&input(None)).unwrap_err(); assert_eq!(error.code(), "mail.rejected"); assert!(error.to_string().contains("550 sender refused")); drop(server); }
    /// A relay with credentials: only possible in tests, the configuration refuses them in clear.
    fn authenticated(port: u16) -> MailService { let mut service = MailService::new(&format!("smtp+plain://127.0.0.1:{port}"), None, "noreply@og.test".into(), Duration::from_secs(5)).unwrap(); service.credentials = Some(("u".into(), "p".into())); service }
    #[test] fn auth_plain_when_offered() { let (port, server) = fake_server(&["250-fake\r\n250 AUTH LOGIN PLAIN\r\n", "235 ok\r\n", "250 ok\r\n", "250 ok\r\n", "250 ok\r\n", "354 go\r\n", "250 queued\r\n", "221 bye\r\n"]); authenticated(port).send(&input(None)).unwrap(); let lines = server.join().unwrap(); assert_eq!(lines[1], format!("AUTH PLAIN {}", encode_base64(b"\0u\0p"))); assert_eq!(lines[2], "MAIL FROM:<noreply@og.test>"); }
    #[test] fn auth_login_when_plain_is_not_offered() { let (port, server) = fake_server(&["250-fake\r\n250-AUTH LOGIN XOAUTH2\r\n250 8BITMIME\r\n", "334 VXNlcm5hbWU6\r\n", "334 UGFzc3dvcmQ6\r\n", "235 ok\r\n", "250 ok\r\n", "250 ok\r\n", "250 ok\r\n", "354 go\r\n", "250 queued\r\n", "221 bye\r\n"]); authenticated(port).send(&input(None)).unwrap(); let lines = server.join().unwrap(); assert_eq!(&lines[1..5], ["AUTH LOGIN".to_owned(), encode_base64(b"u"), encode_base64(b"p"), "MAIL FROM:<noreply@og.test>".to_owned()]); }
    #[test] fn auth_fails_clearly_without_a_known_mechanism() { for (replies, message) in [(&["250-fake\r\n250 AUTH XOAUTH2\r\n", "221 bye\r\n"], "offers AUTH XOAUTH2 only"), (&["250 fake\r\n", "221 bye\r\n"], "offers no authentication")] { let (port, server) = fake_server(replies); let error = authenticated(port).send(&input(None)).unwrap_err(); assert_eq!(error.code(), "mail.auth"); assert!(error.to_string().contains(message), "{error}"); drop(server); } }
    #[test] fn auth_mechanisms_read_both_forms() { assert_eq!(auth_mechanisms("250-host\r\n250-AUTH=LOGIN\r\n250-auth login plain\r\n250 SIZE 1\r\n"), ["LOGIN", "LOGIN", "PLAIN"]); assert!(auth_mechanisms("250-host\r\n250-AUTHX\r\n250 8BITMIME\r\n").is_empty()); }
    #[test] fn starttls_is_required_on_submission() { let (port, server) = fake_server(&["250 fake\r\n", "221 bye\r\n"]); let service = MailService::new(&format!("smtp://127.0.0.1:{port}"), None, "noreply@og.test".into(), Duration::from_secs(5)).unwrap(); assert_eq!(service.send(&input(None)).unwrap_err().code(), "mail.tls"); drop(server); }
    #[test] fn configuration_refuses_clear_credentials_and_bad_urls() { let credentials = Some(("u".to_owned(), "p".to_owned())); assert!(MailService::new("smtp+plain://relay", credentials.clone(), "a@b.c".into(), Duration::from_secs(1)).is_err()); assert!(MailService::new("http://relay", None, "a@b.c".into(), Duration::from_secs(1)).is_err()); assert!(MailService::new("smtps://relay", None, "not-a-mailbox".into(), Duration::from_secs(1)).is_err()); let service = MailService::new("smtps://mail.example.test", credentials, "a@b.c".into(), Duration::from_secs(1)).unwrap(); assert_eq!((service.port, service.security), (465, Security::Implicit)); }
    #[test] fn html_messages_are_multipart_alternative() { let service = MailService::new("smtps://mail.example.test", None, "a@b.c".into(), Duration::from_secs(1)).unwrap(); let message = service.message(&input(Some("<p>Bonjour</p>")), "<id@b.c>", "42", UNIX_EPOCH); assert!(message.starts_with("Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n")); assert!(message.contains("To: =?UTF-8?B?Wm/DqQ==?= <zoe@example.test>\r\n")); assert!(!message.contains("audit@")); assert!(message.contains("multipart/alternative; boundary=\"og-42\"")); assert_eq!(message.matches("--og-42").count(), 3); assert!(!message.lines().any(|line| line.starts_with('.'))); }
    #[test] fn long_subjects_fold_into_short_encoded_words() { let encoded = encode_header(&"é".repeat(40)); assert_eq!(encoded.matches("=?UTF-8?B?").count(), 2); assert!(encoded.split("\r\n ").all(|word| word.len() <= 75)); }
    #[test] fn mailboxes_refuse_injection() { assert_eq!(mailbox_address("Zoé <zoe@example.test>"), Some("zoe@example.test")); assert_eq!(mailbox_address(" a@b.c "), Some("a@b.c")); for bad in ["a@b.c\r\nRCPT TO:<x@y.z>", "a@b.c>", "no-at", "a b@c.d", "@b.c", "a@"] { assert_eq!(mailbox_address(bad), None, "{bad:?}"); } }
}
