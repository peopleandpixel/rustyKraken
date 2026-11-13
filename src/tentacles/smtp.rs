use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use mailparse::{MailHeaderMap, ParsedMail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Matching-Modi für SMTP-E-Mails
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub(crate) enum SmtpTopicMode {
    Subject {
        fallback_topic: String,
    },
    Sender {
        sender_pattern: String,
        topic: String,
        fallback_topic: String,
    },
    All {
        topic: String,
    },
}

/// Konfiguration für den SMTP-Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpTentacleConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_server_name")]
    pub server_name: String,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: u64,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: u64,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    pub topic_mode: SmtpTopicMode,
    #[serde(default)]
    pub sender_mappings: Option<HashMap<String, String>>,
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
    #[serde(default)]
    pub require_auth: bool,
    #[serde(default)]
    pub allowed_senders: Option<Vec<String>>,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    2525
}

fn default_server_name() -> String {
    "rustyKraken".to_string()
}

fn default_max_message_size() -> u64 {
    10 * 1024 * 1024
}

fn default_idle_timeout() -> u64 {
    300
}

fn default_max_connections() -> u32 {
    50
}

pub struct SmtpTentacle {
    name: String,
    config: SmtpTentacleConfig,
}

impl SmtpTentacle {
    pub fn new(name: String, config: SmtpTentacleConfig) -> Self {
        Self { name, config }
    }

    fn matches_pattern(text: &str, pattern: &str) -> bool {
        if pattern.contains('*') {
            let parts: Vec<&str> = pattern.splitn(2, '*').collect();
            let prefix = parts[0];
            let suffix = if parts.len() > 1 { parts[1] } else { "" };
            text.starts_with(prefix) && text.ends_with(suffix)
        } else {
            text == pattern
        }
    }

    fn determine_topic(&self, parsed: &ParsedMail) -> String {
        let headers = parsed.get_headers();
        let from = headers.get_first_value("From").unwrap_or_default();
        let subject = headers.get_first_value("Subject").unwrap_or_default();

        let mut topic = match &self.config.topic_mode {
            SmtpTopicMode::Subject { fallback_topic } => {
                if subject.starts_with('/') {
                    subject.trim_end_matches('/').to_string()
                } else {
                    fallback_topic.clone()
                }
            }
            SmtpTopicMode::Sender {
                sender_pattern,
                topic,
                fallback_topic,
            } => {
                if Self::matches_pattern(&from, sender_pattern) {
                    topic.clone()
                } else {
                    fallback_topic.clone()
                }
            }
            SmtpTopicMode::All { topic } => topic.clone(),
        };

        if let Some(mapped_topic) = self.check_sender_mapping(&from) {
            topic = mapped_topic;
        }
        topic
    }

    fn check_sender_mapping(&self, from: &str) -> Option<String> {
        if let Some(mappings) = &self.config.sender_mappings {
            if let Some(topic) = mappings.get(from) {
                return Some(topic.clone());
            }
            for (pattern, topic) in mappings {
                if Self::matches_pattern(from, pattern) {
                    return Some(topic.clone());
                }
            }
        }
        None
    }

    fn extract_message_id(parsed: &ParsedMail) -> String {
        let headers = parsed.get_headers();
        if let Some(message_id) = headers.get_first_value("Message-ID")
            && !message_id.is_empty()
        {
            return message_id;
        }
        let subject = headers.get_first_value("Subject").unwrap_or_default();
        let from = headers.get_first_value("From").unwrap_or_default();
        let date = headers.get_first_value("Date").unwrap_or_default();
        format!("fallback:{}:{}:{}", subject, from, date)
    }

    fn extract_body(parsed: &ParsedMail) -> String {
        if let Ok(body) = parsed.get_body()
            && !body.is_empty()
        {
            return body;
        }
        String::from_utf8_lossy(parsed.raw_bytes).to_string()
    }

    async fn start_server(&self, event_bus: Arc<EventBus>) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr)
            .await
            .context(format!("Fehler beim Binden des SMTP-Servers an {}", addr))?;

        info!("[{}] SMTP-Server gestartet auf {}", self.name, addr);

        let tls_acceptor = if self.config.tls_cert.is_some() && self.config.tls_key.is_some() {
            match self.create_tls_acceptor().await {
                Ok(acceptor) => {
                    info!("[{}] TLS ist konfiguriert", self.name);
                    Some(acceptor)
                }
                Err(e) => {
                    error!(
                        "[{}] Fehler beim Erstellen des TLS-Acceptors: {}",
                        self.name, e
                    );
                    None
                }
            }
        } else {
            None
        };

        if self.config.require_auth {
            info!("[{}] Authentifizierung ist erforderlich", self.name);
        }

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let event_bus = Arc::clone(&event_bus);
                    let config = self.config.clone();
                    let name = self.name.clone();
                    let tls_acceptor = tls_acceptor.clone();

                    let conn_name = name.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_connection(stream, addr, event_bus, config, name, tls_acceptor)
                                .await
                        {
                            error!("[{}] Fehler bei Verbindung von {}: {}", conn_name, addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("[{}] Fehler beim Akzeptieren: {}", self.name, e);
                }
            }
        }
    }

    async fn create_tls_acceptor(&self) -> Result<TlsAcceptor> {
        let cert_path = self.config.tls_cert.as_ref().unwrap();
        let key_path = self.config.tls_key.as_ref().unwrap();

        let cert_pem = tokio::fs::read(cert_path)
            .await
            .with_context(|| format!("Fehler beim Lesen des Zertifikats: {}", cert_path))?;
        let key_pem = tokio::fs::read(key_path)
            .await
            .with_context(|| format!("Fehler beim Lesen des Schlüssels: {}", key_path))?;

        let certs: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cert_pem.as_slice())
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("Fehler beim Parsen des Zertifikats")?;

        if certs.is_empty() {
            anyhow::bail!("Keine Zertifikate gefunden in {}", cert_path);
        }

        let key_opt = rustls_pemfile::private_key(&mut key_pem.as_slice())
            .context("Fehler beim Parsen des Schlüssels")?;

        let Some(key) = key_opt else {
            anyhow::bail!("Kein privater Schlüssel gefunden in {}", key_path);
        };

        let config = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("Fehler beim Erstellen der TLS-Konfiguration")?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

#[async_trait]
impl Tentacle for SmtpTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "smtp"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        self.start_server(event_bus).await
    }
}

// --- Connection handling ---

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    event_bus: Arc<EventBus>,
    config: SmtpTentacleConfig,
    name: String,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<()> {
    debug!("[{}] Neue Verbindung von {}", name, addr);

    // Hat der Server TLS-Zertifikate? Dann können wir STARTTLS anbieten
    let has_tls = tls_acceptor.is_some();

    let mut stream = match tls_acceptor {
        Some(acceptor) => {
            let tls_stream = acceptor
                .accept(stream)
                .await
                .context("TLS-Handshake fehlgeschlagen")?;
            SmtpStream::Tls(tls_stream)
        }
        None => SmtpStream::Plain(stream),
    };

    send_response(
        &mut stream,
        220,
        &format!("{} ESMTP rustyKraken", config.server_name),
    )
    .await?;

    let mut mail_from: Option<String> = None;
    let mut rcpt_to: Vec<String> = Vec::new();
    let mut authenticated = false;
    let mut line_buf = String::new();

    loop {
        line_buf.clear();
        match stream.read_line(&mut line_buf).await {
            Ok(0) => {
                debug!("[{}] Verbindung von {} geschlossen", name, addr);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                warn!("[{}] Fehler beim Lesen von {}: {}", name, addr, e);
                break;
            }
        };

        if line_buf.is_empty() {
            continue;
        }

        debug!("[{}] Befehl von {}: {}", name, addr, line_buf.trim());

        let result = process_command(
            &line_buf,
            addr,
            &name,
            &config,
            &mut stream,
            &mut mail_from,
            &mut rcpt_to,
            &mut authenticated,
            has_tls,
        )
        .await;

        match result {
            Ok(CmdResult::Continue) => {}
            Ok(CmdResult::StartTls) => {
                // STARTTLS wurde angefordert, Stream upgraden
                send_response(&mut stream, 220, "Ready to start TLS")
                    .await
                    .ok();

                // Stream muss von Plain sein für STARTTLS
                let plain_stream =
                    match std::mem::replace(&mut stream, SmtpStream::Plain(plain_dummy())) {
                        SmtpStream::Plain(s) => s,
                        SmtpStream::Tls(_) | SmtpStream::StartTls(_) => {
                            // Bereits TLS, sollte nicht passieren
                            warn!("[{}] STARTTLS auf bereits verschlüsselter Verbindung", name);
                            break;
                        }
                    };

                // TLS-Acceptor für STARTTLS erstellen
                let acceptor = match config.tls_cert.as_ref().and(config.tls_key.as_ref()) {
                    Some(_) => match create_tls_acceptor_from_config(&config).await {
                        Ok(a) => a,
                        Err(e) => {
                            error!(
                                "[{}] Fehler beim Erstellen des TLS-Acceptors für STARTTLS: {}",
                                name, e
                            );
                            send_response(&mut stream, 454, "TLS nicht verfügbar")
                                .await
                                .ok();
                            break;
                        }
                    },
                    None => {
                        warn!(
                            "[{}] STARTTLS angefordert, aber kein Zertifikat konfiguriert",
                            name
                        );
                        send_response(&mut stream, 454, "TLS nicht konfiguriert")
                            .await
                            .ok();
                        break;
                    }
                };

                // TLS-Handshake
                match acceptor.accept(plain_stream).await {
                    Ok(tls_stream) => {
                        info!("[{}] STARTTLS-Upgrade erfolgreich für {}", name, addr);
                        stream = SmtpStream::StartTls(tls_stream);
                    }
                    Err(e) => {
                        error!("[{}] STARTTLS-Handshake fehlgeschlagen: {}", name, e);
                        break;
                    }
                }

                // Nach STARTTLS-Upgrade muss der Client EHLO erneut senden
                continue;
            }
            Ok(CmdResult::Data) => {
                send_response(
                    &mut stream,
                    354,
                    "Nachricht akzeptieren, beende mit <CRLF>.<CRLF>",
                )
                .await
                .ok();

                match read_data(&mut stream, &name, &config).await {
                    Ok(raw_email) => {
                        if let Err(e) = process_email(&raw_email, &config, &name, &event_bus).await
                        {
                            error!("[{}] Fehler beim Verarbeiten: {}", name, e);
                        }
                        send_response(&mut stream, 250, "OK: Nachricht angenommen")
                            .await
                            .ok();
                    }
                    Err(e) => {
                        error!("[{}] Fehler beim Lesen der Daten: {}", name, e);
                        send_response(&mut stream, 451, "Fehler beim Lesen")
                            .await
                            .ok();
                    }
                }
                mail_from = None;
                rcpt_to.clear();
            }
            Ok(CmdResult::Quit) => {
                send_response(
                    &mut stream,
                    221,
                    &format!("{} sagt Auf Wiedersehen", config.server_name),
                )
                .await
                .ok();
                break;
            }
            Err(e) => {
                warn!("[{}] Fehler bei Befehl von {}: {}", name, addr, e);
            }
        }
    }

    Ok(())
}

/// Erzeugt einen dummy TcpStream für mem::replace (wird nie verwendet)
fn plain_dummy() -> TcpStream {
    // Diese Funktion sollte nie aufgerufen werden, da wir immer SmtpStream::Plain haben
    // wenn wir STARTTLS verarbeiten
    panic!("plain_dummy sollte nie aufgerufen werden")
}

/// Erstellt einen TLS-Acceptor aus der Konfiguration
async fn create_tls_acceptor_from_config(config: &SmtpTentacleConfig) -> Result<TlsAcceptor> {
    let cert_path = config.tls_cert.as_ref().unwrap();
    let key_path = config.tls_key.as_ref().unwrap();

    let cert_pem = tokio::fs::read(cert_path)
        .await
        .with_context(|| format!("Fehler beim Lesen des Zertifikats: {}", cert_path))?;
    let key_pem = tokio::fs::read(key_path)
        .await
        .with_context(|| format!("Fehler beim Lesen des Schlüssels: {}", key_path))?;

    let certs: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Fehler beim Parsen des Zertifikats")?;

    if certs.is_empty() {
        anyhow::bail!("Keine Zertifikate gefunden in {}", cert_path);
    }

    let key_opt = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .context("Fehler beim Parsen des Schlüssels")?;

    let Some(key) = key_opt else {
        anyhow::bail!("Kein privater Schlüssel gefunden in {}", key_path);
    };

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Fehler beim Erstellen der TLS-Konfiguration")?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

enum CmdResult {
    Continue,
    Data,
    Quit,
    StartTls,
}

/// Wrapper für TLS und plain Streams
enum SmtpStream {
    Plain(TcpStream),
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
    /// Stream der nach STARTTLS auf TLS upgegradet wurde
    StartTls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl SmtpStream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            SmtpStream::Plain(s) => s.write_all(buf).await,
            SmtpStream::Tls(s) => s.write_all(buf).await,
            SmtpStream::StartTls(s) => s.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SmtpStream::Plain(s) => s.flush().await,
            SmtpStream::Tls(s) => s.flush().await,
            SmtpStream::StartTls(s) => s.flush().await,
        }
    }

    async fn read_line(&mut self, buf: &mut String) -> std::io::Result<usize> {
        match self {
            SmtpStream::Plain(s) => {
                let mut reader = BufReader::new(s);
                reader.read_line(buf).await
            }
            SmtpStream::Tls(s) => {
                let mut reader = BufReader::new(s);
                reader.read_line(buf).await
            }
            SmtpStream::StartTls(s) => {
                let mut reader = BufReader::new(s);
                reader.read_line(buf).await
            }
        }
    }
}

async fn send_response(stream: &mut SmtpStream, code: u16, message: &str) -> Result<()> {
    let response = format!("{} {}\r\n", code, message);
    stream
        .write_all(response.as_bytes())
        .await
        .context("Fehler beim Senden der SMTP-Antwort")?;
    stream.flush().await.context("Fehler beim Flush")?;
    Ok(())
}

async fn read_data(
    stream: &mut SmtpStream,
    name: &str,
    config: &SmtpTentacleConfig,
) -> Result<Vec<u8>> {
    let mut raw_email = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        match stream.read_line(&mut line).await {
            Ok(0) => return Err(anyhow::anyhow!("Verbindung geschlossen während DATA")),
            Ok(_) => {}
            Err(e) => return Err(anyhow::anyhow!("Fehler beim Lesen: {}", e)),
        };

        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        if trimmed == "." {
            debug!("[{}] Endemarke erreicht, {} Bytes", name, raw_email.len());
            break;
        }

        let line_bytes = if trimmed.starts_with('.') && trimmed.len() > 1 {
            trimmed.as_bytes()[1..].to_vec()
        } else {
            trimmed.as_bytes().to_vec()
        };

        raw_email.extend_from_slice(&line_bytes);
        raw_email.extend_from_slice(b"\r\n");

        if raw_email.len() as u64 > config.max_message_size {
            return Err(anyhow::anyhow!("Nachricht zu groß"));
        }
    }

    Ok(raw_email)
}

#[allow(clippy::too_many_arguments)]
async fn process_command(
    line: &str,
    addr: SocketAddr,
    name: &str,
    config: &SmtpTentacleConfig,
    stream: &mut SmtpStream,
    mail_from: &mut Option<String>,
    rcpt_to: &mut Vec<String>,
    authenticated: &mut bool,
    has_tls: bool,
) -> Result<CmdResult> {
    let line = line.trim().to_string();
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    let command = parts[0].to_uppercase();
    let argument = if parts.len() > 1 {
        parts[1].trim().to_string()
    } else {
        String::new()
    };

    match command.as_str() {
        "HELO" => {
            send_response(stream, 250, &format!("Hallo {}", addr.ip())).await?;
        }
        "EHLO" => {
            let caps = if has_tls {
                format!(
                    "{} ESMTP\r\nSIZE {}\r\n8BITMIME\r\nSTARTTLS\r\nAUTH PLAIN",
                    config.server_name, config.max_message_size
                )
            } else {
                format!(
                    "{} ESMTP\r\nSIZE {}\r\n8BITMIME\r\nAUTH PLAIN",
                    config.server_name, config.max_message_size
                )
            };
            send_response(stream, 250, &caps).await?;
        }
        "STARTTLS" => {
            if has_tls {
                info!("[{}] STARTTLS angefordert von {}", name, addr);
                return Ok(CmdResult::StartTls);
            } else {
                send_response(stream, 502, "STARTTLS nicht verfügbar").await?;
            }
        }
        "AUTH" => {
            if !config.require_auth {
                send_response(stream, 503, "Authentifizierung nicht erforderlich").await?;
            } else if let Some(result) = parse_auth(&line) {
                match result {
                    Ok((user, _)) => {
                        *authenticated = true;
                        info!("[{}] Auth erfolgreich: {}", name, user);
                        send_response(stream, 235, "Auth erfolgreich").await?;
                    }
                    Err(e) => {
                        warn!("[{}] Auth Fehler: {}", name, e);
                        send_response(stream, 535, "Auth fehlgeschlagen").await?;
                    }
                }
            } else {
                send_response(stream, 502, "AUTH-Methode nicht unterstützt").await?;
            }
        }
        "MAIL" => {
            if config.require_auth && !*authenticated {
                send_response(stream, 530, "Auth erforderlich").await?;
                return Ok(CmdResult::Continue);
            }
            if let Some(from) = extract_mail_from(&argument) {
                if !is_sender_allowed(config, &from) {
                    send_response(stream, 550, "Absender nicht erlaubt").await?;
                    return Ok(CmdResult::Continue);
                }
                *mail_from = Some(from);
                send_response(stream, 250, "OK").await?;
            } else {
                send_response(stream, 501, "Syntaxfehler in MAIL FROM").await?;
            }
        }
        "RCPT" => {
            if config.require_auth && !*authenticated {
                send_response(stream, 530, "Auth erforderlich").await?;
                return Ok(CmdResult::Continue);
            }
            if mail_from.is_none() {
                send_response(stream, 503, "Zuerst MAIL FROM").await?;
                return Ok(CmdResult::Continue);
            }
            if let Some(to) = extract_rcpt_to(&argument) {
                rcpt_to.push(to);
                send_response(stream, 250, "OK").await?;
            } else {
                send_response(stream, 501, "Syntaxfehler in RCPT TO").await?;
            }
        }
        "DATA" => {
            if mail_from.is_none() || rcpt_to.is_empty() {
                send_response(stream, 503, "Zuerst MAIL FROM und RCPT TO").await?;
                return Ok(CmdResult::Continue);
            }
            return Ok(CmdResult::Data);
        }
        "QUIT" => return Ok(CmdResult::Quit),
        "RSET" => {
            *mail_from = None;
            rcpt_to.clear();
            send_response(stream, 250, "OK").await?;
        }
        "NOOP" => {
            send_response(stream, 250, "OK").await?;
        }
        _ => {
            send_response(stream, 502, "Unbekannter Befehl").await?;
        }
    }

    Ok(CmdResult::Continue)
}

fn parse_auth(line: &str) -> Option<Result<(String, String), String>> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    match parts[1].to_uppercase().as_str() {
        "PLAIN" => {
            if parts.len() < 3 {
                return Some(Err("Fehlende AUTH-Daten".to_string()));
            }
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(parts[2])
                .ok()?;
            let s = String::from_utf8(decoded).ok()?;
            let p: Vec<&str> = s.split('\0').collect();
            if p.len() >= 2 {
                Some(Ok((p[1].to_string(), p.get(2).unwrap_or(&"").to_string())))
            } else {
                Some(Err("Ungültiges AUTH-Format".to_string()))
            }
        }
        "LOGIN" => Some(Err("LOGIN nicht unterstützt".to_string())),
        _ => None,
    }
}

fn extract_mail_from(arg: &str) -> Option<String> {
    let arg = arg.trim();
    if let Some(start) = arg.find('<')
        && let Some(end) = arg.find('>')
    {
        return Some(arg[start + 1..end].trim().to_string());
    }
    if let Some(stripped) = arg.strip_prefix("FROM:") {
        let email = stripped.trim();
        if !email.is_empty() {
            return Some(email.to_string());
        }
    }
    None
}

fn extract_rcpt_to(arg: &str) -> Option<String> {
    let arg = arg.trim();
    if let Some(start) = arg.find('<')
        && let Some(end) = arg.find('>')
    {
        return Some(arg[start + 1..end].trim().to_string());
    }
    if let Some(stripped) = arg.strip_prefix("TO:") {
        let email = stripped.trim();
        if !email.is_empty() {
            return Some(email.to_string());
        }
    }
    None
}

fn is_sender_allowed(config: &SmtpTentacleConfig, from: &str) -> bool {
    if let Some(allowed) = &config.allowed_senders {
        if allowed.is_empty() {
            return true;
        }
        allowed
            .iter()
            .any(|p| SmtpTentacle::matches_pattern(from, p))
    } else {
        true
    }
}

async fn process_email(
    raw_email: &[u8],
    config: &SmtpTentacleConfig,
    name: &str,
    event_bus: &EventBus,
) -> Result<()> {
    if raw_email.is_empty() {
        return Ok(());
    }

    let parsed = mailparse::parse_mail(raw_email).context("Fehler beim Parsen der E-Mail")?;

    let headers = parsed.get_headers();
    let from = headers.get_first_value("From").unwrap_or_default();
    let subject = headers.get_first_value("Subject").unwrap_or_default();
    let message_id = SmtpTentacle::extract_message_id(&parsed);
    let body = SmtpTentacle::extract_body(&parsed);

    let tentacle = SmtpTentacle::new(name.to_string(), config.clone());
    let topic = tentacle.determine_topic(&parsed);

    info!(
        "[{}] E-Mail: von='{}', betreff='{}', topic='{}'",
        name, from, subject, topic
    );

    let ink = Ink::new(topic, "smtp", name, body).with_metadata(serde_json::json!({
        "from": from,
        "subject": subject,
        "message_id": message_id,
    }));

    event_bus.publish(ink);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_matches_pattern_exact() {
        assert!(SmtpTentacle::matches_pattern(
            "user@example.com",
            "user@example.com"
        ));
        assert!(!SmtpTentacle::matches_pattern(
            "user@example.com",
            "other@example.com"
        ));
    }

    #[test]
    fn test_matches_pattern_wildcard() {
        assert!(SmtpTentacle::matches_pattern(
            "user@example.com",
            "*@example.com"
        ));
        assert!(SmtpTentacle::matches_pattern(
            "alerts@example.com",
            "alerts@*"
        ));
        assert!(!SmtpTentacle::matches_pattern(
            "user@other.com",
            "*@example.com"
        ));
    }

    #[test]
    fn test_extract_mail_from() {
        assert_eq!(
            extract_mail_from("FROM: <user@example.com>"),
            Some("user@example.com".to_string())
        );
        assert_eq!(extract_mail_from("INVALID"), None);
    }

    #[test]
    fn test_extract_rcpt_to() {
        assert_eq!(
            extract_rcpt_to("TO: <recipient@example.com>"),
            Some("recipient@example.com".to_string())
        );
        assert_eq!(extract_rcpt_to("INVALID"), None);
    }
}
