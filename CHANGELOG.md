# Changelog

All notable changes to rustyKraken will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.3.0] - 2025-11-11

### Added

#### Tentacles
- **ntfy Tentacle**: Subscribe to ntfy push notification topics with:
  - JSON Stream connection (HTTP long-polling)
  - WebSocket connection support
  - Topic mapping from ntfy topics to rustyKraken topics
  - Authentication (Basic Auth and Bearer Token)
  - Automatic reconnection with configurable delay
  - ntfy metadata (Title, Priority, Tags, Click, Icon) as Ink metadata
  - 5 unit tests

### Changed
- **Crate Updates**:
  - Added `http 1.3` for WebSocket request building
  - Added `tokio-tungstenite 0.26` for WebSocket support
  - Enabled `stream` feature for `reqwest 0.13`

## [0.2.0] - 2025-11-11

### Added

#### Core Architecture
- **Event Bus**: Pub/Sub system with topic matching and wildcard support (`**` and `*`)
- **Ink Model**: Unified message format with UUID, topic, source, body, metadata, and status
- **Mantle**: SQLite-based storage for all messages with indexing by topic and status
- **Config System**: YAML-based loader for `config/incoming/` (Tentacles) and `config/listener/` (Reflexes) with hot-reload preparation

#### Tentacles (Incoming Sources)
- **HTTP/Webhook Tentacle**: HTTP server with Axum, accepts POST requests with JSON body
- **MQTT Tentacle**: Full MQTT client implementation with:
  - TLS support (rustls, custom CA, mTLS)
  - Last-Will messages
  - Clean session configuration
  - Automatic reconnection with resubscription
  - Topic mapping with MQTT wildcards (`+` and `#`)
- **IMAP Tentacle**: Email reception via IMAP with:
  - Implicit TLS (port 993)
  - STARTTLS (port 143)
  - Three topic modes: Subject, Sender, All
  - Sender mappings with placeholders
  - UID-based tracking with persistent storage
  - IDLE mode for real-time notifications
  - Email parsing with mailparse (text/plain, text/html, multipart)
- **POP3 Tentacle**: Email reception via POP3 with:
  - Implicit TLS (port 995)
  - STARTTLS (port 110)
  - Plain connection (port 110)
  - Three topic modes: Subject, Sender, All
  - Sender mappings with placeholders
  - Message-ID-based deduplication with persistent storage
  - Custom POP3 protocol implementation (async crates were broken)
- **SMTP Tentacle**: SMTP server for receiving emails with:
  - Implicit TLS (port 465)
  - STARTTLS support
  - AUTH PLAIN authentication
  - Dot-stuffing decoding
  - Three topic modes: Subject, Sender, All
  - Sender mappings with placeholders
  - Allowed sender filtering
  - Configurable maximum message size

#### Reflexes (Actions)
- **File Reflex**: JSON logging to files with append mode
- **Command Reflex**: Execute shell commands with environment variables (INK_JSON, INK_TOPIC, INK_BODY, INK_ID)

### Changed
- **Crate Updates**: All external crates updated to latest versions
  - `serde_yaml` → `yaml_serde 0.10.7` (RUSTSEC-2025-0068)
  - `sqlx 0.8` → `0.9`
  - `rumqttc 0.24` → `0.25`
  - `reqwest 0.12` → `0.13`
- **Renaming**: Replaced all occurrences of "octopus" with "kraken"

### Fixed
- **Clippy Warnings**: Fixed all 32 clippy warnings (collapsible_if, single_match, unnecessary_map_or, manual_strip, needless_borrow, sliced_string_as_bytes, too_many_arguments)
- **Compiler Warnings**: Fixed all 20 compiler warnings (unused imports, unused variables, private interface, dead code)

### Security
- TLS support for all email protocols (IMAP, POP3, SMTP)
- STARTTLS implemented for all three protocols
- rustls-based TLS implementation without OpenSSL dependency

[Unreleased]: https://github.com/peopleandpixel/rustyKraken/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.3.0
[0.2.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.2.0
