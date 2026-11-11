# Changelog

All notable changes to rustyKraken will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.8.0] - 2025-11-11

### Added

#### Tentacles
- **REST API Poller Tentacle**: Periodically poll REST APIs and convert responses to Ink messages with:
  - Configurable endpoints with URLs, HTTP methods (GET, POST, PUT, DELETE, PATCH), and intervals
  - Authentication: Bearer token, Basic Auth, and API-Key
  - JSON path extraction (e.g., `data.items`) for nested responses
  - Dynamic topic/body fields per message
  - Custom headers and request body support
  - Rate limiting (requests per minute)
  - Retry logic with configurable max retries and delay
  - 12 unit tests
- **WebSocket Tentacle**: Connect to WebSocket servers and receive real-time messages with:
  - ws:// and wss:// support via `tokio-tungstenite`
  - Three topic modes: `field` (JSON field), `content` (/ prefix), and `all` (fallback)
  - Auto-reconnection with configurable delay
  - Custom headers and subprotocol support
  - Binary message handling (Base64 encoded)
  - Configurable max message size
  - 15 unit tests
- **RSS/Atom Feed Tentacle**: Poll RSS/Atom feeds and convert new entries to Ink messages with:
  - RSS 2.0, RSS 1.0, and Atom feed support via `feed-parser` crate
  - Auto-detection of feed type
  - Duplicate detection to avoid reprocessing entries
  - Four topic modes: `feedtitle`, `entrytitle`, `creator`, `all`
  - Per-feed custom topic override
  - Retry logic with configurable max retries and delay
  - 15 unit tests

### Changed
- **Crate Updates**:
  - Added `feed-parser 2.0` for RSS/Atom feed parsing
  - Added `parking_lot 0.12` for efficient mutex in RSS tentacle

## [0.7.0] - 2025-11-11

### Added

#### Tentacles
- **Stdin Tentacle**: Read messages from standard input, line by line, with:
  - Line-based reading using `tokio::io`
  - Three input formats: `plain` (text), `json` (JSON objects), and `auto` (detects format per line)
  - Topic detection from content — lines starting with `/` extract the topic (e.g., `/alerts message`)
  - Two topic modes: `content` (extract from message) and `all` (use fallback topic)
  - JSON support with `topic`, `body`, and `message` fields
  - Configurable fallback topic (default: `/stdin/default`)
  - EOF and Ctrl+C handling for graceful shutdown
  - 16 unit tests

## [0.6.0] - 2025-11-11

### Added

#### Tentacles
- **File Watcher Tentacle**: Monitor directories for new files and convert them to Ink messages with:
  - `notify` crate for real-time filesystem event monitoring (Create/Modify events)
  - Topic detection modes: `folder` (folder mapping), `filename` (stem or wildcard mapping), `content` (first line as topic), and `all` (combined with priority)
  - File type filtering by extension (case-insensitive, empty = all files)
  - Post-processing actions: `none`, `delete`, or `move` to a destination directory
  - Configurable processing delay (default 500ms) to ensure files are fully written
  - Max file size limit (default 10MB) to skip oversized files
  - Recursive/non-recursive folder watching per folder
  - Rich metadata: file_path, file_name, file_size, folder
  - 11 unit tests

## [0.5.0] - 2025-11-11

### Added

#### Tentacles
- **Discord Tentacle**: Receive messages from Discord via Bot Gateway or HTTP webhook with:
  - Bot mode (Discord Gateway WebSocket connection with heartbeat, session management, reconnection, and sharding support)
  - Webhook mode (Axum-based HTTP server with secret token validation)
  - Topic detection modes: `channel` (channel/guild mapping), `content` (messages starting with `/`), and `all` (combined)
  - Message type support: text, image, video, audio, attachment, embed, reply
  - Filtering via `allowed_channels`, `allowed_guilds`, and `allowed_users` whitelists
  - Rich metadata: author info, member nickname/roles, channel/guild IDs, attachments, embeds, mentions, reply references
  - 13 unit tests

## [0.4.0] - 2025-11-11

### Added

#### Tentacles
- **Telegram Tentacle**: Receive messages from Telegram via Bot API with:
  - Polling mode (long-polling via `getUpdates`)
  - Webhook mode (Axum-based HTTP server with secret token validation)
  - Topic detection modes: `command` (e.g., `/kraken_topic`), `content` (messages starting with `/`), `chat` (chat_id/username mapping), and `all` (combined)
  - Message type support: text, photo, document, voice, audio, video, location, contact
  - Filtering via `allowed_chats` and `allowed_users` whitelists
  - Rich metadata: chat info, sender info, message type, forwarded-from, location, contact data
  - 8 unit tests

[0.4.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.4.0

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

[Unreleased]: https://github.com/peopleandpixel/rustyKraken/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.8.0
[0.7.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.7.0
[0.6.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.6.0
[0.5.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.5.0
[0.4.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.4.0
[0.3.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.3.0
[0.2.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.2.0
