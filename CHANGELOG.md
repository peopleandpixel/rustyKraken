# Changelog

All notable changes to rustyKraken will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.35.0] - 2026-11-11

### Added

#### Tentacles
- **Kubernetes Events Tentacle**: Watch Kubernetes cluster events via API with:
  - Watch API stream with auto-reconnect
  - Namespace filtering
  - Bearer token authentication (Service Account)
  - Four topic modes: `type` (Normal/Warning), `resource` (Pod/Node/etc.), `namespace`, `all`
  - Resource-type-based topic mapping
  - TLS configuration with optional certificate verification
  - Rich metadata: k8s_event_type, k8s_resource_kind, k8s_namespace, k8s_name, k8s_uid, k8s_reason, k8s_labels, k8s_involved_object, k8s_source, k8s_count
  - 7 unit tests for topic resolution, mapping, config, and deserialization

## [0.34.0] - 2026-11-11

### Added

#### Tentacles
- **Docker Events Tentacle**: Subscribe to Docker daemon events via REST API with:
  - Event stream via `/events` endpoint with auto-reconnect
  - Unix socket and TCP/HTTPS host support
  - Event type and action filtering
  - Four topic modes: `type` (event type), `action` (event action), `attribute` (actor attribute), `all`
  - Type and action-based topic mapping
  - TLS configuration with optional certificate verification
  - Rich metadata: docker_type, docker_action, docker_actor_id, docker_scope, actor attributes
  - 8 unit tests for topic resolution, mapping, config, and deserialization

## [0.33.0] - 2026-11-11

### Added

#### Tentacles
- **Journalctl Tentacle**: Tail systemd journal logs as events with:
  - Follow mode using `journalctl -f --output=export`
  - Unit and priority filtering
  - Four topic modes: `identifier` (SYSLOG_IDENTIFIER), `unit` (SYSTEMD_UNIT), `priority`, `all`
  - Identifier and unit-based topic mapping
  - Journal export format parsing with metadata extraction
  - Configurable `--since` time filter
  - Rich metadata: journal_priority, journal_identifier, journal_unit, CODE_FILE, etc.
  - Fallback to polling mode if follow fails
  - 10 unit tests for topic resolution, parsing, config, and command building

## [0.32.0] - 2026-11-11

### Added

#### Tentacles
- **Azure Service Bus Tentacle**: Consume messages from Azure Service Bus Queues and Topics with:
  - REST API-based polling (no additional dependencies)
  - Queue and Topic/Subscription support
  - Three topic modes: `entity` (entity name based), `field` (JSON field extraction), `all` (single topic)
  - Entity-based mapping for custom routing
  - Authentication: SAS Shared Access Signature, Connection String
  - SAS token generation with HMAC-SHA256
  - Configurable poll interval and max messages per poll
  - Binary message support (Base64 encoding)
  - Rich metadata: asb_entity, asb_entity_type, asb_subscription
  - 10 unit tests for topic resolution, auth variants, config, and deserialization

## [0.31.0] - 2026-11-11

### Added

#### Tentacles
- **Kafka Tentacle**: Consume messages from Apache Kafka topics with:
  - Kafka consumer via rdkafka with StreamConsumer (async)
  - Consumer group support with configurable group ID
  - Multiple topic subscriptions
  - Three topic modes: `topic` (Kafka topic based), `field` (JSON field extraction), `all` (single topic)
  - Topic-based mapping for custom routing
  - Authentication: none, SASL Plain, SCRAM-SHA-256, SCRAM-SHA-512
  - SSL/TLS support with configurable CA certificate
  - Configurable session timeout, heartbeat interval, max poll interval
  - Auto offset reset (earliest/latest)
  - Binary message support (Base64 encoding)
  - Rich metadata: kafka_topic, kafka_partition, kafka_offset, kafka headers
  - 12 unit tests for topic resolution, auth variants, config, and deserialization

## [0.30.0] - 2026-11-11

### Added

#### Tentacles
- **RabbitMQ Tentacle**: Consume messages from RabbitMQ queues via AMQP with:
  - AMQP connection with auto-reconnect
  - Multiple queue subscriptions with exchange binding
  - Exchange types: direct, fanout, headers, topic
  - Three topic modes: `routingkey` (routing key based), `field` (JSON field extraction), `all` (single topic)
  - Configurable QoS prefetch count
  - Manual or automatic message acknowledgment
  - Binary message support (Base64 encoding)
  - Rich metadata: rabbitmq_queue, rabbitmq_routing_key
  - 7 unit tests for topic resolution, metadata, and deserialization

## [0.29.0] - 2026-11-11

### Added

#### Tentacles
- **Redis Streams Tentacle**: Consume messages from Redis Streams or Lists with:
  - Two modes: `stream` (XREADGROUP with consumer groups) and `list` (BRPOP)
  - Three topic modes: `name` (stream/list name based), `field` (JSON field extraction), `all` (single topic)
  - Consumer group auto-creation with MKSTREAM
  - Authentication: none, password, URL-based
  - Configurable batch size, block timeout, and reconnect delay
  - Rich metadata: redis_stream, redis_message_id
  - 8 unit tests for topic resolution, metadata, and deserialization

## [0.28.0] - 2026-11-11

### Added

#### Tentacles
- **NATS Tentacle**: Receive messages from NATS subjects with:
  - NATS server connection with auto-reconnect
  - Multiple subject subscriptions with wildcard support (`*` and `>`)
  - Three topic modes: `subject` (NATS subject based), `field` (JSON field extraction), `all` (single topic)
  - Subject-based topic mapping with wildcard substitution
  - Authentication: none, credentials (username/password), token, NKey
  - Binary message support (Base64 encoding)
  - Rich metadata: nats_subject, nats_reply_to
  - 12 unit tests for pattern matching, topic resolution, and metadata

## [0.27.0] - 2026-11-11

### Added

#### Tentacles
- **Syslog Listener Tentacle**: Receive syslog messages via UDP or TCP with:
  - UDP and TCP transport support
  - BSD Syslog (RFC 3164) and RFC 5424 message parsing
  - Auto-detection of syslog format
  - Three topic modes: `facility` (facility/severity based), `appname` (app name based), `all` (single topic)
  - Facility-based topic mapping for custom routing
  - Rich metadata: priority, facility, severity, hostname, appname, procid, msgid, timestamp
  - Configurable bind address, buffer size, and max TCP connections
  - 13 unit tests for parsing, topic resolution, and metadata

## [0.26.0] - 2026-11-11

### Added

#### Tentacles
- **Cron/Scheduler Tentacle**: Generate periodic events based on cron expressions with:
  - Support for 5-field and 6-field cron expressions (second minute hour day month weekday)
  - Multiple independent cron jobs per tentacle configuration
  - Cron field syntax: wildcards (`*`), ranges (`1-5`), steps (`*/15`), lists (`1,3,5`)
  - Configurable topic per job
  - Optional static body text per job
  - Optional metadata map attached to each generated message
  - Optional startup delay before first execution
  - Named jobs for better logging
  - 15 unit tests for cron parsing, schedule matching, and next occurrence calculation

## [0.25.0] - 2026-11-11

### Added

#### Event Bus Optimizations
- **Configurable Channel Size**: EventBus buffer size is now configurable via `EventBusConfig` (default: 1000)
- **Priority Queue**: Messages matching configured priority topics are routed through a dedicated high-priority channel
- **Backpressure Handling**: Configurable backpressure strategy (`block`, `drop_oldest`, `sample`) when the channel is full
- **Metrics**: EventBus now tracks published/dropped message counts, queue size, and latency via `EventBusMetrics`
- **13 new unit tests** for config, metrics, priority, and backpressure features

## [0.24.0] - 2026-11-11

### Added

#### Reflexes
- **File Write Reflex (Extended)**: Write Ink messages to files in multiple formats with:
  - Four output formats: JSON, CSV, plain text, and XML
  - Template engine for file content: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - CSV configuration: customizable columns, header row, delimiter character
  - CSV field escaping (quotes, commas, newlines)
  - XML output with proper character escaping
  - File path templates (e.g., `/var/log/kraken/{{topic}}.log`)
  - File rotation by size with configurable max files and suffix
  - Append and overwrite modes
  - Async file I/O with optional flush
  - 20 unit tests

## [0.23.0] - 2026-11-11

### Added

#### Reflexes
- **Script Reflex**: Execute scripts in Lua, JavaScript (Node.js), or Python with:
  - Three language support: Lua (`lua`), JavaScript (`node`), and Python (`python3`)
  - Subprocess-based execution for portability and isolation
  - Inline script code or external script file path
  - Configurable interpreter path per language
  - Ink data exposed as environment variables: `INK_TOPIC`, `INK_BODY`, `INK_ID`, `INK_TIMESTAMP`, `INK_METADATA` (JSON), `INK_JSON` (full Ink)
  - Template engine for environment variables: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Custom environment variables with template rendering
  - Output capture and optional publish to topic
  - Configurable timeout
  - 11 unit tests

## [0.22.0] - 2026-11-11

### Added

#### Reflexes
- **Aggregator Reflex**: Collect and aggregate Ink messages over time windows with:
  - Time-based aggregation with configurable window (e.g., 5 minutes)
  - Aggregation functions: count, sum, average, min, max
  - Multiple aggregation groups per reflex with independent topic patterns
  - Numeric value extraction from body, JSON field, or metadata
  - Template engine for aggregated messages: `{{count}}`, `{{sum}}`, `{{average}}`, `{{min}}`, `{{max}}`, `{{group_name}}`, `{{topic_pattern}}`, `{{window_start}}`, `{{window_end}}`
  - Rich metadata in aggregated messages (window timestamps, all aggregation values)
  - Flush-on-first mode for testing
  - 12 unit tests

## [0.21.0] - 2026-11-11

### Added

#### Reflexes
- **Database Reflex**: Store Ink messages in a database with:
  - SQLite, PostgreSQL, and MySQL/MariaDB support via `sqlx`
  - Configurable table name and schema
  - Auto-creation of the table on startup
  - Connection pooling with configurable max/min connections
  - Optional metadata JSON column
  - Optional status field (new/processed/archived)
  - Duplicate prevention (ON CONFLICT / INSERT IGNORE)
  - Query by topic pattern (LIKE with escaping)
  - Message count query
  - 12 unit tests

## [0.20.0] - 2026-11-11

### Added

#### Reflexes
- **Home Assistant Reflex**: Send events and service calls to Home Assistant with:
  - Home Assistant REST API integration
  - Two action types: event (fire custom event) and service (call a service)
  - Long-lived access token authentication
  - Template engine for title and message: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Topic-based event name mapping for different custom events
  - Topic-based service call mapping (domain/service format)
  - Topic-based message and title template mapping
  - Additional static data fields merged into event/service data
  - Ink metadata included as event data fields
  - Retry logic with configurable attempts and delay
  - Configurable request timeout
  - 12 unit tests

## [0.19.0] - 2026-11-11

### Added

#### Reflexes
- **Syslog Reflex**: Send messages to syslog servers with:
  - UDP and TCP syslog support
  - BSD and RFC 5424 message format
  - Configurable facility (kernel, user, local0-7, etc.) and severity (emergency through debug)
  - Template engine for message body: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Topic-based facility mapping for per-topic syslog facility
  - Topic-based severity mapping for per-topic log levels
  - Topic-based message template mapping
  - App-name and proc-id fields for RFC 5424 format
  - Retry logic with configurable attempts and delay
  - 12 unit tests

## [0.18.0] - 2026-11-11

### Added

#### Reflexes
- **Matrix/Element Reflex**: Send messages via Matrix Client-Server API with:
  - Matrix homeserver integration via REST API (no heavy dependencies)
  - Template engine for message body: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Message types: text, notice, and emote
  - HTML formatted body support with `org.matrix.custom.html` format
  - Topic-based room ID mapping for routing to different Matrix rooms
  - Topic-based message type mapping for per-topic message styling
  - Topic-based body template mapping for per-topic message formats
  - Bearer token authentication via access token
  - Retry logic with configurable attempts and delay
  - Request timeout configuration
  - 14 unit tests

## [0.17.0] - 2026-11-11

### Added

#### Reflexes
- **HTTP/Webhook Reflex**: Send HTTP requests to external APIs with:
  - Support for GET, POST, PUT, DELETE, and PATCH methods
  - Template engine for body: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - URL template support with placeholders
  - Authentication: Bearer token, Basic auth, and API-Key
  - Custom headers configuration
  - Configurable Content-Type header
  - Retry logic with configurable attempts and delay
  - Request timeout configuration
  - Topic-based URL mapping for routing to different endpoints
  - Topic-based HTTP method mapping
  - Topic-based body template mapping
  - Redirect handling (configurable)
  - 14 unit tests

## [0.16.0] - 2025-11-11

### Added

#### Reflexes
- **E-Mail Reflex**: Send emails via SMTP with:
  - `lettre` crate integration for SMTP transport
  - Configurable sender, recipients, subject, and body
  - HTML and plain text body support
  - File attachment support
  - CC and BCC recipients
  - Template engine for subject and body: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Topic-based recipient mapping for routing to different email addresses
  - Topic-based subject mapping for per-topic email subjects
  - STARTTLS (port 587) and implicit TLS (port 465) support
  - 12 unit tests

## [0.15.0] - 2025-11-11

### Added

#### Reflexes
- **ntfy Reflex**: Send push notifications via ntfy broker with:
  - ntfy broker API integration (POST to topic endpoint)
  - Template engine with placeholders: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Configurable notification title and message templates
  - Priority levels (1-5) with topic-based priority mapping
  - Tags support with topic-based tags mapping
  - Click action URLs for notification tap actions
  - Icon URL support for custom notification icons
  - Markdown formatting support
  - Basic authentication (username/password)
  - Topic-based ntfy topic mapping for routing to different notification topics
  - 12 unit tests

### Changed
- **License**: Changed from MIT to GPL v3 or later

## [0.14.0] - 2025-11-11

### Added

#### Reflexes
- **Discord Reflex**: Send messages via Discord Webhooks with:
  - Discord Webhook API integration
  - Template engine with placeholders: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Embed messages with configurable colors, title, description, and footer
  - Topic-based webhook URL mapping for routing to different channels
  - Topic-based embed color mapping for per-topic visual distinction
  - Custom username and avatar URL for webhook messages
  - Ink metadata as embed fields (configurable max fields, Discord limit: 25)
  - Content truncation at Discord's 2000 character limit
  - Embed description truncation at 4096 characters
  - TTS (text-to-speech) support
  - 15 unit tests

## [0.13.0] - 2025-11-11

### Added

#### Reflexes
- **Telegram Reflex**: Send messages via Telegram Bot API with:
  - Telegram Bot API (sendMessage) integration
  - Template engine with placeholders: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Configurable chat IDs (multiple recipients)
  - Parse mode support: Markdown, HTML, or None (plain text)
  - Topic-based chat ID mapping for routing to different chats
  - Topic-based parse mode mapping for per-topic formatting
  - Message truncation at Telegram's 4096 character limit
  - Optional metadata inclusion in messages (as code block)
  - Custom API base URL support for proxies
  - 10 unit tests

## [0.12.0] - 2025-11-11

### Added

#### Reflexes
- **Tentacle Reflex (Filter/Forward)**: Forward and transform Ink messages within the EventBus with:
  - Topic renaming with exact mapping, prefix mapping (longest-prefix wins), and template engine
  - Body transformation with template engine: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Conditional forwarding based on regex topic and body filters
  - Metadata preservation or filtering with configurable keys
  - Custom metadata fields added to forwarded messages
  - Original topic tracked as `__original_topic` for tracing
  - 16 unit tests

## [0.11.0] - 2025-11-11

### Added

#### Reflexes
- **WhatsApp Reflex**: Send messages via WhatsApp Business API with:
  - WhatsApp Business API (Graph API) integration
  - Template engine with placeholders: `{{topic}}`, `{{body}}`, `{{id}}`, `{{timestamp}}`, `{{metadata.<key>}}`
  - Configurable recipient phone numbers
  - Text and template message types
  - Topic-based recipient mapping
  - Topic-based template name mapping
  - Third-party API support via custom `api_base_url`
  - Optional metadata inclusion in messages
  - 16 unit tests

## [0.10.0] - 2025-11-11

### Added

#### Tentacles
- **AWS SQS Tentacle**: Read messages from Amazon SQS queues with:
  - Queue polling with configurable visibility timeout
  - AWS Signature v4 authentication
  - Topic detection: `attribute` (message attributes), `body` (JSON body), `all` (combined)
  - Queue URL to topic mapping
  - Auto-delete after processing
  - XML response parsing
  - 7 unit tests
- **Google Pub/Sub Tentacle**: Receive messages from Google Cloud Pub/Sub with:
  - Subscription pulling via REST API
  - OAuth2 JWT authentication (Service Account)
  - Topic detection: `attribute`, `data` (Base64 decoded), `all` (combined)
  - Subscription to topic mapping
  - Auto-acknowledge
  - 6 unit tests
- **Slack Tentacle**: Receive messages via Slack Events API with:
  - Axum-based HTTP server for Event Subscription API
  - Signing secret verification (HMAC-SHA256)
  - URL verification challenge handling
  - Topic detection: `channel` (channel mapping), `content` (/ prefix), `all` (combined)
  - Channel and user filtering
  - Rich metadata: user profile, files, thread info
  - 8 unit tests

### Changed
- **Crate Updates**:
  - Added `sha2 0.10` for HMAC-SHA256 signing
  - Added `hmac 0.12` for HMAC operations
  - Added `urlencoding 2.1` for URL encoding
- Added `PartialEq` to `SourceInfo` for test assertions

## [0.9.0] - 2025-11-11

### Added

#### Tentacles
- **Serial Port Tentacle**: Read from serial/COM ports and convert data to Ink messages with:
  - Configurable ports with baud rate, data bits, parity, and stop bits
  - Protocol parsers: `plain` (text), `json` (JSON objects), `csv` (with configurable delimiter), and `custom`
  - Custom delimiter support as hex string (e.g., `0d0a` for `\r\n`)
  - Port-based topic mapping for per-port topic override
  - Auto-reconnection with configurable delay
  - Buffer overflow protection
  - Rich metadata: port, baud_rate, parser
  - 11 unit tests
- **SNMP Trap Tentacle**: Receive SNMP traps via UDP and convert them to Ink messages with:
  - UDP trap listener on configurable address (default: `0.0.0.0:1162`)
  - SNMP v1/v2c/v3 support via `snmp2` crate
  - OID-to-topic mapping with exact match and prefix match
  - Longest-prefix matching for OID mappings
  - Varbinds serialized as JSON body
  - Rich metadata: trap_oid, version, community, message_type, varbind_count
  - 13 unit tests

### Changed
- **Crate Updates**:
  - Added `serial2-tokio 0.1` for async serial port I/O
  - Added `snmp2 0.5` for SNMP PDU parsing

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

[Unreleased]: https://github.com/peopleandpixel/rustyKraken/compare/v0.24.0...HEAD
[0.24.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.24.0
[0.23.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.23.0
[0.22.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.22.0
[0.21.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.21.0
[0.20.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.20.0
[0.19.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.19.0
[0.18.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.18.0
[0.17.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.17.0
[0.16.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.16.0
[0.15.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.15.0
[0.14.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.14.0
[0.13.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.13.0
[0.12.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.12.0
[0.11.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.11.0
[0.10.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.10.0
[0.9.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.9.0
[0.8.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.8.0
[0.7.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.7.0
[0.6.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.6.0
[0.5.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.5.0
[0.4.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.4.0
[0.3.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.3.0
[0.2.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.2.0
