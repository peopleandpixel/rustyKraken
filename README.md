# 🦑 rustyKraken

[![Version](https://img.shields.io/badge/version-0.14.0-blue.svg)](https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.14.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-2024-orange.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/peopleandpixel/rustyKraken/actions/workflows/ci.yml/badge.svg)](https://github.com/peopleandpixel/rustyKraken/actions/workflows/ci.yml)

![rustyKraken Logo](docs/rustyKraken.jpeg)

A centralized message hub written in Rust that collects messages from various sources, sorts them by topic, and routes them to actions.

## Concept

rustyKraken follows the kraken metaphor:

- **Tentacles** (Arms) – Reach into various sources (email, MQTT, HTTP, ntfy, ...) and fetch messages
- **Ink** (Ink) – The unified message format that all sources are converted into
- **Mantle** (Mantle) – SQLite-based storage for all messages
- **Reflexes** (Reflexes) – React to specific topics and trigger actions

## Architecture

```
┌──────────────────────────────────────────────────┐
│                 rustyKraken Core                 │
│                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌─────────┐ │
│  │Tentacles │───▶│  Event Bus   │───▶│Reflexes │ │
│  │(Sources) │    │  (Routing)   │    │(Actions)│ │
│  └──────────┘    └──────────────┘    └─────────┘ │
│       │                  │                │      │
│       ▼                  ▼                ▼      │
│  config/incoming/   Mantle/DB       config/      │
│  (YAML)                              listener/   │
│                                          (YAML)  │
└──────────────────────────────────────────────────┘
```

## Quick Start

### Installation

```bash
cargo build --release
./target/release/rustyKraken
```

### Configuration

#### Tentacles (Incoming Sources)

Create YAML files in `config/incoming/`:

**HTTP Webhook** (`config/incoming/http_webhook.yaml`):
```yaml
name: "webhook_example"
type: "http"
config:
  host: "127.0.0.1"
  port: 8080
  path: "/webhook"
```

Test with:
```bash
curl -X POST http://127.0.0.1:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{"topic": "/kraken/warning", "body": "Temperatur zu hoch: 42°C"}'
```

**MQTT** (`config/incoming/mqtt_kraken.yaml`):
```yaml
name: "mqtt_kraken"
type: "mqtt"
config:
  broker: "localhost"
  port: 1883
  use_tls: false
  username: "user"
  password: "secret"
  topics:
    - "kraken/+"
  topic_mapping:
    "kraken/warning": "/kraken/warning"
  last_will:
    topic: "kraken/status"
    message: "offline"
    qos: 1
  clean_session: true
  reconnect_delay_ms: 5000
  max_reconnect_attempts: 10
```

**IMAP** (`config/incoming/imap_email.yaml`):
```yaml
name: "work_email"
type: "imap"
config:
  host: "imap.example.com"
  port: 993
  username: "user@example.com"
  password: "${EMAIL_PASSWORD}"
  use_tls: true
  starttls: false
  mailbox: "INBOX"
  poll_interval_seconds: 30
  use_idle: true
  mark_as_read: true
  move_to_mailbox: "Processed"
  topic_mode:
    mode: "subject"
    fallback_topic: "/email/inbox"
```

**POP3** (`config/incoming/pop3_email.yaml`):
```yaml
name: "pop3_email"
type: "pop3"
config:
  host: "pop.example.com"
  port: 995
  username: "user@example.com"
  password: "${EMAIL_PASSWORD}"
  use_tls: true
  starttls: false
  poll_interval_seconds: 60
  delete_after_processing: false
  topic_mode:
    mode: "subject"
    fallback_topic: "/email/inbox"
```

**ntfy** (`config/incoming/ntfy_kraken.yaml`):
```yaml
name: "ntfy_kraken"
type: "ntfy"
config:
  server: "https://ntfy.sh"
  topics:
    - "kraken/alerts"
    - "kraken/status"
  connection_type: "json"  # or "websocket"
  topic_mapping:
    "kraken/alerts": "/kraken/alerts"
    "kraken/status": "/kraken/status"
  reconnect_delay: 5
```

**SMTP** (`config/incoming/smtp_server.yaml`):
```yaml
name: "smtp_receiver"
type: "smtp"
config:
  host: "0.0.0.0"
  port: 2525
  server_name: "rustyKraken"
  max_message_size: 10485760
  require_auth: false
  tls_cert: "path/to/cert.pem"
  tls_key: "path/to/key.pem"
  topic_mode:
    mode: "subject"
    fallback_topic: "/email/incoming"
```

**Telegram** (`config/incoming/telegram.yaml`):
```yaml
name: "telegram_bot"
type: "telegram"
config:
  bot_token: "123456:ABC-DEF..."
  connection_type: "polling"  # or "webhook"
  topic_mode: "all"           # command, content, chat, or all
  fallback_topic: "/telegram/default"
  chat_topic_mapping:
    "100": "/kraken/alerts"
  allowed_chats: []           # empty = accept all
  allowed_users: []           # empty = accept all
```

**Discord** (`config/incoming/discord.yaml`):
```yaml
name: "discord_bot"
type: "discord"
config:
  connection_type: "bot"      # or "webhook"
  bot_token: "MTAw..."        # Discord bot token
  topic_mode: "all"           # channel, content, or all
  fallback_topic: "/discord/default"
  channel_topic_mapping:
    "123456789": "/kraken/alerts"
  allowed_channels: []        # empty = accept all
  allowed_guilds: []          # empty = accept all
  allowed_users: []           # empty = accept all
```

**File Watcher** (`config/incoming/file_watcher.yaml`):
```yaml
name: "file_watcher"
type: "file_watcher"
config:
  folders:
    - path: "/watch/inbox"
      topic: "/files/inbox"
      recursive: false
  file_extensions: [".txt", ".json"]
  topic_mode: "all"           # folder, filename, content, or all
  fallback_topic: "/files/default"
  post_process: "move"        # none, delete, or move
  move_to: "/watch/processed"
```

**Stdin** (`config/incoming/stdin.yaml`):
```yaml
name: "stdin_listener"
type: "stdin"
config:
  format: "auto"              # plain, json, or auto
  topic_mode: "content"       # content or all
  fallback_topic: "/stdin/default"
```

**Serial Port** (`config/incoming/serial.yaml`):
```yaml
name: "serial_sensor"
type: "serial"
config:
  ports:
    - port: "/dev/ttyUSB0"
      baud_rate: 9600
      data_bits: 8
      parity: "none"
      stop_bits: 1
      topic: "/serial/sensor"
      parser: "plain"       # plain, json, csv, custom
  reconnect_delay: 5
  buffer_size: 4096
  port_topic_mapping:
    "/dev/ttyUSB1": "/serial/actuator"
```

**SNMP Trap** (`config/incoming/snmp.yaml`):
```yaml
name: "snmp_traps"
type: "snmp"
config:
  listen_address: "0.0.0.0:1162"
  community: "public"
  version: "v2c"            # v1, v2c, v3
  topic: "/snmp/trap"
  oid_topic_mapping:
    "1.3.6.1.6.3.1.1.5.2": "/snmp/linkdown"
    "1.3.6.1.6.3.1.1.5.3": "/snmp/linkup"
  max_packet_size: 65535
```

**AWS SQS** (`config/incoming/aws_sqs.yaml`):
```yaml
name: "sqs_consumer"
type: "aws_sqs"
config:
  region: "eu-central-1"
  queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue"
  poll_interval: 5
  max_messages: 10
  visibility_timeout: 30
  delete_after_processing: true
  topic_mode: "all"           # attribute, body, or all
  fallback_topic: "/sqs/default"
  queue_topic_mapping:
    "https://sqs.eu-central-1.amazonaws.com/123456789/alerts": "/sqs/alerts"
```

**Google Pub/Sub** (`config/incoming/google_pubsub.yaml`):
```yaml
name: "pubsub_listener"
type: "google_pubsub"
config:
  project_id: "my-project"
  subscription: "my-subscription"
  poll_interval: 5
  max_messages: 10
  auto_ack: true
  topic_mode: "all"           # attribute, data, or all
  fallback_topic: "/pubsub/default"
  topic_mapping:
    "my-subscription": "/pubsub/mapped"
```

**Slack** (`config/incoming/slack.yaml`):
```yaml
name: "slack_listener"
type: "slack"
config:
  signing_secret: "your-signing-secret"
  host: "0.0.0.0"
  port: 9880
  path: "/slack/events"
  topic_mode: "all"           # channel, content, or all
  fallback_topic: "/slack/default"
  channel_topic_mapping:
    "C123456": "/slack/alerts"
  allowed_channels: []        # empty = all
  allowed_users: []           # empty = all
```

**REST API Poller** (`config/incoming/rest_api.yaml`):
```yaml
name: "api_poller"
type: "rest_api"
config:
  endpoints:
    - url: "https://api.example.com/data"
      method: "GET"
      topic: "/api/data"
      json_path: "data.items"
      auth:
        type: "bearer"
        token: "my-token"
  interval_seconds: 60
  max_retries: 3
  rate_limit_per_minute: 10
```

**WebSocket** (`config/incoming/websocket.yaml`):
```yaml
name: "ws_listener"
type: "websocket"
config:
  url: "wss://example.com/ws"
  topic_mode: "field"         # field, content, or all
  topic_field: "event_type"
  body_field: "payload"
  fallback_topic: "/ws/default"
  reconnect: true
  reconnect_delay_seconds: 5
```

**RSS/Atom Feed** (`config/incoming/rss.yaml`):
```yaml
name: "feed_reader"
type: "rss"
config:
  feeds:
    - url: "https://example.com/feed.xml"
      feed_type: "auto"       # auto, rss, or atom
    - url: "https://example.com/atom.xml"
      topic: "/feed/custom"
  interval_seconds: 300
  topic_mode: "feedtitle"
  fallback_topic: "/rss/default"
```

#### Reflexes (Actions)

Create YAML files in `config/listener/`:

**File Logging** (`config/listener/file_log.yaml`):
```yaml
name: "log_warnings"
trigger:
  topics:
    - "/kraken/warning"
    - "/home/emergency"
action:
  type: "file"
  config:
    path: "data/warnings.log"
    mode: "append"
```

**Shell Command** (`config/listener/command_echo.yaml`):
```yaml
name: "echo_warning"
trigger:
  topics:
    - "/kraken/**"
action:
  type: "command"
  config:
    command: "echo"
    args: ["Nachricht erhalten"]
```

Available environment variables:
- `INK_JSON` – The entire message as JSON
- `INK_TOPIC` – The topic
- `INK_BODY` – The body content
- `INK_ID` – The UUID

**WhatsApp** (`config/listener/whatsapp_alerts.yaml`):
```yaml
name: "whatsapp_alerts"
trigger:
  topics:
    - "/kraken/alerts/**"
action:
  type: "whatsapp"
  config:
    access_token: "EAA..."
    phone_number_id: "1234567890"
    recipients:
      - "+1234567890"
    message_type: "text"
    body_template: "Alert: {{body}}"
    language: "en_US"
```

**Tentacle (Filter/Forward)** (`config/listener/forward_alerts.yaml`):
```yaml
name: "forward_alerts"
trigger:
  topics:
    - "/sensor/**"
action:
  type: "tentacle"
  config:
    target_topic: "/forwarded/{{topic}}"
    body_template: "[{{topic}}] {{body}}"
    topic_mapping:
      "/sensor/temp": "/iot/temperature"
    topic_prefix_mapping:
      "/sensor/": "/iot/sensor/"
    body_filter: ".*alert.*"
    preserve_metadata: true
    add_metadata:
      forwarded_by: "tentacle-reflex"
```

**Discord** (`config/listener/discord_alerts.yaml`):
```yaml
name: "discord_alerts"
trigger:
  topics:
    - "/kraken/alerts/**"
action:
  type: "discord"
  config:
    webhook_url: "https://discord.com/api/webhooks/123/abc"
    content_template: "Alert: {{body}}"
    use_embeds: true
    embed_title: "{{topic}}"
    embed_color: 16711680
    embed_footer: "rustyKraken"
    username: "Kraken Bot"
    topic_webhook_mapping:
      "/kraken/critical": "https://discord.com/api/webhooks/456/def"
    topic_color_mapping:
      "/kraken/critical": 16711680
      "/kraken/info": 65280
    include_metadata: true
```

**Telegram** (`config/listener/telegram_alerts.yaml`):
```yaml
name: "telegram_alerts"
trigger:
  topics:
    - "/kraken/alerts/**"
action:
  type: "telegram"
  config:
    bot_token: "123456:ABC-DEF"
    chat_ids:
      - "-1001234567890"
    parse_mode: "Markdown"
    body_template: "*Alert:* {{body}}"
    topic_chat_mapping:
      "/kraken/critical": ["-1009876543210"]
    include_metadata: false
```

## Topics

Messages are sorted by a path-based topic system:

```
/kraken/
├── warning/
├── status/
└── data/

/home/
├── lights/
└── temperature/
```

### Wildcards

- `/kraken/**` – Matches `/kraken/warning`, `/kraken/data/sensor`, etc.
- `/kraken/*` – Matches `/kraken/warning`, but not `/kraken/data/sensor`

## Message Schema (Ink)

```json
{
  "id": "uuid-v4",
  "timestamp": "2024-11-11T10:00:00Z",
  "topic": "/kraken/warning",
  "source": {
    "type": "mqtt",
    "name": "kraken_sensors"
  },
  "body": "Temperatur zu hoch: 42°C",
  "metadata": null,
  "status": "new"
}
```

## Planned Features

### Tentacles
- [x] File Watcher Tentacle
- [x] Stdin Tentacle
- [x] Telegram Bot Tentacle
- [x] Discord Bot Tentacle
- [x] REST API Poller Tentacle
- [x] WebSocket Tentacle
- [x] RSS/Atom Feed Tentacle
- [x] Serial Port Tentacle
- [x] SNMP Trap Tentacle
- [x] AWS SQS Tentacle
- [x] Google Pub/Sub Tentacle
- [x] Slack Tentacle

### Reflexes
- [x] Telegram Bot Reflex
- [x] Discord Webhook Reflex
- [ ] ntfy Reflex
- [x] WhatsApp Reflex
- [x] Tentacle Reflex (Filter/Forward)
- [ ] Email (SMTP) Reflex
- [ ] HTTP/Webhook Reflex
- [ ] Matrix Reflex
- [ ] Home Assistant Reflex
- [ ] Database Reflex
- [ ] Script Reflex

### General
- [ ] Web UI for message overview
- [ ] Hot-reload for configuration files
- [ ] Template engine for messages (`{{message.body}}`)
- [ ] Authentication for webhooks
- [ ] Plugin system for dynamic loading
- [ ] Performance optimizations (batch processing, connection pooling)
- [ ] Monitoring and health checks
- [ ] Docker image

## Version

Current version: **0.14.0** (2025-11-11)

See [CHANGELOG.md](CHANGELOG.md) for detailed changes.

## Contributors

- **Jens Reinemuth** – Creator and maintainer

## License

[MIT](LICENSE) © 2025 peopleandpixel

## Repository

[GitHub](https://github.com/peopleandpixel/rustyKraken) · [Issues](https://github.com/peopleandpixel/rustyKraken/issues) · [Releases](https://github.com/peopleandpixel/rustyKraken/releases)
