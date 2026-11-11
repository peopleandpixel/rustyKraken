# 🦑 rustyKraken

[![Version](https://img.shields.io/badge/version-0.4.0-blue.svg)](https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.4.0)
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
┌─────────────────────────────────────────────────┐
│                 rustyKraken Core                 │
│                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌────────┐ │
│  │Tentacles │───▶│  Event Bus   │───▶│Reflexes│ │
│  │(Sources) │    │  (Routing)   │    │(Actions)│ │
│  └──────────┘    └──────────────┘    └────────┘ │
│       │                  │                  │    │
│       ▼                  ▼                  ▼    │
│  config/incoming/   Mantle/DB       config/    │
│  (YAML)                              listener/  │
│                                          (YAML) │
└─────────────────────────────────────────────────┘
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
- [ ] File Watcher Tentacle
- [x] Telegram Bot Tentacle
- [ ] Discord Webhook Tentacle
- [ ] WebSocket Tentacle
- [ ] RSS/Atom Feed Tentacle
- [ ] Serial Port Tentacle
- [ ] SNMP Trap Tentacle

### Reflexes
- [ ] Telegram Bot Reflex
- [ ] Discord Webhook Reflex
- [ ] ntfy Reflex
- [ ] WhatsApp Reflex
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

Current version: **0.4.0** (2025-11-11)

See [CHANGELOG.md](CHANGELOG.md) for detailed changes.

## License

[MIT](LICENSE) © 2025 peopleandpixel

## Repository

[GitHub](https://github.com/peopleandpixel/rustyKraken) · [Issues](https://github.com/peopleandpixel/rustyKraken/issues) · [Releases](https://github.com/peopleandpixel/rustyKraken/releases)
