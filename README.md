# 🦑 rustyKraken

[![Version](https://img.shields.io/badge/version-0.2.0-blue.svg)](https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-2024-orange.svg)](https://www.rust-lang.org/)

Eine zentrale Nachrichten-Hub-Software in Rust, die Nachrichten aus beliebigen Quellen sammelt, nach Themen sortiert und an Aktionen weiterleitet.

## Konzept

rustyKraken folgt der Kraken-Metapher:

- **Tentacles** (Arme) – Greifen in verschiedene Quellen (E-Mail, MQTT, HTTP, ntfy, ...) und holen Nachrichten
- **Ink** (Tinte) – Das einheitliche Nachrichtenformat, in das alle Quellen gebracht werden
- **Mantle** (Mantel) – Die SQLite-basierte Speicherung aller Nachrichten
- **Reflexes** (Reflexe) – Reagieren auf bestimmte Themen und lösen Aktionen aus

## Architektur

```
┌─────────────────────────────────────────────────┐
│                 rustyKraken Core                 │
│                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌────────┐ │
│  │Tentacles │───▶│  Event Bus   │───▶│Reflexes│ │
│  │(Quellen) │    │  (Routierung) │    │(Aktionen)│ │
│  └──────────┘    └──────────────┘    └────────┘ │
│       │                  │                  │    │
│       ▼                  ▼                  ▼    │
│  config/incoming/   Mantle/DB       config/    │
│  (YAML)                              listener/  │
│                                          (YAML) │
└─────────────────────────────────────────────────┘
```

## Schnellstart

### Installation

```bash
cargo build --release
./target/release/rustyKraken
```

### Konfiguration

#### Tentacles (Eingangsquellen)

Erstelle YAML-Dateien in `config/incoming/`:

**HTTP Webhook** (`config/incoming/http_webhook.yaml`):
```yaml
name: "webhook_example"
type: "http"
config:
  host: "127.0.0.1"
  port: 8080
  path: "/webhook"
```

Testen mit:
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

#### Reflexes (Aktionen)

Erstelle YAML-Dateien in `config/listener/`:

**Datei-Logging** (`config/listener/file_log.yaml`):
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

**Shell-Befehl** (`config/listener/command_echo.yaml`):
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

Verfügbare Umgebungsvariablen:
- `INK_JSON` – Die gesamte Nachricht als JSON
- `INK_TOPIC` – Das Topic
- `INK_BODY` – Der Inhalt
- `INK_ID` – Die UUID

## Themen (Topics)

Nachrichten werden nach einem Pfad-basierten Themensystem sortiert:

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

- `/kraken/**` – Passt zu `/kraken/warning`, `/kraken/data/sensor`, etc.
- `/kraken/*` – Passt zu `/kraken/warning`, aber nicht zu `/kraken/data/sensor`

## Nachrichtenschema (Ink)

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

## Geplante Features

### Tentacles
- [ ] ntfy Tentacle
- [ ] File Watcher Tentacle
- [ ] Telegram Bot Tentacle
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
- [ ] E-Mail (SMTP) Reflex
- [ ] HTTP/Webhook Reflex
- [ ] Matrix Reflex
- [ ] Home Assistant Reflex
- [ ] Database Reflex
- [ ] Script Reflex

### Allgemein
- [ ] Web-UI für Nachrichtenübersicht
- [ ] Hot-Reload für Konfigurationsdateien
- [ ] Template-Engine für Nachrichten (`{{message.body}}`)
- [ ] Authentifizierung für Webhooks
- [ ] Plugin-System für dynamisches Laden
- [ ] Performance-Optimierungen (Batch-Verarbeitung, Connection-Pooling)
- [ ] Monitoring und Health-Checks
- [ ] Docker-Image

## Version

Aktuelle Version: **0.2.0** (2025-11-11)

Siehe [CHANGELOG.md](CHANGELOG.md) für detaillierte Änderungsübersicht.

## Lizenz

[MIT](LICENSE) © 2025 peopleandpixel

## Repository

[GitHub](https://github.com/peopleandpixel/rustyKraken) · [Issues](https://github.com/peopleandpixel/rustyKraken/issues) · [Releases](https://github.com/peopleandpixel/rustyKraken/releases)
