---
sessionId: session-261112-222346-1xon
---

# Requirements

### Overview
Derzeit erstellen ~29 Reflexes und ~30 Tentacles各自 eigenen `reqwest::Client` via `reqwest::Client::new()` oder `Client::builder()`. Das bedeutet keine Connection-Pooling-Wiederverwendung, kein HTTP/2-Multiplexing, und hoher Overhead pro Anfrage.

### Goals
- Alle HTTP-basierten Reflexes und Tentacles sollen den geteilten `AppContext` nutzen
- Der `AppContext` wird einmalig in `main.rs` erstellt und per Dependency Injection injiziert
- Connection Pooling, HTTP/2 Multiplexing, und konfigurierbare Pool-Parameter

### Scope
**In Scope:**
- `appcontext.rs` bleibt wie vorhanden (AppContext mit http_client, http_client_danger_accept_invalid_certs, http_client_custom)
- Alle Reflexes ohne `app_ctx` in `::new()` bekommen den Parameter
- Alle Tentacles ohne `app_ctx` in `::new()` bekommen den Parameter
- `main.rs`: `create_reflex_handler()` und `create_tentacle()` übergeben `app_ctx`
- Alle Tests werden aktualisiert

**Out of Scope:**
- Reflexes/Tentacles ohne HTTP (z.B. CommandReflex, FileReflex, ScriptReflex) bleiben unverändert
- MQTT-basierte Reflexes (MqttPublishReflex, Zigbee2MqttReflex) bleiben unverändert (nutzen rumqttc, nicht reqwest)
- NATS-basierte Reflexes bleiben unverändert

### Reflexes zu ändern (29)
| Reflex | Hat HTTP? | Aktion |
|--------|-----------|--------|
| elasticsearch.rs | ✅ | `app_ctx` hinzufügen, `http_client_danger_accept_invalid_certs()` nutzen |
| esphome.rs | ✅ | dito |
| fhem.rs | ✅ | dito |
| grafana.rs | ✅ | dito |
| influxdb.rs | ✅ | dito |
| kubernetes.rs | ✅ | dito |
| n8n.rs | ✅ | dito |
| openhab.rs | ✅ | dito |
| powerdns.rs | ✅ | dito |
| printer_3d.rs | ✅ | dito |
| prometheus.rs | ✅ | dito |
| puppet.rs | ✅ | dito |
| s3_minio.rs | ✅ | dito |
| saltstack.rs | ✅ | dito |
| shelly.rs | ✅ | dito |
| signal.rs | ✅ | dito |
| tasmota.rs | ✅ | dito |
| temporal.rs | ✅ | dito |
| thehive.rs | ✅ | dito |
| make.rs | ❌ (shell) | `app_ctx` hinzufügen, aber keinen Client |
| mqtt_publish.rs | ❌ (rumqttc) | `app_ctx` hinzufügen, aber keinen Client |
| nats.rs | ❌ (async_nats) | `app_ctx` hinzufügen, aber keinen Client |
| zigbee2mqtt.rs | ❌ (rumqttc) | `app_ctx` hinzufügen, aber keinen Client |
| tts.rs | ❌ (shell) | `app_ctx` hinzufügen, aber keinen Client |
| sound_alert.rs | ❌ (shell) | `app_ctx` hinzufügen, aber keinen Client |
| fail2ban.rs | ❌ (shell) | `app_ctx` hinzufügen, aber keinen Client |
| home_assistant.rs | ✅ | `app_ctx` hinzufügen, `http_client()` nutzen |
| http.rs | ✅ | `app_ctx` hinzufügen, `http_client_custom()` nutzen |
| matrix.rs | ✅ | `app_ctx` hinzufügen, `http_client()` nutzen |
| database.rs | ❌ (sqlx) | `app_ctx` hinzufügen, aber keinen Client |
| email.rs | ❌ (lettre) | `app_ctx` hinzufügen, aber keinen Client |
| syslog.rs | ❌ (UDP) | `app_ctx` hinzufügen, aber keinen Client |

### Tentacles zu ändern (30)
| Tentacle | Hat HTTP? | Aktion |
|----------|-----------|--------|
| discord.rs | ✅ | `app_ctx` hinzufügen, `http_client()` nutzen |
| webhooks_poller.rs | ✅ | bereits teilweise, vervollständigen |
| geofence.rs | ✅ | `app_ctx` hinzufügen |
| ntfy.rs | ✅ | `app_ctx` hinzufügen |
| rest_api.rs | ✅ | `app_ctx` hinzufügen |
| rss.rs | ✅ | `app_ctx` hinzufügen |
| telegram.rs | ✅ | `app_ctx` hinzufügen |
| kubernetes.rs | ✅ | bereits erledigt |
| docker_events.rs | ✅ | bereits erledigt |
| azure_servicebus.rs | ✅ | bereits erledigt |
| aws_sqs.rs | ✅ | bereits erledigt |
| google_pubsub.rs | ✅ | `app_ctx` hinzufügen |
| + 18 weitere ohne HTTP | ❌ | `app_ctx` hinzufügen für Konsistenz |

### Nicht-HTTP Tentacles (bleiben ohne Client, bekommen aber app_ctx für Konsistenz)
ble.rs, caldav.rs, cron.rs, database_poller.rs, elasticsearch.rs, file_watcher.rs, ftp_sftp.rs, google_chat.rs, graphql.rs, grpc.rs, http.rs, imap.rs, irc.rs, journalctl.rs, kafka.rs, matrix.rs, modbus.rs, mqtt.rs, nats.rs, packet_capture.rs, pop3.rs, rabbitmq.rs, redis_streams.rs, serial_port.rs, signal.rs, slack.rs, smtp.rs, snmp.rs, stdin_tentacle.rs, syslog_listener.rs, teams.rs, websocket.rs, xmpp.rs, zigbee.rs, zwave.rs

### Key Decisions
1. **Alle `::new()` Signaturen bekommen `app_ctx: &AppContext`** — auch wenn sie keinen HTTP-Client nutzen, für einheitliche API
2. **HTTP-Reflexes nutzen `http_client_danger_accept_invalid_certs()`** wenn `verify_ssl: false` konfiguriert
3. **Nicht-HTTP-Reflexes speichern `app_ctx` nicht** — sie bekommen den Parameter nur für API-Konsistenz
4. **Tests erstellen `AppContext::new()` lokal** — keine globale State-Abhängigkeit

### Architecture
```
main.rs
  └─ AppContext::new()          ← einmaliger HTTP-Client mit Pool
       ├─ create_tentacle(&config, &app_ctx)
       │    ├─ DiscordTentacle::new(..., &app_ctx)  ← nutzt app_ctx.http_client()
       │    ├─ WebhooksPollerTentacle::new(..., &app_ctx)
       │    └─ ... alle 47 Tentacles
       └─ create_reflex_handler(&config, &event_bus, &app_ctx)
            ├─ ElasticsearchReflex::new(..., &app_ctx)  ← nutzt app_ctx.http_client_danger_accept_invalid_certs()
            ├─ SignalReflex::new(..., &app_ctx)
            └─ ... alle 60 Reflexes
```

### Risks
- **Breaking change für alle Tests**: Jeder Test der `::new()` aufruft muss `AppContext::new()` erstellen
- **Kein Rollback ohne Neustart**: Einmal geänderte Signaturen sind breaking changes
- **Mittlerer Aufwand**: ~60 Reflex-Dateien + ~47 Tentacle-Dateien + main.rs + alle Tests

# Technical Design

### Current State
- `appcontext.rs` existiert mit `AppContext::new()`, `http_client()`, `http_client_danger_accept_invalid_certs()`, `http_client_custom()`
- `main.rs` erstellt `app_ctx` und übergibt ihn an `create_tentacle()` und `create_reflex_handler()`
- `create_reflex_handler()` hat `_app_ctx: &appcontext::AppContext` Parameter, aber nutzt ihn nicht
- `create_tentacle()` hat `app_ctx: &appcontext::AppContext` Parameter, aber nutzt ihn nicht
- **29 Reflexes** erstellen eigenen `reqwest::Client` statt den geteilten zu nutzen
- **~30 Tentacles** erstellen eigenen `reqwest::Client` oder haben keine HTTP-Integration

### Approach

#### Phase 1: HTTP-Reflexes (18 Dateien)
Jeder HTTP-Reflex bekommt:
1. `app_ctx: &AppContext` Parameter in `::new()`
2. `client: reqwest::Client` Feld wird befüllt via `app_ctx.http_client_danger_accept_invalid_certs()` oder `app_ctx.http_client()`
3. Tests bekommen `let app_ctx = AppContext::new();`

Pattern (elasticsearch.rs als Beispiel):
```rust
pub fn new(name: String, config: ElasticsearchReflexConfig, app_ctx: &AppContext) -> Self {
    let client = if config.verify_ssl {
        app_ctx.http_client().as_ref().clone()
    } else {
        app_ctx.http_client_danger_accept_invalid_certs()
    };
    Self { name, config, client }
}
```

#### Phase 2: Nicht-HTTP-Reflexes (11 Dateien)
Bekommen `app_ctx: &AppContext` Parameter, aber speichern keinen Client:
```rust
pub fn new(name: String, config: MakeReflexConfig, _app_ctx: &AppContext) -> Self {
    Self { name, config }
}
```

#### Phase 3: HTTP-Tentacles (10 Dateien)
Ähnlich wie HTTP-Reflexes: `app_ctx` in `::new()`, Client aus `app_ctx`.

#### Phase 4: Nicht-HTTP-Tentacles (20 Dateien)
Bekommen `app_ctx` Parameter für API-Konsistenz, aber nutzen ihn nicht.

#### Phase 5: main.rs
- `create_reflex_handler()`: `app_ctx` an alle `::new()` Calls weitergeben
- `create_tentacle()`: `app_ctx` an alle `::new()` Calls weitergeben

#### Phase 6: Tests
Alle Tests in allen betroffenen Dateien bekommen `let app_ctx = AppContext::new();` und übergeben es an `::new()`.

### File-by-File Breakdown

#### Reflexes mit HTTP (18 Dateien)
1. `elasticsearch.rs` — `verify_ssl` → `http_client_danger_accept_invalid_certs()`
2. `esphome.rs` — dito
3. `fhem.rs` — dito
4. `grafana.rs` — dito
5. `influxdb.rs` — dito
6. `kubernetes.rs` — dito
7. `n8n.rs` — dito
8. `openhab.rs` — dito
9. `powerdns.rs` — dito
10. `printer_3d.rs` — dito
11. `prometheus.rs` — dito
12. `puppet.rs` — dito
13. `s3_minio.rs` — dito
14. `saltstack.rs` — dito
15. `shelly.rs` — dito
16. `signal.rs` — dito
17. `tasmota.rs` — dito
18. `temporal.rs` — dito
19. `thehive.rs` — dito
20. `home_assistant.rs` — `http_client()` mit timeout
21. `http.rs` — `http_client_custom(verify_ssl, follow_redirects)`
22. `matrix.rs` — `http_client()` mit timeout

#### Reflexes ohne HTTP (11 Dateien)
23. `make.rs` — `_app_ctx`, kein Client
24. `mqtt_publish.rs` — `_app_ctx`, rumqttc
25. `nats.rs` — `_app_ctx`, async_nats
26. `zigbee2mqtt.rs` — `_app_ctx`, rumqttc
27. `tts.rs` — `_app_ctx`, shell
28. `sound_alert.rs` — `_app_ctx`, shell
29. `fail2ban.rs` — `_app_ctx`, shell
30. `database.rs` — `_app_ctx`, sqlx
31. `email.rs` — `_app_ctx`, lettre
32. `syslog.rs` — `_app_ctx`, UDP
33. `irc.rs` — `_app_ctx`, irc-framework

#### Tentacles mit HTTP (10 Dateien)
34. `discord.rs` — `http_client()`
35. `webhooks_poller.rs` — bereits teilweise, vervollständigen
36. `geofence.rs` — `http_client()`
37. `ntfy.rs` — `http_client()`
38. `rest_api.rs` — `http_client()`
39. `rss.rs` — `http_client()`
40. `telegram.rs` — `http_client()`
41. `google_pubsub.rs` — `http_client()`
42. `kubernetes.rs` — bereits erledigt
43. `docker_events.rs` — bereits erledigt
44. `azure_servicebus.rs` — bereits erledigt
45. `aws_sqs.rs` — bereits erledigt

#### Tentacles ohne HTTP (20 Dateien)
46-65: ble, caldav, cron, database_poller, elasticsearch, file_watcher, ftp_sftp, google_chat, graphql, grpc, http, imap, irc, journalctl, kafka, matrix, modbus, mqtt, nats, packet_capture, pop3, rabbitmq, redis_streams, serial_port, signal, slack, smtp, snmp, stdin_tentacle, syslog_listener, teams, websocket, xmpp, zigbee, zwave

### Verification Strategy
- `cargo build` — 0 errors, 0 warnings
- `cargo clippy` — 0 warnings
- `cargo fmt` — 0 changes
- `cargo test` — alle Tests bestanden

# Delivery Steps

### ✓ Step 1: HTTP-Reflexes umstellen auf AppContext
Alle 22 HTTP-Reflexes bekommen `app_ctx: &AppContext` in `::new()` und nutzen den geteilten Client.
- elasticsearch.rs, esphome.rs, fhem.rs, grafana.rs, influxdb.rs, kubernetes.rs, n8n.rs, openhab.rs, powerdns.rs, printer_3d.rs, prometheus.rs, puppet.rs, s3_minio.rs, saltstack.rs, shelly.rs, signal.rs, tasmota.rs, temporal.rs, thehive.rs: `verify_ssl` → `http_client_danger_accept_invalid_certs()`
- home_assistant.rs, matrix.rs: `http_client()` mit timeout
- http.rs: `http_client_custom(verify_ssl, follow_redirects)`
- Alle Tests in diesen Dateien bekommen `let app_ctx = AppContext::new();`
- main.rs: `create_reflex_handler()` übergibt `app_ctx` an diese Reflexes

### ✓ Step 2: Nicht-HTTP-Reflexes umstellen auf AppContext
Alle 11 nicht-HTTP-Reflexes bekommen `_app_ctx: &AppContext` für API-Konsistenz.
- make.rs, mqtt_publish.rs, nats.rs, zigbee2mqtt.rs, tts.rs, sound_alert.rs, fail2ban.rs, database.rs, email.rs, syslog.rs, irc.rs
- Tests bekommen `let app_ctx = AppContext::new();`
- main.rs: `create_reflex_handler()` übergibt `app_ctx` an diese Reflexes

### ✓ Step 3: HTTP-Tentacles umstellen auf AppContext
Alle HTTP-Tentacles (die noch nicht erledigt sind) bekommen `app_ctx: &AppContext`.
- discord.rs: `http_client()` für Gateway URL Fetch
- webhooks_poller.rs: vervollständigen (bereits teilweise)
- geofence.rs, ntfy.rs, rest_api.rs, rss.rs, telegram.rs, google_pubsub.rs: `http_client()`
- Tests bekommen `let app_ctx = AppContext::new();`
- main.rs: `create_tentacle()` übergibt `app_ctx` an diese Tentacles

### ✓ Step 4: Nicht-HTTP-Tentacles umstellen auf AppContext
Alle verbleibenden ~30 Tentacles bekommen `_app_ctx: &AppContext` für API-Konsistenz.
- ble.rs, caldav.rs, cron.rs, database_poller.rs, elasticsearch.rs, file_watcher.rs, ftp_sftp.rs, google_chat.rs, graphql.rs, grpc.rs, http.rs, imap.rs, irc.rs, journalctl.rs, kafka.rs, matrix.rs, modbus.rs, mqtt.rs, nats.rs, packet_capture.rs, pop3.rs, rabbitmq.rs, redis_streams.rs, serial_port.rs, signal.rs, slack.rs, smtp.rs, snmp.rs, stdin_tentacle.rs, syslog_listener.rs, teams.rs, websocket.rs, xmpp.rs, zigbee.rs, zwave.rs
- Tests bekommen `let app_ctx = AppContext::new();`
- main.rs: `create_tentacle()` übergibt `app_ctx` an diese Tentacles

### ✓ Step 5: Quality Gates, Version, CHANGELOG, Commit
Finale Validierung und Release.
- `cargo fmt` — Formatierung
- `cargo clippy` — 0 Warnungen
- `cargo build` — 0 Fehler, 0 Warnungen
- `cargo test` — alle Tests bestanden
- Version: 0.57.0 (Cargo.toml, README.md Badge, CHANGELOG.md)
- tasks.md: Section 3.6 Punkte als erledigt markieren
- Git commit & push nach origin/main