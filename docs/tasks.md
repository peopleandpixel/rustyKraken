nun # 🦑 rustyKraken – Tasks

Abhakbare Aufgabenliste für eine vollständige rustyKraken-Software, gruppiert nach Themenbereichen.

---

## 1. Tentacles (Nachrichtenquellen)

### 1.1 Bereits implementiert

- [x] **HTTP/Webhook-Tentacle** – Empfängt Nachrichten via POST-Requests (Axum)
- [x] **MQTT-Tentacle** – Vollständige Implementierung mit rumqttc 0.25
  - [x] TLS-Verbindung mit rustls (System-Root-CAs, custom CA, mTLS)
  - [x] Last-Will-Messages
  - [x] Clean Session Konfiguration
  - [x] QoS 0/1/2 Unterstützung
  - [x] Authentifizierung (Username/Password)
  - [x] Topic-Mapping mit Wildcard-Substitution (+ und #)
  - [x] Automatische Reconnection mit konfigurierbarer Verzögerung
  - [x] Resubscription nach Reconnection via try_subscribe
  - [x] 7 Unit-Tests für Topic-Matching und Mapping
- [x] **IMAP-Tentacle** – Vollständige Implementierung mit async-imap (TLS, UID-Speicherung, IDLE-Modus, Subject/Sender/All-Modi)

### 1.2 Zu implementierende Tentacles

#### E-Mail & Messaging

- [x] **POP3-Tentacle** – Vollständige Implementierung mit persistenter Message-ID-Speicherung
  - [x] POP3-Verbindung mit TLS (Port 995)
  - [x] POP3-Verbindung ohne TLS (plain, Port 110)
  - [x] STARTTLS-Option (fällt auf plain zurück)
  - [x] Nachrichtendownload und Parsing mit mailparse
  - [x] Topic-Erkennung aus Betreff/Absender
  - [x] Drei Topic-Modi: subject, sender, all
  - [x] Platzhalter-Unterstützung für Absender-Muster
  - [x] Absender-Mappings für zusätzliche Topic-Regeln
  - [x] Optionales Löschen nach Verarbeitung
  - [x] Persistente Message-ID-Speicherung (Vermeidung von Duplikaten)
  - [x] Fallback-Topic, wenn Betreff nicht mit "/" beginnt
  - [x] Umgebungsvariable-Referenzen im Passwort (${VAR_NAME})
  - [x] Generischer Pop3Client<S> für TLS und plain Verbindungen
  - [x] 4 Unit-Tests (Pattern-Matching, Storage)
- [x] **SMTP-Tentacle** – E-Mails via SMTP empfangen (als SMTP-Server)
  - [x] Eigener SMTP-Server auf Basis von tokio
  - [x] TLS-Unterstützung (implizite TLS, Port 465)
  - [x] Topic-Erkennung aus Betreff (Subject-Modus)
  - [x] Topic-Erkennung aus Absender (Sender-Modus)
  - [x] Alle E-Mails an festes Topic (All-Modus)
  - [x] Absender-Mappings mit Platzhaltern
  - [x] AUTH PLAIN Authentifizierung
  - [x] Erlaubte Absender filtern
  - [x] Dot-stuffing Entfernunge
  - [x] Maximale Nachrichtengröße konfigurierbar
  - [x] 4 Unit-Tests (Pattern-Matching, MAIL FROM, RCPT TO)

#### Push-Benachrichtigungen

- [x] **ntfy-Tentacle** – Empfängt Nachrichten vom ntfy-Push-Benachrichtigungsserver
  - [x] JSON Stream-Verbindung (HTTP long-polling)
  - [x] WebSocket-Verbindung zum ntfy-Broker
  - [x] Topic-Mapping von ntfy-Themen zu Kraken-Topics
  - [x] Authentifizierung (Bearer-Token/Basic Auth)
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] ntfy-Metadaten (Title, Priority, Tags, Click, Icon) als Ink-Metadaten
  - [x] 5 Unit-Tests (Topic-Mapping, Ink-Erstellung, JSON-Parsing, WebSocket-Events)
- [x] **Telegram-Tentacle** – Empfängt Nachrichten via Telegram Bot API
  - [x] Webhook-Modus mit Axum-Server
  - [x] Polling-Modus mit long-polling (getUpdates)
  - [x] Topic-Erkennung aus Commands (/kraken_topic)
  - [x] Topic-Erkennung aus Nachrichteninhalt (/topic-Präfix)
  - [x] Topic-Erkennung aus Chat-ID (chat_topic_mapping)
  - [x] Username-basiertes Topic-Mapping
  - [x] Gruppennachrichten, private Chats und Kanäle unterstützen
  - [x] Nachrichtentyp-Erkennung (Text, Foto, Dokument, Voice, Audio, Video, Location, Contact)
  - [x] Secret-Token-Validierung für Webhooks
  - [x] Allowed-Chats/Allowed-Users-Filter
  - [x] Webhook-Registrierung beim Telegram-Server
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] 8 Unit-Tests (Command-Extraktion, Topic-Modi, Ink-Erstellung, Filter, Nachrichtentypen, Webhook)
- [x] **Discord-Tentacle** – Empfängt Nachrichten via Discord Bot Gateway oder Webhook
  - [x] Discord Bot mit Gateway-Verbindung (WebSocket)
  - [x] Heartbeat und Session-Management
  - [x] MESSAGE_CREATE Event-Verarbeitung
  - [x] Webhook-Modus mit Axum-Server
  - [x] Topic-Mapping aus Channel, Guild oder Nachrichteninhalt
  - [x] Topic-Erkennung aus Nachrichteninhalt (/topic-Präfix)
  - [x] Channel-basiertes Topic-Mapping
  - [x] Guild-basiertes Topic-Mapping
  - [x] Drei Topic-Modi: channel, content, all
  - [x] Nachrichtentyp-Erkennung (Text, Bild, Video, Audio, Anhang, Embed, Reply)
  - [x] Rich Metadata (Author, Member/Nickname/Roles, Channel, Guild, Attachments, Embeds, Mentions)
  - [x] Allowed-Channels/Allowed-Guilds/Allowed-Users-Filter
  - [x] Secret-Token-Validierung für Webhooks
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] Sharding-Unterstützung
  - [x] Gateway Intents Konfiguration
  - [x] 13 Unit-Tests (Topic-Modi, Filter, Ink-Erstellung, Nachrichtentypen)

#### Dateibasierte Quellen

- [x] **File Watcher Tentacle** – Überwacht Ordner auf neue Dateien
  - [x] `notify`-Crate für Dateisystem-Events
  - [x] Konfigurierbare Ordner mit Topic-Mapping
  - [x] Dateityp-Filter (z.B. nur .txt, .json, .csv)
  - [x] Nach Verarbeitung verschieben/löschen
  - [x] Topic-Erkennung: folder, filename, content, all (kombiniert)
  - [x] Filename-basiertes Topic-Mapping mit Wildcard-Support
  - [x] Fallback-Topic, wenn kein Topic erkannt wird
  - [x] Rekursive Ordnerüberwachung konfigurierbar
  - [x] Verarbeitungsverzögerung für vollständige Dateischreibung
  - [x] Maximale Dateigröße konfigurierbar
  - [x] Rich Metadata (file_path, file_name, file_size, folder)
  - [x] 11 Unit-Tests (Pattern-Matching, Topic-Modi, Filter, Deserialisierung, Fallback)
- [x] **Stdin Tentacle** – Liest Nachrichten von der Standardeingabe
  - [x] Zeilenbasiertes Lesen
  - [x] JSON oder plain-text Format
  - [x] Auto-Erkennung des Formats (JSON wenn Zeile mit { beginnt)
  - [x] Topic-Erkennung aus Inhalt (/topic-Präfix)
  - [x] Zwei Topic-Modi: content (aus Inhalt), all (Fallback-Topic)
  - [x] Fallback-Topic konfigurierbar
  - [x] JSON-Felder: "topic", "body", "message"
  - [x] EOF und Ctrl+C Handling
  - [x] 16 Unit-Tests (Topic-Extraktion, Body-Extraktion, JSON-Parsing, Format-Erkennung, Konfiguration)

#### API & Web Services

- [x] **REST API Poller Tentacle** – Periodisches Abfragen von REST-APIs
  - [x] Konfigurierbare URLs und Intervalle
  - [x] Authentifizierung (Bearer, Basic, API-Key)
  - [x] JSON-Parsing und Topic-Mapping (json_path, topic_field, body_field)
  - [x] Rate-Limiting und Fehlerbehandlung (Retry-Logik)
  - [x] HTTP-Methoden: GET, POST, PUT, DELETE, PATCH
  - [x] Custom-Header und Request-Body
  - [x] 12 Unit-Tests (JSON-Pfad, String-Feld, Auth, Konfiguration)
- [x] **WebSocket Tentacle** – Verbindet zu WebSocket-Servern
  - [x] Einweg-Verbindung (nur empfangen)
  - [x] Topic-Mapping aus Nachrichteninhalt (field, content, all)
  - [x] ws:// und wss:// Unterstützung
  - [x] Auto-Reconnection mit konfigurierbarer Verzögerung
  - [x] Custom-Header und Subprotocol-Unterstützung
  - [x] Binär-Nachrichten als Base64
  - [x] Maximale Nachrichtengröße konfigurierbar
  - [x] 15 Unit-Tests (Topic-Modi, Body-Extraktion, Konfiguration, Header)
- [x] **RSS/Atom Feed Tentacle** – Liest RSS/Atom-Feeds
  - [x] Periodisches Polling konfigurierbarer Feeds
  - [x] Neue Einträge erkennen und als Ink speichern (Duplicate Detection)
  - [x] Topic aus Feed-Titel, Eintragstitel, Creator oder Fallback
  - [x] RSS 2.0, RSS 1.0 und Atom Feed Unterstützung
  - [x] Auto-Erkennung des Feed-Typs
  - [x] Retry-Logik bei Fehlern
  - [x] 15 Unit-Tests (Topic-Modi, Body-Erstellung, Feed-Parsing, Konfiguration)

#### IoT & Hardware

- [x] **Serial Port Tentacle** – Liest von seriellen Schnittstellen
  - [x] Konfigurierbare COM-Ports und Baudraten
  - [x] Protokoll-Parser (plain, JSON, CSV, custom)
  - [x] Custom-Delimiter als Hex-String
  - [x] Port-basiertes Topic-Mapping
  - [x] Auto-Reconnection mit konfigurierbarer Verzögerung
  - [x] Buffer-Overflow-Schutz
  - [x] Rich Metadata (port, baud_rate, parser)
  - [x] 11 Unit-Tests (Topic-Modi, Parser, Konfiguration, Deserialisierung)
- [x] **SNMP Trap Tentacle** – Empfängt SNMP-Traps
  - [x] UDP-Trap-Listener auf konfigurierbarer Adresse
  - [x] SNMP v1/v2c/v3 Unterstützung
  - [x] OID-Mapping zu Topics (exact match und prefix match)
  - [x] Längstes-Prefix-Matching für OID-Mappings
  - [x] Varbinds als JSON-Body
  - [x] Rich Metadata (trap_oid, version, community, message_type)
  - [x] 13 Unit-Tests (Topic-Modi, SNMP-Werte, Konfiguration, Deserialisierung)

#### Cloud & Dienste

- [x] **AWS SQS Tentacle** – Liest von Amazon SQS-Queues
  - [x] Queue-Polling mit Visibility Timeout
  - [x] AWS Signature v4 Authentifizierung
  - [x] Topic aus Message-Attributen (attribute, body, all Modi)
  - [x] Queue-URL zu Topic Mapping
  - [x] Nachrichten nach Verarbeitung löschen
  - [x] XML-Response Parsing
  - [x] 7 Unit-Tests (Topic-Modi, XML-Parsing, Konfiguration)
- [x] **Google Pub/Sub Tentacle** – Empfängt von Google Cloud Pub/Sub
  - [x] Subscription-Pulling via REST API
  - [x] OAuth2 JWT Authentifizierung (Service Account)
  - [x] Topic-Mapping (attribute, data, all Modi)
  - [x] Base64-Daten Decodierung
  - [x] Auto-Acknowledge
  - [x] 6 Unit-Tests (Topic-Modi, Base64, Ink-Erstellung)
- [x] **Slack Tentacle** – Empfängt Nachrichten via Slack Events API
  - [x] Event Subscription API mit Axum-Server
  - [x] Signing Secret Verifizierung (HMAC-SHA256)
  - [x] URL Verification Challenge
  - [x] Topic aus Channel oder Nachricht (channel, content, all Modi)
  - [x] Channel und User Filtering
  - [x] Rich Metadata (User-Profile, Files, Thread)
  - [x] 8 Unit-Tests (Topic-Modi, Filtering, Ink-Erstellung)
- [x] **Azure Service Bus Tentacle** – Empfängt Nachrichten von Azure Service Bus Queues/Topics
  - [x] REST API-basiertes Polling (keine zusätzlichen Dependencies)
  - [x] Queue und Topic/Subscription Support
  - [x] Drei Topic-Modi: entity, field, all
  - [x] Entity-basiertes Mapping für custom Routing
  - [x] Authentifizierung: SAS Shared Access Signature, Connection String
  - [x] SAS Token Generierung mit HMAC-SHA256
  - [x] Konfigurierbares Poll-Interval und max Nachrichten pro Poll
  - [x] Binäre Nachrichten als Base64
  - [x] Reiche Metadaten: asb_entity, asb_entity_type, asb_subscription
  - [x] 10 Unit-Tests (Topic-Auflösung, Auth-Varianten, Konfiguration, Deserialisierung)
- [x] **RabbitMQ Tentacle** – Empfängt Nachrichten via AMQP-Protokoll
  - [x] AMQP-Verbindung mit Auto-Reconnect
  - [x] Mehrere Queue-Abonnements mit Exchange-Binding
  - [x] Exchange-Typen: direct, fanout, headers, topic
  - [x] Drei Topic-Modi: routingkey, field, all
  - [x] Konfigurierbarer QoS Prefetch-Count
  - [x] Manuelles oder automatisches Message-Acknowledgment
  - [x] Binäre Nachrichten als Base64
  - [x] Reiche Metadaten: rabbitmq_queue, rabbitmq_routing_key
  - [x] 7 Unit-Tests (Topic-Auflösung, Metadaten, Deserialisierung)
- [x] **Kafka Tentacle** – Konsumiert von Apache Kafka Topics
  - [x] Kafka Consumer via rdkafka mit StreamConsumer (async)
  - [x] Consumer Group Support mit konfigurierbarer Group ID
  - [x] Multiple Topic-Abonnements
  - [x] Drei Topic-Modi: topic, field, all
  - [x] Topic-basiertes Mapping für custom Routing
  - [x] Authentifizierung: none, SASL Plain, SCRAM-SHA-256, SCRAM-SHA-512
  - [x] SSL/TLS Support mit konfigurierbarem CA-Zertifikat
  - [x] Konfigurierbarer Session Timeout, Heartbeat Interval, Max Poll Interval
  - [x] Auto Offset Reset (earliest/latest)
  - [x] Binäre Nachrichten als Base64
  - [x] Reiche Metadaten: kafka_topic, kafka_partition, kafka_offset, kafka headers
  - [x] 12 Unit-Tests (Topic-Auflösung, Auth-Varianten, Konfiguration, Deserialisierung)
- [x] **Redis Streams Tentacle** – Konsumiert von Redis Streams oder Lists (LPUSH/BRPOP)
  - [x] Zwei Modi: stream (XREADGROUP) und list (BRPOP)
  - [x] Drei Topic-Modi: name, field, all
  - [x] Consumer Group Auto-Erstellung mit MKSTREAM
  - [x] Authentifizierung: none, password, URL
  - [x] Konfigurierbare Batch-Größe, Block-Timeout, Reconnect-Delay
  - [x] Reiche Metadaten: redis_stream, redis_message_id
  - [x] 8 Unit-Tests (Topic-Auflösung, Metadaten, Deserialisierung)
- [x] **NATS Tentacle** – Empfängt Nachrichten von NATS Subjects
  - [x] NATS Server-Verbindung mit Auto-Reconnect
  - [x] Mehrere Subject-Abonnements mit Wildcard-Support (* und >)
  - [x] Drei Topic-Modi: subject, field, all
  - [x] Subject-basiertes Topic-Mapping mit Wildcard-Substitution
  - [x] Authentifizierung: none, credentials, token, NKey
  - [x] Binäre Nachrichten als Base64
  - [x] Reiche Metadaten: nats_subject, nats_reply_to
  - [x] 12 Unit-Tests (Pattern-Matching, Topic-Auflösung, Metadaten, Deserialisierung)

#### System & Infrastruktur

- [x] **Journalctl/systemd Tentacle** – Tailt systemd Journal-Logs als Events
  - [x] Follow-Modus mit `journalctl -f --output=export`
  - [x] Unit und Priority Filtering
  - [x] Vier Topic-Modi: identifier, unit, priority, all
  - [x] Identifier und Unit-basiertes Topic-Mapping
  - [x] Journal Export Format Parsing mit Metadaten-Extraktion
  - [x] Konfigurierbarer `--since` Zeitfilter
  - [x] Reiche Metadaten: journal_priority, journal_identifier, journal_unit
  - [x] Fallback zu Polling-Modus wenn Follow fehlschlägt
  - [x] 10 Unit-Tests (Topic-Auflösung, Parsing, Konfiguration, Command Building)
- [x] **Docker Events Tentacle** – Subscribt zu Docker Daemon Events (container start/stop, image pull, etc.)
  - [x] Event-Stream via `/events` Endpoint mit Auto-Reconnect
  - [x] Unix Socket und TCP/HTTPS Host Support
  - [x] Event-Type und Action Filtering
  - [x] Vier Topic-Modi: type, action, attribute, all
  - [x] Type und Action-basiertes Topic-Mapping
  - [x] TLS-Konfiguration mit optionaler Zertifikatsverifizierung
  - [x] Reiche Metadaten: docker_type, docker_action, docker_actor_id, docker_scope
  - [x] 8 Unit-Tests (Topic-Auflösung, Mapping, Konfiguration, Deserialisierung)
- [x] **Kubernetes Events Tentacle** – Watcht Kubernetes Cluster Events via API
  - [x] Watch API Stream mit Auto-Reconnect
  - [x] Namespace Filtering
  - [x] Bearer Token Authentifizierung (Service Account)
  - [x] Vier Topic-Modi: type, resource, namespace, all
  - [x] Resource-Type-basiertes Topic-Mapping
  - [x] TLS-Konfiguration mit optionaler Zertifikatsverifizierung
  - [x] Reiche Metadaten: k8s_event_type, k8s_resource_kind, k8s_namespace, k8s_labels
  - [x] 7 Unit-Tests (Topic-Auflösung, Mapping, Konfiguration, Deserialisierung)
- [x] **Network Packet Capture Tentacle** – Listet auf spezifische Netzwerk-Muster (via libpcap)
  - [x] libpcap-Integration via pcap-Crate
  - [x] Konfigurierbare Netzwerk-Schnittstelle
  - [x] BPF-Filter-Strings (z.B. "tcp port 80")
  - [x] Vier Topic-Modi: protocol, port, ip, all
  - [x] Port-basiertes Topic-Mapping
  - [x] IP-basiertes Topic-Mapping
  - [x] Protokoll-Filter (TCP, UDP, ICMP, ARP, DNS, HTTP, HTTPS)
  - [x] Rate-Limiting (max Pakete pro Sekunde)
  - [x] Promiscuous-Modus konfigurierbar
  - [x] Reiche Metadaten: protocol, src_ip, dst_ip, src_port, dst_port, packet_length, hex_data
  - [x] 13 Unit-Tests (Topic-Modi, Protokoll-Filter, Paket-Parsing, Deserialisierung)
- [x] **Syslog Listener Tentacle** – UDP/TCP Syslog-Server zum Empfangen von Logs von anderen Geräten
  - [x] UDP und TCP Transport-Protokoll
  - [x] BSD Syslog (RFC 3164) und RFC 5424 Parsing
  - [x] Automatische Format-Erkennung
  - [x] Drei Topic-Modi: facility, appname, all
  - [x] Facility-basiertes Topic-Mapping
  - [x] Reiche Metadaten: priority, facility, severity, hostname, appname, procid, msgid, timestamp
  - [x] Konfigurierbare Bind-Adresse, Puffergröße, max TCP-Verbindungen
  - [x] 13 Unit-Tests (Parsing, Topic-Auflösung, Metadaten, Deserialisierung)

#### Web & APIs (zusätzlich)

- [x] **GraphQL Subscriptions Tentacle** – Subscribt zu real-time GraphQL Endpoints
  - [x] GraphQL-WebSocket-Verbindung (graphql-ws Protokoll)
  - [x] Connection-Init und connection_ack Handshake
  - [x] Mehrere Subscriptions pro Verbindung
  - [x] Drei Topic-Modi: subscription, field, all
  - [x] Subscription-Namen-basiertes Topic-Mapping
  - [x] JSON-Feld-Extraktion für Topic
  - [x] Subscription-Variablen Support
  - [x] Auto-Reconnect mit konfigurierbarer Verzögerung
  - [x] Reiche Metadaten: subscription_name, subscription_id
  - [x] 9 Unit-Tests (Topic-Modi, Mapping, Messages, Deserialisierung)
- [x] **gRPC Tentacle** – gRPC Server/Stream Listener für protobuf-basierte Services
   - [x] gRPC-JSON-Transcoding über HTTP/2 (keine proto-Dateien nötig)
   - [x] Vier Aufruftypen: unary, server_streaming, client_streaming, bidirectional
   - [x] Drei Topic-Modi: method, field, all
   - [x] Method-basiertes Topic-Mapping
   - [x] Authentifizierung: None, Bearer Token, API Key, Custom Header
   - [x] Konfigurierbare Request-Payload
   - [x] Polling-Interval für Unary-Modus
   - [x] Streaming-Timeout und max Nachrichten pro Session
   - [x] Auto-Reconnect mit konfigurierbarer Verzögerung
   - [x] TLS-Support mit optionalem CA-Zertifikat
   - [x] gRPC-Metadata als Custom-Headers
   - [x] 16 Unit-Tests (Topic-Modi, Auth-Varianten, Header, Status-Extraktion, Ink-Erstellung)
- [ ] **CalDAV/CardDAV Tentacle** – Monitoriert Calendar Events oder Contacts für Triggers
- [ ] **Webhooks Poller Tentacle** – Periodisches Pollen von externen HTTP Endpoints für neue Daten

#### Data & Storage

- [x] **Database Poller Tentacle** – Periodisches Abfragen von SQL/NoSQL Datenbanken für neue/geänderte Zeilen
  - [x] Cross-Database-Support via sqlx AnyPool (SQLite, PostgreSQL, MySQL)
  - [x] Konfigurierbare SQL-Abfrage zum Pollen
  - [x] Drei Topic-Modi: table, field, all
  - [x] Tabellen-basiertes Topic-Mapping
  - [x] Incremental Polling mit Offset-Tracking
  - [x] Change-Detection mit konfigurierbaren Vergleichsoperatoren
  - [x] Konfigurierbares Poll-Interval, max Zeilen pro Poll, Query-Timeout
  - [x] Automatische Datenbank-Typ-Erkennung aus URL
  - [x] Reiche Metadaten: alle Spaltenwerte als Metadaten
  - [x] 11 Unit-Tests (Topic-Modi, Datenbank-Typ-Erkennung, Ink-Erstellung, Deserialisierung)
- [x] **Elasticsearch Tentacle** – Watcht Elasticsearch Indices für neue Dokumente
  - [x] REST API-basiertes Polling des _search Endpoints
  - [x] Konfigurierbare Indices (Wildcard-Support)
  - [x] Custom Search-Query (JSON)
  - [x] Drei Topic-Modi: index, field, all
  - [x] Index-basiertes Topic-Mapping
  - [x] Authentifizierung: None, Basic, API Key, Bearer Token
  - [x] Scroll API Support für große Ergebnismengen
  - [x] Change-Detection mit Last-Seen-Feld-Tracking
  - [x] Konfigurierbares Poll-Interval und max Dokumente pro Poll
  - [x] Reiche Metadaten: es_index, es_doc_id, alle Dokument-Felder
  - [x] 11 Unit-Tests (Topic-Modi, Auth-Varianten, Ink-Erstellung, Deserialisierung)
- [x] **FTP/SFTP Tentacle** – Monitoriert FTP/SFTP Verzeichnisse für neue Datei-Uploads
   - [x] Drei Protokolle: FTP, FTPES, SFTP
   - [x] REST API Mode für FTP-Server mit REST-Schnittstelle
   - [x] Dateimuster-Filterung
   - [x] Konfigurierbares Polling-Interval
   - [x] Delete after Download
   - [x] Max Dateigröße Limit
   - [x] Authentifizierung: Password, SSH Key
   - [x] 15 Unit-Tests

#### Scheduling & Time

- [x] **Cron/Scheduler Tentacle** – Generiert periodische Events basierend auf Cron-Ausdrücken (interner Trigger)
  - [x] 5-Feld und 6-Feld Cron-Ausdrücke (second minute hour day month weekday)
  - [x] Mehrere unabhängige Cron-Jobs pro Tentacle-Konfiguration
  - [x] Cron-Feld-Syntax: Wildcards (*), Bereiche (1-5), Schritte (*/15), Listen (1,3,5)
  - [x] Konfigurierbares Topic pro Job
  - [x] Optionaler statischer Body-Text pro Job
  - [x] Optionales Metadata-Map an jede generierte Nachricht
  - [x] Optionaler Startup-Delay vor erster Ausführung
  - [x] Benannte Jobs für besseres Logging
  - [x] 15 Unit-Tests (Cron-Parsing, Schedule-Matching, Next Occurrence, Deserialisierung)
- [x] **Geofence Tentacle** – Trigger Events wenn Geräte geografische Bereiche betreten/verlassen
   - [x] Zwei Zonenformen: Circle und Polygon
   - [x] Haversine Distanzberechnung
   - [x] Ray-Casting Algorithmus für Polygone
   - [x] Enter/Exit/Both Event-Typen
   - [x] Hysteresis gegen Flapping
   - [x] Konfigurierbarer Cooldown
   - [x] REST API Position Polling
   - [x] 12 Unit-Tests

#### Messaging & Communication (zusätzlich)

- [x] **Matrix Tentacle** – Empfängt Events von Matrix Rooms via Matrix Client-Server API
   - [x] Matrix Client-Server API v3 Timeline Endpoint
   - [x] Mehrere Räume gleichzeitig überwachen
   - [x] Access Token Authentifizierung
   - [x] Nachrichtentyp-Filterung
   - [x] Since Token Pagination
   - [x] Konfigurierbarer Home Server
   - [x] 10 Unit-Tests
- [x] **Signal Tentacle** – Listet auf Nachrichten via Signal Desktop oder Signal Gateway
   - [x] Signal Service API Integration
   - [x] REST API Mode für Signal Gateway
   - [x] API Token Authentifizierung
   - [x] Kontakt- und Gruppenüberwachung
   - [x] 10 Unit-Tests
- [x] **Microsoft Teams Tentacle** – Webhook-basierte oder bot-basierte eingehende Nachrichten von Teams Channels
   - [x] Webhook Mode für Incoming Webhooks
   - [x] Azure AD Authentifizierung
   - [x] Mehrere Channels gleichzeitig
   - [x] 10 Unit-Tests
- [ ] **Google Chat Tentacle** – Eingehende Webhooks oder Bot Events von Google Workspace Chat
- [x] **IRC Tentacle** – Verbindet zu IRC Channels und fängt Nachrichten ab
   - [x] Direkte TCP-Verbindung zu IRC-Servern
   - [x] Nickname-Registrierung und Channel-Join
   - [x] PING/PONG Keepalive
   - [x] PRIVMSG Parsing mit Own-Message-Filterung
   - [x] TLS Support
   - [x] Auto-Reconnect
   - [x] 10 Unit-Tests
- [x] **XMPP/Jabber Tentacle** – Listet auf Nachrichten in XMPP-Netzwerken
   - [x] XMPP Server via REST API (Prosody HTTP API)
   - [x] MUC (Multi-User Chat) Raumüberwachung
   - [x] Private Kontaktüberwachung
   - [x] Passwort und API Token Authentifizierung
   - [x] TLS Support
   - [x] 12 Unit-Tests

#### IoT & Hardware (zusätzlich)

- [ ] **Modbus Tentacle** – Industrieller Protokoll zum Lesen von Sensor-Daten von PLCs und Geräten
- [ ] **BLE/Bluetooth Low Energy Tentacle** – Listet auf BLE Advertisements oder verbindet mit BLE Peripherals
- [ ] **Zigbee Tentacle** – Home Automation Protokoll (via Zigbee2MQTT Bridge oder nativ)
- [ ] **Z-Wave Tentacle** – Weiteres Home Automation Protokoll

---

## 2. Reflexes (Aktionen & Listener)

### 2.1 Bereits implementiert

- [x] **File-Reflex** – Schreibt Nachrichten in Dateien (JSON-Logging)
- [x] **Command-Reflex** – Führt Shell-Befehle aus mit Umgebungsvariablen (INK_JSON, INK_TOPIC, INK_BODY, INK_ID)

### 2.2 Zu implementierende Reflexes

#### Messaging & Benachrichtigungen

- [x] **WhatsApp-Reflex** – Sendet Nachrichten via WhatsApp
  - [x] Integration mit WhatsApp Business API (Graph API)
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Konfigurierbare Empfängernummern
  - [x] Text- und Template-Nachrichten
  - [x] Topic-basiertes Recipient-Mapping
  - [x] Topic-basiertes Template-Mapping
  - [x] Drittanbieter-API-Unterstützung (custom api_base_url)
  - [x] Optionale Metadata-Einbindung in Nachrichten
  - [x] 16 Unit-Tests (Message-Type, Recipients, Templates, Body-Rendering, Config)
- [x] **Telegram-Reflex** – Sendet Nachrichten via Telegram Bot
  - [x] Bot-Token-Konfiguration
  - [x] Chat-ID-Konfiguration (pro Reflex oder dynamisch via topic_chat_mapping)
  - [x] Markdown- und HTML-Formatierung (parse_mode: Markdown, HTML, None)
  - [x] Topic-basiertes Chat-ID-Mapping
  - [x] Topic-basiertes Parse-Mode-Mapping
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Nachrichtentrunkierung bei Telegram-Limit (4096 Zeichen)
  - [x] Optionale Metadata-Einbindung in Nachrichten
  - [x] Custom API Base URL für Proxies
  - [x] 10 Unit-Tests (Chat-ID-Mapping, Parse-Mode-Mapping, Template-Rendering, Trunkierung, Metadata, Deserialisierung)
- [x] **Discord-Reflex** – Sendet Nachrichten via Discord Webhooks oder Bot
  - [x] Webhook-URL-Konfiguration
  - [x] Embed-Nachrichten mit Farben und Feldern
  - [x] Avatar und Benutzernamen konfigurieren
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Webhook-URL-Mapping
  - [x] Topic-basiertes Embed-Farben-Mapping
  - [x] Ink-Metadaten als Embed-Felder
  - [x] Embed-Titel, Beschreibung, Footer mit Templates
  - [x] 15 Unit-Tests (Webhook-Mapping, Farben, Templates, Embeds, Trunkierung, Deserialisierung)
- [x] **ntfy-Reflex** – Sendet Push-Benachrichtigungen via ntfy
  - [x] Broker-URL konfigurierbar
  - [x] Priorität, Tags, Klick-Links
  - [x] Authentifizierung (Basic Auth)
  - [x] Template-Engine für Titel und Nachricht ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes ntfy-Topic-Mapping
  - [x] Topic-basiertes Prioritäten-Mapping
  - [x] Topic-basiertes Tags-Mapping
  - [x] Icon und Markdown-Unterstützung
  - [x] 12 Unit-Tests (Topic-Mapping, Prioritäten, Tags, Templates, Metadaten, Deserialisierung)
- [x] **E-Mail-Reflex** – Sendet E-Mails via SMTP
  - [x] `lettre`-Integration (bereits in Cargo.toml)
  - [x] Absender, Empfänger, Betreff konfigurierbar
  - [x] HTML- und Text-Body
  - [x] Anhänge unterstützen
  - [x] CC und BCC Empfänger
  - [x] Template-Engine für Betreff und Body ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Recipient-Mapping
  - [x] Topic-basiertes Subject-Mapping
  - [x] STARTTLS und implizite TLS Unterstützung
  - [x] 12 Unit-Tests (Recipient-Mapping, Subject-Mapping, Templates, Metadaten, Deserialisierung)

#### Web & API

- [x] **HTTP/Webhook-Reflex** – Sendet HTTP-Requests an externe APIs
  - [x] GET, POST, PUT, DELETE, PATCH Methoden
  - [x] Header-Konfiguration
  - [x] JSON-Body mit Template-Engine
  - [x] Authentifizierung (Bearer, Basic, API-Key)
  - [x] Retry-Logik bei Fehlern
  - [x] Topic-basiertes URL-Mapping
  - [x] Topic-basiertes Method-Mapping
  - [x] Topic-basiertes Body-Mapping
  - [x] URL-Template mit Platzhaltern
  - [x] Konfigurierbarer Timeout und Content-Type
  - [x] Redirect-Handling konfigurierbar
  - [x] 14 Unit-Tests (URL-Mapping, Method-Mapping, Templates, Metadaten, Auth, Deserialisierung)
- [x] **Matrix/Element-Reflex** – Sendet Nachrichten via Matrix
  - [x] Matrix Client-Server REST API Integration
  - [x] Raum-ID und Access-Token-Konfiguration
  - [x] Nachrichtentypen: text, notice, emote
  - [x] HTML-formatierter Body mit `org.matrix.custom.html`
  - [x] Template-Engine für Body ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Room-ID-Mapping
  - [x] Topic-basiertes Message-Type-Mapping
  - [x] Topic-basiertes Body-Template-Mapping
  - [x] Retry-Logik bei Fehlern
  - [x] Konfigurierbarer Timeout
  - [x] 14 Unit-Tests (Room-Mapping, Message-Type, Templates, Metadaten, Payload, Deserialisierung)

#### Dateibasierte Aktionen

- [x] **File Write Reflex** – Schreibt Nachrichten in Dateien (erweitert)
  - [x] Verschiedene Formate (JSON, CSV, plain text, XML)
  - [x] Template-Engine für Dateiinhalte
  - [x] Rotation und Größenlimits
  - [x] CSV-Konfiguration (Spalten, Header, Delimiter)
  - [x] XML-Ausgabe mit Escape-Funktion
  - [x] Pfad-Template mit Platzhaltern
  - [x] 20 Unit-Tests (Formate, Templates, Rotation, Deserialisierung)
- [x] **Database Reflex** – Speichert Nachrichten in einer Datenbank
  - [x] SQLite-Unterstützung via sqlx
  - [x] PostgreSQL-Unterstützung via sqlx
  - [x] MySQL/MariaDB-Unterstützung via sqlx
  - [x] Konfigurierbare Tabellennamen und Schemata
  - [x] Auto-Creation der Tabelle beim Start
  - [x] Connection Pooling mit konfigurierbarer Größe
  - [x] Metadaten als JSON-Spalte (optional)
  - [x] Status-Feld (optional)
  - [x] Duplikatsvermeidung (ON CONFLICT / INSERT IGNORE)
  - [x] Abfrage nach Topic-Muster (LIKE)
  - [x] Nachrichten-Zählung
  - [x] 12 Unit-Tests (Konfiguration, Deserialisierung, SQL-Generierung, Status-Mapping, Pattern-Escaping)

#### System & Automatisierung

- [x] **Script-Reflex** – Führt Skripte aus (erweitert)
  - [x] Lua-Skripting via Subprozess (lua)
  - [x] JavaScript-Skripting via Subprozess (node)
  - [x] Python-Skripting via Subprozess (python3)
  - [x] Zugriff auf Ink-Daten via Umgebungsvariablen (INK_TOPIC, INK_BODY, INK_ID, INK_TIMESTAMP, INK_METADATA, INK_JSON)
  - [x] Inline-Skriptcode oder externe Skriptdatei
  - [x] Konfigurierbarer Interpreter-Pfad
  - [x] Template-Engine für Umgebungsvariablen ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Custom Umgebungsvariablen mit Template-Rendering
  - [x] Ausgabe-Erfassung und optionales Publish an Topic
  - [x] Konfigurierbarer Timeout
  - [x] 11 Unit-Tests (Template-Rendering, Interpreter-Auswahl, Konfiguration, Deserialisierung)
- [x] **Home Assistant Reflex** – Sendet Events an Home Assistant
  - [x] REST API Integration
  - [x] Event-Fire und Service-Call
  - [x] Long-lived Access Token Authentifizierung
  - [x] Zwei Aktionstypen: event (custom event) und service (service call)
  - [x] Template-Engine für Titel und Nachricht ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Event-Name-Mapping
  - [x] Topic-basiertes Service-Call-Mapping (domain/service)
  - [x] Topic-basiertes Message- und Title-Template-Mapping
  - [x] Zusätzliche statische Daten (additional_data)
  - [x] Ink-Metadaten als Event-Data-Felder
  - [x] Retry-Logik bei Fehlern
  - [x] Konfigurierbarer Timeout
  - [x] 12 Unit-Tests (Event-Mapping, Service-Mapping, Templates, Metadaten, Payload, Deserialisierung)
- [x] **Syslog Reflex** – Sendet Nachrichten an Syslog
  - [x] UDP/TCP Syslog
  - [x] Konfigurierbare Priorität und Facility
  - [x] BSD und RFC 5424 Message-Format
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Facility-Mapping
  - [x] Topic-basiertes Severity-Mapping
  - [x] Topic-basiertes Message-Template-Mapping
  - [x] Retry-Logik bei Fehlern
  - [x] 12 Unit-Tests (Facility-Mapping, Severity-Mapping, Templates, Metadaten, BSD/RFC5424-Format, Deserialisierung)

#### Transformation & Routing

- [x] **Tentacle Reflex (Filter/Forward)** – Filtert und weiterleitet Nachrichten
  - [x] Bedingte Weiterleitung basierend auf Topic- und Body-Filtern (Regex)
  - [x] Topic-Umbenennung mit exaktem Mapping, Prefix-Mapping und Template-Engine
  - [x] Body-Transformation mit Template-Engine ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Metadata-Erhaltung oder -Filterung mit konfigurierbaren Schlüsseln
  - [x] Custom Metadata-Felder hinzufügen
  - [x] Original-Topic als __original_topic für Tracing
  - [x] 16 Unit-Tests (Topic-Mapping, Prefix-Mapping, Template-Rendering, Filter, Metadata, Deserialisierung)
- [x] **Aggregator Reflex** – Sammelt und aggregiert Nachrichten
  - [x] Zeitbasierte Aggregation mit konfigurierbarem Fenster
  - [x] Aggregationsfunktionen: count, sum, average, min, max
  - [x] Mehrere Aggregationsgruppen pro Reflex
  - [x] Topic-Muster-Matching pro Gruppe
  - [x] Numerische Wertextraktion aus Body, JSON-Feld oder Metadaten
  - [x] Template-Engine für aggregierte Nachrichten ({{count}}, {{sum}}, {{average}}, {{min}}, {{max}}, {{group_name}}, {{topic_pattern}}, {{window_start}}, {{window_end}})
  - [x] Reiche Metadaten in aggregierten Nachrichten
  - [x] Flush-on-first-Modus für Tests
  - [x] 12 Unit-Tests (Konfiguration, Akkumulator, Template-Rendering, Wertextraktion, Topic-Matching, Deserialisierung)

#### Messaging & Communication (zusätzlich)

- [ ] **Matrix Reflex** – Sendet Nachrichten an Matrix Rooms (completes bidirectional Matrix capability)
- [ ] **Signal Reflex** – Sendet Nachrichten via Signal
- [ ] **Microsoft Teams Reflex** – Postet an Teams via Webhooks oder Bot API
- [ ] **Google Chat Reflex** – Sendet Cards oder Nachrichten an Google Chat Spaces
- [ ] **IRC Reflex** – Sendet Nachrichten an IRC Channels
- [ ] **Pushbullet Reflex** – Sendet Push-Benachrichtigungen an Geräte
- [ ] **Gotify Reflex** – Self-hosted Push-Benachrichtigungsdienst

#### Cloud & Queue Services

- [ ] **Azure Service Bus Reflex** – Publish Messages zu Azure Queues/Topics
- [ ] **RabbitMQ Reflex** – Publish zu AMQP Exchanges
- [ ] **Kafka Reflex** – Produce Messages zu Kafka Topics
- [ ] **NATS Reflex** – Publish zu NATS Subjects
- [ ] **AWS SNS Reflex** – Publish zu Amazon Simple Notification Service

#### System & Infrastruktur

- [ ] **Docker Reflex** – Trigger Docker Actions (start/stop/restart containers, create images)
- [ ] **Kubernetes Reflex** – Trigger kubectl Actions oder API Calls
- [ ] **Ansible Reflex** – Trigger Ansible Playbooks
- [ ] **SaltStack Reflex** – Execute Salt States oder Commands
- [ ] **Puppet Reflex** – Trigger Puppet Runs
- [ ] **PowerDNS Reflex** – Update DNS Records dynamisch

#### Home Automation & IoT

- [ ] **MQTT Publish Reflex** – Publish Messages zu MQTT Topics (completes bidirectional MQTT capability)
- [ ] **Zigbee2MQTT Reflex** – Control Zigbee Devices via MQTT Bridge
- [ ] **Shelly Reflex** – Control Shelly Smart Devices
- [ ] **Tasmota Reflex** – Control Tasmota-flashed Devices
- [ ] **ESPHome Reflex** – Control ESPHome Devices
- [ ] **FHEM Reflex** – Interact with FHEM Home Automation
- [ ] **openHAB Reflex** – Send Commands to openHAB

#### Notifications

- [ ] **Pushover Reflex** – Sendet Push-Benachrichtigungen via Pushover API
- [ ] **Bark Reflex** – iOS Push-Benachrichtigungen via Bark Server
- [ ] **Custom Webhook Reflex** – Generic HTTP POST an beliebige Webhook-URLs mit Templating
- [ ] **Sound/Alert Reflex** – Spielt Sounds oder triggert Alerts auf dem lokalen Rechner
- [ ] **LED/Display Reflex** – Update LED Matrices, E-Ink Displays oder Smart Displays

#### Data & Storage

- [ ] **Elasticsearch Reflex** – Index Events in Elasticsearch
- [ ] **InfluxDB Reflex** – Schreibt Time-Series Daten nach InfluxDB
- [ ] **Prometheus Reflex** – Exponiert Metriken im Prometheus Format
- [ ] **S3/MinIO Reflex** – Upload Event-Daten zu Object Storage
- [ ] **FTP/SFTP Upload Reflex** – Upload Dateien zu entfernten FTP/SFTP Servern

#### Security & Monitoring

- [ ] **Fail2Ban Reflex** – Add/Remove IPs von Fail2Ban
- [ ] **Grafana Reflex** – Sendet Annotations oder triggert Alerts in Grafana
- [ ] **TheHive Reflex** – Erstellt Cases in TheHive SIEM
- [ ] **Shodan Reflex** – Query oder Update Shodan Daten

#### Scheduling & Workflow

- [ ] **n8n/Node-RED Reflex** – Trigger externe Automation Workflows
- [ ] **Make.com (Integromat) Reflex** – Trigger Make Scenarios
- [ ] **Zapier Reflex** – Trigger Zapier Webhooks
- [ ] **Temporal Reflex** – Startet Temporal Workflow Executions

#### Creative & Fun

- [ ] **TTS (Text-to-Speech) Reflex** – Konvertiert Event-Daten zu Speech und spielt sie ab
- [ ] **Printer Reflex** – Druckt Event-Zusammenfassungen oder Labels
- [ ] **3D Printer Reflex** – Trigger 3D Print Jobs

---

## 3. Performanceoptimierungen

### 3.1 Event Bus

- [x] **Channel-Größe optimieren** – Aktuelle feste Größe (1000) durch konfigurierbare Größe ersetzen
- [x] **Prioritäts-Warteschlange** – Wichtige Nachrichten (z.B. Warnungen) priorisieren
- [x] **Backpressure-Handling** – Verhaltun bei überlastetem EventBus definieren (drop oldest, block, sample)
- [x] **Metriken** – Nachrichtenrate, Latenz, Queue-Größe messen und exponieren

### 3.2 Mantle (Speicherung)

- [ ] **Batch-Speicherung** – Mehrere Ink-Nachrichten in einem Transaction speichern
- [ ] **WAL-Modus** – SQLite Write-Ahead Logging aktivieren für bessere Schreibperformance
- [ ] **Vakuum und Optimierung** – Regelmäßige Datenbankwartung planen
- [ ] **Partitionierung** – Alte Nachrichten in Archiv-Tabellen verschieben
- [ ] **Full-Text-Suche** – FTS5-Index für Volltextsuche in Nachrichteninhalt
- [ ] **Alternative Datenbanken** – PostgreSQL/MySQL-Support als Option neben SQLite
- [ ] **Komprimierung** – Große Nachrichten komprimiert speichern (zlib/zstd)

### 3.3 Tentacles

- [ ] **Connection Pooling** – Wiederverwendung von Verbindungen (IMAP, HTTP)
- [ ] **Async I/O Optimierung** – Alle I/O-Operationen sind asynchron (prüfen und optimieren)
- [ ] **Rate Limiting** – Pro Tentacle konfigurierbare Rate-Limits
- [ ] **Circuit Breaker** – Bei wiederholten Fehlern temporär pausieren
- [ ] **Parallel Processing** – Mehrere Nachrichten parallel verarbeiten

### 3.4 Reflexes

- [ ] **Parallel Execution** – Mehrere Reflexes parallel ausführen
- [ ] **Retry-Logik** – Exponentielles Backoff bei fehlgeschlagenen Aktionen
- [ ] **Dead Letter Queue** – Fehlgeschlagene Nachrichten separat speichern
- [ ] **Throttling** – Maximal N Aktionen pro Zeitfenster

### 3.5 Allgemein

- [ ] **Memory Profiling** – Speicherverbrauch mit `dhat` oder `heaptrack` analysieren
- [ ] **CPU Profiling** – Hotspots mit `perf` oder `flamegraph` identifizieren
- [ ] **Tokio Console** – Integration für Runtime-Metriken und Debugging
- [ ] **Prometheus Metriken** – Exponiere Metriken via `/metrics` Endpoint
- [ ] **Graceful Shutdown** – Sauberes Beenden aller Tentacles und Reflexes mit Signal-Handling

---

## 4. Sonstige Optimierungen & Features

### 4.1 Konfiguration

- [ ] **Hot-Reload** – Config-Änderungen ohne Neustart erkennen und anwenden
  - [ ] `notify`-Crate für `config/incoming/` und `config/listener/` überwachen
  - [ ] Neue Tentacles/Reflexes dynamisch starten
  - [ ] Entfernte Tentacles/Reflexes sauber stoppen
- [ ] **Config-Validierung** – Schema-Validierung beim Laden mit Fehlermeldungen
- [ ] **Umgebungsvariablen** – `${VAR}`-Ersetzung in allen Config-Dateien
- [ ] **Secrets-Management** – Integration mit HashiCorp Vault oder similar
- [ ] **Config-Beispiele** – Vollständige Beispiel-Configs für alle Tentacle/Reflex-Typen

### 4.2 Template-Engine

- [ ] **Template-Sprache** – `{{message.body}}`, `{{message.topic}}`, `{{message.timestamp}}` etc.
- [ ] **Filter und Funktionen** – `{{message.body | uppercase}}`, `{{message.body | truncate: 100}}`
- [ ] **Bedingungen** – `{% if message.status == "new" %}...{% endif %}`
- [ ] **Crate-Auswahl** – `tera`, `askama`, oder `minijinja` evaluieren und integrieren

### 4.3 Web-UI & API

- [ ] **REST API** – HTTP-API für Abfragen und Steuerung
  - [ ] `GET /api/ink` – Liste der neuesten Nachrichten
  - [ ] `GET /api/ink/{id}` – Einzelne Nachricht
  - [ ] `GET /api/ink/topic/{topic}` – Nachrichten nach Topic
  - [ ] `POST /api/ink` – Manuelles Einspeisen von Nachrichten
  - [ ] `GET /api/health` – Health-Check
  - [ ] `GET /api/status` – Status aller Tentacles und Reflexes
- [ ] **Web-UI** – Einfache Weboberfläche
  - [ ] Nachrichtenübersicht mit Filter und Suche
  - [ ] Echtzeit-Updates via Server-Sent Events (SSE) oder WebSocket
  - [ ] Config-Editor für Tentacles und Reflexes
  - [ ] Dashboard mit Statistiken
- [ ] **Authentifizierung** – Schutz der API und Web-UI
  - [ ] Basic Auth oder Token-basiert
  - [ ] JWT-Unterstützung
  - [ ] Role-Based Access Control (RBAC)

### 4.4 Logging & Monitoring

- [ ] **Strukturiertes Logging** – JSON-Logs mit `tracing-json`
- [ ] **Log-Level pro Modul** – Fein granulare Log-Konfiguration
- [ ] **Health Checks** – Regelmäßige Prüfung aller Tentacles und Reflexes
- [ ] **Alerting** – Benachrichtigung bei kritischen Fehlern
- [ ] **Audit-Log** – Protokollierung von Config-Änderungen und Aktionen

### 4.5 Testing & Qualität

- [ ] **Integrationstests** – End-to-End-Tests mit echten Diensten (Docker-Container)
- [ ] **Unit-Tests** – Erhöhte Testabdeckung für alle Module
- [ ] **Fuzz-Testing** – `cargo-fuzz` für Eingabevalidierung
- [ ] **CI/CD Pipeline** – GitHub Actions oder GitLab CI
  - [ ] Build und Test bei jedem Commit
  - [ ] Clippy und rustfmt-Prüfung
  - [ ] Security-Audits mit `cargo-audit`
- [ ] **Dokumentation** – KDoc-Kommentare für alle öffentlichen APIs
- [ ] **Benchmark-Tests** – Performance-Tests mit `criterion`

### 4.6 Deployment & Betrieb

- [ ] **Systemd-Service** – `.service`-Datei für Linux-Daemon
- [ ] **Docker-Image** – Offizielles Docker-Image mit Multi-Stage-Build
- [ ] **Docker Compose** – Beispiel-Setup mit MQTT, ntfy, etc.
- [ ] **Systemd-Path** – Auto-Start bei Systemstart
- [ ] **Log-Rotation** – `logrotate`-Konfiguration
- [ ] **Update-Mechanismus** – Selbstupdate oder Package-Manager-Integration
- [ ] **Plattformen** – Releases für Linux, macOS, Windows

### 4.7 Sicherheit

- [ ] **Input-Validierung** – Alle externen Eingaben validieren und sanitieren
- [ ] **TLS-Zertifizierung** – Zertifikatsvalidierung bei allen externen Verbindungen
- [ ] **Command-Injection-Schutz** – Shell-Befehle im Command-Reflex sicher ausführen
- [ ] **Rate-Limiting** – Schutz vor DoS bei HTTP-Webhook
- [ ] **CORS-Konfiguration** – Bei Web-UI und API
- [ ] **Security-Audit** – Regelmäßige `cargo-audit`-Prüfung

### 4.8 Plugin-System

- [ ] **Dynamisches Laden** – Tentacles und Reflexes als separate Shared Libraries (.so/.dll)
- [ ] **Plugin-API** – Stabile, versionierte API für externe Plugins
- [ ] **Plugin-Registry** – Zentrale Liste verfügbarer Plugins
- [ ] **Sandboxing** – Isolation von Plugins für Sicherheit

### 4.9 Nachrichtenversionierung & Migration

- [ ] **Ink-Schema-Versionierung** – Versionsfeld im Ink-Format
- [ ] **Migration-Tools** – Hilfsprogramme für Datenbank- und Config-Migrationen
- [ ] **Abwärtskompatibilität** – Alte Config-Formate unterstützen

### 4.10 Dokumentation

- [ ] **Architektur-Dokument** – Detaillierte Beschreibung der Architektur
- [ ] **API-Dokumentation** – Auto-generiert via `cargo doc`
- [ ] **Tutorials** – Schritt-für-Schritt-Anleitungen für häufige Use-Cases
- [ ] **FAQ** – Häufig gestellte Fragen und Lösungen
- [ ] **Changelog** – Versionshistorie mit `cargo-release`

---

## 5. Known Issues & Technische Schulden

- [ ] **IMAP STARTTLS** – async-imap unterstützt STARTTLS nicht direkt; Config-Option warnt und fällt auf implizite TLS zurück
- [ ] **IMAP IDLE-Loop** – Beendet sich nach der ersten Nachricht und bricht zur Hauptschleife zurück; kontinuierlicher IDLE-Betrieb optimieren
- [ ] **Warnungen** – 19 Compiler-Warnungen (unused imports, dead code, etc.) beseitigen
- [ ] **serde_yaml deprecated** – Ersatz `yaml_serde` via Package-Renaming verwendet; auf stabile Lösung achten
- [ ] **imap-proto Warnings** – Future-Incompatibility-Warnungen in imap-proto v0.10.2; Upgrade-Pfad prüfen
- [ ] **Mantle Session Ownership** – Session wird in `fetch_and_process_new_emails` consumed; IDLE-Modus muss neu verbinden

---

## Priorisierung

### Phase 1 – MVP-Ergänzung (hohe Priorität)
- Template-Engine
- HTTP/Webhook-Reflex
- E-Mail-Reflex (lettre)
- Config Hot-Reload
- REST API (Basis)
- Warnungen beseitigen
- Integrationstests

### Phase 2 – Erweiterung (mittlere Priorität)
- Telegram-Reflex
- Discord-Reflex
- File Watcher Tentacle
- Web-UI (Basis)
- Batch-Speicherung im Mantle
- Prometheus Metriken

### Phase 3 – Polish & Skalierung (niedrigere Priorität)
- Plugin-System
- Alternative Datenbanken
- Authentifizierung & RBAC
- Docker-Image und Deployment
- Performance-Profiling und Optimierung
- Fuzz-Testing
