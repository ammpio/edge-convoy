# Convoy: MQTT Bridge with SQLite Cache (Rust)

## 0) Design Principles

* **Pure bridge**: Connect two MQTT brokers - a local and a remote
* **Smallest viable feature set**:
  * MQTT **v3.1.1** support
  * **Local broker**: No auth/TLS (typically localhost)
  * **Remote broker**: TLS + username/password (optional mTLS)
  * **Topic mapping**: Add/remove prefixes, wildcards (+, #)
  * **Caching**: SQLite-backed offline buffer for messages destined **local→remote only**
* **Single binary**, single config file (TOML with bridge + cache config)

---

## 1) Runtime Topology

```
[ Broker A (local) ]  <────┐
  localhost:1883            │
  No auth/TLS               │
                            │
                      ┌─────▼──────┐
                      │   Convoy   │
                      │   Bridge   │  (rumqttc clients)
                      └─────┬──────┘
                            │
                      ┌─────▼──────┐
                      │   SQLite   │  (cache A→B when B down)
                      │   Cache    │
                      └────────────┘
                            │
                            │
[ Broker B (remote) ] <─────┘
  mqtt.example.com:8883
  TLS + auth
```

**Bridge operation**:
* Connects to **Broker A (local)** as MQTT client (no auth/TLS)
* Connects to **Broker B (remote)** as MQTT client (TLS + username/password)
* Subscribes to configured topics on each broker
* Forwards messages bidirectionally (A↔B) based on topic rules
* **Caches A→B messages** when B is unavailable or publish fails
* **NO caching for B→A** (real-time only)
* Replays cached messages in FIFO order when B reconnects
* **Publishes bridge state** to remote broker with LWT (Last Will and Testament)

---

## 2) Configuration (TOML)

```toml
[bridge]
# Connection to local broker (Broker A) - MQTT v3.1.1
local_addr = "127.0.0.1:1883"
local_client_id = "convoy-local"
local_keep_alive_secs = 30
local_clean_session = false

# Connection to remote broker (Broker B) - MQTT v3.1.1
remote_addr = "mqtt.example.com:8883"
remote_client_id = "convoy-remote"
remote_keep_alive_secs = 30
remote_clean_session = false
remote_username = "edge_device_01"
remote_password = "secure_password"
max_inflight = 100

# Bridge state publishing (LWT on remote broker)
state_topic = "bridge/convoy/state"  # topic on remote broker
state_online_payload = "1"               # payload when connected
state_offline_payload = "0"              # LWT payload when disconnected

# TLS for remote connection (native-tls)
[bridge.tls]
ca_file = "/etc/ssl/certs/ca-certificates.crt"

# Optional mTLS with PKCS12 client certificate
# client_cert = "/etc/convoy/client.p12"
# client_password = "password"

# Topics to forward LOCAL -> REMOTE (with caching)
# Maps local topic to <remote_prefix>/<local_topic>
[[bridge.forward]]
local_filter = "d/#"
remote_prefix = "a/node_123/"
qos = 1

[[bridge.forward]]
local_filter = "telemetry/#"
remote_prefix = "devices/edge1/"
qos = 1

# Topics to forward REMOTE -> LOCAL (no caching)
# Strips remote_prefix from remote topic before forwarding to local
[[bridge.subscribe]]
remote_filter = "a/node_123/u/#"
remote_prefix = "a/node_123/"  # strip this prefix
qos = 1

[[bridge.subscribe]]
remote_filter = "commands/edge1/#"
remote_prefix = "commands/edge1/"  # strip this prefix
qos = 1

# -------------------------
# SQLite cache (for A→B only)
# -------------------------

[cache]
sqlite_path = "/var/lib/convoy/cache.sqlite"

# Cache policy
cache_qos0 = false          # cache QoS0? (default false)
max_rows   = 500000         # hard cap to avoid unbounded growth
eviction   = "drop_oldest"  # "drop_oldest" | "reject_new"

# Replay behavior
flush_batch        = 1000   # messages per replay batch
flush_interval_ms  = 100    # replay tick interval
busy_timeout_ms    = 5000   # SQLite busy timeout

# SQLite durability
synchronous = "FULL"        # "FULL" | "NORMAL" | "OFF"
```

---

## 3) Behavior (Concise Rules)

### 3.1 Topic Mapping

**Wildcards in filters**:
* `+` matches single level: `sensors/+/temp` matches `sensors/room1/temp`
* `#` matches multiple levels: `data/#` matches `data/sensor/room1/temp`

**Mapping rules**:

**For LOCAL → REMOTE (forward rules)**:
* Maps `<local_topic>` to `<remote_prefix>/<local_topic>`
* Example: `local_filter = "d/#"` with `remote_prefix = "a/node_123/"`
  * Local topic `d/foo` → Remote topic `a/node_123/d/foo`
  * Local topic `d/bar/baz` → Remote topic `a/node_123/d/bar/baz`
* If no `remote_prefix`, forwards topic as-is

**For REMOTE → LOCAL (subscribe rules)**:
* Strips `remote_prefix` from remote topic before forwarding to local
* Example: `remote_filter = "a/node_123/u/#"` with `remote_prefix = "a/node_123/"`
  * Remote topic `a/node_123/u/foo` → Local topic `u/foo`
  * Remote topic `a/node_123/u/bar/baz` → Local topic `u/bar/baz`
* If no `remote_prefix`, forwards topic as-is

**QoS**: Per-rule QoS; can be different from original message QoS.

### 3.2 Upstream (A → B, with caching)

1. Bridge subscribes to local broker (A) for configured `local_filter` topics
2. On message receipt from A:
   * Apply topic mapping to get target topic for B
   * If **B connected** and publish succeeds → **no caching**
   * If **B disconnected** or **publish fails**:
     * **Enqueue** to SQLite: `topic`, `payload`, `qos`, `retain`, `ts_enqueued`
     * Skip QoS0 if `cache_qos0=false`

3. When B reconnects, **replay worker** drains SQLite in FIFO order:
   * Publish with configured QoS for that topic
   * Delete row after MQTT ack (PUBACK for QoS1, PUBCOMP for QoS2)
   * Process in batches: `flush_batch` messages every `flush_interval_ms`

### 3.3 Downstream (B → A, no caching)

* Bridge subscribes to remote broker (B) for configured `remote_filter` topics
* On message receipt from B:
  * Apply topic mapping to get target topic for A
  * Publish to A immediately (best-effort)
  * **No caching** if A is down (message lost)

### 3.4 Connection Handling

**On startup**:
* Connect to both A and B concurrently
* Set Last Will and Testament (LWT) on remote connection:
  * Topic: `state_topic`
  * Payload: `state_offline_payload` (e.g., "0")
  * QoS: 1, Retain: true
* On successful remote connection, publish:
  * Topic: `state_topic`
  * Payload: `state_online_payload` (e.g., "1")
  * QoS: 1, Retain: true
* Subscribe to configured topics on each broker
* Start replay worker (if B connected and cache not empty)

**On A disconnect**:
* Keep trying to reconnect (with backoff)
* Messages from B are lost (not cached)
* Cached A→B messages wait for B
* Bridge state remains online (A disconnect doesn't affect remote state)

**On B disconnect**:
* Keep trying to reconnect (with backoff)
* Messages from A are cached
* Messages from B won't arrive (B is down)
* LWT automatically publishes offline state to remote broker

**On reconnect**:
* Re-subscribe to topics
* Publish online state to remote broker (if B reconnected)
* Resume normal operation
* Start replaying cache (if B reconnected)

### 3.5 Ordering & Duplicates

* **A→B**: FIFO order guaranteed via SQLite `id` (autoincrement)
* **B→A**: No ordering guarantee (no caching)
* **Duplicates**: Possible after reconnect per MQTT semantics (QoS1/2 retries)

### 3.6 Limits & Eviction

Before enqueue, check `max_rows`:
* **drop_oldest**: Delete oldest rows to make room, then insert
* **reject_new**: Drop new message, log warning

---

## 4) SQLite Schema

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = FULL;      -- configurable
PRAGMA busy_timeout = 5000;      -- configurable

CREATE TABLE IF NOT EXISTS msg_queue (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  topic       BLOB NOT NULL,
  payload     BLOB NOT NULL,
  qos         INTEGER NOT NULL,   -- 0/1/2
  retain      INTEGER NOT NULL,   -- 0/1
  ts_enqueued INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_fifo ON msg_queue(id);
CREATE INDEX IF NOT EXISTS idx_queue_topic ON msg_queue(topic);
```

---

## 5) CLI

```bash
convoy --config /etc/convoy/config.toml
```

**Commands**:
* `convoy --config <path>` - Run bridge (default)
* `convoy --config <path> --log-level debug` - Set log level

---

## 6) Testing Setup

Integration tests use Docker Compose to run two MQTT brokers:
* **Local broker** (mosquitto): `localhost:1883` (no auth/TLS)
* **Remote broker** (mosquitto): `localhost:8883` (TLS + auth)

```yaml
# docker-compose.test.yml
version: '3'
services:
  local-broker:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./test-harness/mosquitto-local.conf:/mosquitto/config/mosquitto.conf

  remote-broker:
    image: eclipse-mosquitto:2
    ports:
      - "8883:8883"
    volumes:
      - ./test-harness/mosquitto-remote.conf:/mosquitto/config/mosquitto.conf
      - ./test-harness/certs:/mosquitto/certs
```

**Test scenarios**:
1. Basic forwarding (A→B and B→A)
2. Caching when remote is down
3. Cache replay on reconnect
4. Topic mapping with wildcards and prefixes
5. Bridge state publishing with LWT

---

## 7) Implementation Notes (Rust)

**Components**:
* `BridgeClient`: Manages two MQTT connections (local + remote), topic mapping, forwarding
* `CacheManager`: SQLite operations (enqueue, dequeue, eviction)
* `ReplayWorker`: Drains cache when remote connected

**Crates**:
* `rumqttc` (MQTT client v3.1.1 with native-tls)
* `rusqlite` (SQLite)
* `tokio` (async runtime)
* `serde`, `toml` (configuration)
* `tracing`, `tracing-subscriber` (logging)
* `thiserror` (errors)
* `clap` (CLI)
* `native-tls` (TLS)

**Key design**:
* Two `AsyncClient` instances (one for A, one for B)
* Single event loop using `tokio::select!` on both connections
* Topic mapping with simple prefix prepend/strip
* Cache only used for A→B direction
* Replay worker as separate tokio task
* LWT set on remote connection for bridge state

---

## 8) Acceptance Criteria

1. ✅ **MQTT v3.1.1** support
2. ✅ Connects to **local broker** (no auth/TLS)
3. ✅ Connects to **remote broker** (TLS + username/password, optional mTLS)
4. ✅ **Bidirectional forwarding** with topic mapping (wildcards, prefixes)
5. ✅ **Cache A→B messages** when B is down or publish fails
6. ✅ **Replay cache** in FIFO order when B reconnects
7. ✅ **No caching for B→A** (real-time only)
8. ✅ Cache limits & eviction policy enforced
9. ✅ Clean shutdown and restart with cache intact

---

## 9) Open Choices

* **Topic mapping**: Simple prefix prepend/strip (mosquitto-style)
* **QoS on forward**: Use rule QoS (not original message QoS)
* **Retain flag**: Forward as-is from original message
* **Clean session**: Configurable per connection (default: false for persistence)
* **Bridge state**: Published to remote broker with LWT (retain=true, QoS=1)

---

## 10) Example Scenarios

### Scenario 1: Edge to Cloud (Mosquitto-style)

```toml
[bridge]
state_topic = "bridge/edge1/state"
state_online_payload = "1"
state_offline_payload = "0"

# Forward local d/# to remote a/node_123/d/#
[[bridge.forward]]
local_filter = "d/#"
remote_prefix = "a/node_123/"
qos = 1

# Forward remote a/node_123/u/# to local u/#
[[bridge.subscribe]]
remote_filter = "a/node_123/u/#"
remote_prefix = "a/node_123/"
qos = 1
```

* Local publishes `d/sensors/temp` → Remote receives `a/node_123/d/sensors/temp` (cached if remote down)
* Remote publishes `a/node_123/u/commands/restart` → Local receives `u/commands/restart` (NOT cached if local down)
* Bridge publishes state to remote: `bridge/edge1/state` = "1" (LWT = "0")

### Scenario 2: Cloud Telemetry

```toml
[[bridge.forward]]
local_filter = "telemetry/#"
remote_prefix = "devices/edge1/"
qos = 1

[[bridge.subscribe]]
remote_filter = "commands/edge1/#"
remote_prefix = "commands/edge1/"
qos = 1
```

* Local publishes `telemetry/cpu` → Remote receives `devices/edge1/telemetry/cpu`
* Remote publishes `commands/edge1/update` → Local receives `update`

### Scenario 3: No Prefix (As-Is Forwarding)

```toml
[[bridge.forward]]
local_filter = "raw/#"
# No remote_prefix = forward as-is
qos = 1
```

* Local publishes `raw/data` → Remote receives `raw/data`
