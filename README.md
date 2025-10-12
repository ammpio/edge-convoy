# Convoy

A reliable MQTT bridge with SQLite message caching - for edge devices with patchy connectivity.

This is an opinionated implementation. Currently it has the following limitations:
- TLS support via `native-tls` and SQLite support via system library - to reduce binary size for edge environments
- Topic mapping is not fully dynamic; currently always based on prefix on remote broker
- No MQTT v5 support (only v3.1.1)

## Features

- **Bidirectional MQTT bridging** between local and remote brokers
- **SQLite-backed message cache** for local→remote messages when remote is unavailable
- **Automatic cache replay** when connection is restored (FIFO order)
- **Topic mapping** with wildcard support (`+`, `#`)
- **TLS support** for secure remote connections
- **MQTT v3.1.1** support (default)
- **Bridge state publishing** with Last Will and Testament (LWT)
- **Configurable cache policies** (size limits, eviction strategies)

## Use Cases

- **Edge-to-Cloud**: Bridge edge devices to cloud MQTT brokers with resilience to network outages
- **IoT Gateways**: Forward sensor data from local networks to remote servers
- **Data Aggregation**: Collect telemetry from local devices and batch forward to cloud

## Quick Start

### As a Standalone Application

#### 1. Build

```bash
cargo build --release
```

#### 2. Configure

Copy the example configuration and edit it:

```bash
cp config.example.toml config.toml
# Edit config.toml with your broker settings
```

#### 3. Run

```bash
./target/release/convoy --config config.toml
```

Or with debug logging:

```bash
./target/release/convoy --config config.toml --log-level debug
```

### As a Library

Add to your `Cargo.toml` (no default features suppresses default CLI dependencies):

```toml
[dependencies]
convoy = { version = "0.1", default-features = false }
```

**Example usage** (see `examples/programmatic.rs` for a complete example):

```rust
use convoy::{Bridge, BridgeConfig, BrokerConfig, CacheConfig, CacheManager, ForwardRule, TlsConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure bridge programmatically
    let bridge_config = BridgeConfig {
        local: BrokerConfig {
            addr: "127.0.0.1:1883".to_string(),
            client_id: "convoy-local".to_string(),
            keep_alive_secs: 30,
            clean_session: false,
            max_inflight: 100,
            username: None,
            password: None,
            tls: None,
        },
        remote: BrokerConfig {
            addr: "mqtt.example.com:8883".to_string(),
            client_id: "convoy-remote".to_string(),
            keep_alive_secs: 30,
            clean_session: false,
            max_inflight: 100,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            tls: Some(TlsConfig {
                ca_file: Some("/etc/ssl/certs/ca-certificates.crt".into()),
                client_cert: None,
                client_password: None,
                danger_accept_invalid_certs: false,
            }),
        },
        state_topic: "bridge/state".to_string(),
        state_online_payload: "1".to_string(),
        state_offline_payload: "0".to_string(),
        forward: vec![ForwardRule {
            local_filter: "sensors/#".to_string(),
            remote_prefix: "devices/edge1/".to_string(),
            qos: 1,
        }],
        subscribe: vec![],
    };

    // Configure cache with defaults
    let cache_config = CacheConfig {
        sqlite_path: "/tmp/convoy-cache.db".into(),
        ..Default::default()
    };

    // Create cache and bridge
    let cache = Arc::new(CacheManager::new(cache_config)?);
    let bridge = Bridge::new(bridge_config, cache).await?;

    // Run bridge
    bridge.run().await?;
    Ok(())
}
```

Run the example:

```bash
cargo run --example programmatic
```

## Configuration

See `config.example.toml` for a fully documented configuration template. Key sections:

### Bridge Settings

```toml
[bridge]
# Local broker (no auth/TLS)
local_addr = "127.0.0.1:1883"
local_client_id = "convoy-local"

# Remote broker (TLS + auth)
remote_addr = "mqtt.example.com:8883"
remote_client_id = "convoy-remote"
remote_username = "device01"
remote_password = "secret"

# Bridge state topic (published to remote)
state_topic = "bridge/convoy/state"
state_online_payload = "1"
state_offline_payload = "0"
```

### Topic Forwarding (Local → Remote)

Messages are cached if remote is unavailable:

```toml
[[bridge.forward]]
local_filter = "sensors/#"
remote_prefix = "devices/edge1/"
qos = 1
```

Example: Local `sensors/temp` → Remote `devices/edge1/sensors/temp`

### Topic Subscription (Remote → Local)

Messages are NOT cached (real-time only):

```toml
[[bridge.subscribe]]
remote_filter = "commands/edge1/#"
remote_prefix = "commands/edge1/"
qos = 1
```

Example: Remote `commands/edge1/restart` → Local `restart`

### Cache Settings

```toml
[cache]
sqlite_path = "/var/lib/convoy/cache.sqlite"
max_rows = 500000              # Maximum cached messages
eviction = "drop_oldest"       # or "reject_new"
flush_batch = 1000             # Messages per replay batch
flush_interval_ms = 100        # Replay interval
```

## How It Works

### Message Flow

1. **Local → Remote** (with caching):
   - Bridge subscribes to configured topics on local broker
   - When message arrives, applies topic mapping
   - If remote connected: publishes immediately
   - If remote down or publish fails: caches to SQLite
   - On reconnect: replays cache in FIFO order

2. **Remote → Local** (no caching):
   - Bridge subscribes to configured topics on remote broker
   - When message arrives, applies topic mapping and publishes to local
   - If local is down, message is lost (no caching)

### Topic Mapping

- **Wildcards**:
  - `+` matches single level: `sensors/+/temp` matches `sensors/room1/temp`
  - `#` matches multiple levels: `data/#` matches `data/sensor/temp/value`

- **Prefix mapping**:
  - Forward: prepends `remote_prefix` to local topic
  - Subscribe: strips `remote_prefix` from remote topic

### Bridge State

- On connect: publishes online state to `state_topic` (retained)
- On disconnect: LWT publishes offline state to `state_topic`
- Allows remote systems to monitor bridge health

## Architecture

```
┌──────────────────┐
│  Local Broker    │  (localhost:1883, no auth)
│   (Mosquitto)    │
└────────┬─────────┘
         │
    ┌────▼────┐
    │ Bridge  │  (rumqttc clients)
    │ Client  │
    └────┬────┘
         │
    ┌────▼────┐
    │ SQLite  │  (cache A→B only)
    │  Cache  │
    └────┬────┘
         │
┌────────▼─────────┐
│  Remote Broker   │  (TLS, port 8883)
│  (e.g. AWS IoT)  │
└──────────────────┘
```

## Cargo Features

- **`cli`** (default): Enables the command-line interface with TOML config file support
  - Required dependencies: `clap`, `toml`, `tracing-subscriber`
  - Use this feature when building the standalone binary
  - Library users don't need this feature (set `default-features = false`)

## Implementation Notes

- **MQTT Protocol**: v3.1.1 (via rumqttc)
- **Cache**: SQLite with WAL mode for concurrency
- **TLS**: native-tls with support for:
  - Custom CA certificates (PEM format)
  - Client certificates for mTLS (PKCS12 format)
  - System certificate store as fallback
- **Async Runtime**: Tokio

## Testing

Run unit tests:

```bash
cargo test
```

For integration testing with actual MQTT brokers, see `SPECS.md` section 6.

## CLI Options

```
convoy [OPTIONS]

Options:
  -c, --config <CONFIG>        Path to config file [default: config.toml]
  -l, --log-level <LOG_LEVEL>  Log level (trace|debug|info|warn|error) [default: info]
  -h, --help                   Print help
```

## License

See SPECS.md for design documentation and acceptance criteria.
