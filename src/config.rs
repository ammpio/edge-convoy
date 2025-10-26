#![allow(clippy::result_large_err)]

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Top-level configuration for the MQTT bridge.
///
/// This combines both bridge connection settings and cache configuration.
///
/// # Examples
///
/// ```no_run
/// use convoy::{Config, BridgeConfig, BrokerConfig, CacheConfig, TlsConfig};
///
/// let config = Config {
///     bridge: BridgeConfig {
///         local: BrokerConfig {
///             addr: "127.0.0.1:1883".into(),
///             client_id: "bridge-local".into(),
/// # keep_alive_secs: 30,
/// # clean_session: false,
/// # max_inflight: 100,
/// # username: None,
/// # password: None,
/// # tls: None,
///         },
///         remote: BrokerConfig {
///             addr: "mqtt.example.com:8883".into(),
///             client_id: "bridge-remote".into(),
/// # keep_alive_secs: 30,
/// # clean_session: false,
/// # max_inflight: 100,
/// # username: None,
/// # password: None,
/// # tls: None,
///         },
/// # state_topic: "bridge/state".into(),
/// # state_online_payload: "1".into(),
/// # state_offline_payload: "0".into(),
/// # forward: vec![],
/// # subscribe: vec![],
///     },
///     cache: CacheConfig::default(),
/// };
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Config {
    /// Bridge connection settings
    pub bridge: BridgeConfig,
    /// Cache configuration
    pub cache: CacheConfig,
}

/// Configuration for MQTT bridge connections and behavior.
///
/// Defines settings for both local and remote broker connections, forwarding rules,
/// and bridge state publishing with Last Will and Testament (LWT).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BridgeConfig {
    /// Local broker configuration
    pub local: BrokerConfig,
    /// Remote broker configuration
    pub remote: BrokerConfig,

    /// Topic for publishing bridge online/offline state (published to remote)
    pub state_topic: String,
    /// Payload to publish when bridge comes online
    pub state_online_payload: String,
    /// Payload to publish via LWT when bridge goes offline
    pub state_offline_payload: String,

    /// Rules for forwarding messages between brokers
    pub forward: Vec<ForwardRule>,
}

/// Configuration for a single MQTT broker connection.
///
/// This struct is used for both local and remote broker configurations,
/// providing symmetric support for TLS and authentication on either side.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrokerConfig {
    /// Broker address (e.g., "127.0.0.1:1883" or "mqtt.example.com:8883")
    pub addr: String,
    /// Client ID for this connection
    pub client_id: String,
    /// Keep-alive interval in seconds
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u16,
    /// Whether to use clean session
    #[serde(default)]
    pub clean_session: bool,
    /// Maximum number of in-flight messages
    #[serde(default = "default_max_inflight")]
    pub max_inflight: u16,

    /// Username for broker authentication
    pub username: Option<String>,
    /// Password for broker authentication
    pub password: Option<String>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,
}

/// TLS configuration for secure broker connections.
///
/// Supports both server authentication (via CA certificate) and optional
/// client authentication (mTLS via PKCS12 certificate).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to CA certificate file for server verification
    pub ca_file: Option<PathBuf>,
    /// Path to PKCS12 client certificate file for mTLS
    pub client_cert: Option<PathBuf>,
    /// Password for PKCS12 client certificate
    pub client_password: Option<String>,
    /// **INSECURE**: Skip TLS certificate verification (testing only)
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
}

/// Rule for forwarding messages between brokers.
///
/// Functions in a similar way to Mosquitto's `topic` directive
/// (see [Mosquitto documentation](https://mosquitto.org/man/mosquitto-conf-5.html)),
/// with two exceptions:
/// - The `direction` can only be "out" or "in", not "both"
/// - The default `qos` is 1, not 0
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct ForwardRule {
    /// MQTT topic pattern; can include wildcards (`+`, `#`)
    pub topic_pattern: String,
    /// Direction of forwarding (out = local -> remote, in = remote -> local)
    pub direction: ForwardDirection,
    /// Local prefix. Removed from topic for "out", prepended for "in"
    #[serde(default)]
    pub local_prefix: String,
    /// Remote prefix. Removed from topic for "in", prepended for "out"
    #[serde(default)]
    pub remote_prefix: String,
    /// QoS level for forwarded messages (0, 1, or 2); default is 1
    #[serde(default = "default_forward_qos")]
    pub qos: u8,
}

/// Configuration for SQLite message cache.
///
/// Controls cache behavior, eviction policies, and replay settings for messages
/// that cannot be immediately delivered to the remote broker.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CacheConfig {
    /// Path to SQLite database file
    pub sqlite_path: PathBuf,

    /// Whether to cache QoS 0 messages (default: false)
    #[serde(default)]
    pub cache_qos0: bool,
    /// Maximum number of messages to store (default: 500,000)
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    /// Policy for handling cache overflow
    #[serde(default = "default_eviction")]
    pub eviction: EvictionPolicy,

    /// Number of messages to replay per batch (default: 1000)
    #[serde(default = "default_flush_batch")]
    pub flush_batch: usize,
    /// Milliseconds between replay batches (default: 100)
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// SQLite busy timeout in milliseconds (default: 5000)
    #[serde(default = "default_busy_timeout_ms")]
    pub busy_timeout_ms: u64,

    /// SQLite synchronous mode (default: Full)
    #[serde(default = "default_synchronous")]
    pub synchronous: SynchronousMode,
}

/// Policy for handling cache overflow when `max_rows` is reached.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    /// Delete oldest messages to make room for new ones
    DropOldest,
    /// Reject new messages when cache is full
    RejectNew,
}

/// SQLite synchronous mode setting.
///
/// Controls durability vs. performance tradeoff. See [SQLite documentation]
/// (https://www.sqlite.org/pragma.html#pragma_synchronous) for details.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum SynchronousMode {
    /// Maximum durability (slowest)
    Full,
    /// Balanced durability and performance
    Normal,
    /// Minimal durability (fastest, risk of corruption on crash)
    Off,
}

/// Direction of forwarding (out = local -> remote, in = remote -> local)
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ForwardDirection {
    /// Forwarding from local to remote
    Out,
    /// Forwarding from remote to local
    In,
}

fn default_forward_qos() -> u8 {
    1
}

fn default_keep_alive_secs() -> u16 {
    30
}

fn default_max_inflight() -> u16 {
    100
}

fn default_max_rows() -> usize {
    500_000
}

fn default_eviction() -> EvictionPolicy {
    EvictionPolicy::DropOldest
}

fn default_flush_batch() -> usize {
    10
}

fn default_flush_interval_ms() -> u64 {
    100
}

fn default_busy_timeout_ms() -> u64 {
    5000
}

fn default_synchronous() -> SynchronousMode {
    SynchronousMode::Full
}

impl Config {
    /// Load configuration from a TOML file (requires `cli` feature)
    #[cfg(feature = "cli")]
    pub fn from_file(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Create a new Config programmatically
    pub fn new(bridge: BridgeConfig, cache: CacheConfig) -> Self {
        Self { bridge, cache }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            sqlite_path: "cache.db".into(),
            cache_qos0: false,
            max_rows: default_max_rows(),
            eviction: default_eviction(),
            flush_batch: default_flush_batch(),
            flush_interval_ms: default_flush_interval_ms(),
            busy_timeout_ms: default_busy_timeout_ms(),
            synchronous: default_synchronous(),
        }
    }
}
