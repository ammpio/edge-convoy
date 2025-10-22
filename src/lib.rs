//! Convoy - MQTT Bridge with SQLite Cache
//!
//! A lightweight, reliable MQTT bridge with SQLite-backed message caching for offline resilience.
//!
//! # Features
//!
//! - **Bidirectional MQTT bridging** between local and remote brokers
//! - **SQLite-backed message cache** for local→remote messages when remote is unavailable
//! - **Automatic cache replay** when connection is restored (FIFO order)
//! - **Topic mapping** with wildcard support (`+`, `#`)
//! - **TLS support** for secure remote connections
//! - **Bridge state publishing** with Last Will and Testament (LWT)
//!
//! # Example
//!
//! ```no_run
//! use convoy::{BridgeConfig, BrokerConfig, CacheConfig, ForwardRule, TlsConfig, Bridge};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure bridge
//!     let bridge_config = BridgeConfig {
//!         local: BrokerConfig {
//!             addr: "127.0.0.1:1883".to_string(),
//!             client_id: "bridge-local".to_string(),
//!             keep_alive_secs: 30,
//!             clean_session: false,
//!             max_inflight: 100,
//!             username: None,
//!             password: None,
//!             tls: None,
//!         },
//!         remote: BrokerConfig {
//!             addr: "mqtt.example.com:8883".to_string(),
//!             client_id: "bridge-remote".to_string(),
//!             keep_alive_secs: 30,
//!             clean_session: false,
//!             max_inflight: 100,
//!             username: Some("user".to_string()),
//!             password: Some("pass".to_string()),
//!             tls: None,
//!         },
//!         state_topic: "bridge/state".to_string(),
//!         state_online_payload: "1".to_string(),
//!         state_offline_payload: "0".to_string(),
//!         forward: vec![
//!             ForwardRule {
//!                 local_filter: "sensors/#".to_string(),
//!                 remote_prefix: "device1/".to_string(),
//!                 qos: 1,
//!             }
//!         ],
//!         subscribe: vec![],
//!     };
//!
//!     // Configure cache
//!     let cache_config = CacheConfig {
//!         sqlite_path: "/tmp/bridge-cache.db".into(),
//!         cache_qos0: false,
//!         max_rows: 100000,
//!         eviction: convoy::EvictionPolicy::DropOldest,
//!         flush_batch: 1000,
//!         flush_interval_ms: 100,
//!         busy_timeout_ms: 5000,
//!         synchronous: convoy::SynchronousMode::Full,
//!     };
//!
//!     // Create cache manager
//!     let cache = convoy::CacheManager::new(cache_config)?;
//!
//!     // Create and run bridge
//!     let bridge = Bridge::new(bridge_config, cache).await?;
//!     bridge.run().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod bridge;
pub mod cache;
pub mod config;
pub mod error;
pub mod replay;
pub mod tasks;
pub mod topic;
pub mod util;

// Re-export main types for convenient access
pub use bridge::Bridge;
pub use cache::{CacheManager, CachedMessage};
pub use config::{
    BridgeConfig, BrokerConfig, CacheConfig, Config, EvictionPolicy, ForwardRule, SubscribeRule,
    SynchronousMode, TlsConfig,
};
pub use error::{BridgeError, Result};
pub use topic::{apply_forward_mapping, apply_subscribe_mapping, topic_matches_filter};
