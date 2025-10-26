use convoy::{
    Bridge, BridgeConfig, BrokerConfig, CacheConfig, ForwardDirection, ForwardRule, TlsConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Note: For logging, initialize tracing_subscriber or use your own logging setup
    // Example with tracing_subscriber (add it as a dev-dependency):
    // tracing_subscriber::fmt::init();

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
            username: Some("edge_device_01".to_string()),
            password: Some("secure_password".to_string()),
            tls: Some(TlsConfig {
                ca_file: Some("/etc/ssl/certs/ca-certificates.crt".into()),
                client_cert: None,
                client_password: None,
                danger_accept_invalid_certs: false,
            }),
        },
        state_topic: "bridge/convoy/state".to_string(),
        state_online_payload: "1".to_string(),
        state_offline_payload: "0".to_string(),
        forward: vec![ForwardRule {
            topic_pattern: "sensors/#".to_string(),
            direction: ForwardDirection::Out,
            local_prefix: "".to_string(),
            remote_prefix: "devices/edge1/".to_string(),
            qos: 1,
        }],
    };

    // Configure cache with defaults
    let cache_config = CacheConfig {
        sqlite_path: "/tmp/convoy-cache.db".into(),
        ..Default::default()
    };

    println!("Creating bridge...");

    // Create and run bridge
    let bridge = Bridge::new(bridge_config, cache_config)?;

    println!("Bridge created, starting event loop...");
    println!("Press Ctrl+C to stop");

    bridge.run()?;

    Ok(())
}
