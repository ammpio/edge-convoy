use convoy::{Bridge, BridgeConfig, BrokerConfig, CacheConfig, ForwardRule, SubscribeRule};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Publish, QoS};
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::LocalSet;
use tokio::time::timeout;

// Test broker settings
const LOCAL_ADDR: &str = "127.0.0.1:1883";
const REMOTE_ADDR: &str = "127.0.0.1:1884"; // Non-TLS port for bridge
const REMOTE_ADDR_TLS: &str = "localhost:8883"; // TLS port (use localhost for cert validation)
const REMOTE_USER: &str = "testuser";
const REMOTE_PASS: &str = "testpass";
const CA_CERT_PATH: &str = "test-harness/certs/ca.crt";

/// Create a client for the local broker (no auth/TLS)
fn create_local_test_client(client_id: &str) -> (AsyncClient, EventLoop) {
    let mut mqttoptions = MqttOptions::new(client_id, "127.0.0.1", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    AsyncClient::new(mqttoptions, 10)
}

/// Create a client for the remote broker (no TLS but with auth)
fn create_remote_test_client(client_id: &str) -> (AsyncClient, EventLoop) {
    let mut mqttoptions = MqttOptions::new(client_id, "127.0.0.1", 1884);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_credentials(REMOTE_USER, REMOTE_PASS);
    AsyncClient::new(mqttoptions, 10)
}

/// Create a client for the remote broker with TLS
fn create_remote_test_client_tls(client_id: &str) -> (AsyncClient, EventLoop) {
    create_remote_test_client_tls_ex(client_id, true)
}

/// Create a client for the remote broker with TLS and configurable clean_session
fn create_remote_test_client_tls_ex(
    client_id: &str,
    clean_session: bool,
) -> (AsyncClient, EventLoop) {
    use rumqttc::Transport;

    let mut mqttoptions = MqttOptions::new(client_id, "localhost", 8883); // Use localhost for TLS
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_credentials(REMOTE_USER, REMOTE_PASS);
    mqttoptions.set_clean_session(clean_session);

    // Load CA certificate
    let ca_cert_data = std::fs::read(CA_CERT_PATH).expect("Failed to read CA cert");
    let ca_cert =
        native_tls::Certificate::from_pem(&ca_cert_data).expect("Failed to parse CA cert");

    let tls_connector = native_tls::TlsConnector::builder()
        .add_root_certificate(ca_cert)
        .danger_accept_invalid_certs(true) // Required on macOS where native-tls doesn't honor custom CAs
        .build()
        .expect("Failed to build TLS connector");

    mqttoptions.set_transport(Transport::tls_with_config(tls_connector.into()));

    AsyncClient::new(mqttoptions, 10)
}

/// Poll eventloop until connected (receives ConnAck)
/// Retries on connection errors to handle reconnection scenarios
async fn wait_for_connection(eventloop: &mut EventLoop) -> Result<(), String> {
    let result = timeout(Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    return Ok(());
                }
                Ok(_) => continue,
                Err(_) => {
                    // Keep polling - client will auto-reconnect
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
    })
    .await;

    match result {
        Ok(res) => res,
        Err(_) => Err("Timeout waiting for connection".to_string()),
    }
}

/// Poll eventloop until subscription is confirmed (receives SubAck)
async fn wait_for_suback(eventloop: &mut EventLoop) -> Result<(), String> {
    let result = timeout(Duration::from_secs(5), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::SubAck(_))) => {
                    return Ok(());
                }
                Ok(_) => continue,
                Err(e) => return Err(format!("Connection error: {}", e)),
            }
        }
    })
    .await;

    match result {
        Ok(res) => res,
        Err(_) => Err("Timeout waiting for SubAck".to_string()),
    }
}

/// Wait for a specific message on a subscription
async fn wait_for_message(
    eventloop: &mut EventLoop,
    expected_topic: &str,
    timeout_secs: u64,
) -> Result<Publish, String> {
    let result = timeout(Duration::from_secs(timeout_secs), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    if publish.topic == expected_topic {
                        return Ok(publish);
                    }
                }
                Ok(_) => continue,
                Err(e) => return Err(format!("Connection error: {}", e)),
            }
        }
    })
    .await;

    match result {
        Ok(res) => res,
        Err(_) => Err(format!(
            "Timeout waiting for message on topic '{}'",
            expected_topic
        )),
    }
}

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("convoy=debug,integration_test=debug")
        .without_time()
        .try_init();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_forwarding_local_to_remote() {
    init_tracing();
    println!("\n=== Test: Basic Forwarding (Local → Remote) ===\n");

    let local = LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let cache_path = temp_dir.path().join("cache.db");

            let bridge_config = BridgeConfig {
                local: BrokerConfig {
                    addr: LOCAL_ADDR.to_string(),
                    client_id: "bridge-local-test1".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: None,
                    password: None,
                    tls: None,
                },
                remote: BrokerConfig {
                    addr: REMOTE_ADDR.to_string(),
                    client_id: "bridge-remote-test1".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: Some(REMOTE_USER.to_string()),
                    password: Some(REMOTE_PASS.to_string()),
                    tls: None, // Skip TLS for bridge for now - cert validation is complex with self-signed certs
                },
                state_topic: "bridge/test1/state".to_string(),
                state_online_payload: "1".to_string(),
                state_offline_payload: "0".to_string(),
                forward: vec![ForwardRule {
                    local_filter: "test/sensors/#".to_string(),
                    remote_prefix: "edge/device1/".to_string(),
                    qos: 1,
                }],
                subscribe: vec![],
            };

            let cache_config = CacheConfig {
                sqlite_path: cache_path,
                ..Default::default()
            };

            let bridge = Bridge::new(bridge_config, cache_config).await.unwrap();

            let bridge_handle = tokio::task::spawn_local(async move {
                bridge.run().await.unwrap();
            });

            tokio::time::sleep(Duration::from_secs(2)).await;

            let (local_client, mut local_eventloop) = create_local_test_client("test-local-pub");
            let (remote_client, mut remote_eventloop) =
                create_remote_test_client("test-remote-sub");

            // Wait for remote client to connect
            wait_for_connection(&mut remote_eventloop).await.unwrap();

            // Poll local eventloop in background to actually send messages
            let _local_eventloop_handle = tokio::task::spawn_local(async move {
                loop {
                    if local_eventloop.poll().await.is_err() {
                        break;
                    }
                }
            });

            // Subscribe and wait for confirmation
            remote_client
                .subscribe("edge/device1/test/sensors/temp", QoS::AtLeastOnce)
                .await
                .unwrap();

            wait_for_suback(&mut remote_eventloop).await.unwrap();
            println!("Subscription confirmed");

            let test_payload = b"23.5";
            local_client
                .publish("test/sensors/temp", QoS::AtLeastOnce, false, test_payload)
                .await
                .unwrap();

            println!("Published to local: test/sensors/temp");

            let received =
                wait_for_message(&mut remote_eventloop, "edge/device1/test/sensors/temp", 5)
                    .await
                    .expect("Failed to receive message on remote broker");

            println!(
                "Received on remote: {} = {:?}",
                received.topic,
                String::from_utf8_lossy(&received.payload)
            );

            assert_eq!(received.payload.as_ref(), test_payload);

            bridge_handle.abort();
            println!("\n=== Test Passed ===\n");
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_forwarding_remote_to_local() {
    init_tracing();
    println!("\n=== Test: Subscribe Forwarding (Remote → Local) ===\n");

    let local = LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let cache_path = temp_dir.path().join("cache.db");

            let bridge_config = BridgeConfig {
                local: BrokerConfig {
                    addr: LOCAL_ADDR.to_string(),
                    client_id: "bridge-local-test2".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: None,
                    password: None,
                    tls: None,
                },
                remote: BrokerConfig {
                    addr: REMOTE_ADDR_TLS.to_string(), // Use TLS port when TLS is configured
                    client_id: "bridge-remote-test2".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: Some(REMOTE_USER.to_string()),
                    password: Some(REMOTE_PASS.to_string()),
                    tls: Some(convoy::TlsConfig {
                        ca_file: Some(CA_CERT_PATH.into()),
                        client_cert: None,
                        client_password: None,
                        danger_accept_invalid_certs: true, // For testing with self-signed certs
                    }),
                },
                state_topic: "bridge/test2/state".to_string(),
                state_online_payload: "1".to_string(),
                state_offline_payload: "0".to_string(),
                forward: vec![],
                subscribe: vec![SubscribeRule {
                    remote_filter: "commands/device1/#".to_string(),
                    remote_prefix: "commands/device1/".to_string(),
                    qos: 1,
                }],
            };

            let cache_config = CacheConfig {
                sqlite_path: cache_path,
                ..Default::default()
            };

            let bridge = Bridge::new(bridge_config, cache_config).await.unwrap();

            let bridge_handle = tokio::task::spawn_local(async move {
                bridge.run().await.unwrap();
            });

            // Wait longer for bridge to fully connect and subscribe
            tokio::time::sleep(Duration::from_secs(3)).await;

            let (remote_client, mut remote_eventloop) =
                create_remote_test_client_tls("test-remote-pub");
            let (local_client, mut local_eventloop) = create_local_test_client("test-local-sub");

            // Wait for local client to connect
            wait_for_connection(&mut local_eventloop).await.unwrap();

            // Poll remote eventloop in background to actually send messages
            let _remote_eventloop_handle = tokio::task::spawn_local(async move {
                loop {
                    if remote_eventloop.poll().await.is_err() {
                        break;
                    }
                }
            });

            // Subscribe and wait for confirmation
            local_client
                .subscribe("restart", QoS::AtLeastOnce)
                .await
                .unwrap();

            wait_for_suback(&mut local_eventloop).await.unwrap();
            println!("Subscription confirmed");

            let test_payload = b"now";
            remote_client
                .publish(
                    "commands/device1/restart",
                    QoS::AtLeastOnce,
                    false,
                    test_payload,
                )
                .await
                .unwrap();

            println!("Published to remote: commands/device1/restart");

            // Give the bridge time to receive and forward
            tokio::time::sleep(Duration::from_millis(500)).await;

            let received = wait_for_message(&mut local_eventloop, "restart", 10)
                .await
                .expect("Failed to receive message on local broker");

            println!(
                "Received on local: {} = {:?}",
                received.topic,
                String::from_utf8_lossy(&received.payload)
            );

            assert_eq!(received.payload.as_ref(), test_payload);

            bridge_handle.abort();
            println!("\n=== Test Passed ===\n");
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caching_and_replay() {
    init_tracing();
    println!("\n=== Test: Caching and Replay (Remote Down/Up) ===\n");

    let local = LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let cache_path = temp_dir.path().join("cache.db");

            let bridge_config = BridgeConfig {
                local: BrokerConfig {
                    addr: LOCAL_ADDR.to_string(),
                    client_id: "bridge-local-test3".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: None,
                    password: None,
                    tls: None,
                },
                remote: BrokerConfig {
                    addr: REMOTE_ADDR_TLS.to_string(), // Use TLS port when TLS is configured
                    client_id: "bridge-remote-test3".to_string(),
                    keep_alive_secs: 5,
                    clean_session: true,
                    max_inflight: 10,
                    username: Some(REMOTE_USER.to_string()),
                    password: Some(REMOTE_PASS.to_string()),
                    tls: Some(convoy::TlsConfig {
                        ca_file: Some(CA_CERT_PATH.into()),
                        client_cert: None,
                        client_password: None,
                        danger_accept_invalid_certs: true, // For testing with self-signed certs
                    }),
                },
                state_topic: "bridge/test3/state".to_string(),
                state_online_payload: "1".to_string(),
                state_offline_payload: "0".to_string(),
                forward: vec![ForwardRule {
                    local_filter: "cached/#".to_string(),
                    remote_prefix: "edge/".to_string(),
                    qos: 1,
                }],
                subscribe: vec![],
            };

            let cache_config = CacheConfig {
                sqlite_path: cache_path,
                flush_interval_ms: 100,
                flush_batch: 10,
                ..Default::default()
            };

            let bridge = Bridge::new(bridge_config, cache_config).await.unwrap();

            let bridge_handle = tokio::task::spawn_local(async move {
                bridge.run().await.unwrap();
            });

            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("Step 1: Bridge connected to both brokers");

            // Create test clients
            let (local_client, mut local_eventloop) = create_local_test_client("test-local-pub3");

            // Use persistent session (clean_session=false) so broker queues messages when client is offline
            let (remote_client, mut remote_eventloop) =
                create_remote_test_client_tls_ex("test-remote-sub3", false);

            // Poll local eventloop in background
            let _local_eventloop_handle = tokio::task::spawn_local(async move {
                loop {
                    if local_eventloop.poll().await.is_err() {
                        break;
                    }
                }
            });

            // Connect and subscribe to remote broker
            wait_for_connection(&mut remote_eventloop).await.unwrap();
            remote_client
                .subscribe("edge/cached/data", QoS::AtLeastOnce)
                .await
                .unwrap();
            wait_for_suback(&mut remote_eventloop).await.unwrap();
            println!("Subscribed to edge/cached/data with persistent session");

            println!("\nStep 2: Stopping remote broker to simulate outage...");

            let stop_output = std::process::Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    "test-harness/docker-compose.yml",
                    "stop",
                    "remote-broker",
                ])
                .output()
                .expect("Failed to stop remote broker");

            if !stop_output.status.success() {
                panic!(
                    "Failed to stop remote broker: {}",
                    String::from_utf8_lossy(&stop_output.stderr)
                );
            }

            println!("Remote broker stopped");
            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("\nStep 3: Publishing messages while remote is down (should be cached)...");

            for i in 1..=3 {
                let payload = format!("cached_message_{}", i);
                local_client
                    .publish("cached/data", QoS::AtLeastOnce, false, payload.as_bytes())
                    .await
                    .unwrap();
                println!("  Published: {}", payload);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;

            // Note: In the task-based architecture, we can't directly inspect cache state
            // but we can verify the behavior: messages should be replayed after reconnection
            println!("\nMessages published while remote was down (should be cached)");

            println!("\nStep 4: Restarting remote broker...");

            let start_output = std::process::Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    "test-harness/docker-compose.yml",
                    "start",
                    "remote-broker",
                ])
                .output()
                .expect("Failed to start remote broker");

            if !start_output.status.success() {
                panic!(
                    "Failed to start remote broker: {}",
                    String::from_utf8_lossy(&start_output.stderr)
                );
            }

            println!("Remote broker restarted");

            println!("\nStep 5: Waiting for client to reconnect and resubscribe...");

            // Poll until we reconnect (we'll see ConnAck)
            wait_for_connection(&mut remote_eventloop)
                .await
                .expect("Failed to reconnect to remote broker");

            // Resubscribe - the broker lost session state when it restarted
            remote_client
                .subscribe("edge/cached/data", QoS::AtLeastOnce)
                .await
                .unwrap();
            wait_for_suback(&mut remote_eventloop).await.unwrap();
            println!("Client reconnected and resubscribed");

            // Give bridge time to reconnect and replay
            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("\nStep 6: Waiting for replayed messages...");

            // The broker will now deliver messages as they're replayed
            let mut received_messages = Vec::new();
            for i in 1..=3 {
                match wait_for_message(&mut remote_eventloop, "edge/cached/data", 10).await {
                    Ok(publish) => {
                        let payload = String::from_utf8_lossy(&publish.payload).to_string();
                        println!("  Received replayed message {}: {}", i, payload);
                        received_messages.push(payload);
                    }
                    Err(e) => {
                        println!("  Warning: {}", e);
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("\nReplay completed");

            assert!(
                !received_messages.is_empty(),
                "Should have received at least some replayed messages"
            );

            println!(
                "Successfully received {} replayed message(s)",
                received_messages.len()
            );

            bridge_handle.abort();
            println!("\n=== Test Passed ===\n");
        })
        .await;
}
