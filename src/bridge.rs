#![allow(clippy::result_large_err)]

use crate::cache::CacheManager;
use crate::config::{BridgeConfig, BrokerConfig};
use crate::error::Result;
use rumqttc::{
    AsyncClient, EventLoop, LastWill, MqttOptions, QoS, Transport,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

pub struct Bridge {
    config: BridgeConfig,
    cache: Arc<CacheManager>,
    cache_config: crate::config::CacheConfig,
    local_client: AsyncClient,
    remote_client: AsyncClient,
    local_eventloop: EventLoop,
    remote_eventloop: EventLoop,
}

impl Bridge {
    pub async fn new(config: BridgeConfig, cache: CacheManager) -> Result<Self> {
        let cache_config = cache.config.clone();
        let cache = Arc::new(cache);

        // Create local MQTT client
        let (local_client, local_eventloop) = create_mqtt_client(&config.local, None)?;

        // Create remote MQTT client with LWT
        let lwt = LastWill::new(
            &config.state_topic,
            config.state_offline_payload.as_bytes(),
            QoS::AtLeastOnce,
            true, // retain
        );
        let (remote_client, remote_eventloop) = create_mqtt_client(&config.remote, Some(lwt))?;

        Ok(Self {
            config,
            cache,
            cache_config,
            local_client,
            local_eventloop,
            remote_client,
            remote_eventloop,
        })
    }

    /// Get a cloned reference to the cache manager.
    ///
    /// Returns an `Arc<CacheManager>` that can be used to inspect cache state,
    /// which is particularly useful in tests.
    ///
    /// This method only clones the `Arc`, not the underlying cache data.
    pub fn cache(&self) -> Arc<CacheManager> {
        Arc::clone(&self.cache)
    }

    pub async fn run(self) -> Result<()> {
        use crate::tasks::*;

        // Extract fields we need
        let Bridge {
            config,
            cache: _,  // CacheManager not used in new architecture
            cache_config,
            local_client,
            remote_client,
            local_eventloop,
            remote_eventloop,
        } = self;

        // Create channels for local broker task
        let (local_cmd_tx, local_cmd_rx) = tokio::sync::mpsc::channel(100);
        let (local_event_tx, local_event_rx) = tokio::sync::mpsc::channel(100);

        // Create channels for remote broker task
        let (remote_cmd_tx, remote_cmd_rx) = tokio::sync::mpsc::channel(100);
        let (remote_event_tx, remote_event_rx) = tokio::sync::mpsc::channel(100);

        // Create channels for cache task
        let (cache_cmd_tx, cache_cmd_rx) = tokio::sync::mpsc::channel(100);

        // Create channels for replay task
        let (replay_cmd_tx, replay_cmd_rx) = tokio::sync::mpsc::channel(100);

        // Spawn local broker task
        tokio::spawn(local_broker::local_broker_task(
            local_client,
            local_eventloop,
            local_cmd_rx,
            local_event_tx,
        ));

        // Spawn remote broker task with DNS fix parameters
        tokio::spawn(remote_broker::remote_broker_task(
            config.remote.clone(),
            config.state_topic.clone(),
            config.state_offline_payload.clone(),
            remote_client,
            remote_eventloop,
            remote_cmd_rx,
            remote_event_tx,
        ));

        // Spawn cache task
        tokio::spawn(cache::cache_task(
            cache_config.clone(),
            cache_cmd_rx,
        ));

        // Spawn replay task
        tokio::spawn(replay::replay_task(
            replay_cmd_rx,
            cache_cmd_tx.clone(),
            remote_cmd_tx.clone(),
            cache_config.flush_batch,
            cache_config.flush_interval_ms,
        ));

        // Run router task (blocks until shutdown)
        router::router_task(
            config,
            local_event_rx,
            remote_event_rx,
            local_cmd_tx,
            remote_cmd_tx,
            cache_cmd_tx,
            replay_cmd_tx,
        )
        .await;

        Ok(())
    }

}

fn create_mqtt_client(
    broker_config: &BrokerConfig,
    lwt: Option<LastWill>,
) -> Result<(AsyncClient, EventLoop)> {
    let (host, port) = parse_broker_addr(&broker_config.addr);

    let mut mqttoptions = MqttOptions::new(&broker_config.client_id, host, port);

    mqttoptions.set_keep_alive(Duration::from_secs(broker_config.keep_alive_secs as u64));
    mqttoptions.set_clean_session(broker_config.clean_session);
    mqttoptions.set_inflight(broker_config.max_inflight);

    // Set credentials if provided
    if let (Some(username), Some(password)) = (&broker_config.username, &broker_config.password) {
        mqttoptions.set_credentials(username, password);
    }

    // Set Last Will and Testament if provided
    if let Some(lwt) = lwt {
        mqttoptions.set_last_will(lwt);
    }

    // Configure TLS with native-tls if provided
    if let Some(tls_config) = &broker_config.tls {
        use native_tls::TlsConnector;

        let mut tls_builder = TlsConnector::builder();

        // Disable certificate verification if requested (for testing only!)
        if tls_config.danger_accept_invalid_certs {
            tls_builder.danger_accept_invalid_certs(true);
            warn!("TLS certificate verification disabled - this is insecure!");
        }

        // Add custom CA certificate if provided
        if let Some(ca_file) = &tls_config.ca_file {
            if ca_file.exists() {
                let ca_cert_data = std::fs::read(ca_file)?;
                let cert = native_tls::Certificate::from_pem(&ca_cert_data)?;
                tls_builder.add_root_certificate(cert);
                tracing::info!("Loaded custom CA certificate from {:?}", ca_file);

                // Note: On macOS, native-tls uses Security.framework which may not honor
                // programmatically-added CA certificates. If you encounter "certificate not trusted"
                // errors with a valid CA, consider either:
                // 1. Adding the CA to the macOS system keychain
                // 2. Using danger_accept_invalid_certs = true (insecure, for testing only)
                // 3. Switching to rustls (would require changing dependency features)
                #[cfg(target_os = "macos")]
                debug!("macOS detected: native-tls may not honor custom CA certificates");
            } else {
                tracing::warn!("CA file not found: {:?}", ca_file);
            }
        }

        // Add client certificate for mTLS if provided
        if let (Some(client_cert_path), Some(password)) =
            (&tls_config.client_cert, &tls_config.client_password)
        {
            if client_cert_path.exists() {
                let client_cert_data = std::fs::read(client_cert_path)?;
                let identity = native_tls::Identity::from_pkcs12(&client_cert_data, password)?;
                tls_builder.identity(identity);
                tracing::info!(
                    "Loaded client certificate for mTLS from {:?}",
                    client_cert_path
                );
            } else {
                tracing::warn!("Client certificate file not found: {:?}", client_cert_path);
            }
        }

        let tls_connector = tls_builder.build()?;
        mqttoptions.set_transport(Transport::tls_with_config(tls_connector.into()));
        info!("TLS enabled for broker connection with native-tls");
    }

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    Ok((client, eventloop))
}

fn parse_broker_addr(addr: &str) -> (String, u16) {
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() == 2 {
        let host = parts[0].to_string();
        let port = parts[1].parse().unwrap_or(1883);
        (host, port)
    } else {
        (addr.to_string(), 1883)
    }
}
