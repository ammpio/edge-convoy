#![allow(clippy::result_large_err)]

use crate::cache::CacheManager;
use crate::config::{BridgeConfig, BrokerConfig};
use crate::error::Result;
use crate::topic::{apply_forward_mapping, apply_subscribe_mapping, topic_matches_filter};
use crate::util::payload_hash;
use backoff::{ExponentialBackoff, backoff::Backoff};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, Publish, QoS, Transport,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

pub struct Bridge {
    config: BridgeConfig,
    cache: Arc<CacheManager>,
    local_client: AsyncClient,
    local_eventloop: EventLoop,
    remote_client: AsyncClient,
    remote_eventloop: EventLoop,
    remote_connected: Arc<tokio::sync::RwLock<bool>>,
    replay_trigger: Arc<Notify>,
    remote_backoff: ExponentialBackoff,
    remote_backoff_until: Option<Instant>,
}

impl Bridge {
    pub async fn new(config: BridgeConfig, cache: CacheManager) -> Result<Self> {
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

        let remote_connected = Arc::new(tokio::sync::RwLock::new(false));
        let replay_trigger = Arc::new(Notify::new());

        // Configure exponential backoff for remote connection retries
        let remote_backoff = create_connection_backoff();

        Ok(Self {
            config,
            cache,
            local_client,
            local_eventloop,
            remote_client,
            remote_eventloop,
            remote_connected,
            replay_trigger,
            remote_backoff,
            remote_backoff_until: None,
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

    pub async fn run(mut self) -> Result<()> {
        // Subscribe to topics on both brokers
        self.subscribe_topics().await?;

        // Spawn replay worker
        let replay_handle = tokio::spawn({
            let cache = Arc::clone(&self.cache);
            let remote_client = self.remote_client.clone();
            let remote_connected = Arc::clone(&self.remote_connected);
            let replay_trigger = Arc::clone(&self.replay_trigger);
            let flush_batch = self.cache.config.flush_batch;
            let flush_interval_ms = self.cache.config.flush_interval_ms;

            async move {
                crate::replay::replay_worker(
                    cache,
                    remote_client,
                    remote_connected,
                    replay_trigger,
                    flush_batch,
                    flush_interval_ms,
                )
                .await
            }
        });

        // Run event loops
        loop {
            tokio::select! {
                local_event = self.local_eventloop.poll() => {
                    if let Err(e) = self.handle_local_event(local_event).await {
                        error!("Local event error: {}", e);
                    }
                }
                remote_event = self.remote_eventloop.poll(), if self.should_poll_remote() => {
                    if let Err(e) = self.handle_remote_event(remote_event).await {
                        error!("Remote event error: {}", e);
                    }
                }
                _ = tokio::time::sleep_until(self.remote_backoff_until.unwrap_or(Instant::now() + Duration::from_secs(86400))), if self.remote_backoff_until.is_some() => {
                    // Backoff period complete, allow reconnection
                    debug!("Backoff period complete, allowing remote reconnection");
                    self.remote_backoff_until = None;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received shutdown signal");
                    break;
                }
            }
        }

        replay_handle.abort();
        Ok(())
    }

    fn should_poll_remote(&self) -> bool {
        // Only poll remote if we're not in backoff period
        match self.remote_backoff_until {
            None => true,
            Some(until) => Instant::now() >= until,
        }
    }

    async fn subscribe_topics(&self) -> Result<()> {
        // Subscribe to local topics (for forwarding to remote)
        for rule in &self.config.forward {
            let qos = qos_from_u8(rule.qos);
            self.local_client.subscribe(&rule.local_filter, qos).await?;
            info!(
                "Subscribed to local topic: {} (QoS {})",
                rule.local_filter, rule.qos
            );
        }

        // Subscribe to remote topics (for forwarding to local)
        for rule in &self.config.subscribe {
            let qos = qos_from_u8(rule.qos);
            self.remote_client
                .subscribe(&rule.remote_filter, qos)
                .await?;
            info!(
                "Subscribed to remote topic: {} (QoS {})",
                rule.remote_filter, rule.qos
            );
        }

        Ok(())
    }

    async fn handle_local_event(
        &mut self,
        event: std::result::Result<Event, rumqttc::ConnectionError>,
    ) -> Result<()> {
        match event {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                self.handle_local_publish(publish).await?;
            }
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                info!("Connected to local broker");
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                warn!("Disconnected from local broker");
            }
            Ok(_) => {}
            Err(e) => {
                error!("Local connection error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }

    async fn handle_remote_event(
        &mut self,
        event: std::result::Result<Event, rumqttc::ConnectionError>,
    ) -> Result<()> {
        match event {
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                info!("Connected to remote broker");
                *self.remote_connected.write().await = true;

                // Reset backoff on successful connection
                self.remote_backoff.reset();
                self.remote_backoff_until = None;

                // Publish online state
                let qos = QoS::AtLeastOnce;
                self.remote_client
                    .publish(
                        &self.config.state_topic,
                        qos,
                        true, // retain
                        self.config.state_online_payload.as_bytes(),
                    )
                    .await?;
                info!("Published online state to {}", self.config.state_topic);

                // Trigger replay worker
                self.replay_trigger.notify_one();
            }
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                self.handle_remote_publish(publish).await?;
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                warn!("Disconnected from remote broker");
                *self.remote_connected.write().await = false;
            }
            Ok(_) => {}
            Err(e) => {
                error!("Remote connection error: {}", e);
                *self.remote_connected.write().await = false;

                // Schedule retry with exponential backoff
                if let Some(delay) = self.remote_backoff.next_backoff() {
                    self.remote_backoff_until = Some(Instant::now() + delay);
                    warn!("Waiting {:?} before reconnecting to remote broker", delay);
                }
            }
        }
        Ok(())
    }

    async fn handle_local_publish(&self, publish: Publish) -> Result<()> {
        let topic = publish.topic.clone();
        debug!(
            "Received from local: {} (hash={})",
            topic,
            payload_hash(&publish.payload)
        );

        // Find matching forward rule
        for rule in &self.config.forward {
            if topic_matches_filter(&topic, &rule.local_filter) {
                let remote_topic = apply_forward_mapping(&topic, rule);
                let qos_num = rule.qos;
                let qos = qos_from_u8(qos_num);

                // Try to publish to remote
                let is_connected = *self.remote_connected.read().await;

                if is_connected {
                    match self
                        .remote_client
                        .publish(&remote_topic, qos, publish.retain, publish.payload.clone())
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                "Forwarded to remote: {} -> {} (hash={})",
                                topic,
                                remote_topic,
                                payload_hash(&publish.payload)
                            );
                        }
                        Err(e) => {
                            warn!("Failed to publish to remote: {}, caching", e);
                            self.cache_message(&remote_topic, &publish, qos_num)?;
                        }
                    }
                } else {
                    // Remote not connected, cache the message
                    debug!("Remote disconnected, caching message");
                    self.cache_message(&remote_topic, &publish, qos_num)?;
                }

                break;
            }
        }

        Ok(())
    }

    async fn handle_remote_publish(&self, publish: Publish) -> Result<()> {
        let topic = publish.topic.clone();
        debug!(
            "Received from remote: {} (hash={})",
            topic,
            payload_hash(&publish.payload)
        );

        // Find matching subscribe rule
        for rule in &self.config.subscribe {
            if topic_matches_filter(&topic, &rule.remote_filter) {
                match apply_subscribe_mapping(&topic, rule) {
                    Ok(local_topic) => {
                        let qos = qos_from_u8(rule.qos);

                        // Publish to local (best effort, no caching)
                        if let Err(e) = self
                            .local_client
                            .publish(&local_topic, qos, publish.retain, publish.payload.clone())
                            .await
                        {
                            warn!("Failed to publish to local: {}", e);
                        } else {
                            debug!(
                                "Forwarded to local: {} -> {} (hash={})",
                                topic,
                                local_topic,
                                payload_hash(&publish.payload)
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Topic mapping error: {}", e);
                    }
                }

                break;
            }
        }

        Ok(())
    }

    fn cache_message(&self, topic: &str, publish: &Publish, qos: u8) -> Result<()> {
        self.cache
            .enqueue(topic.as_bytes(), &publish.payload, qos, publish.retain)?;
        Ok(())
    }
}

fn create_connection_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(30),
        max_elapsed_time: None, // Retry forever
        multiplier: 2.0,
        randomization_factor: 0.2, // 20% jitter (down from default 50%)
        ..Default::default()
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

fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}
