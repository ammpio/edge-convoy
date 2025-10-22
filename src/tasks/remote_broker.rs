#![allow(clippy::result_large_err)]

use crate::tasks::messages::{RemoteCommand, RemoteEvent};
use crate::config::BrokerConfig;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, QoS, Transport};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

const MAX_CONSECUTIVE_ERRORS: usize = 5;

/// Remote broker task that owns the eventloop and continuously polls it.
///
/// Handles both incoming events from the broker and outgoing commands
/// from the router and replay worker.
///
/// Automatically recreates the eventloop after persistent connection failures
/// to work around DNS caching issues.
pub async fn remote_broker_task(
    broker_config: BrokerConfig,
    state_topic: String,
    state_offline_payload: String,
    mut client: AsyncClient,
    mut eventloop: EventLoop,
    mut command_rx: mpsc::Receiver<RemoteCommand>,
    event_tx: mpsc::Sender<RemoteEvent>,
) {
    info!("Remote broker task started");

    let mut consecutive_errors = 0;

    loop {
        tokio::select! {
            biased;

            // Poll the eventloop continuously (always prioritized)
            poll_result = eventloop.poll() => {
                match poll_result {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        info!("Remote broker connected");
                        consecutive_errors = 0; // Reset error counter on successful connection
                        if event_tx.send(RemoteEvent::Connected).await.is_err() {
                            error!("Router has shut down, stopping remote broker task");
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        debug!("Remote broker received publish: {}", publish.topic);
                        let event = RemoteEvent::MessageReceived {
                            topic: publish.topic,
                            payload: publish.payload.to_vec(),
                            qos: publish.qos as u8,
                            retain: publish.retain,
                        };
                        if event_tx.send(event).await.is_err() {
                            error!("Router has shut down, stopping remote broker task");
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::Disconnect)) => {
                        warn!("Remote broker disconnected");
                        if event_tx.send(RemoteEvent::Disconnected).await.is_err() {
                            error!("Router has shut down, stopping remote broker task");
                            break;
                        }
                    }
                    Ok(_) => {
                        // Other events we don't care about
                        debug!("Remote broker event: {:?}", poll_result);
                    }
                    Err(e) => {
                        error!("Remote broker connection error: {}", e);
                        consecutive_errors += 1;

                        // Check if we should recreate the eventloop
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            warn!(
                                "Consecutive connection errors reached {}, recreating eventloop to clear DNS cache",
                                consecutive_errors
                            );

                            // Recreate client and eventloop
                            match recreate_eventloop(&broker_config, &state_topic, &state_offline_payload) {
                                Ok((new_client, new_eventloop)) => {
                                    client = new_client;
                                    eventloop = new_eventloop;
                                    consecutive_errors = 0;
                                    info!("Remote broker eventloop recreated successfully");
                                }
                                Err(recreate_err) => {
                                    error!("Failed to recreate eventloop: {}", recreate_err);
                                }
                            }
                        }

                        let _ = event_tx.send(RemoteEvent::Error(e.to_string())).await;
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }

            // Handle commands from router and replay worker
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    RemoteCommand::Subscribe { topic, qos } => {
                        debug!("Remote broker subscribing to: {} (QoS {:?})", topic, qos);
                        if let Err(e) = client.subscribe(&topic, qos).await {
                            error!("Failed to subscribe to {}: {}", topic, e);
                        }
                    }
                    RemoteCommand::Publish { topic, payload, qos, retain, response } => {
                        debug!("Remote broker publishing to: {} ({} bytes)", topic, payload.len());
                        let result = client.publish(&topic, qos, retain, payload).await
                            .map_err(Into::into);

                        // Send acknowledgment if requested (for replay worker)
                        if let Some(response_tx) = response {
                            let _ = response_tx.send(result);
                        } else if let Err(e) = result {
                            error!("Failed to publish to {}: {}", topic, e);
                        }
                    }
                    RemoteCommand::RecreateEventloop { response } => {
                        info!("Manual eventloop recreation requested");
                        match recreate_eventloop(&broker_config, &state_topic, &state_offline_payload) {
                            Ok((new_client, new_eventloop)) => {
                                client = new_client;
                                eventloop = new_eventloop;
                                consecutive_errors = 0;
                                info!("Remote broker eventloop recreated successfully");
                                let _ = response.send(Ok(()));
                            }
                            Err(e) => {
                                error!("Failed to recreate eventloop: {}", e);
                                let _ = response.send(Err(e));
                            }
                        }
                    }
                }
            }
        }
    }

    info!("Remote broker task shutting down");
}

/// Recreate the MQTT client and eventloop with fresh DNS resolution
fn recreate_eventloop(
    broker_config: &BrokerConfig,
    state_topic: &str,
    state_offline_payload: &str,
) -> crate::error::Result<(AsyncClient, EventLoop)> {
    let (host, port) = parse_broker_addr(&broker_config.addr);
    let mut mqttoptions = MqttOptions::new(&broker_config.client_id, host, port);

    mqttoptions.set_keep_alive(std::time::Duration::from_secs(broker_config.keep_alive_secs as u64));
    mqttoptions.set_clean_session(broker_config.clean_session);
    mqttoptions.set_inflight(broker_config.max_inflight);

    // Set credentials if provided
    if let (Some(username), Some(password)) = (&broker_config.username, &broker_config.password) {
        mqttoptions.set_credentials(username, password);
    }

    // Set Last Will and Testament
    let lwt = LastWill::new(
        state_topic,
        state_offline_payload.as_bytes(),
        QoS::AtLeastOnce,
        true, // retain
    );
    mqttoptions.set_last_will(lwt);

    // Configure TLS if provided
    if let Some(tls_config) = &broker_config.tls {
        use native_tls::TlsConnector;

        let mut tls_builder = TlsConnector::builder();

        if tls_config.danger_accept_invalid_certs {
            tls_builder.danger_accept_invalid_certs(true);
        }

        if let Some(ca_file) = &tls_config.ca_file && ca_file.exists() {
            let ca_cert_data = std::fs::read(ca_file)?;
            let cert = native_tls::Certificate::from_pem(&ca_cert_data)?;
            tls_builder.add_root_certificate(cert);
        }

        if let (Some(client_cert_path), Some(password)) =
            (&tls_config.client_cert, &tls_config.client_password)
            && client_cert_path.exists()
        {
            let client_cert_data = std::fs::read(client_cert_path)?;
            let identity = native_tls::Identity::from_pkcs12(&client_cert_data, password)?;
            tls_builder.identity(identity);
        }

        let tls_connector = tls_builder.build()?;
        mqttoptions.set_transport(Transport::tls_with_config(tls_connector.into()));
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
