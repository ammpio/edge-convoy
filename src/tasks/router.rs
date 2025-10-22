use crate::config::BridgeConfig;
use crate::tasks::messages::*;
use crate::topic::{apply_forward_mapping, apply_subscribe_mapping, topic_matches_filter};
use crate::util::payload_hash;
use rumqttc::QoS;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Router task that contains all business logic for message routing.
///
/// Responsibilities:
/// - Handle connection state management
/// - Apply topic mapping rules
/// - Make caching decisions
/// - Trigger replay when appropriate
pub async fn router_task(
    config: BridgeConfig,
    mut local_event_rx: mpsc::Receiver<LocalEvent>,
    mut remote_event_rx: mpsc::Receiver<RemoteEvent>,
    local_cmd_tx: mpsc::Sender<LocalCommand>,
    remote_cmd_tx: mpsc::Sender<RemoteCommand>,
    cache_cmd_tx: mpsc::Sender<CacheCommand>,
    replay_cmd_tx: mpsc::Sender<ReplayCommand>,
) {
    info!("Router task started");

    let mut remote_connected = false;

    loop {
        tokio::select! {
            // Handle local broker events
            Some(event) = local_event_rx.recv() => {
                if let Err(e) = handle_local_event(
                    event,
                    &config,
                    &local_cmd_tx,
                    &remote_cmd_tx,
                    &cache_cmd_tx,
                    remote_connected,
                ).await {
                    error!("Error handling local event: {}", e);
                }
            }

            // Handle remote broker events
            Some(event) = remote_event_rx.recv() => {
                if let Err(e) = handle_remote_event(
                    event,
                    &config,
                    &local_cmd_tx,
                    &remote_cmd_tx,
                    &replay_cmd_tx,
                    &mut remote_connected,
                ).await {
                    error!("Error handling remote event: {}", e);
                }
            }

            // Shutdown signal
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }

    info!("Router task shutting down");
}

async fn handle_local_event(
    event: LocalEvent,
    config: &BridgeConfig,
    local_cmd_tx: &mpsc::Sender<LocalCommand>,
    remote_cmd_tx: &mpsc::Sender<RemoteCommand>,
    cache_cmd_tx: &mpsc::Sender<CacheCommand>,
    remote_connected: bool,
) -> crate::error::Result<()> {
    match event {
        LocalEvent::Connected => {
            info!("Local broker connected, subscribing to topics");
            // Subscribe to all forward rules
            for rule in &config.forward {
                let qos = qos_from_u8(rule.qos);
                let _ = local_cmd_tx
                    .send(LocalCommand::Subscribe {
                        topic: rule.local_filter.clone(),
                        qos,
                    })
                    .await;
                info!(
                    "Subscribed to local topic: {} (QoS {})",
                    rule.local_filter, rule.qos
                );
            }
        }
        LocalEvent::Disconnected => {
            warn!("Local broker disconnected");
        }
        LocalEvent::MessageReceived {
            topic,
            payload,
            qos: _,
            retain,
        } => {
            debug!(
                "Received from local: {} (hash={})",
                topic,
                payload_hash(&payload)
            );

            // Find matching forward rule
            for rule in &config.forward {
                if topic_matches_filter(&topic, &rule.local_filter) {
                    let remote_topic = apply_forward_mapping(&topic, rule);
                    let qos_num = rule.qos;
                    let qos_enum = qos_from_u8(qos_num);

                    if remote_connected {
                        // Try to publish to remote
                        if remote_cmd_tx
                            .send(RemoteCommand::Publish {
                                topic: remote_topic.clone(),
                                payload: payload.clone(),
                                qos: qos_enum,
                                retain,
                                response: None, // Router doesn't wait for ack
                            })
                            .await
                            .is_ok()
                        {
                            debug!(
                                "Forwarded to remote: {} -> {} (hash={})",
                                topic,
                                remote_topic,
                                payload_hash(&payload)
                            );
                        } else {
                            warn!("Remote broker task unavailable, caching");
                            cache_message(cache_cmd_tx, &remote_topic, &payload, qos_num, retain)
                                .await?;
                        }
                    } else {
                        // Remote not connected, cache the message
                        debug!("Remote disconnected, caching message");
                        cache_message(cache_cmd_tx, &remote_topic, &payload, qos_num, retain)
                            .await?;
                    }

                    break;
                }
            }
        }
        LocalEvent::Error(e) => {
            error!("Local broker error: {}", e);
        }
    }
    Ok(())
}

async fn handle_remote_event(
    event: RemoteEvent,
    config: &BridgeConfig,
    local_cmd_tx: &mpsc::Sender<LocalCommand>,
    remote_cmd_tx: &mpsc::Sender<RemoteCommand>,
    replay_cmd_tx: &mpsc::Sender<ReplayCommand>,
    remote_connected: &mut bool,
) -> crate::error::Result<()> {
    match event {
        RemoteEvent::Connected => {
            info!("Remote broker connected");
            *remote_connected = true;

            // Publish online state
            let qos = QoS::AtLeastOnce;
            let _ = remote_cmd_tx
                .send(RemoteCommand::Publish {
                    topic: config.state_topic.clone(),
                    payload: config.state_online_payload.as_bytes().to_vec(),
                    qos,
                    retain: true,
                    response: None,
                })
                .await;
            info!("Published online state to {}", config.state_topic);

            // Subscribe to remote topics
            for rule in &config.subscribe {
                let qos = qos_from_u8(rule.qos);
                let _ = remote_cmd_tx
                    .send(RemoteCommand::Subscribe {
                        topic: rule.remote_filter.clone(),
                        qos,
                    })
                    .await;
                info!(
                    "Subscribed to remote topic: {} (QoS {})",
                    rule.remote_filter, rule.qos
                );
            }

            // Trigger replay
            let _ = replay_cmd_tx.send(ReplayCommand::RemoteConnected).await;
        }
        RemoteEvent::Disconnected => {
            warn!("Remote broker disconnected");
            *remote_connected = false;
            let _ = replay_cmd_tx.send(ReplayCommand::RemoteDisconnected).await;
        }
        RemoteEvent::MessageReceived {
            topic,
            payload,
            qos: _,
            retain,
        } => {
            debug!(
                "Received from remote: {} (hash={})",
                topic,
                payload_hash(&payload)
            );

            // Find matching subscribe rule
            for rule in &config.subscribe {
                if topic_matches_filter(&topic, &rule.remote_filter) {
                    match apply_subscribe_mapping(&topic, rule) {
                        Ok(local_topic) => {
                            let qos_enum = qos_from_u8(rule.qos);

                            // Publish to local (best effort, no caching)
                            if local_cmd_tx
                                .send(LocalCommand::Publish {
                                    topic: local_topic.clone(),
                                    payload: payload.clone(),
                                    qos: qos_enum,
                                    retain,
                                })
                                .await
                                .is_ok()
                            {
                                debug!(
                                    "Forwarded to local: {} -> {} (hash={})",
                                    topic,
                                    local_topic,
                                    payload_hash(&payload)
                                );
                            } else {
                                warn!("Local broker task unavailable");
                            }
                        }
                        Err(e) => {
                            warn!("Topic mapping error: {}", e);
                        }
                    }

                    break;
                }
            }
        }
        RemoteEvent::Error(e) => {
            error!("Remote broker error: {}", e);
            *remote_connected = false;
            let _ = replay_cmd_tx.send(ReplayCommand::RemoteDisconnected).await;
        }
    }
    Ok(())
}

async fn cache_message(
    cache_cmd_tx: &mpsc::Sender<CacheCommand>,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
) -> crate::error::Result<()> {
    let (response_tx, response_rx) = oneshot::channel();
    cache_cmd_tx
        .send(CacheCommand::Enqueue {
            topic: topic.as_bytes().to_vec(),
            payload: payload.to_vec(),
            qos,
            retain,
            response: response_tx,
        })
        .await
        .map_err(|_| {
            crate::error::BridgeError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Cache task unavailable",
            ))
        })?;

    response_rx.await.map_err(|_| {
        crate::error::BridgeError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "Cache task dropped response",
        ))
    })??;

    Ok(())
}

fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}
