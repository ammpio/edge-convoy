use crate::tasks::messages::{LocalCommand, LocalEvent};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Local broker task that owns the eventloop and continuously polls it.
///
/// Handles both incoming events from the broker and outgoing commands
/// from the router.
pub async fn local_broker_task(
    client: AsyncClient,
    mut eventloop: EventLoop,
    mut command_rx: mpsc::Receiver<LocalCommand>,
    event_tx: mpsc::Sender<LocalEvent>,
) {
    info!("Local broker task started");

    loop {
        tokio::select! {
            biased;

            // Poll the eventloop continuously (always prioritized)
            poll_result = eventloop.poll() => {
                match poll_result {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        info!("Local broker connected");
                        if event_tx.send(LocalEvent::Connected).await.is_err() {
                            error!("Router has shut down, stopping local broker task");
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        debug!("Local broker received publish: {}", publish.topic);
                        let event = LocalEvent::MessageReceived {
                            topic: publish.topic,
                            payload: publish.payload.to_vec(),
                            qos: publish.qos as u8,
                            retain: publish.retain,
                        };
                        if event_tx.send(event).await.is_err() {
                            error!("Router has shut down, stopping local broker task");
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::Disconnect)) => {
                        warn!("Local broker disconnected");
                        if event_tx.send(LocalEvent::Disconnected).await.is_err() {
                            error!("Router has shut down, stopping local broker task");
                            break;
                        }
                    }
                    Ok(_) => {
                        // Other events we don't care about
                        debug!("Local broker event: {:?}", poll_result);
                    }
                    Err(e) => {
                        error!("Local broker connection error: {}", e);
                        let _ = event_tx.send(LocalEvent::Error(e.to_string())).await;
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }

            // Handle commands from router
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    LocalCommand::Subscribe { topic, qos } => {
                        debug!("Local broker subscribing to: {} (QoS {:?})", topic, qos);
                        if let Err(e) = client.subscribe(&topic, qos).await {
                            error!("Failed to subscribe to {}: {}", topic, e);
                        }
                    }
                    LocalCommand::Publish { topic, payload, qos, retain } => {
                        debug!("Local broker publishing to: {} ({} bytes)", topic, payload.len());
                        if let Err(e) = client.publish(&topic, qos, retain, payload).await {
                            error!("Failed to publish to {}: {}", topic, e);
                        }
                    }
                }
            }
        }
    }

    info!("Local broker task shutting down");
}
