use crate::tasks::messages::{CacheCommand, RemoteCommand, ReplayCommand};
use crate::util::payload_hash;
use chrono;
use rumqttc::QoS;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

/// Replay task that coordinates cache replay when remote is connected.
///
/// This task:
/// - Waits for remote connection
/// - Requests message batches from cache
/// - Sends publish commands to remote broker
/// - Waits for acknowledgment
/// - Deletes successfully published messages from cache
pub async fn replay_task(
    mut command_rx: mpsc::Receiver<ReplayCommand>,
    cache_cmd_tx: mpsc::Sender<CacheCommand>,
    remote_cmd_tx: mpsc::Sender<RemoteCommand>,
    flush_batch: usize,
    flush_interval_ms: u64,
) {
    info!(
        "Replay task started (batch={}, interval={}ms)",
        flush_batch, flush_interval_ms
    );

    let mut remote_connected = false;

    loop {
        tokio::select! {
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    ReplayCommand::Trigger => {
                        debug!("Replay triggered");
                        if remote_connected {
                            replay_cache(&cache_cmd_tx, &remote_cmd_tx, flush_batch, flush_interval_ms).await;
                        } else {
                            debug!("Remote not connected, skipping replay");
                        }
                    }
                    ReplayCommand::RemoteConnected => {
                        info!("Remote connected, enabling replay");
                        remote_connected = true;
                        // Immediately trigger replay
                        replay_cache(&cache_cmd_tx, &remote_cmd_tx, flush_batch, flush_interval_ms).await;
                    }
                    ReplayCommand::RemoteDisconnected => {
                        info!("Remote disconnected, pausing replay");
                        remote_connected = false;
                    }
                }
            }

            // Periodic check for cached messages (every 5 seconds)
            _ = sleep(Duration::from_secs(5)) => {
                if remote_connected {
                    // Check if there are messages to replay
                    let (count_tx, count_rx) = oneshot::channel();
                    if cache_cmd_tx.send(CacheCommand::Count { response: count_tx }).await.is_ok()
                        && let Ok(Ok(count)) = count_rx.await
                        && count > 0
                    {
                        debug!("Periodic check: {} messages in cache, triggering replay", count);
                        replay_cache(&cache_cmd_tx, &remote_cmd_tx, flush_batch, flush_interval_ms).await;
                    }
                }
            }
        }
    }
}

async fn replay_cache(
    cache_cmd_tx: &mpsc::Sender<CacheCommand>,
    remote_cmd_tx: &mpsc::Sender<RemoteCommand>,
    flush_batch: usize,
    flush_interval_ms: u64,
) {
    // Get count first
    let (count_tx, count_rx) = oneshot::channel();
    if cache_cmd_tx
        .send(CacheCommand::Count { response: count_tx })
        .await
        .is_err()
    {
        error!("Cache task has shut down");
        return;
    }

    let count = match count_rx.await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            error!("Failed to get cache count: {}", e);
            return;
        }
        Err(_) => {
            error!("Cache task dropped response channel");
            return;
        }
    };

    if count == 0 {
        return;
    }

    info!("Replaying {} cached messages", count);

    // Drain the cache in batches
    loop {
        // Request batch
        let (dequeue_tx, dequeue_rx) = oneshot::channel();
        if cache_cmd_tx
            .send(CacheCommand::DequeueBatch {
                limit: flush_batch,
                response: dequeue_tx,
            })
            .await
            .is_err()
        {
            error!("Cache task has shut down");
            break;
        }

        let messages = match dequeue_rx.await {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(e)) => {
                error!("Failed to dequeue messages: {}", e);
                break;
            }
            Err(_) => {
                error!("Cache task dropped response channel");
                break;
            }
        };

        if messages.is_empty() {
            debug!("Cache drained");
            break;
        }

        debug!("Replaying batch of {} messages", messages.len());

        // Track successfully published message IDs for batch deletion
        let mut published_ids = Vec::with_capacity(messages.len());

        // Publish each message
        for msg in &messages {
            let topic = String::from_utf8_lossy(&msg.topic).to_string();
            let qos = qos_from_u8(msg.qos);

            // Calculate delay between enqueue and replay
            let now = chrono::Utc::now().timestamp();
            let delay_seconds = now - msg.ts_enqueued;

            // Create response channel for acknowledgment
            let (response_tx, response_rx) = oneshot::channel();

            // Send publish command to remote broker
            if remote_cmd_tx
                .send(RemoteCommand::Publish {
                    topic: topic.clone(),
                    payload: msg.payload.clone(),
                    qos,
                    retain: msg.retain,
                    response: Some(response_tx),
                })
                .await
                .is_err()
            {
                error!("Remote broker task has shut down");
                break;
            }

            // Wait for acknowledgment
            match response_rx.await {
                Ok(Ok(_)) => {
                    info!(
                        "Replayed message: {} (id={}, delay={}s, hash={})",
                        topic,
                        msg.id,
                        delay_seconds,
                        payload_hash(&msg.payload)
                    );
                    published_ids.push(msg.id);
                }
                Ok(Err(e)) => {
                    error!(
                        "Failed to replay message {}: {}, stopping replay",
                        msg.id, e
                    );
                    break;
                }
                Err(_) => {
                    error!("Remote broker task dropped response channel");
                    break;
                }
            }
        }

        // Batch delete all successfully published messages
        if !published_ids.is_empty() {
            let (delete_tx, delete_rx) = oneshot::channel();
            if cache_cmd_tx
                .send(CacheCommand::DeleteBatch {
                    ids: published_ids.clone(),
                    response: delete_tx,
                })
                .await
                .is_err()
            {
                error!("Cache task has shut down");
                break;
            }

            match delete_rx.await {
                Ok(Ok(_)) => {
                    debug!(
                        "Deleted batch of {} messages from cache",
                        published_ids.len()
                    );
                }
                Ok(Err(e)) => {
                    error!(
                        "Failed to delete batch of {} messages: {}",
                        published_ids.len(),
                        e
                    );
                }
                Err(_) => {
                    error!("Cache task dropped response channel");
                }
            }
        }

        // Wait before next batch
        sleep(Duration::from_millis(flush_interval_ms)).await;
    }

    // Check final count
    let (count_tx, count_rx) = oneshot::channel();
    if cache_cmd_tx
        .send(CacheCommand::Count { response: count_tx })
        .await
        .is_ok()
    {
        match count_rx.await {
            Ok(Ok(0)) => {
                info!("All cached messages replayed successfully");
            }
            Ok(Ok(remaining)) => {
                info!("{} messages remaining in cache", remaining);
            }
            Ok(Err(e)) => {
                error!("Failed to get cache count: {}", e);
            }
            Err(_) => {}
        }
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
