use crate::cache::CacheManager;
use crate::util::payload_hash;
use chrono;
use rumqttc::{AsyncClient, QoS};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, trace, warn};

pub async fn replay_worker(
    cache: Arc<CacheManager>,
    remote_client: AsyncClient,
    remote_connected: Arc<RwLock<bool>>,
    replay_trigger: Arc<Notify>,
    flush_batch: usize,
    flush_interval_ms: u64,
) {
    info!(
        "Replay worker started (batch={}, interval={}ms)",
        flush_batch, flush_interval_ms
    );

    loop {
        // Wait for remote to be connected
        while !*remote_connected.read().await {
            trace!("Replay worker waiting for remote connection...");
            sleep(Duration::from_secs(1)).await;
        }

        // Check if cache has messages
        let count = match cache.count() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to get cache count: {}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if count == 0 {
            // No messages, wait for trigger or timeout
            tokio::select! {
                _ = replay_trigger.notified() => {
                    debug!("Running replay");
                }
                _ = sleep(Duration::from_secs(5)) => {
                    // Periodic check
                }
            }
            continue;
        }

        info!("Replaying {} cached messages", count);

        // Drain the cache in batches
        loop {
            // Check if still connected
            if !*remote_connected.read().await {
                warn!("Remote disconnected during replay, pausing");
                break;
            }

            // Dequeue batch
            let messages = match cache.dequeue_batch(flush_batch) {
                Ok(msgs) => msgs,
                Err(e) => {
                    error!("Failed to dequeue messages: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    break;
                }
            };

            if messages.is_empty() {
                debug!("Cache drained");
                break;
            }

            debug!("Replaying batch of {} messages", messages.len());

            // Publish each message
            for msg in messages {
                let topic = String::from_utf8_lossy(&msg.topic).to_string();
                let qos = qos_from_u8(msg.qos);

                // Calculate delay between enqueue and replay
                let now = chrono::Utc::now().timestamp();
                let delay_seconds = now - msg.ts_enqueued;

                match remote_client
                    .publish(&topic, qos, msg.retain, msg.payload.clone())
                    .await
                {
                    Ok(_) => {
                        info!(
                            "Replayed message: {} (id={}, delay={}s, hash={})",
                            topic,
                            msg.id,
                            delay_seconds,
                            payload_hash(&msg.payload)
                        );

                        // Delete from cache after successful publish
                        if let Err(e) = cache.delete_message(msg.id) {
                            error!("Failed to delete message {}: {}", msg.id, e);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to replay message {}: {}, stopping replay",
                            msg.id, e
                        );
                        // Stop replaying on error, will retry later
                        break;
                    }
                }
            }

            // Wait before next batch
            sleep(Duration::from_millis(flush_interval_ms)).await;
        }

        // Check final count
        match cache.count() {
            Ok(0) => {
                info!("All cached messages replayed successfully");
            }
            Ok(remaining) => {
                info!("{} messages remaining in cache", remaining);
            }
            Err(e) => {
                error!("Failed to get cache count: {}", e);
            }
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
