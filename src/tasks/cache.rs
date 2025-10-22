#![allow(clippy::result_large_err)]

use crate::cache::CachedMessage;
use crate::config::{CacheConfig, EvictionPolicy, SynchronousMode};
use crate::error::Result;
use crate::tasks::messages::CacheCommand;
use crate::util::payload_hash;
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

/// Cache task that owns the SQLite connection and handles all database operations.
///
/// All blocking rusqlite operations are wrapped in `spawn_blocking` to avoid blocking
/// the async runtime. The connection is wrapped in Arc<Mutex<>> to allow moving into
/// spawn_blocking closures.
pub async fn cache_task(
    config: CacheConfig,
    mut command_rx: mpsc::Receiver<CacheCommand>,
) {
    // Open database connection
    let conn = match open_database(&config) {
        Ok(conn) => Arc::new(Mutex::new(conn)),
        Err(e) => {
            tracing::error!("Failed to open cache database: {}", e);
            return;
        }
    };

    info!("Cache task started");

    // Process commands
    while let Some(cmd) = command_rx.recv().await {
        match cmd {
            CacheCommand::Enqueue {
                topic,
                payload,
                qos,
                retain,
                response,
            } => {
                let conn = Arc::clone(&conn);
                let config = config.clone();
                let result = tokio::task::spawn_blocking(move || {
                    enqueue(&conn, &config, &topic, &payload, qos, retain)
                })
                .await
                .unwrap_or_else(|e| Err(crate::error::BridgeError::Io(std::io::Error::other(
                    format!("spawn_blocking panicked: {}", e),
                ))));
                let _ = response.send(result);
            }
            CacheCommand::DequeueBatch { limit, response } => {
                let conn = Arc::clone(&conn);
                let result = tokio::task::spawn_blocking(move || dequeue_batch(&conn, limit))
                    .await
                    .unwrap_or_else(|e| Err(crate::error::BridgeError::Io(std::io::Error::other(
                        format!("spawn_blocking panicked: {}", e),
                    ))));
                let _ = response.send(result);
            }
            CacheCommand::DeleteBatch { ids, response } => {
                let conn = Arc::clone(&conn);
                let result = tokio::task::spawn_blocking(move || delete_batch(&conn, &ids))
                    .await
                    .unwrap_or_else(|e| Err(crate::error::BridgeError::Io(std::io::Error::other(
                        format!("spawn_blocking panicked: {}", e),
                    ))));
                let _ = response.send(result);
            }
            CacheCommand::Count { response } => {
                let conn = Arc::clone(&conn);
                let result = tokio::task::spawn_blocking(move || count(&conn))
                    .await
                    .unwrap_or_else(|e| Err(crate::error::BridgeError::Io(std::io::Error::other(
                        format!("spawn_blocking panicked: {}", e),
                    ))));
                let _ = response.send(result);
            }
        }
    }

    info!("Cache task shutting down");
}

fn open_database(config: &CacheConfig) -> Result<Connection> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = Path::new(&config.sqlite_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let conn = Connection::open(&config.sqlite_path)?;

    // Set WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")?;

    // Set synchronous mode
    let sync_mode = match config.synchronous {
        SynchronousMode::Full => "FULL",
        SynchronousMode::Normal => "NORMAL",
        SynchronousMode::Off => "OFF",
    };
    conn.pragma_update(None, "synchronous", sync_mode)?;

    // Set busy timeout
    conn.busy_timeout(std::time::Duration::from_millis(config.busy_timeout_ms))?;

    // Create schema
    conn.execute(
        "CREATE TABLE IF NOT EXISTS msg_queue (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            topic       BLOB NOT NULL,
            payload     BLOB NOT NULL,
            qos         INTEGER NOT NULL,
            retain      INTEGER NOT NULL,
            ts_enqueued INTEGER NOT NULL
        )",
        [],
    )?;

    info!("SQLite cache initialized at {:?}", config.sqlite_path);
    Ok(conn)
}

fn enqueue(
    conn: &Arc<Mutex<Connection>>,
    config: &CacheConfig,
    topic: &[u8],
    payload: &[u8],
    qos: u8,
    retain: bool,
) -> Result<()> {
    // Skip QoS 0 messages if configured
    if qos == 0 && !config.cache_qos0 {
        debug!("Skipping QoS 0 message (cache_qos0=false)");
        return Ok(());
    }

    // Lock is synchronous - we're already in spawn_blocking
    let conn = conn.blocking_lock();

    let row_count: usize = conn.query_row("SELECT COUNT(*) FROM msg_queue", [], |row| row.get(0))?;

    // Check if we need to evict
    if config.max_rows > 0 && row_count >= config.max_rows {
        match config.eviction {
            EvictionPolicy::DropOldest => {
                let to_delete = row_count - config.max_rows + 1;
                conn.execute(
                    "DELETE FROM msg_queue WHERE id IN (
                        SELECT id FROM msg_queue ORDER BY id LIMIT ?1
                    )",
                    [to_delete],
                )?;
                debug!("Evicted {} oldest messages", to_delete);
            }
            EvictionPolicy::RejectNew => {
                warn!(
                    "Cache full, rejecting new message (max_rows={})",
                    config.max_rows
                );
                return Err(crate::error::BridgeError::CacheFull(format!(
                    "Cache at max_rows limit: {}",
                    config.max_rows
                )));
            }
        }
    }

    // Insert the message
    let ts_enqueued = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO msg_queue (topic, payload, qos, retain, ts_enqueued)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![topic, payload, qos, retain, ts_enqueued],
    )?;

    info!(
        "Enqueued message: topic={:?}, qos={}, size={}, hash={}",
        String::from_utf8_lossy(topic),
        qos,
        payload.len(),
        payload_hash(payload)
    );
    debug!("Queue size: {}", row_count + 1);

    Ok(())
}

fn dequeue_batch(conn: &Arc<Mutex<Connection>>, limit: usize) -> Result<Vec<CachedMessage>> {
    let conn = conn.blocking_lock();

    let mut stmt = conn.prepare(
        "SELECT id, topic, payload, qos, retain, ts_enqueued
         FROM msg_queue
         ORDER BY id
         LIMIT ?1",
    )?;

    let messages = stmt
        .query_map([limit], |row| {
            Ok(CachedMessage {
                id: row.get(0)?,
                topic: row.get(1)?,
                payload: row.get(2)?,
                qos: row.get(3)?,
                retain: row.get::<_, i32>(4)? != 0,
                ts_enqueued: row.get(5)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(messages)
}

fn delete_batch(conn: &Arc<Mutex<Connection>>, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }

    let conn = conn.blocking_lock();

    // Build query with placeholders
    let placeholders = ids.iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let query_str = format!("DELETE FROM msg_queue WHERE id IN ({})", placeholders);

    let mut stmt = conn.prepare(&query_str)?;
    let params: Vec<&dyn rusqlite::ToSql> = ids.iter()
        .map(|id| id as &dyn rusqlite::ToSql)
        .collect();
    stmt.execute(params.as_slice())?;

    Ok(())
}

fn count(conn: &Arc<Mutex<Connection>>) -> Result<usize> {
    let conn = conn.blocking_lock();
    let count: usize = conn.query_row("SELECT COUNT(*) FROM msg_queue", [], |row| row.get(0))?;
    Ok(count)
}
