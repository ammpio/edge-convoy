#![allow(clippy::result_large_err)]

use crate::config::{CacheConfig, EvictionPolicy, SynchronousMode};
use crate::error::Result;
use crate::mqtt_utils::payload_hash;
use rusqlite::{Connection, params};
use std::path::Path;
use tracing::{debug, info, warn};

/// A cached MQTT message awaiting delivery to the remote broker.
///
/// Messages are stored in SQLite with metadata to support FIFO replay
/// and delay tracking for debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedMessage {
    /// Unique message ID (autoincrement)
    pub id: i64,
    /// MQTT topic as bytes
    pub topic: Vec<u8>,
    /// Message payload as bytes
    pub payload: Vec<u8>,
    /// QoS level (0, 1, or 2)
    pub qos: u8,
    /// Whether message should be retained
    pub retain: bool,
    /// Unix timestamp when message was enqueued
    pub ts_enqueued: i64,
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
        "CREATE TABLE IF NOT EXISTS messages (
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
    conn: &Connection,
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

    let row_count: usize = count(conn)?;

    // Check if we need to evict
    if config.max_rows > 0 && row_count >= config.max_rows {
        match config.eviction {
            EvictionPolicy::DropOldest => {
                let to_delete = row_count - config.max_rows + 1;
                conn.execute(
                    "DELETE FROM messages WHERE id IN (
                        SELECT id FROM messages ORDER BY id LIMIT ?1
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
        "INSERT INTO messages (topic, payload, qos, retain, ts_enqueued)
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

fn dequeue_batch(conn: &Connection, limit: usize) -> Result<Vec<CachedMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, topic, payload, qos, retain, ts_enqueued
         FROM messages
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

fn delete_batch(conn: &Connection, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }

    // Build query with placeholders
    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let query_str = format!("DELETE FROM messages WHERE id IN ({})", placeholders);

    let mut stmt = conn.prepare(&query_str)?;
    let params: Vec<&dyn rusqlite::ToSql> =
        ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
    stmt.execute(params.as_slice())?;

    Ok(())
}

fn count(conn: &Connection) -> Result<usize> {
    let count: usize = conn.query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))?;
    Ok(count)
}
