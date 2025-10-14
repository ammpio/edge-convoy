#![allow(clippy::result_large_err)]

use crate::config::{CacheConfig, EvictionPolicy, SynchronousMode};
use crate::error::{BridgeError, Result};
use crate::util::payload_hash;
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::{Arc, Mutex};
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

/// SQLite-backed cache manager for MQTT messages.
///
/// Provides thread-safe message queueing with configurable eviction policies
/// and efficient batch retrieval for replay.
///
/// # Thread Safety
///
/// `CacheManager` is safe to share across threads via `Arc`. All operations
/// use internal locking to ensure consistency.
pub struct CacheManager {
    conn: Arc<Mutex<Connection>>,
    /// Cache configuration
    pub config: CacheConfig,
}

impl CacheManager {
    /// Create a new cache manager with the given configuration.
    ///
    /// Opens or creates the SQLite database and initializes the schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or initialized.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use convoy::{CacheManager, CacheConfig};
    ///
    /// let config = CacheConfig::default();
    /// let cache = CacheManager::new(config)?;
    /// # Ok::<(), convoy::BridgeError>(())
    /// ```
    pub fn new(config: CacheConfig) -> Result<Self> {
        let conn = Self::open_database(&config)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            config,
        })
    }

    fn open_database(config: &CacheConfig) -> Result<Connection> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = Path::new(&config.sqlite_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&config.sqlite_path)?;

        // Set WAL mode (PRAGMA can return results, so use pragma_update)
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

    /// Enqueue a message to the cache.
    ///
    /// Messages are stored in FIFO order and subject to configured eviction policies
    /// if the cache is full.
    ///
    /// # Arguments
    ///
    /// * `topic` - MQTT topic as bytes
    /// * `payload` - Message payload as bytes  
    /// * `qos` - QoS level (0, 1, or 2)
    /// * `retain` - Whether message should be retained
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The cache is full and eviction policy is `RejectNew`
    /// - Database operations fail
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use convoy::{CacheManager, CacheConfig};
    /// # let cache = CacheManager::new(CacheConfig::default())?;
    /// cache.enqueue(b"sensors/temp", b"23.5", 1, false)?;
    /// # Ok::<(), convoy::BridgeError>(())
    /// ```
    pub fn enqueue(&self, topic: &[u8], payload: &[u8], qos: u8, retain: bool) -> Result<()> {
        // Skip QoS 0 messages if configured
        if qos == 0 && !self.config.cache_qos0 {
            debug!("Skipping QoS 0 message (cache_qos0=false)");
            return Ok(());
        }

        let conn = self.conn.lock().unwrap();

        let row_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM msg_queue", [], |row| row.get(0))?;

        // Check if we need to evict
        if self.config.max_rows > 0 && row_count >= self.config.max_rows as i64 {
            match self.config.eviction {
                EvictionPolicy::DropOldest => {
                    let to_delete = row_count - self.config.max_rows as i64 + 1;
                    conn.execute(
                        "DELETE FROM msg_queue WHERE id IN (
                                SELECT id FROM msg_queue ORDER BY id LIMIT ?1
                            )",
                        params![to_delete],
                    )?;
                    debug!("Evicted {} oldest messages", to_delete);
                }
                EvictionPolicy::RejectNew => {
                    warn!(
                        "Cache full, rejecting new message (max_rows={})",
                        self.config.max_rows
                    );
                    return Err(BridgeError::CacheFull(format!(
                        "Cache at max_rows limit: {}",
                        self.config.max_rows
                    )));
                }
            }
        }

        // Insert the message
        let ts_enqueued = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO msg_queue (topic, payload, qos, retain, ts_enqueued)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![topic, payload, qos, retain as i32, ts_enqueued],
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

    /// Dequeue a batch of messages from the cache in FIFO order.
    ///
    /// Messages are returned but not removed from the cache. Call `delete_message`
    /// after successful delivery to remove them.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of messages to retrieve
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub fn dequeue_batch(&self, limit: usize) -> Result<Vec<CachedMessage>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT id, topic, payload, qos, retain, ts_enqueued
             FROM msg_queue
             ORDER BY id
             LIMIT ?1",
        )?;

        let messages = stmt
            .query_map(params![limit], |row| {
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

    /// Delete a message from the cache by ID.
    ///
    /// This should be called after successfully delivering a message to the remote broker.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub fn delete_message(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM msg_queue WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Get the number of messages currently in the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub fn count(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM msg_queue", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Check if the cache is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    /// Clear all cached messages.
    ///
    /// **Warning**: This permanently deletes all cached messages.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub fn clear(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM msg_queue", [])?;
        info!("Cache cleared");
        Ok(())
    }
}
