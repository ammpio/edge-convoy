use bytes::Bytes;
use flume::Sender;
use rumqttc::QoS;

use crate::cache::CachedMessage;
use crate::error::Result;
use crate::mqtt_utils::MqttMessage;

// ============================================================================
// MQTT Worker Messages
// ============================================================================

#[derive(Debug)]
pub enum MqttCommand {
    Publish {
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        /// Optional response channel for publish confirmation
        response: Option<Sender<Result<()>>>,
    },
}

#[derive(Debug)]
pub enum MqttEvent {
    Connected,
    Disconnected,
    Message(MqttMessage),
    Error(String),
}

// ============================================================================
// Cache Messages
// ============================================================================

#[derive(Debug)]
pub enum CacheCommand {
    Enqueue {
        topic: Bytes,
        payload: Bytes,
        qos: u8,
        retain: bool,
        response: Sender<Result<()>>,
    },
    DequeueBatch {
        limit: usize,
        response: Sender<Result<Vec<CachedMessage>>>,
    },
    DeleteBatch {
        ids: Vec<i64>,
        response: Sender<Result<()>>,
    },
    Count {
        response: Sender<Result<usize>>,
    },
}

// ============================================================================
// Replay Messages
// ============================================================================

#[derive(Debug)]
pub enum ReplayCommand {
    /// Trigger replay to start/resume
    Trigger,
    /// Notify that remote is connected
    RemoteConnected,
    /// Notify that remote is disconnected
    RemoteDisconnected,
}
