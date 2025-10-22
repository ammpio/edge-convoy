use crate::error::Result;
use crate::tasks::cache::CachedMessage;
use rumqttc::QoS;
use tokio::sync::oneshot;

// ============================================================================
// Local Broker Messages
// ============================================================================

#[derive(Debug)]
pub enum LocalCommand {
    Subscribe {
        topic: String,
        qos: QoS,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    },
}

#[derive(Debug)]
pub enum LocalEvent {
    Connected,
    Disconnected,
    MessageReceived {
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    },
    Error(String),
}

// ============================================================================
// Remote Broker Messages
// ============================================================================

#[derive(Debug)]
pub enum RemoteCommand {
    Subscribe {
        topic: String,
        qos: QoS,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
        /// Optional response channel for replay acknowledgment
        response: Option<oneshot::Sender<Result<()>>>,
    },
    /// Recreate the eventloop (for DNS resolution issues)
    RecreateEventloop {
        response: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug)]
pub enum RemoteEvent {
    Connected,
    Disconnected,
    MessageReceived {
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    },
    Error(String),
}

// ============================================================================
// Cache Messages
// ============================================================================

#[derive(Debug)]
pub enum CacheCommand {
    Enqueue {
        topic: Vec<u8>,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
        response: oneshot::Sender<Result<()>>,
    },
    DequeueBatch {
        limit: usize,
        response: oneshot::Sender<Result<Vec<CachedMessage>>>,
    },
    DeleteBatch {
        ids: Vec<i64>,
        response: oneshot::Sender<Result<()>>,
    },
    Count {
        response: oneshot::Sender<Result<usize>>,
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
