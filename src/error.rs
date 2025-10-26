use thiserror::Error;

use crate::messages::MqttEvent;

#[derive(Error, Debug)]
#[allow(clippy::result_large_err)]
pub enum BridgeError {
    #[error("MQTT error: {0}")]
    MqttClient(#[from] rumqttc::ClientError),

    #[error("MQTT connection error: {0}")]
    MqttConnection(#[from] rumqttc::ConnectionError),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel send error: {0}")]
    Channel(#[from] flume::SendError<MqttEvent>),

    #[cfg(feature = "cli")]
    #[error("TOML deserialization error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("Topic mapping error: {0}")]
    TopicMapping(String),

    #[error("Cache full: {0}")]
    CacheFull(String),
}

pub type Result<T> = std::result::Result<T, BridgeError>;
