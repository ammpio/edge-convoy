use thiserror::Error;

#[derive(Error, Debug)]
#[allow(clippy::result_large_err)]
pub enum BridgeError {
    #[error("MQTT error: {0}")]
    Mqtt(#[from] rumqttc::ClientError),

    #[error("MQTT connection error: {0}")]
    MqttConnection(#[from] rumqttc::ConnectionError),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "cli")]
    #[error("TOML deserialization error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("Topic mapping error: {0}")]
    TopicMapping(String),

    #[error("Cache full: {0}")]
    CacheFull(String),
}

pub type Result<T> = std::result::Result<T, BridgeError>;
