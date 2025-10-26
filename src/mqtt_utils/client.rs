#![allow(clippy::result_large_err)]

use std::time::Duration;

use rumqttc::{Client, Connection, LastWill, MqttOptions, Transport};
use tracing::{debug, info, warn};

use crate::config::BrokerConfig;
use crate::error::Result;

pub fn create_mqtt_client(
    broker_config: &BrokerConfig,
    lwt: Option<LastWill>,
) -> Result<(Client, Connection)> {
    let (host, port) = parse_broker_addr(&broker_config.addr);

    let mut mqttoptions = MqttOptions::new(&broker_config.client_id, host, port);

    mqttoptions.set_keep_alive(Duration::from_secs(broker_config.keep_alive_secs as u64));
    mqttoptions.set_clean_session(broker_config.clean_session);
    mqttoptions.set_inflight(broker_config.max_inflight);

    // Set credentials if provided
    if let (Some(username), Some(password)) = (&broker_config.username, &broker_config.password) {
        mqttoptions.set_credentials(username, password);
    }

    // Set Last Will and Testament if provided
    if let Some(lwt) = lwt {
        mqttoptions.set_last_will(lwt);
    }

    // Configure TLS with native-tls if provided
    if let Some(tls_config) = &broker_config.tls {
        use native_tls::TlsConnector;

        let mut tls_builder = TlsConnector::builder();

        // Disable certificate verification if requested (for testing only!)
        if tls_config.danger_accept_invalid_certs {
            tls_builder.danger_accept_invalid_certs(true);
            warn!("TLS certificate verification disabled - this is insecure!");
        }

        // Add custom CA certificate if provided
        if let Some(ca_file) = &tls_config.ca_file {
            if ca_file.exists() {
                let ca_cert_data = std::fs::read(ca_file)?;
                let cert = native_tls::Certificate::from_pem(&ca_cert_data)?;
                tls_builder.add_root_certificate(cert);
                tracing::info!("Loaded custom CA certificate from {:?}", ca_file);

                // Note: On macOS, native-tls uses Security.framework which may not honor
                // programmatically-added CA certificates. If you encounter "certificate not trusted"
                // errors with a valid CA, consider either:
                // 1. Adding the CA to the macOS system keychain
                // 2. Using danger_accept_invalid_certs = true (insecure, for testing only)
                // 3. Switching to rustls (would require changing dependency features)
                #[cfg(target_os = "macos")]
                debug!("macOS detected: native-tls may not honor custom CA certificates");
            } else {
                tracing::warn!("CA file not found: {:?}", ca_file);
            }
        }

        // Add client certificate for mTLS if provided
        if let (Some(client_cert_path), Some(password)) =
            (&tls_config.client_cert, &tls_config.client_password)
        {
            if client_cert_path.exists() {
                let client_cert_data = std::fs::read(client_cert_path)?;
                let identity = native_tls::Identity::from_pkcs12(&client_cert_data, password)?;
                tls_builder.identity(identity);
                tracing::info!(
                    "Loaded client certificate for mTLS from {:?}",
                    client_cert_path
                );
            } else {
                tracing::warn!("Client certificate file not found: {:?}", client_cert_path);
            }
        }

        let tls_connector = tls_builder.build()?;
        mqttoptions.set_transport(Transport::tls_with_config(tls_connector.into()));
        info!("TLS enabled for broker connection with native-tls");
    }

    let (client, connection) = Client::new(mqttoptions, 10);
    Ok((client, connection))
}

fn parse_broker_addr(addr: &str) -> (String, u16) {
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() == 2 {
        let host = parts[0].to_string();
        let port = parts[1].parse().unwrap_or(1883);
        (host, port)
    } else {
        (addr.to_string(), 1883)
    }
}
