#![allow(clippy::result_large_err)]

use std::thread;
use std::time::Duration;

use tracing::{debug, debug_span};

use crate::config::{BridgeConfig, CacheConfig, ForwardDirection};
use crate::error::Result;
use crate::mqtt_utils::state;
use crate::mqtt_utils::topic::MqttSubscription;
use crate::tasks::MqttActor;

pub struct Bridge {
    config: BridgeConfig,
    cache_config: CacheConfig,
}

impl Bridge {
    pub fn new(config: BridgeConfig, cache_config: CacheConfig) -> Result<Self> {
        Ok(Self {
            config,
            cache_config,
        })
    }

    pub fn run(self) -> Result<()> {
        // Extract fields we need
        let Bridge {
            config,
            cache_config,
        } = self;

        // Create channels for local MQTT thread
        let (local_event_tx, local_event_rx) = flume::bounded(100);
        let (local_cmd_tx, local_cmd_rx) = flume::bounded(100);

        // Create channels for remote MQTT thread
        let (remote_event_tx, remote_event_rx) = flume::bounded(100);
        let (remote_cmd_tx, remote_cmd_rx) = flume::bounded(100);

        let local_subscriptions = config
            .forward
            .iter()
            .filter(|rule| rule.direction == ForwardDirection::Out)
            .map(MqttSubscription::from_forward_rule)
            .collect();

        let remote_subscriptions = config
            .forward
            .iter()
            .filter(|rule| rule.direction == ForwardDirection::In)
            .map(MqttSubscription::from_forward_rule)
            .collect();

        debug!("Local subscriptions: {:?}", local_subscriptions);
        debug!("Remote subscriptions: {:?}", remote_subscriptions);

        // Spawn local MQTT worker
        thread::spawn(move || {
            let _span = debug_span!("mqtt_conn", mqtt_conn = "local").entered();
            MqttActor::new(
                config.local.clone(),
                local_subscriptions,
                None,
                None,
                local_event_tx,
                local_cmd_rx,
            )
            .unwrap()
            .run()
        });

        // Spawn remote MQTT worker
        thread::spawn(move || {
            let _span = debug_span!("mqtt_conn", mqtt_conn = "remote").entered();
            MqttActor::new(
                config.remote.clone(),
                remote_subscriptions,
                Some(state::connected_msg(
                    &config.state_topic,
                    &config.state_online_payload,
                )),
                Some(state::disconnected_lwt(
                    &config.state_topic,
                    &config.state_offline_payload,
                )),
                remote_event_tx,
                remote_cmd_rx,
            )
            .unwrap()
            .run()
        });

        // Run router task (blocks until shutdown)
        loop {
            thread::sleep(Duration::from_secs(1));
        }

        Ok(())
    }
}
