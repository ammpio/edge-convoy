#![allow(clippy::result_large_err)]

use std::thread;
use std::time::Duration;

use flume::{Receiver, Sender};
use rumqttc::{Client, Connection, Event, Incoming, LastWill, Outgoing};
use tracing::{debug, error};

use crate::BridgeError;
use crate::config::BrokerConfig;
use crate::error::Result;
use crate::messages::{MqttCommand, MqttEvent};
use crate::mqtt_utils::client::create_mqtt_client;
use crate::mqtt_utils::topic::MqttSubscription;

pub struct MqttActor {
    client: Client,
    connection: Connection,
    subscriptions: Vec<MqttSubscription>,
    event_tx: Sender<MqttEvent>,
    cmd_rx: Receiver<MqttCommand>,
}

impl MqttActor {
    pub fn new(
        config: BrokerConfig,
        subscriptions: Vec<MqttSubscription>,
        last_will: Option<LastWill>,
        event_tx: Sender<MqttEvent>,
        cmd_rx: Receiver<MqttCommand>,
    ) -> Result<Self> {
        let (client, connection) = create_mqtt_client(&config, last_will)?;
        Ok(Self {
            client,
            connection,
            subscriptions,
            event_tx,
            cmd_rx,
        })
    }

    pub fn run(mut self) -> Result<()> {
        loop {
            // drain commands
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                self.handle_cmd(cmd)?;
            }
            // poll event loop
            if let Some(notification) = self.connection.iter().next() {
                self.handle_notify(notification)?;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn handle_cmd(&mut self, cmd: MqttCommand) -> Result<()> {
        match cmd {
            MqttCommand::Publish {
                topic,
                payload,
                qos,
                retain,
                response,
            } => {
                let result = self
                    .client
                    .publish(&topic, qos, retain, payload)
                    .map_err(BridgeError::from);
                if result.is_ok() {
                    debug!("Published to topic: {}", topic);
                } else {
                    error!("Failed to publish to topic: {}", topic);
                }
                if let Some(response) = response {
                    let _ = response.send(result);
                }
                Ok(())
            }
        }
    }

    fn handle_notify(
        &mut self,
        notification: std::result::Result<Event, rumqttc::ConnectionError>,
    ) -> Result<()> {
        match notification {
            Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                debug!("Connected = {:?}", connack);
                for subscription in &self.subscriptions {
                    self.client
                        .subscribe(&subscription.topic, subscription.qos)?;
                    debug!("Subscribed to topic: {}", subscription.topic);
                }
                self.event_tx.send(MqttEvent::Connected)?;
            }
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                debug!("Publish = {:?}", publish);
            }
            Ok(Event::Incoming(Incoming::Subscribe(subscribe))) => {
                debug!("Subscribe = {:?}", subscribe);
            }
            Ok(Event::Incoming(Incoming::Unsubscribe(unsubscribe))) => {
                debug!("Unsubscribe = {:?}", unsubscribe);
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                debug!("Disconnect");
            }
            Ok(Event::Outgoing(Outgoing::Publish(publish))) => {
                debug!("Publish = {:?}", publish);
            }
            Ok(Event::Outgoing(Outgoing::Subscribe(subscribe))) => {
                debug!("Subscribe = {:?}", subscribe);
            }
            Ok(Event::Outgoing(Outgoing::Unsubscribe(unsubscribe))) => {
                debug!("Unsubscribe = {:?}", unsubscribe);
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                debug!("Disconnect");
            }
            Err(e) => {
                error!("MQTT error: {}", e);
                let _ = self.handle_err(&e);
                return Ok(());
            }
            _ => {
                debug!("Other MQTT event: {:?}", notification);
            }
        }
        Ok(())
    }

    fn handle_err(&mut self, _: &rumqttc::ConnectionError) -> Result<()> {
        /* backoff/reconnect */
        Ok(())
    }
}
