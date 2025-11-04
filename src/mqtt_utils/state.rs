use bytes::Bytes;
use rumqttc::{LastWill, QoS};

use crate::mqtt_utils::MqttMessage;

pub fn connected_msg(state_topic: &str, state_online_payload: &str) -> MqttMessage {
    MqttMessage {
        topic: state_topic.into(),
        payload: Bytes::from(state_online_payload.to_string()),
        qos: QoS::AtLeastOnce,
        retain: true,
    }
}

pub fn disconnected_lwt(state_topic: &str, state_offline_payload: &str) -> LastWill {
    LastWill::new(state_topic, state_offline_payload, QoS::AtLeastOnce, true)
}
