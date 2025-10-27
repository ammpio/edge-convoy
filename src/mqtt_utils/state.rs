use rumqttc::{LastWill, QoS};

use crate::mqtt_utils::MqttMessage;

pub fn connected_msg(state_topic: &str, state_online_payload: &str) -> MqttMessage {
    MqttMessage {
        topic: state_topic.into(),
        payload: state_online_payload.into(),
        qos: QoS::AtLeastOnce,
        retain: true,
    }
}

pub fn disconnected_lwt(state_topic: &str, state_offline_payload: &str) -> LastWill {
    LastWill::new(state_topic, state_offline_payload, QoS::AtLeastOnce, true)
}
