use rumqttc::{LastWill, QoS};

pub fn state_lwt(state_topic: &str, state_offline_payload: &str) -> LastWill {
    LastWill::new(state_topic, state_offline_payload, QoS::AtLeastOnce, true)
}
