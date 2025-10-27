use rumqttc::QoS;

#[derive(Debug)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

// Not really used
// impl MqttMessage {
//     pub fn new(
//         topic: impl Into<String>,
//         payload: impl Into<Vec<u8>>,
//         qos: QoS,
//         retain: bool,
//     ) -> MqttMessage {
//         MqttMessage {
//             topic: topic.into(),
//             payload: payload.into(),
//             qos,
//             retain,
//         }
//     }
// }
