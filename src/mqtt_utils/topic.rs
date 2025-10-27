#![allow(clippy::result_large_err)]

use rumqttc::QoS;

use crate::config::{ForwardDirection, ForwardRule};
use crate::error::{BridgeError, Result};
use crate::mqtt_utils::qos_from_u8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSubscription {
    pub topic: String,
    pub qos: QoS,
}

impl MqttSubscription {
    /// MQTT subcription from forward rule
    ///
    /// Generates an MQTTSubscription struct for the topic on which the rule listens
    /// (local for ForwardDirection::Out; remote for ForwardDirection::In)
    pub fn from_forward_rule(rule: &ForwardRule) -> Self {
        let topic = match rule.direction {
            ForwardDirection::Out => add_prefix(&rule.topic_pattern, &rule.local_prefix).unwrap(),
            ForwardDirection::In => add_prefix(&rule.topic_pattern, &rule.remote_prefix).unwrap(),
        };
        let qos = qos_from_u8(rule.qos);
        Self { topic, qos }
    }
}

/// Apply forward rules to a topic
///
/// Takes as input the topic on which a message is received
/// Returns the topic on which the mssage should be published
pub fn apply_forward_rule(topic: &str, rule: &ForwardRule) -> Result<String> {
    match rule.direction {
        ForwardDirection::Out => add_prefix(
            &strip_prefix(topic, &rule.local_prefix)?,
            &rule.remote_prefix,
        ),
        ForwardDirection::In => add_prefix(
            &strip_prefix(topic, &rule.remote_prefix)?,
            &rule.local_prefix,
        ),
    }
}

fn strip_prefix(topic: &str, prefix: &str) -> Result<String> {
    if topic == prefix {
        Ok("".to_string())
    } else if let Some(stripped) = topic.strip_prefix(&with_slash(prefix)) {
        Ok(stripped.to_string())
    } else {
        Err(BridgeError::TopicMapping(format!(
            "Topic '{}' does not start with prefix '{}'",
            topic, prefix
        )))
    }
}

fn add_prefix(topic: &str, prefix: &str) -> Result<String> {
    if topic.is_empty() {
        Ok(prefix.to_string())
    } else if prefix.is_empty() {
        Ok(topic.to_string())
    } else {
        Ok(format!("{}{}", with_slash(prefix), topic))
    }
}

fn with_slash(prefix: &str) -> String {
    if prefix.is_empty() {
        "".to_string()
    } else {
        format!("{}/", prefix.trim_end_matches("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_forward_out() {
        let rule = ForwardRule {
            topic_pattern: "d/#".to_string(),
            direction: ForwardDirection::Out,
            local_prefix: "".to_string(),
            remote_prefix: "a/node_123".to_string(),
            qos: 1,
        };

        assert_eq!(
            apply_forward_rule("d/foo", &rule).unwrap(),
            "a/node_123/d/foo"
        );
        assert_eq!(
            apply_forward_rule("d/bar/baz", &rule).unwrap(),
            "a/node_123/d/bar/baz"
        );

        // No prefix
        let rule_no_prefix = ForwardRule {
            topic_pattern: "raw/#".to_string(),
            direction: ForwardDirection::Out,
            local_prefix: "".to_string(),
            remote_prefix: "".to_string(),
            qos: 1,
        };

        assert_eq!(
            apply_forward_rule("raw/data", &rule_no_prefix).unwrap(),
            "raw/data"
        );
    }

    #[test]
    fn test_apply_forward_in() {
        let rule = ForwardRule {
            topic_pattern: "a/node_123/u/#".to_string(),
            direction: ForwardDirection::In,
            local_prefix: "".to_string(),
            remote_prefix: "a/node_123".to_string(),
            qos: 1,
        };

        assert_eq!(
            apply_forward_rule("a/node_123/u/foo", &rule).unwrap(),
            "u/foo"
        );
        assert_eq!(
            apply_forward_rule("a/node_123/u/bar/baz", &rule).unwrap(),
            "u/bar/baz"
        );

        // No prefix
        let rule_no_prefix = ForwardRule {
            topic_pattern: "commands/#".to_string(),
            direction: ForwardDirection::In,
            local_prefix: "".to_string(),
            remote_prefix: "".to_string(),
            qos: 1,
        };
        assert_eq!(
            apply_forward_rule("commands/restart", &rule_no_prefix).unwrap(),
            "commands/restart"
        );
    }
}
