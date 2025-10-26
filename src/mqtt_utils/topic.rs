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
    pub fn from_forward_rule(forward_rule: &ForwardRule) -> Self {
        Self {
            topic: match forward_rule.direction {
                ForwardDirection::Out => {
                    add_prefix(&forward_rule.topic_pattern, &forward_rule.local_prefix).unwrap()
                }
                ForwardDirection::In => {
                    add_prefix(&forward_rule.topic_pattern, &forward_rule.remote_prefix).unwrap()
                }
            },
            qos: qos_from_u8(forward_rule.qos),
        }
    }
}

/// Apply forward rules to a topic
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
