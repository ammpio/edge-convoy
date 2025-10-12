#![allow(clippy::result_large_err)]

use crate::config::{ForwardRule, SubscribeRule};
use crate::error::{BridgeError, Result};

/// Check if a topic matches an MQTT topic filter (with + and # wildcards)
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_parts: Vec<&str> = topic.split('/').collect();
    let filter_parts: Vec<&str> = filter.split('/').collect();

    let mut topic_idx = 0;
    let mut filter_idx = 0;

    while filter_idx < filter_parts.len() && topic_idx < topic_parts.len() {
        let filter_part = filter_parts[filter_idx];

        match filter_part {
            "#" => {
                // Multi-level wildcard - must be last in filter
                return filter_idx == filter_parts.len() - 1;
            }
            "+" => {
                // Single-level wildcard - matches exactly one level
                topic_idx += 1;
                filter_idx += 1;
            }
            _ => {
                // Literal match required
                if topic_parts[topic_idx] != filter_part {
                    return false;
                }
                topic_idx += 1;
                filter_idx += 1;
            }
        }
    }

    // Check if we consumed everything
    if filter_idx < filter_parts.len() {
        // If we have remaining filter parts, the only way to match is if
        // the next (and last) filter part is "#" AND there are more topic parts
        // However, since we exited the loop, we know topic_idx >= topic_parts.len()
        // So this can't match
        return false;
    }

    // We consumed all filter parts, check if we also consumed all topic parts
    topic_idx == topic_parts.len()
}

/// Apply forward rule: prepend remote_prefix to local topic
pub fn apply_forward_mapping(local_topic: &str, rule: &ForwardRule) -> String {
    if rule.remote_prefix.is_empty() {
        local_topic.to_string()
    } else {
        // Ensure no double slashes
        let prefix = rule.remote_prefix.trim_end_matches('/');
        format!("{}/{}", prefix, local_topic)
    }
}

/// Apply subscribe rule: strip remote_prefix from remote topic
pub fn apply_subscribe_mapping(remote_topic: &str, rule: &SubscribeRule) -> Result<String> {
    if rule.remote_prefix.is_empty() {
        return Ok(remote_topic.to_string());
    }

    let prefix = rule.remote_prefix.trim_end_matches('/');
    let prefix_with_slash = format!("{}/", prefix);

    if let Some(stripped) = remote_topic.strip_prefix(&prefix_with_slash) {
        Ok(stripped.to_string())
    } else if remote_topic == prefix {
        // Edge case: topic is exactly the prefix
        Ok(String::new())
    } else {
        Err(BridgeError::TopicMapping(format!(
            "Topic '{}' does not start with prefix '{}'",
            remote_topic, prefix
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matches_filter() {
        // Exact matches
        assert!(topic_matches_filter("a/b/c", "a/b/c"));
        assert!(!topic_matches_filter("a/b/c", "a/b/d"));

        // Single-level wildcard (+)
        assert!(topic_matches_filter("a/b/c", "a/+/c"));
        assert!(topic_matches_filter("a/b/c", "+/b/c"));
        assert!(topic_matches_filter("a/b/c", "+/+/+"));
        assert!(!topic_matches_filter("a/b/c", "a/+"));

        // Multi-level wildcard (#)
        assert!(topic_matches_filter("a/b/c", "a/#"));
        assert!(topic_matches_filter("a/b/c", "a/b/#"));
        assert!(topic_matches_filter("a/b/c", "#"));
        assert!(topic_matches_filter("a", "#"));
        assert!(!topic_matches_filter("a/b/c", "a/b/c/#"));

        // Combined wildcards
        assert!(topic_matches_filter("a/b/c/d", "a/+/c/#"));
        assert!(topic_matches_filter("a/x/c/anything", "a/+/c/#"));
        assert!(!topic_matches_filter("a/x/c", "a/+/c/#")); // # needs at least one more level
    }

    #[test]
    fn test_apply_forward_mapping() {
        let rule = ForwardRule {
            local_filter: "d/#".to_string(),
            remote_prefix: "a/node_123".to_string(),
            qos: 1,
        };

        assert_eq!(apply_forward_mapping("d/foo", &rule), "a/node_123/d/foo");
        assert_eq!(
            apply_forward_mapping("d/bar/baz", &rule),
            "a/node_123/d/bar/baz"
        );

        // No prefix
        let rule_no_prefix = ForwardRule {
            local_filter: "raw/#".to_string(),
            remote_prefix: String::new(),
            qos: 1,
        };

        assert_eq!(
            apply_forward_mapping("raw/data", &rule_no_prefix),
            "raw/data"
        );
    }

    #[test]
    fn test_apply_subscribe_mapping() {
        let rule = SubscribeRule {
            remote_filter: "a/node_123/u/#".to_string(),
            remote_prefix: "a/node_123".to_string(),
            qos: 1,
        };

        assert_eq!(
            apply_subscribe_mapping("a/node_123/u/foo", &rule).unwrap(),
            "u/foo"
        );
        assert_eq!(
            apply_subscribe_mapping("a/node_123/u/bar/baz", &rule).unwrap(),
            "u/bar/baz"
        );

        // No prefix
        let rule_no_prefix = SubscribeRule {
            remote_filter: "commands/#".to_string(),
            remote_prefix: String::new(),
            qos: 1,
        };

        assert_eq!(
            apply_subscribe_mapping("commands/restart", &rule_no_prefix).unwrap(),
            "commands/restart"
        );
    }
}
