// Utility functions

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Generate a short hash of message payload for logging purposes.
///
/// This creates an 8-character hexadecimal hash (32 bits) that can be used
/// to identify and track messages through the system. The same payload will
/// always produce the same hash.
///
/// # Examples
///
/// ```
/// use convoy::util::payload_hash;
///
/// let hash1 = payload_hash(b"test message");
/// let hash2 = payload_hash(b"test message");
/// assert_eq!(hash1, hash2); // Same payload = same hash
/// ```
pub fn payload_hash(payload: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    let hash = hasher.finish();
    // Use first 8 hex digits (32 bits)
    format!("{:08x}", (hash >> 32) as u32)
}
