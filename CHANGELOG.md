# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-01-01

### Added
- Initial release of Convoy MQTT bridge
- Bidirectional MQTT bridging between local and remote brokers (MQTT v3.1.1)
- SQLite-backed message caching for local→remote messages
- Automatic cache replay in FIFO order when remote reconnects
- Topic mapping with wildcard support (`+`, `#`)
- TLS support with optional mTLS for remote connections
- Bridge state publishing with Last Will and Testament (LWT)
- Configurable cache eviction policies (drop oldest or reject new)
- Exponential backoff for connection retries
- Message timestamp tracking for replay delay debugging
- Comprehensive test suite with integration tests

[Unreleased]: https://github.com/yourusername/convoy/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourusername/convoy/releases/tag/v0.1.0

