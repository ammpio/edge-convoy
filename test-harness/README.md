# Test Harness

This directory contains the test infrastructure for Convoy integration tests, including Docker Compose setup for MQTT brokers, TLS certificates, and configuration files.

## Setup

The integration test environment includes:

- **Local Broker** (Mosquitto): Port 1883, no authentication, no TLS
- **Remote Broker** (Mosquitto):
  - Port 1884: No TLS, requires authentication
  - Port 8883: TLS, requires authentication

## Quick Start

```bash
# From the project root directory:

# 1. Setup (generate certs and password file)
make test-setup

# 2. Start brokers
make test-start

# 3. Run integration tests
make integration-test

# 4. Stop brokers
make test-stop

# 5. Clean up
make test-teardown
```

## Manual Setup

```bash
# Generate TLS certificates
./test-harness/scripts/generate-certs.sh

# Generate password file
./test-harness/scripts/generate-passwd.sh

# Start brokers
cd test-harness && docker-compose up -d

# Stop brokers
cd test-harness && docker-compose down
```

## Test Credentials

- **Username**: `testuser`
- **Password**: `testpass`

## Files

- `docker-compose.yml` - Docker Compose configuration
- `mosquitto-local.conf` - Local broker config (no auth/TLS)
- `mosquitto-remote.conf` - Remote broker config (TLS + auth)
- `certs/` - Generated TLS certificates (gitignored)
- `passwd` - Generated password file (gitignored)
- `scripts/generate-certs.sh` - Certificate generation script
- `scripts/generate-passwd.sh` - Password file generation script

## Integration Tests

The integration tests are located in `tests/integration_test.rs` and cover:

1. **Basic Forwarding (Local → Remote)**: Tests message forwarding from local to remote broker
2. **Subscribe Forwarding (Remote → Local)**: Tests message forwarding from remote to local broker
3. **Caching and Replay**: Tests that messages are cached when remote is down and replayed when it comes back up

**Note:** Integration tests must run sequentially (not in parallel) because they share the same Docker broker instances. The Makefile already configures this with `--test-threads=1`.

## Debugging

To view broker logs:
```bash
make test-logs
```

To run a specific test:
```bash
cargo test --test integration_test <test_name> -- --nocapture
```

## Notes

- The tests use LocalSet for spawning !Send futures (rumqttc EventLoop is !Send)
- The remote broker has both TLS (8883) and non-TLS (1884) listeners for flexibility

### TLS Testing on macOS

Tests that use TLS currently set `danger_accept_invalid_certs = true` due to a known limitation:
**native-tls on macOS doesn't honor custom CA certificates** added via `add_root_certificate()`.

This is because native-tls uses macOS Security.framework, which requires CAs to be in the system keychain.
The same self-signed certificates work fine with OpenSSL tools (e.g., `mosquitto_sub`, `openssl s_client`).

**Workaround for production use on macOS:**
1. Add your custom CA to the macOS system keychain, or
2. Use certificates signed by a system-trusted CA, or
3. Deploy on Linux where custom CAs work as expected
