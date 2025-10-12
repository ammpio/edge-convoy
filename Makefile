.PHONY: help build test clean test-setup test-teardown test-start test-stop test-logs integration-test

help:
	@echo "Convoy Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build              - Build the project"
	@echo "  test               - Run unit tests"
	@echo "  test-setup         - Generate certs and password file for integration tests"
	@echo "  test-start         - Start test MQTT brokers (requires test-setup)"
	@echo "  test-stop          - Stop test MQTT brokers"
	@echo "  test-teardown      - Stop brokers and clean up test artifacts"
	@echo "  test-logs          - Show logs from test brokers"
	@echo "  integration-test   - Run integration tests (requires test-start)"
	@echo "  clean              - Clean build artifacts"

build:
	cargo build --release --features cli

test:
	cargo test -- --test-threads=1

test-setup:
	@echo "Setting up integration test environment..."
	@./test-harness/scripts/generate-certs.sh
	@./test-harness/scripts/generate-passwd.sh
	@echo "Test environment setup complete!"

test-start: test-setup
	@echo "Starting MQTT brokers..."
	@cd test-harness && docker-compose up -d
	@echo "Waiting for brokers to be healthy..."
	@sleep 3
	@cd test-harness && docker-compose ps
	@echo "MQTT brokers are running!"
	@echo ""
	@echo "Local broker:  localhost:1883 (no auth)"
	@echo "Remote broker: localhost:8883 (TLS + auth)"
	@echo "  Username: testuser"
	@echo "  Password: testpass"

test-stop:
	@echo "Stopping MQTT brokers..."
	@cd test-harness && docker-compose stop

test-restart-remote:
	@echo "Restarting remote broker..."
	@cd test-harness && docker-compose restart remote-broker
	@sleep 2

test-teardown:
	@echo "Tearing down test environment..."
	@cd test-harness && docker-compose down -v
	@rm -f test-harness/passwd
	@echo "Test environment cleaned up!"

test-logs:
	@cd test-harness && docker-compose logs -f

integration-test: test-start
	@echo "Running integration tests..."
	@cargo test --test integration_test -- --test-threads=1 --nocapture
	@echo "Integration tests complete!"

clean:
	cargo clean
