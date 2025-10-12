#!/bin/bash
# Generate Mosquitto password file for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/.."

USERNAME="testuser"
PASSWORD="testpass"

echo "Generating password file for user: $USERNAME"

# Use docker to generate the password file
docker run --rm -v "$TEST_DIR:/data" eclipse-mosquitto:2 \
  mosquitto_passwd -b -c /data/passwd "$USERNAME" "$PASSWORD"

echo "Password file generated: $TEST_DIR/passwd"
echo "  Username: $USERNAME"
echo "  Password: $PASSWORD"
