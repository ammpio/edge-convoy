#!/bin/bash
# Generate self-signed CA and server certificates for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../certs"

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "Generating CA certificate..."
# Generate CA private key
openssl genrsa -out ca.key 2048

# Generate CA certificate
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt -subj "/C=US/ST=Test/L=Test/O=Convoy Test/CN=Test CA"

echo "Generating server certificate..."
# Generate server private key
openssl genrsa -out server.key 2048

# Generate server certificate signing request
openssl req -new -key server.key -out server.csr -subj "/C=US/ST=Test/L=Test/O=Convoy Test/CN=localhost"

# Create extensions file for SAN
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = remote-broker
IP.1 = 127.0.0.1
EOF

# Sign the server certificate with CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -extfile server.ext

# Clean up
rm -f server.csr server.ext ca.srl

# Set permissions
chmod 644 ca.crt server.crt
chmod 600 ca.key server.key

echo "Certificates generated successfully:"
echo "  CA cert: $CERT_DIR/ca.crt"
echo "  Server cert: $CERT_DIR/server.crt"
echo "  Server key: $CERT_DIR/server.key"
