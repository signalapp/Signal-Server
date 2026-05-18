#!/bin/sh
# Generates self-signed local testing certificates for OmnibusH2ServerTest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

PASSWORD="password"
DAYS=36500
OMNIBUS_KS="$SCRIPT_DIR/omnibus-h2-server-test-keystore.p12"

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$WORK_DIR/foo-rsa.key" 2>/dev/null;
openssl req -new -x509 -key  "$WORK_DIR/foo-rsa.key" -out  "$WORK_DIR/foo-rsa.crt" -days "$DAYS" -subj "/CN=foo.example.com" -addext "subjectAltName=DNS:foo.example.com"
openssl pkcs12 -export -in "$WORK_DIR/foo-rsa.crt" -inkey "$WORK_DIR/foo-rsa.key" -out "$WORK_DIR/foo-rsa.p12" -name foo -passout pass:$PASSWORD
keytool -importkeystore -noprompt -srckeystore "$WORK_DIR/foo-rsa.p12" -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore "$OMNIBUS_KS" -deststoretype PKCS12 -deststorepass $PASSWORD

echo "Wrote keystore to $OMNIBUS_KS"
