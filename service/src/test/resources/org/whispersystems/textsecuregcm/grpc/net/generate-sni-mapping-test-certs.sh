#!/bin/sh
# Generates self-signed local testing certificates for SniMapperTest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

PASSWORD="password"
DAYS=36500
SNI_KS="$SCRIPT_DIR/sni-mapper-test-keystore.p12" 

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$WORK_DIR/foo-rsa.key" 2>/dev/null;
openssl req -new -x509 -key  "$WORK_DIR/foo-rsa.key" -out  "$WORK_DIR/foo-rsa.crt" -days "$DAYS" -subj "/CN=foo.example.com" -addext "subjectAltName=DNS:foo.example.com"

openssl genpkey -algorithm Ed25519 -out "$WORK_DIR/foo-ed25519.key" 2>/dev/null;
openssl req -new -x509 -key  "$WORK_DIR/foo-ed25519.key" -out  "$WORK_DIR/foo-ed25519.crt" -days "$DAYS" -subj "/CN=foo.example.com" -addext "subjectAltName=DNS:foo.example.com"

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$WORK_DIR/bar-rsa.key" 2>/dev/null;
openssl req -new -x509 -key  "$WORK_DIR/bar-rsa.key" -out  "$WORK_DIR/bar-rsa.crt" -days "$DAYS" -subj "/CN=bar.example.com" -addext "subjectAltName=DNS:bar.example.com"

openssl genpkey -algorithm Ed25519 -out "$WORK_DIR/bar-ed25519.key" 2>/dev/null;
openssl req -new -x509 -key  "$WORK_DIR/bar-ed25519.key" -out  "$WORK_DIR/bar-ed25519.crt" -days "$DAYS" -subj "/CN=BAR.EXAMPLE.COM" -addext "subjectAltName=DNS:BAR.EXAMPLE.COM"

openssl pkcs12 -export -in "$WORK_DIR/foo-rsa.crt" -inkey "$WORK_DIR/foo-rsa.key" -out "$WORK_DIR/foo-rsa.p12" -name foo -passout pass:$PASSWORD
openssl pkcs12 -export -in "$WORK_DIR/foo-ed25519.crt" -inkey "$WORK_DIR/foo-ed25519.key" -out "$WORK_DIR/foo-ed25519.p12" -name foo-ed25519 -passout pass:$PASSWORD
openssl pkcs12 -export -in "$WORK_DIR/bar-rsa.crt" -inkey "$WORK_DIR/bar-rsa.key" -out "$WORK_DIR/bar-rsa.p12" -name bar -passout pass:$PASSWORD
openssl pkcs12 -export -in "$WORK_DIR/bar-ed25519.crt" -inkey "$WORK_DIR/bar-ed25519.key" -out "$WORK_DIR/bar-ed25519.p12" -name bar-ed25519 -passout pass:$PASSWORD

keytool -importkeystore -noprompt -srckeystore "$WORK_DIR/foo-ed25519.p12" -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore "$SNI_KS" -deststoretype PKCS12 -deststorepass $PASSWORD
keytool -importkeystore -noprompt -srckeystore "$WORK_DIR/foo-rsa.p12" -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore "$SNI_KS" -deststoretype PKCS12 -deststorepass $PASSWORD
keytool -importkeystore -noprompt -srckeystore "$WORK_DIR/bar-ed25519.p12" -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore "$SNI_KS" -deststoretype PKCS12 -deststorepass $PASSWORD
keytool -importkeystore -noprompt -srckeystore "$WORK_DIR/bar-rsa.p12" -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore "$SNI_KS" -deststoretype PKCS12 -deststorepass $PASSWORD

echo "Wrote 4 certificates for two SNIs to $SNI_KS"
