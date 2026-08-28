#!/bin/sh
# Generate a local self-signed TLS certificate for the secure profile.
# This is NOT a publicly trusted certificate.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/secrets/tls}"
mkdir -p "$OUT"
chmod 700 "$OUT" 2>/dev/null || true

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate a local TLS certificate" >&2
  exit 1
fi

openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout "$OUT/server.key" \
  -out "$OUT/server.crt" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

chmod 600 "$OUT/server.key" 2>/dev/null || true
chmod 644 "$OUT/server.crt" 2>/dev/null || true
echo "TLS_CERT_FILE $OUT/server.crt"
echo "TLS_KEY_FILE $OUT/server.key"
echo "Self-signed certificate for local acceptance only. Do not print the private key."
