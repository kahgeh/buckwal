#!/usr/bin/env bash
# Test TLS material for the RustFS harness. Usage: tls-material.sh DIR   (DIR must not exist)
# Creates in DIR: ca.pem, other-ca.pem, server/rustfs_cert.pem, server/rustfs_key.pem, encryption.key.
# CA private keys, CSRs and serials are deleted before exit. Nothing secret is printed.
set -euo pipefail

die() { echo "tls-material.sh: $*" >&2; exit 1; }

[ $# -eq 1 ] || die "usage: tls-material.sh DIR"
DIR="$1"
[ ! -e "$DIR" ] || die "$DIR already exists"
command -v openssl >/dev/null || die "openssl not found"

umask 077
mkdir -p "$DIR/server"
chmod 0700 "$DIR"
cd "$DIR"
LOG="$(mktemp)"
trap 'rm -f ca.key other-ca.key server.csr ca.srl "$LOG"' EXIT
# openssl chatter is hidden; shown only on failure (it never contains key material).
openssl() { command openssl "$@" >"$LOG" 2>&1 || { cat "$LOG" >&2; die "openssl $1 failed"; }; }

make_ca() { # NAME CN
  openssl ecparam -name prime256v1 -genkey -noout -out "$1.key"
  openssl req -x509 -new -key "$1.key" -sha256 -days 1 -subj "/CN=$2" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$1.pem"
}
make_ca ca "buckwal test CA"
make_ca other-ca "buckwal unrelated CA"

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out server/rustfs_key.pem
openssl req -new -key server/rustfs_key.pem -subj /CN=localhost -out server.csr
EXT="$(printf 'subjectAltName=DNS:localhost,DNS:host.containers.internal,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n')"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -sha256 \
  -extfile <(printf '%s\n' "$EXT") -out server/rustfs_cert.pem

command openssl rand -hex 32 > encryption.key

chmod 0644 ca.pem other-ca.pem
chmod 0755 server
chmod 0644 server/rustfs_cert.pem server/rustfs_key.pem
chmod 0600 encryption.key
echo "TLS material in $DIR"
