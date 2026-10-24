#!/usr/bin/env bash
# Local RustFS harness for buckwal. Usage: rustfs.sh up|down|reset|stop|start|pause|unpause|env
#   reset: down, then up: the volume is recreated, so the bucket is empty.
# Several containers: BUCKWAL_RUSTFS_NAME, BUCKWAL_RUSTFS_VOLUME and BUCKWAL_RUSTFS_PORT name a
#   container, its volume and its host port (defaults: buckwal-rustfs, buckwal-objects, 9290).
#   A name or volume that does not start with `buckwal-` is refused, so no other container or
#   volume can be touched. `scripts/test.sh` runs four of them at once.
# TLS mode: set BUCKWAL_TLS_DIR to a directory made by scripts/tls-material.sh. RustFS then
#   serves HTTPS only on the same port, with that directory's server/ certificate; curl trusts
#   its ca.pem, and `env` also prints BUCKWAL_CA_BUNDLE. A container is plain or TLS for its
#   whole life: switch modes with `reset`.
set -euo pipefail

IMAGE="docker.io/rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff"
NAME="${BUCKWAL_RUSTFS_NAME:-buckwal-rustfs}"
VOLUME="${BUCKWAL_RUSTFS_VOLUME:-buckwal-objects}"
PORT="${BUCKWAL_RUSTFS_PORT:-9290}"
BUCKET="buckwal"
REGION="us-east-1"
AK="buckwal-admin"
SK="buckwal-local-secret-not-real"
TLS_DIR="${BUCKWAL_TLS_DIR:-}"
CURL_TLS=()
if [ -n "$TLS_DIR" ]; then
  ENDPOINT="https://localhost:$PORT"
  CURL_TLS=(--cacert "$TLS_DIR/ca.pem")
  WANT_MODE=TLS
else
  ENDPOINT="http://127.0.0.1:$PORT"
  WANT_MODE=plain
fi

die() { echo "rustfs.sh: $*" >&2; exit 1; }

case "$NAME" in buckwal-?*) ;; *) die "BUCKWAL_RUSTFS_NAME must start with buckwal-: $NAME" ;; esac
case "$VOLUME" in buckwal-?*) ;; *) die "BUCKWAL_RUSTFS_VOLUME must start with buckwal-: $VOLUME" ;; esac
case "$PORT" in ''|*[!0-9]*) die "BUCKWAL_RUSTFS_PORT must be a number: $PORT" ;; esac

print_env() {
  echo "export BUCKWAL_ENDPOINT=$ENDPOINT BUCKWAL_REGION=$REGION BUCKWAL_PATH_STYLE=true BUCKWAL_BUCKET=$BUCKET BUCKWAL_ACCESS_KEY=$AK BUCKWAL_SECRET_KEY=$SK"
  if [ -n "$TLS_DIR" ]; then echo "export BUCKWAL_CA_BUNDLE=$TLS_DIR/ca.pem"; fi
}

ensure_machine() {
  if ! podman info >/dev/null 2>&1; then
    echo "starting podman machine..." >&2
    podman machine start >&2
  fi
}

container_state() { podman inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null || true; }

# The mode an existing container was created in, from its RUSTFS_TLS_PATH.
container_mode() {
  local env_lines  # captured first: grep -q closing the pipe early fails the pipeline under pipefail
  env_lines="$(podman inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$NAME" 2>/dev/null || true)"
  if grep -q '^RUSTFS_TLS_PATH=' <<<"$env_lines"; then
    echo TLS
  else
    echo plain
  fi
}

check_mode() {
  local have; have="$(container_mode)"
  [ "$have" = "$WANT_MODE" ] || die "$NAME runs in $have mode; run rustfs.sh reset"
}

port_in_use() { lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; }

signed() { # METHOD URL -> http code
  curl -sS -o /dev/null -w '%{http_code}' -X "$1" --max-time 10 ${CURL_TLS[@]+"${CURL_TLS[@]}"} \
    --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$2"
}

wait_ready() {
  local i code
  for i in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 ${CURL_TLS[@]+"${CURL_TLS[@]}"} "$ENDPOINT/health" 2>/dev/null || true)"
    [ "$code" = 200 ] && return 0
    code="$(signed GET "$ENDPOINT/" 2>/dev/null || true)"
    [ "$code" = 200 ] && return 0
    sleep 1
  done
  die "RustFS not ready after 60s (podman logs $NAME)"
}

make_bucket() {
  local code i
  for i in $(seq 1 30); do
    code="$(signed PUT "$ENDPOINT/$BUCKET")"
    case "$code" in
      200|409) return 0 ;;
      5??) sleep 1 ;;  # backend still initialising
      *) die "create bucket failed: HTTP $code" ;;
    esac
  done
  die "create bucket failed: HTTP $code after retries"
}

cmd_up() {
  ensure_machine
  local state; state="$(container_state)"
  [ -z "$state" ] || check_mode
  case "$state" in
    running) ;;
    "")
      if [ -n "$TLS_DIR" ]; then
        [ -r "$TLS_DIR/server/rustfs_cert.pem" ] && [ -r "$TLS_DIR/ca.pem" ] \
          || die "no TLS material in $TLS_DIR (scripts/tls-material.sh DIR)"
      fi
      port_in_use && die "port $PORT is held by something other than $NAME; free it first"
      podman volume exists "$VOLUME" || podman volume create "$VOLUME" >/dev/null
      local tls_args=()
      if [ -n "$TLS_DIR" ]; then
        tls_args=(-v "$TLS_DIR/server":/certs:ro -e RUSTFS_TLS_PATH=/certs)
      fi
      podman run -d --name "$NAME" \
        -v "$VOLUME":/data:U ${tls_args[@]+"${tls_args[@]}"} \
        -e RUSTFS_ACCESS_KEY="$AK" -e RUSTFS_SECRET_KEY="$SK" \
        -e RUSTFS_VOLUMES=/data -e RUSTFS_ADDRESS=0.0.0.0:9000 \
        -e RUSTFS_CONSOLE_ENABLE=false \
        -p "127.0.0.1:$PORT:9000" "$IMAGE" >/dev/null
      ;;
    paused) podman unpause "$NAME" >/dev/null ;;
    *)
      port_in_use && die "port $PORT is held by something other than $NAME; free it first"
      podman start "$NAME" >/dev/null
      ;;
  esac
  wait_ready
  make_bucket
  echo "RustFS up at $ENDPOINT bucket=$BUCKET" >&2
}

cmd_down() {
  ensure_machine
  podman rm -f "$NAME" >/dev/null 2>&1 || true
  podman volume rm -f "$VOLUME" >/dev/null 2>&1 || true
  ! podman volume exists "$VOLUME" || die "volume $VOLUME is still there after removal"
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  reset) cmd_down; cmd_up ;;
  stop) podman stop "$NAME" >/dev/null ;;
  start) check_mode; podman start "$NAME" >/dev/null; wait_ready ;;
  pause) podman pause "$NAME" >/dev/null ;;
  unpause) check_mode; podman unpause "$NAME" >/dev/null ;;
  env) print_env ;;
  *) die "usage: rustfs.sh up|down|reset|stop|start|pause|unpause|env" ;;
esac
