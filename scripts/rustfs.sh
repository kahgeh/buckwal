#!/usr/bin/env bash
# Local RustFS harness for turso_durable. Usage: rustfs.sh up|down|reset|stop|start|pause|unpause|env
#   reset: down, then up: the volume is recreated, so the bucket is empty.
set -euo pipefail

IMAGE="docker.io/rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff"
NAME="turso-durable-rustfs"
VOLUME="turso-durable-objects"
PORT=9290
BUCKET="turso-durable"
REGION="us-east-1"
AK="turso-durable-admin"
SK="turso-durable-local-secret-not-real"
ENDPOINT="http://127.0.0.1:$PORT"

die() { echo "rustfs.sh: $*" >&2; exit 1; }

print_env() {
  echo "export DURABLE_ENDPOINT=$ENDPOINT DURABLE_REGION=$REGION DURABLE_PATH_STYLE=true DURABLE_BUCKET=$BUCKET DURABLE_ACCESS_KEY=$AK DURABLE_SECRET_KEY=$SK"
}

ensure_machine() {
  if ! podman info >/dev/null 2>&1; then
    echo "starting podman machine..." >&2
    podman machine start >&2
  fi
}

container_state() { podman inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null || true; }

port_in_use() { lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; }

signed() { # METHOD URL -> http code
  curl -sS -o /dev/null -w '%{http_code}' -X "$1" --max-time 10 \
    --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$2"
}

wait_ready() {
  local i code
  for i in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$ENDPOINT/health" 2>/dev/null || true)"
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
  case "$(container_state)" in
    running) ;;
    "")
      port_in_use && die "port $PORT is held by something other than $NAME; free it first"
      podman volume exists "$VOLUME" || podman volume create "$VOLUME" >/dev/null
      podman run -d --name "$NAME" \
        -v "$VOLUME":/data:U \
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
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  reset) cmd_down; cmd_up ;;
  stop) podman stop "$NAME" >/dev/null ;;
  start) podman start "$NAME" >/dev/null; wait_ready ;;
  pause) podman pause "$NAME" >/dev/null ;;
  unpause) podman unpause "$NAME" >/dev/null ;;
  env) print_env ;;
  *) die "usage: rustfs.sh up|down|reset|stop|start|pause|unpause|env" ;;
esac
