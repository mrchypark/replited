#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE_DIR="$ROOT_DIR/example"
BIN_DIR="$EXAMPLE_DIR/bin"
RUNTIME_DIR="$EXAMPLE_DIR/runtime"
PRIMARY_DIR="$RUNTIME_DIR/primary"
REPLICA_DIR="$RUNTIME_DIR/replica"
LOG_DIR="$RUNTIME_DIR/logs"
PRIMARY_CONFIG="$RUNTIME_DIR/primary.toml"
REPLICA_CONFIG="$RUNTIME_DIR/replica.toml"
PRIMARY_PID_FILE="$RUNTIME_DIR/primary-replited.pid"
REPLICA_PID_FILE="$RUNTIME_DIR/replica-sidecar.pid"
PRIMARY_PB_PID_FILE="$RUNTIME_DIR/primary-pocketbase.pid"

PRIMARY_URL="${PRIMARY_URL:-http://127.0.0.1:8090}"
REPLICA_URL="${REPLICA_URL:-http://127.0.0.1:8091}"
PRIMARY_HTTP="${PRIMARY_HTTP:-127.0.0.1:8090}"
REPLICA_HTTP="${REPLICA_HTTP:-127.0.0.1:8091}"
ADMIN_EMAIL="${POCKETBASE_ADMIN_EMAIL:-test@example.com}"
ADMIN_PASS="${POCKETBASE_ADMIN_PASS:-password123456}"
POCKETBASE_VERSION="${POCKETBASE_VERSION:-0.36.9}"
POCKETBASE_BIN="${POCKETBASE_BIN:-$BIN_DIR/pocketbase}"
REPLITED_BIN="${REPLITED_BIN:-$ROOT_DIR/target/release/replited}"
DEMO_HOLD="${DEMO_HOLD:-1}"

cleanup() {
  for pid_file in "$PRIMARY_PID_FILE" "$REPLICA_PID_FILE" "$PRIMARY_PB_PID_FILE"; do
    if [[ -f "$pid_file" ]]; then
      kill "$(cat "$pid_file")" >/dev/null 2>&1 || true
      rm -f "$pid_file"
    fi
  done
}

trap cleanup EXIT INT TERM

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

detect_asset_name() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Darwin:arm64) echo "pocketbase_${POCKETBASE_VERSION}_darwin_arm64.zip" ;;
    Darwin:x86_64) echo "pocketbase_${POCKETBASE_VERSION}_darwin_amd64.zip" ;;
    Linux:arm64|Linux:aarch64) echo "pocketbase_${POCKETBASE_VERSION}_linux_arm64.zip" ;;
    Linux:x86_64) echo "pocketbase_${POCKETBASE_VERSION}_linux_amd64.zip" ;;
    *)
      echo "unsupported platform for auto-download: $os $arch" >&2
      exit 1
      ;;
  esac
}

ensure_pocketbase() {
  if [[ -x "$POCKETBASE_BIN" ]]; then
    return
  fi

  require_cmd curl
  require_cmd unzip
  require_cmd shasum
  mkdir -p "$BIN_DIR"

  local asset zip_path checksums_path expected_zip_hash actual_zip_hash
  asset="$(detect_asset_name)"
  zip_path="$BIN_DIR/$asset"
  checksums_path="$BIN_DIR/checksums.txt"

  echo "Downloading PocketBase $POCKETBASE_VERSION ($asset)..."
  curl -fsSL -o "$zip_path" "https://github.com/pocketbase/pocketbase/releases/download/v${POCKETBASE_VERSION}/${asset}"
  curl -fsSL -o "$checksums_path" "https://github.com/pocketbase/pocketbase/releases/download/v${POCKETBASE_VERSION}/checksums.txt"

  expected_zip_hash="$(awk -v name="$asset" '$2 == name { print $1 }' "$checksums_path")"
  actual_zip_hash="$(shasum -a 256 "$zip_path" | awk '{print $1}')"
  if [[ -z "$expected_zip_hash" || "$expected_zip_hash" != "$actual_zip_hash" ]]; then
    echo "PocketBase zip checksum mismatch for $asset" >&2
    exit 1
  fi

  unzip -o "$zip_path" -d "$BIN_DIR" >/dev/null
  chmod +x "$POCKETBASE_BIN"
}

wait_for_health() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 120); do
    if curl -fsS "$url/api/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name failed health check: $url" >&2
  return 1
}

wait_for_file() {
  local file="$1"
  local label="$2"
  for _ in $(seq 1 120); do
    if [[ -f "$file" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "$label never appeared: $file" >&2
  return 1
}

if [[ ! -x "$REPLITED_BIN" ]]; then
  echo "missing replited binary at $REPLITED_BIN" >&2
  echo "run: cargo build --release" >&2
  exit 1
fi

require_cmd python3
require_cmd curl
ensure_pocketbase

rm -rf "$RUNTIME_DIR"
mkdir -p "$PRIMARY_DIR" "$REPLICA_DIR" "$LOG_DIR"

PRIMARY_DB="$PRIMARY_DIR/data.db"
PRIMARY_CACHE="$PRIMARY_DIR/cache"
REPLICA_DB="$REPLICA_DIR/data.db"
REPLICA_CACHE="$REPLICA_DIR/cache"
mkdir -p "$PRIMARY_CACHE" "$REPLICA_CACHE"

sed \
  -e "s|__PRIMARY_LOG_DIR__|$LOG_DIR|g" \
  -e "s|__PRIMARY_DB__|$PRIMARY_DB|g" \
  -e "s|__PRIMARY_CACHE__|$PRIMARY_CACHE|g" \
  "$EXAMPLE_DIR/config/primary.host.toml" > "$PRIMARY_CONFIG"

sed \
  -e "s|__REPLICA_LOG_DIR__|$LOG_DIR|g" \
  -e "s|__REPLICA_DB__|$REPLICA_DB|g" \
  -e "s|__REPLICA_CACHE__|$REPLICA_CACHE|g" \
  -e "s|__PRIMARY_DB__|$PRIMARY_DB|g" \
  "$EXAMPLE_DIR/config/replica.host.toml" > "$REPLICA_CONFIG"

"$POCKETBASE_BIN" serve --http "$PRIMARY_HTTP" --dir "$PRIMARY_DIR" >"$LOG_DIR/primary-pocketbase.log" 2>&1 &
echo $! > "$PRIMARY_PB_PID_FILE"
wait_for_health "$PRIMARY_URL" "Primary PocketBase"

"$REPLITED_BIN" --config "$PRIMARY_CONFIG" replicate >"$LOG_DIR/primary-replited.log" 2>&1 &
echo $! > "$PRIMARY_PID_FILE"
sleep 2

"$POCKETBASE_BIN" superuser upsert "$ADMIN_EMAIL" "$ADMIN_PASS" --dir "$PRIMARY_DIR" >/dev/null

"$REPLITED_BIN" \
  --config "$REPLICA_CONFIG" \
  replica-sidecar \
  --force-restore \
  --exec "$POCKETBASE_BIN serve --http $REPLICA_HTTP --dir $REPLICA_DIR" \
  >"$LOG_DIR/replica-sidecar.log" 2>&1 &
echo $! > "$REPLICA_PID_FILE"

wait_for_file "$REPLICA_DB" "Replica DB"
wait_for_health "$REPLICA_URL" "Replica PocketBase"

PRIMARY_URL="$PRIMARY_URL" \
REPLICA_URL="$REPLICA_URL" \
POCKETBASE_ADMIN_EMAIL="$ADMIN_EMAIL" \
POCKETBASE_ADMIN_PASS="$ADMIN_PASS" \
python3 "$EXAMPLE_DIR/verify_pocketbase.py"

echo
echo "PocketBase live demo is running."
echo "Primary UI: $PRIMARY_URL/_/"
echo "Replica UI: $REPLICA_URL/_/"
echo "Primary PocketBase log: $LOG_DIR/primary-pocketbase.log"
echo "Primary replited log: $LOG_DIR/primary-replited.log"
echo "Replica sidecar log: $LOG_DIR/replica-sidecar.log"
echo

if [[ "$DEMO_HOLD" == "0" ]]; then
  exit 0
fi

echo "Processes remain up for inspection until this script exits."
echo "Press Ctrl+C when done."
while true; do
  sleep 60
done
