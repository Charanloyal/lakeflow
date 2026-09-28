#!/bin/sh
# Create-or-update the connector (PUT is idempotent) and wait until its task is RUNNING.
set -eu
CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
NAME="${CONNECTOR_NAME:-lakeflow-cdc}"
CONFIG="${CONNECTOR_CONFIG:-/config/lakeflow-cdc.json}"

i=0
until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 90 ] && { echo "FATAL: Kafka Connect not reachable at $CONNECT_URL" >&2; exit 1; }
  echo "waiting for Kafka Connect ($i/90)"; sleep 2
done

curl -fsS -X PUT -H 'Content-Type: application/json' --data "@$CONFIG" "$CONNECT_URL/connectors/$NAME/config" >/dev/null
echo "connector $NAME registered"

i=0
while :; do
  status=$(curl -fsS "$CONNECT_URL/connectors/$NAME/status" || true)
  case "$status" in
    *'"tasks":[{"id":0,"state":"RUNNING"'*) echo "connector $NAME is RUNNING"; exit 0 ;;
    *'"state":"FAILED"'*) echo "FATAL: connector failed: $status" >&2; exit 1 ;;
  esac
  i=$((i + 1)); [ "$i" -gt 60 ] && { echo "FATAL: connector not running after 120s: $status" >&2; exit 1; }
  sleep 2
done
