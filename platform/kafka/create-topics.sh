#!/usr/bin/env bash
# Idempotently create topics from topics.conf. Broker auto-creation is disabled, so this file is the inventory.
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPICS_FILE="${TOPICS_FILE:-/config/topics.conf}"
KT=/opt/kafka/bin/kafka-topics.sh

for attempt in $(seq 1 60); do
  if "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then break; fi
  echo "waiting for Kafka at $BOOTSTRAP ($attempt/60)"; sleep 2
done

while read -r name partitions retention policy; do
  case "$name" in ''|'#'*) continue ;; esac
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$name" \
    --partitions "$partitions" --replication-factor 1 \
    --config retention.ms="$retention" --config cleanup.policy="$policy" --config min.insync.replicas=1
  current=$("$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" | awk -F'PartitionCount: ' 'NF>1{split($2,a," "); print a[1]; exit}')
  if [ "$current" != "$partitions" ]; then
    echo "FATAL: topic $name has $current partitions, topics.conf declares $partitions (changing the count breaks key ordering; see ADR-0001)" >&2
    exit 1
  fi
done < "$TOPICS_FILE"
echo "topics ready"
