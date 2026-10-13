#!/usr/bin/env bash
# Idempotently create the Iceberg warehouse bucket and verify it can be read (one-shot, like kafka-init).
# Bitnami's MINIO_DEFAULT_BUCKETS only creates it as a start-up side effect that the MinIO health check does not
# cover. When that is skipped, the catalog later fails with NoSuchBucket and Spark crash-loops, so this makes the
# bucket an explicit, checked dependency of the REST catalog.
set -euo pipefail
MC="$(command -v mc || echo /opt/bitnami/minio-client/bin/mc)"
BUCKET="${WAREHOUSE_BUCKET:-warehouse}"
export MC_CONFIG_DIR=/tmp/.mc
# Generated secrets are URL-safe (secrets.token_urlsafe), so they can be embedded in the alias URL.
export MC_HOST_lakeflow="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@${MINIO_ENDPOINT:-minio:9000}"

for attempt in $(seq 1 30); do
  if "$MC" mb --ignore-existing "lakeflow/$BUCKET" >/dev/null 2>&1 && "$MC" ls "lakeflow/$BUCKET" >/dev/null 2>&1; then
    echo "bucket $BUCKET is ready"
    exit 0
  fi
  echo "waiting for MinIO to accept bucket operations ($attempt/30)"
  sleep 2
done

echo "FAILED: could not create or read bucket $BUCKET; the last error follows" >&2
"$MC" mb --ignore-existing "lakeflow/$BUCKET"
exit 1
