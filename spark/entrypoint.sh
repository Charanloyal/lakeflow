#!/usr/bin/env bash
set -euo pipefail
exec /opt/spark/bin/spark-submit \
  --master "local[${SPARK_LOCAL_CORES:-2}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.ui.port=4040 \
  --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" \
  /opt/lakeflow/jobs/cdc_stream.py
