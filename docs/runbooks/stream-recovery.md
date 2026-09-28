# Runbook: streaming query down, stalled or lagging

Alerts: `LakeFlowStreamDown`, `LakeFlowStreamStalled`, `LakeFlowConsumerLagHigh`, `LakeFlowFreshnessSlaBreached`,
`LakeFlowReplicationSlotLagHigh`, `LakeFlowDebeziumHeartbeatStale`.

## 1. Locate the broken hop

| Check | Command | Healthy |
|---|---|---|
| Container state | `make status` | every service `running (healthy)`; `kafka-init`/`connect-init` `exited (0)` |
| Connector | `curl -s localhost:8083/connectors/lakeflow-cdc/status` | connector and task `RUNNING` |
| Spark query | `curl -s localhost:9108/metrics \| grep lakeflow_stream_query_active` | `1.0` |
| Lag per topic | Overview page, or `lakeflow_consumer_lag_messages` in Prometheus | falls back toward 0 after bursts |
| Slot WAL retained | `lakeflow_replication_slot_lag_bytes` | stays small, drops after each heartbeat |

## 2. Spark stopped or crash-looping

1. `make logs SERVICE=spark`. The Python traceback names the failing stage (decode, silver MERGE, bronze append,
   DLQ MERGE, batch record).
2. Restart with `docker compose restart spark`. The query resumes from the checkpoint in the `spark-checkpoints`
   volume. The last micro-batch is replayed if it did not commit its offsets, and ADR-0003 explains why that replay
   is idempotent.
3. Verify with the Recovery Lab history (`attempts > 1` on the replayed batch) and the quality checks
   `bronze.event_id_unique` and `orders.primary_key_unique`. Both must stay at 0.
4. Never delete the checkpoint to "unstick" the stream. That resets Kafka offsets and forces a full re-read. Bronze
   dedup absorbs it, but freshness and cost suffer. If the checkpoint really is corrupt, stop Spark and move the
   directory aside. On the next start the job sees no query metadata and generates a new `stream-epoch`, so the
   reused batch ids cannot collide with the old ones (ADR-0003).

## 3. Connector failed or heartbeat stale

1. `curl -s localhost:8083/connectors/lakeflow-cdc/status | python -m json.tool` shows the task trace.
2. Restart only the task: `curl -X POST localhost:8083/connectors/lakeflow-cdc/tasks/0/restart`. Debezium resumes
   from its committed source offset (LSN). Changes made while it was down are not lost, because the replication
   slot retains the WAL.
3. If authentication fails, the `debezium` role password in `.env` does not match the database volume. Regenerating
   secrets needs `make clean`.

## 4. Replication slot lag growing

An inactive slot retains WAL forever and eventually fills the disk of the source database.

- If the connector is down, fix it first (section 3).
- If the connector is running but lag still grows, check that heartbeats happen:
  `SELECT ts FROM lakeflow_ops.debezium_heartbeat`. Heartbeats advance the slot even when the captured tables are
  idle.
- Last resort, with data loss for the lakehouse: drop the slot and re-snapshot. Document the incident and run a
  backfill (`make backfill CONTRACT=orders`) plus reconciliation afterwards.

## 5. Freshness SLA breached while the query is running

- Compare `lakeflow_stream_input_rows_per_second` with `processed_rows_per_second` on the Grafana dashboard. If
  processing is slower than input, the micro-batch is the bottleneck. Read the per-stage timings in
  `ops.batch_commits.stage_ms` (Pipeline page) to see which stage is slow.
- A high `lakeflow_stream_commit_retries_total` usually means compaction and the stream are competing. Move the
  maintenance window (runbook `maintenance.md`).
