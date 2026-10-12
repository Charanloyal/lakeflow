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

   > **Warning.** Moving the checkpoint aside is safe only while Kafka still holds every offset the old
   > checkpoint had not committed. If the stream failed with a `failOnDataLoss` error (offsets already expired),
   > a new checkpoint starts at the *earliest retained* offset. That turns a loud failure into **silent loss**.
   > Follow section 6 instead.

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
  backfill (`make backfill CONTRACT=orders`) plus reconciliation afterwards. A snapshot only re-reads rows that
  still exist. **Rows deleted while the slot was gone are not repaired.** They stay live in silver and show up as
  `*.source_reconciliation` mismatches (section 6).
- PostgreSQL invalidates the slot by itself once it retains more than `max_slot_wal_keep_size` (2 GB in
  `docker-compose.yml`). The connector then fails, and the same last-resort path applies.

## 5. Freshness SLA breached while the query is running

- Compare `lakeflow_stream_input_rows_per_second` with `processed_rows_per_second` on the Grafana dashboard. If
  processing is slower than input, the micro-batch is the bottleneck. Read the per-stage timings in
  `ops.batch_commits.stage_ms` (Pipeline page) to see which stage is slow.
- A high `lakeflow_stream_commit_retries_total` usually means compaction and the stream are competing. Move the
  maintenance window (runbook `maintenance.md`).

## 6. What a restart can recover, and what needs a rebuild

Restarting a component is safe whenever its resume position still exists upstream. When it does not, recovery is
a re-snapshot or a rebuild, never an offset reset.

| Situation | Symptom | Restart recovers it? | Supported repair | Evidence afterwards |
|---|---|---|---|---|
| Spark crash or restart | `LakeFlowStreamDown`, then the query resumes | **Yes.** It replays from `offsets/N` | `docker compose restart spark` | `attempts > 1` on the replayed batch; `bronze.event_id_unique` = 0 |
| Debezium task restart | heartbeat stale, then recovers | **Yes**, while the slot exists | restart the task (section 3) | duplicates counted, none stored |
| Spark down longer than Kafka retention (7 d for CDC topics) | query fails with a `failOnDataLoss` error | **No.** The offsets expired | Keep the checkpoint. Re-snapshot the affected tables (backfill, **experimental**) or rebuild (below) | reconciliation = 0 |
| Checkpoint lost, Kafka still holds every unprocessed offset | new `stream-epoch` in the logs | **Yes**, by re-reading from earliest. Labels use the new epoch | none | duplicates counted, none stored |
| Replication slot dropped or invalidated (`max_slot_wal_keep_size`) | connector fails: slot missing or invalidated | **No** | New slot plus snapshot (section 4). Fix deleted rows by hand or rebuild | reconciliation = 0 |
| Source database recreated or restored to another history (LSNs restart lower) | new changes stored as `stale`, silver stops changing | **No, and it is silent** | **Unsupported.** Stop the stream and rebuild the lakehouse | reconciliation = 0 after the rebuild |
| Iceberg snapshot expired | time travel (`FOR VERSION AS OF`) older than 1 h fails | n/a (current state unaffected) | none; the retention window is a policy choice | — |

**Rebuild (destructive).** Stop Spark. Drop the silver, bronze and ops tables, or the whole warehouse. Move the
checkpoint aside. Recreate the connector with a fresh slot and `snapshot.mode=initial`. Then let the stream
re-ingest. This discards the bronze event history and the DLQ. `make clean` does all of this for the local stack
by deleting **every** volume, including the source database. Never run it on data you need.

Safe diagnostics, all read-only: `make status`, `make logs SERVICE=spark`, the connector status endpoint, the
quality checks (`*.source_reconciliation`, `*.primary_key_unique`) and `SELECT * FROM lakehouse.ops.batch_commits
ORDER BY committed_at DESC`.
