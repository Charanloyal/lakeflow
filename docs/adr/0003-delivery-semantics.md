# ADR-0003: End-to-end delivery and deduplication semantics

**Status**: Accepted
**Date**: 2026-09-25

## Context
v1 called itself exactly-once without qualification, yet it appended every micro-batch, so replays duplicated
rows. The individual components give different guarantees. This ADR defines precisely what LakeFlow guarantees
at each boundary, which identifiers make that verifiable, and what happens under each failure.

## Decision
**Guarantee statement:** delivery is **at-least-once** from PostgreSQL to Spark. The *effect* on the lakehouse
is **idempotent**: after any combination of redelivery, crash, or replay, the silver tables equal the source
state as of the latest applied LSN, bronze holds one row per change event, and the DLQ holds one row per rejected
record. This is an effectively-once *outcome*, not exactly-once *delivery*. Nothing claims otherwise.

| Boundary | Guarantee | Mechanism / identifier |
|---|---|---|
| PostgreSQL → Debezium | At-least-once, in commit order | Logical slot `lakeflow_cdc` (pgoutput). Debezium commits the source offset (LSN) every 5 s (`OFFSET_FLUSH_INTERVAL_MS`). After a crash it resumes from the last flushed LSN and re-emits later changes. |
| Debezium → Kafka | At-least-once | Producer `acks=all`. A connector restart can duplicate records. They are identical except for Debezium's processing `ts_ms`. |
| Kafka → Spark | Replayable, ordered per key | Offsets are stored in the Spark checkpoint (`offsets/N` is written before batch N runs, `commits/N` after it succeeds). No consumer-group commits. |
| Spark → Iceberg | Idempotent writes (effectively once) | See the rules below. Batch N is re-executed if the process dies before `commits/N` exists. |
| Iceberg → Trino | Snapshot isolation | Readers only see committed snapshots, never partial batches. |

**Event identity:** `event_id = sha256(table | pk | source.lsn | source.txId | op | sha256(before, after))`.
It excludes Kafka coordinates and connector processing time, so a redelivered event has the same id, and two
different changes cannot share one.

**Sink rules** (the executable spec is `libs/lakeflow_core/semantics.py`, checked against Spark in
`spark/tests/test_sink.py`):
1. Duplicates in the same batch keep the first occurrence by (topic, partition, offset).
2. `event_id` already in bronze from an earlier batch → `duplicate`: skipped everywhere.
3. Per primary key, only the batch-latest (highest LSN) event is merged. Earlier ones are `superseded`.
4. Silver MERGE applies only if `s._source_lsn > t._source_lsn` (the **LSN guard**). Deletes are tombstones
   that keep their LSN, so an out-of-order older update is `stale` and cannot resurrect a row.
5. Bronze appends only `event_id`s it does not yet have. The check is recomputed on every commit retry.
6. The DLQ is keyed by Kafka coordinates (insert-if-absent). Replays update a record only if their replay-topic
   position is newer.
7. `ops.batch_commits` is upserted by (pipeline, stream_epoch, batch_id), and `attempts` counts replays. That
   makes recovery visible instead of silent.
8. Labels stay the same on replay: `_prev_source_lsn` restores the pre-batch baseline if silver was already
   updated by the crashed attempt.

**Late data:** the watermark is max(source_ts) − 5 min over committed batches. Late events are **never dropped**:
they are flagged `is_late` and go through the same LSN guard. There is no streaming aggregation in the core
path, so no state depends on the watermark.

**Failure matrix** (tests in brackets):

| Failure | Outcome |
|---|---|
| Spark killed mid-batch (before Iceberg commit) | Batch re-runs from `offsets/N`. No partial data because Iceberg commits are atomic. [`e2e: crash_now`] |
| Spark killed after Iceberg commit, before `commits/N` | Batch re-runs. Every write is a no-op, and `attempts=2` is recorded. [`test_replayed_batch_after_crash_is_idempotent`, `e2e: crash_after_commit`] |
| Debezium restart | Changes after the last flushed LSN are re-emitted and counted as `duplicate`. [`e2e: connector restart`] |
| Duplicate / out-of-order / late record injected | `duplicate` / `stale` / `is_late`. Silver unchanged. [`test_duplicates_are_stored_once`, `test_out_of_order_event_is_stale`] |
| Malformed or contract-violating record | DLQ with a reason code. The stream continues (poison-pill safe decoder). [`test_invalid_records_go_to_dlq...`] |
| Kafka retention exceeded before processing | `failOnDataLoss=true` stops the query loudly. Recover with the backfill DAG (incremental snapshot). |
| Replication slot lag | `max_slot_wal_keep_size=2GB` protects the source disk. The heartbeat keeps the slot advancing while tables are idle. Lag is exported as a metric. |

## Consequences
- Correctness needs a strictly increasing per-row source ordering key. PostgreSQL LSNs provide it, because row
  locks serialize changes to a row. Other sources need an equivalent key, such as a version column.
- Each batch reads the bronze partitions and silver keys it touches. That cost grows with table size and is
  bounded by the bronze day partitioning, the event_id bloom filter, and silver bucketing.
- A wiped checkpoint replays Kafka from `earliest`. That is safe because of the rules above, but labels are
  computed against the new `stream_epoch`.
- Kafka Connect exactly-once source support (KIP-618) could cut duplicates at the source. It is not needed for
  correctness here and is left as a production option.
