# ADR-0001: Kafka topic partitioning, keys and retention

**Status**: Accepted
**Date**: 2026-09-25

## Context
Debezium writes one change event per row change. Downstream correctness depends on processing every change to a
row in commit order. Kafka only orders records within a partition. The local stack runs a single KRaft broker,
but the topic layout should carry over unchanged to a multi-broker cluster.

## Decision
- **Key = source primary key.** Debezium's key is the JSON primary key (`{"order_id": ...}`). The default
  partitioner hashes it with murmur2, so every change to a row lands in the same partition in WAL commit order.
- **3 partitions per CDC topic** (`platform/kafka/topics.conf`). That is enough for the local Spark `local[2]`
  consumer and small enough to keep the file count low. Production sizing is `partitions >= peak MB/s ÷
  per-partition consumer MB/s`, rounded up to allow for consumer parallelism.
- **Topics as code.** Broker auto-creation is disabled. `kafka-init` creates topics idempotently and **fails** if a
  topic's partition count differs from the file, because silently adding partitions remaps keys and breaks
  per-key ordering for in-flight rows.
- **Retention 7 days, `cleanup.policy=delete`.** Kafka is the replay buffer, not the system of record. Anything
  older is recovered from bronze (Iceberg) or with a Debezium incremental snapshot (the backfill DAG).
  Compaction was rejected: it discards intermediate changes that bronze and the event trace need.
- **Tombstones on.** Debezium emits a null-value record after each delete, which keeps the topics usable by
  compacting consumers. The pipeline counts tombstones and ignores them, because the `op=d` event already
  carries the delete.
- **Separate replay topic** (`lakeflow.replay.cdc`). DLQ replays never rewrite history on the source topics. The
  original topic travels in the `lakeflow-original-topic` header.
- Ordering is **not** required across tables. Referential-integrity checks allow a grace period (ADR-0004).

## Consequences
- Throughput per table is bounded by per-partition consumer speed times 3. Raising it requires a planned
  repartition: create a new topic with more partitions and switch the connector's topic routing, preferably at
  a quiet period.
- A single hot key cannot be parallelized. That is acceptable for order-level CDC.
- Local durability is single-broker (`replication.factor=1`, `min.insync.replicas=1`). Production uses RF=3,
  `min.insync.replicas=2`, and `acks=all` (the Kafka Connect default).
