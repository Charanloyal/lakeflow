# ADR-0002: Iceberg table layout, partitioning and file-size controls

**Status**: Accepted
**Date**: 2026-09-25

## Context
Streaming micro-batches commit every few seconds, which creates many small files and snapshots. Silver tables
need row-level upserts and deletes. Bronze is an append-only change log that is queried by time range and
looked up by `event_id` for deduplication.

## Decision
- **Catalog:** an Iceberg REST catalog, persisted in PostgreSQL. Spark and Trino share it, so both engines see
  the same snapshots. (v1 used a Hadoop catalog in Spark and an unconfigured REST catalog in Trino.)
- **bronze.cdc_events:** `PARTITIONED BY (days(source_ts), source_table)`. Each duplicate lookup reads only the
  days present in the batch, because a redelivered event has the same `source_ts` as the original. A Parquet
  bloom filter on `event_id` lets readers skip row groups.
- **silver.orders / silver.customers:** `PARTITIONED BY (bucket(4, <pk>))`, format v2, **merge-on-read** for
  MERGE/UPDATE/DELETE. Each micro-batch writes small delete files instead of rewriting data files, and
  compaction folds those delete files back in later.
- **Soft deletes.** A delete becomes a tombstone row (`is_deleted=true`) that keeps the latest `_source_lsn`.
  Hard deletes would let a replayed older update resurrect the row. Consumers filter `NOT is_deleted`.
- **Small-file controls:** `write.target-file-size-bytes=64MB`, `write.distribution-mode=hash` (one writer per
  partition per commit), metadata cleanup (`write.metadata.delete-after-commit.enabled`,
  `previous-versions-max=50`), and a scheduled Airflow compaction (`optimize`) plus snapshot expiry and
  orphan-file removal through Trino.
- **Partition evolution guidance:** for time-range queries on silver, add `days(updated_at)` with
  `ALTER TABLE ... ADD PARTITION FIELD` (metadata-only; old files keep their old spec). Size bucket counts so
  that each bucket holds at least 1 target file. With 4 buckets, you only need to evolve after about 256 MB per
  table.

## Consequences
- Reads pay the merge-on-read cost until compaction runs. The benchmark measures query latency before and
  after `optimize` on the same query.
- Trino maintenance and Spark MERGE can conflict under optimistic concurrency. The sink retries conflicts
  (`ValidationException`/`CommitFailedException`), and every write is idempotent (ADR-0003), so retries are safe.
- Four buckets is a local-scale choice. Changing it later is a partition evolution, not a rewrite.
