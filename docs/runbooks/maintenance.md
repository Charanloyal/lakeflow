# Runbook: Iceberg table maintenance and backfill

Code: `libs/lakeflow_core/maintenance.py` and `libs/lakeflow_core/backfill.py`. Airflow DAGs, `make maintenance`,
`make backfill` and the integration tests all call these same modules.

## Scheduled maintenance (`lakeflow_table_maintenance`, hourly)

For `bronze.cdc_events`, `silver.orders`, `silver.customers`, `ops.dlq_events` and `ops.batch_commits`:

1. `ALTER TABLE ... EXECUTE optimize(file_size_threshold => '64MB')` compacts small files. It also folds the
   merge-on-read position-delete files written by the silver MERGE into new data files.
2. `expire_snapshots(retention_threshold => '1h')` drops snapshot metadata older than the retention.
3. `remove_orphan_files(retention_threshold => '1h')` deletes files that no snapshot references, for example
   leftovers from failed commits.

**Evidence.** Each table gets a row in `lakehouse.ops.maintenance_runs`. The row holds data files, delete files and
bytes before and after, snapshot counts, and the rewrite snapshot id. `fingerprint_match` compares the row count
plus an order-insensitive checksum of the rewrite snapshot with its parent snapshot. If they differ, the task
fails, because compaction must never change data.

**Concurrency with the stream.** Both writers use Iceberg optimistic concurrency. When a Spark commit loses the
race, the sink retries the idempotent statement (`lakeflow_stream_commit_retries_total`). When the Trino rewrite
loses, the task fails and Airflow retries it. Many retries (`LakeFlowCommitConflictsHigh`) mean the maintenance
window overlaps heavy write traffic. Schedule it for a quieter period, or compact older partitions only.

### Retention: demo vs production

The local demo uses **1h**: `iceberg.expire-snapshots.min-retention` and `iceberg.remove-orphan-files.min-retention`
in `platform/trino/catalog/lakehouse.properties`. That keeps storage small on a laptop. Production should keep Trino's **7d** default, or whatever time-travel window
the business needs, because expired snapshots can no longer be queried with `FOR VERSION AS OF`.

## Manual run

```bash
make maintenance                                      # all tables
python scripts/lakeflowctl.py maintenance --tables silver.orders
```

Each table prints one JSON record, and the command exits non-zero if any table failed.

## Backfill / repair (`lakeflow_backfill`, manual)

A backfill inserts a Debezium `execute-snapshot` signal into `lakeflow_ops.debezium_signal`. Debezium then runs an
**incremental snapshot**: it reads the selected rows in chunks while streaming continues, dedups them against
concurrent changes using watermarks, and emits them as `op = r` events. Those events use the normal contract
validation and LSN-guarded MERGE. A backfill therefore restores missing or corrupted lakehouse rows but cannot
overwrite a newer change.

```bash
make backfill CONTRACT=orders KEYS="0b6d2f4e-8c1a-4d3b-9e7f-5a6b7c8d9e0f"   # specific keys
python scripts/lakeflowctl.py backfill customers --updated-since 2026-09-25T00:00:00+00:00
```

Filters are structured (UUID keys, `updated_since`) and validated by `lakeflow_core.backfill`. Raw SQL is never
passed to the connector. After a backfill, run the quality checks. `*.source_reconciliation` must return 0.
