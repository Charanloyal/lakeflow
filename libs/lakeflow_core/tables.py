"""Physical layout of LakeFlow Iceberg tables, derived from the contracts (single source for DDL and queries)."""

from __future__ import annotations

from .contracts import ContractRegistry

CATALOG = "lakehouse"
BRONZE_TABLE = "bronze.cdc_events"
DLQ_TABLE = "ops.dlq_events"
BATCH_TABLE = "ops.batch_commits"
QUALITY_TABLE = "ops.quality_results"
MAINTENANCE_TABLE = "ops.maintenance_runs"
NAMESPACES = ("bronze", "silver", "ops")

SILVER_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("is_deleted", "boolean"),
    ("deleted_at", "timestamp"),
    ("_source_lsn", "bigint"),
    ("_prev_source_lsn", "bigint"),
    ("_source_ts", "timestamp"),
    ("_source_op", "string"),
    ("_event_id", "string"),
    ("_contract_version", "int"),
    ("_batch_id", "bigint"),
    ("_stream_epoch", "string"),
    ("_ingested_at", "timestamp"),
)

BRONZE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "string"),
    ("source_table", "string"),
    ("primary_key", "string"),
    ("op", "string"),
    ("source_lsn", "bigint"),
    ("source_tx_id", "bigint"),
    ("source_ts", "timestamp"),
    ("source_snapshot", "string"),
    ("debezium_ts", "timestamp"),
    ("kafka_topic", "string"),
    ("kafka_partition", "int"),
    ("kafka_offset", "bigint"),
    ("kafka_ts", "timestamp"),
    ("contract_name", "string"),
    ("contract_version", "int"),
    ("drift_fields", "array<string>"),
    ("before_json", "string"),
    ("after_json", "string"),
    ("is_late", "boolean"),
    ("apply_outcome", "string"),
    ("injection_id", "string"),
    ("replay_of", "string"),
    ("batch_id", "bigint"),
    ("stream_epoch", "string"),
    ("committed_at", "timestamp"),
)

DLQ_COLUMNS: tuple[tuple[str, str], ...] = (
    ("dlq_id", "string"),
    ("first_seen_at", "timestamp"),
    ("last_seen_at", "timestamp"),
    ("kafka_topic", "string"),
    ("kafka_partition", "int"),
    ("kafka_offset", "bigint"),
    ("kafka_ts", "timestamp"),
    ("source_topic", "string"),
    ("contract_name", "string"),
    ("error_code", "string"),
    ("error_detail", "string"),
    ("violations", "array<string>"),
    ("raw_key", "string"),
    ("raw_value", "string"),
    ("injection_id", "string"),
    ("status", "string"),
    ("replay_attempts", "int"),
    ("last_replay_position", "string"),
    ("resolved_at", "timestamp"),
    ("resolved_batch_id", "bigint"),
    ("batch_id", "bigint"),
)

BATCH_COLUMNS: tuple[tuple[str, str], ...] = (
    ("pipeline", "string"),
    ("stream_epoch", "string"),
    ("batch_id", "bigint"),
    ("attempts", "int"),
    ("spark_app_id", "string"),
    ("started_at", "timestamp"),
    ("committed_at", "timestamp"),
    ("duration_ms", "bigint"),
    ("stage_ms", "map<string,bigint>"),
    ("input_rows", "bigint"),
    ("valid_rows", "bigint"),
    ("dlq_rows", "bigint"),
    ("tombstones", "bigint"),
    ("duplicates", "bigint"),
    ("applied", "bigint"),
    ("superseded", "bigint"),
    ("stale", "bigint"),
    ("late_rows", "bigint"),
    ("commit_retries", "int"),
    ("kafka_offsets", "string"),
    ("watermark_ms", "bigint"),
    ("max_source_ts_ms", "bigint"),
    ("freshness_p50_ms", "bigint"),
    ("freshness_p95_ms", "bigint"),
    ("freshness_max_ms", "bigint"),
    ("contract_versions", "string"),
    ("snapshot_ids", "string"),
    ("added_data_files", "bigint"),
    ("added_files_bytes", "bigint"),
)


def silver_table(registry: ContractRegistry, name: str) -> str:
    return registry.current(name).target_table


def silver_data_columns(registry: ContractRegistry, name: str) -> list[tuple[str, str]]:
    columns: list[tuple[str, str]] = []
    for spec in registry.all_fields(name).values():
        if spec.target_column is not None:
            columns.append((spec.target_column, "string" if spec.pii_handling == "hash" else spec.logical_type))
    return columns


def silver_columns(registry: ContractRegistry, name: str) -> list[tuple[str, str]]:
    return silver_data_columns(registry, name) + list(SILVER_METADATA_COLUMNS)


def ddl_columns(columns) -> str:
    return ",\n  ".join(f"{name} {kind}" for name, kind in columns)
