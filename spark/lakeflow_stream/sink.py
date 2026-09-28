"""Idempotent micro-batch sink: Kafka batch -> DLQ / silver MERGE / bronze append / batch record.

The rules live in lakeflow_core.semantics (the executable spec) and in ADR-0003. Every write can be
re-executed safely, so Spark replaying a batch after a crash (checkpoint offsets written, commit marker
missing) cannot duplicate data:

* silver: MERGE guarded by `s._source_lsn > t._source_lsn`, deletes are soft tombstones
* bronze: append only event_ids not yet present (recomputed on every retry)
* DLQ: insert-if-absent keyed by Kafka coordinates; replays update only if their position is newer
* batch record: upsert keyed by (pipeline, stream_epoch, batch_id); `attempts` counts replays on purpose
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce

from lakeflow_core import tables as T
from lakeflow_core.contracts import ContractError, ContractRegistry, contracts_fingerprint, load_registry
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import Settings
from .control import ControlChannel
from .metrics import PipelineMetrics
from .udf import make_decoder

log = logging.getLogger("lakeflow.sink")

RETRIABLE_MARKERS = (
    "CommitFailedException",
    "ValidationException",
    "CommitStateUnknownException",
    "Found conflicting files",
)
COMMON_PROPS = {
    "format-version": "2",
    "write.target-file-size-bytes": str(64 * 1024 * 1024),
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
    "commit.retry.num-retries": "10",
    "history.expire.max-snapshot-age-ms": str(24 * 3600 * 1000),
}
MERGE_ON_READ = {"write.merge.mode": "merge-on-read", "write.update.mode": "merge-on-read", "write.delete.mode": "merge-on-read"}


@dataclass
class SinkContext:
    spark: SparkSession
    settings: Settings
    registry: ContractRegistry
    epoch: str
    app_id: str
    metrics: PipelineMetrics
    control: ControlChannel | None = None
    publish_ops: bool = False
    watermark_ms: int | None = None
    watermark_batch: int | None = None
    clock: Callable[[], float] = time.time

    def table(self, name: str) -> str:
        return f"{self.settings.catalog}.{name}"


# ---------------------------------------------------------------------------------------------------- DDL


def _create_table(spark: SparkSession, name: str, columns, partitioning: str, props: dict) -> None:
    props_sql = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} ({T.ddl_columns(columns)}) USING iceberg {partitioning} "
        f"TBLPROPERTIES ({props_sql})"
    )
    existing = {f.name for f in spark.table(name).schema.fields}
    for column, kind in columns:
        if column not in existing:
            log.info("schema evolution: ALTER TABLE %s ADD COLUMN %s %s", name, column, kind)
            spark.sql(f"ALTER TABLE {name} ADD COLUMN {column} {kind}")


def ensure_tables(spark: SparkSession, registry: ContractRegistry, catalog: str) -> None:
    for namespace in T.NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    bronze_props = {**COMMON_PROPS, "write.parquet.bloom-filter-enabled.column.event_id": "true"}
    _create_table(spark, f"{catalog}.{T.BRONZE_TABLE}", T.BRONZE_COLUMNS,
                  "PARTITIONED BY (days(source_ts), source_table)", bronze_props)
    _create_table(spark, f"{catalog}.{T.DLQ_TABLE}", T.DLQ_COLUMNS, "", COMMON_PROPS)
    _create_table(spark, f"{catalog}.{T.BATCH_TABLE}", T.BATCH_COLUMNS, "", COMMON_PROPS)
    for name in registry.names:
        contract = registry.current(name)
        _create_table(
            spark,
            f"{catalog}.{contract.target_table}",
            T.silver_columns(registry, name),
            f"PARTITIONED BY (bucket(4, {contract.primary_key[0]}))",
            {**COMMON_PROPS, **MERGE_ON_READ},
        )


# ---------------------------------------------------------------------------------------------- helpers


def _with_retries(ctx: SinkContext, stage: str, fn: Callable[[], object], attempts: int = 5) -> int:
    """Run an idempotent write, retrying optimistic-concurrency conflicts. Returns the number of retries."""
    for attempt in range(1, attempts + 1):
        try:
            fn()
            return attempt - 1
        except Exception as exc:  # noqa: BLE001 - Py4J wraps Java exceptions
            text = f"{type(exc).__name__}: {exc}"
            if attempt == attempts or not any(marker in text for marker in RETRIABLE_MARKERS):
                raise
            ctx.metrics.retries.labels(stage=stage).inc()
            log.warning("%s commit conflict (attempt %s/%s), retrying: %s", stage, attempt, attempts, text[:300])
            time.sleep(min(0.5 * 2**attempt, 8.0))
    return attempts


def _latest_snapshot(spark: SparkSession, table: str, app_id: str) -> dict | None:
    """Newest snapshot written by this Spark application (falls back to the newest snapshot overall)."""
    rows = spark.sql(
        f"SELECT snapshot_id, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 10"  # noqa: S608
    ).collect()
    if not rows:
        return None
    mine = [r for r in rows if (r["summary"] or {}).get("spark.app.id") == app_id]
    row = mine[0] if mine else rows[0]
    summary = row["summary"] or {}
    return {
        "snapshot_id": row["snapshot_id"],
        "added_data_files": int(summary.get("added-data-files", 0)),
        "added_files_bytes": int(summary.get("added-files-size", 0)),
    }


def _header(key: str):
    return F.expr(f"CAST((try_element_at(filter(headers, h -> h.key = '{key}'), 1)).value AS STRING)")


def _maybe_reload_contracts(ctx: SinkContext) -> None:
    try:
        fingerprint = contracts_fingerprint(ctx.settings.contracts_dir)
    except OSError:
        return
    if fingerprint == ctx.registry.fingerprint:
        return
    try:
        registry = load_registry(ctx.settings.contracts_dir)
        ensure_tables(ctx.spark, registry, ctx.settings.catalog)
    except (ContractError, ValueError) as exc:
        ctx.metrics.contract_reloads.labels(result="rejected").inc()
        log.error("contract change rejected, keeping previous registry: %s", exc)
        return
    ctx.registry = registry
    ctx.metrics.contract_reloads.labels(result="applied").inc()
    log.info("contracts reloaded: %s", {n: [c.version for c in registry.versions(n)] for n in registry.names})


# ------------------------------------------------------------------------------------------------ batch


def decode(ctx: SinkContext, batch_df: DataFrame) -> DataFrame:
    base = batch_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key"),
        F.col("value"),
        _header("lakeflow-original-topic").alias("original_topic"),
        _header("lakeflow-replay-of").alias("replay_of"),
        _header("lakeflow-injection-id").alias("injection_id"),
    )
    decoder = make_decoder(ctx.registry, ctx.settings.pii_key)
    keep = [c for c in base.columns if c not in ("key", "value")]
    with_struct = base.withColumn("_d", decoder(F.coalesce("original_topic", "kafka_topic"), F.col("key"), F.col("value")))
    return with_struct.select(*keep, "_d.*")


def _watermark_before(ctx: SinkContext, batch_id: int) -> int:
    if ctx.watermark_batch == batch_id - 1 and ctx.watermark_ms is not None:
        return ctx.watermark_ms
    rows = ctx.spark.sql(
        f"SELECT watermark_ms FROM {ctx.table(T.BATCH_TABLE)} "  # noqa: S608 - internal identifiers
        f"WHERE pipeline = '{ctx.settings.pipeline}' AND stream_epoch = '{ctx.epoch}' AND batch_id < {batch_id} "
        "ORDER BY batch_id DESC LIMIT 1"
    ).collect()
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


def _bronze_lookup(ctx: SinkContext, lo_ms: int, hi_ms: int) -> DataFrame:
    return (
        ctx.spark.table(ctx.table(T.BRONZE_TABLE))
        .where(F.col("source_ts").between(F.expr(f"timestamp_millis({lo_ms})"), F.expr(f"timestamp_millis({hi_ms})")))
        .groupBy("event_id")
        .agg(F.first("stream_epoch").alias("_x_epoch"), F.first("batch_id").alias("_x_batch"))
    )


def plan(ctx: SinkContext, decoded: DataFrame, batch_id: int, watermark_ms: int, lo_ms: int, hi_ms: int) -> DataFrame:
    """Every unique valid event of the batch with apply_outcome in {applied, superseded, stale, duplicate}."""
    valid = decoded.where(F.col("status") == "valid").withColumn("source_ts", F.expr("timestamp_millis(source_ts_ms)"))
    first = Window.partitionBy("event_id").orderBy("kafka_topic", "kafka_partition", "kafka_offset")
    unique = valid.withColumn("_dup_rank", F.row_number().over(first)).where("_dup_rank = 1").drop("_dup_rank")

    joined = unique.join(_bronze_lookup(ctx, lo_ms, hi_ms), "event_id", "left")
    own_attempt = (F.col("_x_epoch") == F.lit(ctx.epoch)) & (F.col("_x_batch") == F.lit(batch_id))
    prior_dup = F.col("_x_batch").isNotNull() & ~F.coalesce(own_attempt, F.lit(False))
    joined = joined.withColumn("_prior_dup", prior_dup).drop("_x_epoch", "_x_batch")
    is_late = F.col("source_ts_ms") < F.lit(watermark_ms)

    candidates = joined.where(~F.col("_prior_dup"))
    by_key = Window.partitionBy("source_table", "primary_key").orderBy(F.col("source_lsn").desc(), F.col("kafka_offset").desc())
    candidates = candidates.withColumn("_key_rank", F.row_number().over(by_key))

    parts = []
    for name in ctx.registry.names:
        contract = ctx.registry.current(name)
        pk = contract.primary_key[0]
        silver = ctx.spark.table(ctx.table(contract.target_table)).select(
            F.col(pk).cast("string").alias("primary_key"),
            F.col("_source_lsn").alias("_s_lsn"),
            F.col("_prev_source_lsn").alias("_s_prev"),
            F.col("_batch_id").alias("_s_batch"),
            F.col("_stream_epoch").alias("_s_epoch"),
        )
        events = candidates.where(F.col("source_table") == contract.source_table).join(silver, "primary_key", "left")
        same_attempt = (F.col("_s_epoch") == F.lit(ctx.epoch)) & (F.col("_s_batch") == F.lit(batch_id))
        baseline = F.when(F.coalesce(same_attempt, F.lit(False)), F.col("_s_prev")).otherwise(F.col("_s_lsn"))
        outcome = (
            F.when(baseline.isNotNull() & (F.col("source_lsn") <= baseline), F.lit("stale"))
            .when(F.col("_key_rank") > 1, F.lit("superseded"))
            .otherwise(F.lit("applied"))
        )
        parts.append(events.withColumn("apply_outcome", outcome).drop("_s_lsn", "_s_prev", "_s_batch", "_s_epoch"))

    duplicates = joined.where(F.col("_prior_dup")).withColumn("_key_rank", F.lit(None).cast("int"))
    duplicates = duplicates.withColumn("apply_outcome", F.lit("duplicate"))
    planned = reduce(DataFrame.unionByName, parts, duplicates)
    return planned.withColumn("is_late", is_late).drop("_prior_dup")


def _silver_source(ctx: SinkContext, applied: DataFrame, name: str, batch_id: int) -> DataFrame:
    data_columns = T.silver_data_columns(ctx.registry, name)
    schema = StructType([StructField(column, StringType(), True) for column, _ in data_columns])
    image = F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json"))
    parsed = applied.withColumn("_img", F.from_json(image, schema))
    is_delete = F.col("op") == "d"
    columns = [F.col(f"_img.{column}").cast(kind).alias(column) for column, kind in data_columns] + [
        is_delete.alias("is_deleted"),
        F.when(is_delete, F.col("source_ts")).alias("deleted_at"),
        F.col("source_lsn").alias("_source_lsn"),
        F.lit(None).cast("bigint").alias("_prev_source_lsn"),
        F.col("source_ts").alias("_source_ts"),
        F.col("op").alias("_source_op"),
        F.col("event_id").alias("_event_id"),
        F.col("contract_version").cast("int").alias("_contract_version"),
        F.lit(batch_id).cast("bigint").alias("_batch_id"),
        F.lit(ctx.epoch).alias("_stream_epoch"),
        F.current_timestamp().alias("_ingested_at"),
    ]
    return parsed.select(*columns)


def merge_silver_sql(ctx: SinkContext, name: str, view: str) -> str:
    contract = ctx.registry.current(name)
    pk = contract.primary_key[0]
    data = [column for column, _ in T.silver_data_columns(ctx.registry, name)]
    everything = [column for column, _ in T.silver_columns(ctx.registry, name)]
    meta = [
        "_source_lsn = s._source_lsn",
        "_prev_source_lsn = t._source_lsn",
        "_source_ts = s._source_ts",
        "_source_op = s._source_op",
        "_event_id = s._event_id",
        "_contract_version = s._contract_version",
        "_batch_id = s._batch_id",
        "_stream_epoch = s._stream_epoch",
        "_ingested_at = s._ingested_at",
    ]
    delete_set = ["is_deleted = true", "deleted_at = s.deleted_at", *meta]
    upsert_set = [f"{c} = s.{c}" for c in data if c != pk] + ["is_deleted = false", "deleted_at = CAST(NULL AS TIMESTAMP)", *meta]
    return (
        f"MERGE INTO {ctx.table(contract.target_table)} t USING {view} s ON t.{pk} = s.{pk} "
        f"WHEN MATCHED AND s._source_lsn > t._source_lsn AND s._source_op = 'd' THEN UPDATE SET {', '.join(delete_set)} "
        f"WHEN MATCHED AND s._source_lsn > t._source_lsn THEN UPDATE SET {', '.join(upsert_set)} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(everything)}) VALUES ({', '.join('s.' + c for c in everything)})"
    )


def _bronze_rows(ctx: SinkContext, planned: DataFrame, batch_id: int, committed_at: datetime) -> DataFrame:
    rows = planned.where(F.col("apply_outcome") != "duplicate").select(
        "event_id", "source_table", "primary_key", "op", "source_lsn", "source_tx_id", "source_ts", "source_snapshot",
        F.expr("timestamp_millis(debezium_ts_ms)").alias("debezium_ts"), "kafka_topic", "kafka_partition",
        "kafka_offset", "kafka_ts", "contract_name", "contract_version", "drift_fields", "before_json", "after_json",
        "is_late", "apply_outcome", "injection_id", "replay_of",
        F.lit(batch_id).cast("bigint").alias("batch_id"), F.lit(ctx.epoch).alias("stream_epoch"),
        F.lit(committed_at).alias("committed_at"),
    )
    return rows.select(*[F.col(c).cast(kind).alias(c) for c, kind in T.BRONZE_COLUMNS])


def _dlq_rows(ctx: SinkContext, decoded: DataFrame, batch_id: int, seen_at: datetime) -> DataFrame:
    position = F.format_string("%05d:%020d", F.col("kafka_partition"), F.col("kafka_offset"))
    coordinates_id = F.sha2(F.concat_ws("|", "kafka_topic", "kafka_partition", "kafka_offset"), 256)
    common = [
        F.col("kafka_topic"), F.col("kafka_partition"), F.col("kafka_offset"), F.col("kafka_ts"),
        F.coalesce("original_topic", "kafka_topic").alias("source_topic"), F.col("contract_name"),
        F.col("raw_key"), F.col("raw_value"), F.col("injection_id"), position.alias("replay_position"),
        F.col("replay_of").isNotNull().alias("is_replay"), F.lit(seen_at).alias("seen_at"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
    ]
    rejected = decoded.where(F.col("status") == "dlq").select(
        F.coalesce("replay_of", coordinates_id).alias("dlq_id"), *common,
        F.col("error_code"), F.col("error_detail"), F.col("violations"),
        F.lit("open").alias("new_status"), F.lit(None).cast("timestamp").alias("resolved_at"),
        F.lit(None).cast("bigint").alias("resolved_batch_id"),
    )
    resolved = decoded.where((F.col("status") == "valid") & F.col("replay_of").isNotNull()).select(
        F.col("replay_of").alias("dlq_id"), *common,
        F.lit(None).cast("string").alias("error_code"), F.lit(None).cast("string").alias("error_detail"),
        F.lit(None).cast("array<string>").alias("violations"), F.lit("replayed").alias("new_status"),
        F.lit(seen_at).alias("resolved_at"), F.lit(batch_id).cast("bigint").alias("resolved_batch_id"),
    )
    latest = Window.partitionBy("dlq_id").orderBy(F.col("replay_position").desc())
    return rejected.unionByName(resolved).withColumn("_r", F.row_number().over(latest)).where("_r = 1").drop("_r")


DLQ_MERGE = (
    "MERGE INTO {table} t USING {view} s ON t.dlq_id = s.dlq_id "
    "WHEN MATCHED AND s.is_replay AND (t.last_replay_position IS NULL OR s.replay_position > t.last_replay_position) "
    "THEN UPDATE SET replay_attempts = t.replay_attempts + 1, last_seen_at = s.seen_at, "
    "last_replay_position = s.replay_position, status = s.new_status, error_code = coalesce(s.error_code, t.error_code), "
    "error_detail = coalesce(s.error_detail, t.error_detail), violations = coalesce(s.violations, t.violations), "
    "resolved_at = s.resolved_at, resolved_batch_id = s.resolved_batch_id "
    "WHEN NOT MATCHED AND NOT s.is_replay THEN INSERT (dlq_id, first_seen_at, last_seen_at, kafka_topic, "
    "kafka_partition, kafka_offset, kafka_ts, source_topic, contract_name, error_code, error_detail, violations, "
    "raw_key, raw_value, injection_id, status, replay_attempts, last_replay_position, resolved_at, resolved_batch_id, "
    "batch_id) VALUES (s.dlq_id, s.seen_at, s.seen_at, s.kafka_topic, s.kafka_partition, s.kafka_offset, s.kafka_ts, "
    "s.source_topic, s.contract_name, s.error_code, s.error_detail, s.violations, s.raw_key, s.raw_value, "
    "s.injection_id, 'open', 0, NULL, NULL, NULL, s.batch_id)"
)

BATCH_MERGE = (
    "MERGE INTO {table} t USING {view} s "
    "ON t.pipeline = s.pipeline AND t.stream_epoch = s.stream_epoch AND t.batch_id = s.batch_id "
    "WHEN MATCHED THEN UPDATE SET attempts = t.attempts + 1, {updates} "
    "WHEN NOT MATCHED THEN INSERT *"
)


def process_batch(ctx: SinkContext, batch_df: DataFrame, batch_id: int) -> dict | None:
    """foreachBatch entry point. Returns the batch record (None for empty batches)."""
    started = ctx.clock()
    stage_ms: dict[str, int] = {}
    mark = [started]

    def lap(name: str) -> None:
        now = ctx.clock()
        stage_ms[name] = int((now - mark[0]) * 1000)
        mark[0] = now

    _maybe_reload_contracts(ctx)
    decoded = decode(ctx, batch_df).persist(StorageLevel.MEMORY_AND_DISK)
    planned = None
    try:
        s = decoded.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(F.when(F.col("status") == "valid", 1).otherwise(0)).alias("valid_rows"),
            F.sum(F.when(F.col("status") == "dlq", 1).otherwise(0)).alias("dlq_rows"),
            F.sum(F.when(F.col("status") == "tombstone", 1).otherwise(0)).alias("tombstones"),
            F.countDistinct(F.when(F.col("status") == "valid", F.col("event_id"))).alias("unique_valid"),
            F.min(F.when(F.col("status") == "valid", F.col("source_ts_ms"))).alias("min_ts"),
            F.max(F.when(F.col("status") == "valid", F.col("source_ts_ms"))).alias("max_ts"),
        ).first()
        if s["input_rows"] == 0:
            return None
        offsets: dict[str, dict[str, list[int]]] = {}
        for row in decoded.groupBy("kafka_topic", "kafka_partition").agg(F.min("kafka_offset"), F.max("kafka_offset")).collect():
            offsets.setdefault(row[0], {})[str(row[1])] = [int(row[2]), int(row[3])]
        lap("decode")

        watermark_before = _watermark_before(ctx, batch_id)
        watermark_after = watermark_before
        outcome_counts: dict[str, int] = {}
        per_table: dict[str, dict[str, int]] = {}
        versions: dict[str, dict[str, int]] = {}
        late_rows = 0
        retries = 0
        snapshots: dict[str, dict] = {}
        freshness = (None, None, None)

        if s["valid_rows"]:
            planned = plan(ctx, decoded, batch_id, watermark_before, int(s["min_ts"]), int(s["max_ts"]))
            planned = planned.persist(StorageLevel.MEMORY_AND_DISK)
            for row in planned.groupBy("source_table", "apply_outcome", "contract_name", "contract_version", "is_late").count().collect():
                table, outcome, contract, version, late, count = row
                outcome_counts[outcome] = outcome_counts.get(outcome, 0) + count
                per_table.setdefault(table, {})[outcome] = per_table.get(table, {}).get(outcome, 0) + count
                if outcome != "duplicate":
                    versions.setdefault(contract, {})[str(version)] = versions.get(contract, {}).get(str(version), 0) + count
                    late_rows += count if late else 0
            watermark_after = max(watermark_before, int(s["max_ts"]) - ctx.settings.allowed_lateness_ms)
            lap("plan")

            for name in ctx.registry.names:
                contract = ctx.registry.current(name)
                if not per_table.get(contract.source_table, {}).get("applied"):
                    continue
                applied = planned.where((F.col("source_table") == contract.source_table) & (F.col("apply_outcome") == "applied"))
                view = f"lakeflow_silver_src_{name}"
                _silver_source(ctx, applied, name, batch_id).createOrReplaceTempView(view)
                sql = merge_silver_sql(ctx, name, view)
                retries += _with_retries(ctx, f"silver_{name}", lambda sql=sql: ctx.spark.sql(sql))
                snap = _latest_snapshot(ctx.spark, ctx.table(contract.target_table), ctx.app_id)
                if snap:
                    snapshots[contract.target_table] = snap
            lap("silver")

        committed_at = datetime.now(timezone.utc)
        commit_ms = int(committed_at.timestamp() * 1000)

        if planned is not None:
            if outcome_counts.get("applied"):
                q = planned.where(F.col("apply_outcome") == "applied").select(
                    (F.lit(commit_ms) - F.col("source_ts_ms")).alias("f")
                ).agg(F.percentile_approx("f", [0.5, 0.95], 10000).alias("p"), F.max("f").alias("m")).first()
                freshness = (int(q["p"][0]), int(q["p"][1]), int(q["m"]))
            bronze = _bronze_rows(ctx, planned, batch_id, committed_at)
            lo, hi = int(s["min_ts"]), int(s["max_ts"])

            def append_bronze() -> None:
                fresh = bronze.join(_bronze_lookup(ctx, lo, hi).select("event_id"), "event_id", "left_anti")
                fresh.writeTo(ctx.table(T.BRONZE_TABLE)).append()

            retries += _with_retries(ctx, "bronze", append_bronze)
            snap = _latest_snapshot(ctx.spark, ctx.table(T.BRONZE_TABLE), ctx.app_id)
            if snap:
                snapshots[T.BRONZE_TABLE] = snap
            lap("bronze")

        has_replays = decoded.where(F.col("replay_of").isNotNull()).limit(1).count() > 0
        if s["dlq_rows"] or has_replays:
            _dlq_rows(ctx, decoded, batch_id, committed_at).createOrReplaceTempView("lakeflow_dlq_src")
            sql = DLQ_MERGE.format(table=ctx.table(T.DLQ_TABLE), view="lakeflow_dlq_src")
            retries += _with_retries(ctx, "dlq", lambda: ctx.spark.sql(sql))
            lap("dlq")

        duplicates = (s["valid_rows"] - s["unique_valid"]) + outcome_counts.get("duplicate", 0)
        finished = ctx.clock()
        record = {
            "pipeline": ctx.settings.pipeline,
            "stream_epoch": ctx.epoch,
            "batch_id": batch_id,
            "attempts": 1,
            "spark_app_id": ctx.app_id,
            "started_at": datetime.fromtimestamp(started, timezone.utc),
            "committed_at": committed_at,
            "duration_ms": int((finished - started) * 1000),
            "stage_ms": stage_ms,
            "input_rows": int(s["input_rows"]),
            "valid_rows": int(s["valid_rows"]),
            "dlq_rows": int(s["dlq_rows"]),
            "tombstones": int(s["tombstones"]),
            "duplicates": int(duplicates),
            "applied": outcome_counts.get("applied", 0),
            "superseded": outcome_counts.get("superseded", 0),
            "stale": outcome_counts.get("stale", 0),
            "late_rows": late_rows,
            "commit_retries": retries,
            "kafka_offsets": json.dumps(offsets, sort_keys=True),
            "watermark_ms": watermark_after,
            "max_source_ts_ms": None if s["max_ts"] is None else int(s["max_ts"]),
            "freshness_p50_ms": freshness[0],
            "freshness_p95_ms": freshness[1],
            "freshness_max_ms": freshness[2],
            "contract_versions": json.dumps(versions, sort_keys=True),
            "snapshot_ids": json.dumps({k: v["snapshot_id"] for k, v in snapshots.items()}, sort_keys=True),
            "added_data_files": sum(v["added_data_files"] for v in snapshots.values()),
            "added_files_bytes": sum(v["added_files_bytes"] for v in snapshots.values()),
        }
        columns = [c for c, _ in T.BATCH_COLUMNS]
        ctx.spark.createDataFrame([tuple(record[c] for c in columns)], T.ddl_columns(T.BATCH_COLUMNS).replace("\n", " ")) \
            .createOrReplaceTempView("lakeflow_batch_src")
        updates = ", ".join(f"{c} = s.{c}" for c in columns if c not in ("pipeline", "stream_epoch", "batch_id", "attempts"))
        sql = BATCH_MERGE.format(table=ctx.table(T.BATCH_TABLE), view="lakeflow_batch_src", updates=updates)
        _with_retries(ctx, "batch_record", lambda: ctx.spark.sql(sql))

        ctx.watermark_ms, ctx.watermark_batch = watermark_after, batch_id
        _publish(ctx, record)
        _record_metrics(ctx, record, per_table, snapshots)
        if ctx.control is not None:
            ctx.control.maybe_crash_after_commit(batch_id, outcome_counts.get("applied", 0))
        return record
    except Exception:
        ctx.metrics.batches.labels(result="failed").inc()
        raise
    finally:
        decoded.unpersist()
        if planned is not None:
            planned.unpersist()


def _publish(ctx: SinkContext, record: dict) -> None:
    if not ctx.publish_ops or not ctx.settings.ops_topic:
        return
    payload = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in record.items()}
    try:
        ctx.spark.createDataFrame([(f"{record['stream_epoch']}:{record['batch_id']}", json.dumps(payload))], "key string, value string") \
            .write.format("kafka").option("kafka.bootstrap.servers", ctx.settings.kafka_bootstrap) \
            .option("topic", ctx.settings.ops_topic).save()
    except Exception:  # noqa: BLE001 - the ops notification is best effort; Iceberg holds the durable record
        log.exception("could not publish batch record to %s", ctx.settings.ops_topic)


def _record_metrics(ctx: SinkContext, record: dict, per_table: dict, snapshots: dict) -> None:
    m = ctx.metrics
    m.batches.labels(result="committed").inc()
    m.batch_seconds.observe(record["duration_ms"] / 1000.0)
    m.last_batch_id.set(record["batch_id"])
    m.last_batch_ts.set(record["committed_at"].timestamp())
    m.watermark.set(record["watermark_ms"] / 1000.0)
    for table, outcomes in per_table.items():
        for outcome, count in outcomes.items():
            m.events.labels(table=table, outcome=outcome).inc(count)
    if record["dlq_rows"]:
        m.events.labels(table="*", outcome="dlq").inc(record["dlq_rows"])
    if record["tombstones"]:
        m.events.labels(table="*", outcome="tombstone").inc(record["tombstones"])
    if record["freshness_p95_ms"] is not None:
        m.freshness_p50.set(record["freshness_p50_ms"] / 1000.0)
        m.freshness_p95.set(record["freshness_p95_ms"] / 1000.0)
        m.freshness_max.set(record["freshness_max_ms"] / 1000.0)
    for table, snap in snapshots.items():
        if snap["added_data_files"]:
            m.files_written.labels(table=table).inc(snap["added_data_files"])
            m.file_bytes.labels(table=table).set(snap["added_files_bytes"] / snap["added_data_files"])
