"""Record-level trace and the event explorer (bronze via Trino + live Kafka tail + checkpoint + PostgreSQL)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fastapi import HTTPException

from ..domain.checkpoint import read_checkpoint
from ..domain.trace import assemble

log = logging.getLogger("lakeflow.trace")
BRONZE_COLUMNS = (
    "event_id, op, source_lsn, source_tx_id, source_ts, source_snapshot, debezium_ts, kafka_topic, kafka_partition, "
    "kafka_offset, kafka_ts, contract_version, before_json, after_json, is_late, apply_outcome, injection_id, replay_of, "
    "batch_id, stream_epoch, committed_at"
)


def contract_for(ctx, table: str):
    if table not in ctx.registry.names:
        raise HTTPException(404, f"unknown table {table!r}; expected one of {ctx.registry.names}")
    return ctx.registry.current(table)


def _json(text):
    try:
        return None if text is None else json.loads(text)
    except ValueError:
        return {"_unparseable": text}


def trace(ctx, table: str, key: str) -> dict:
    contract = contract_for(ctx, table)
    pk = contract.primary_key[0]
    sources, errors = [], []

    source_row = None
    try:
        with ctx.clients.source() as conn:
            source_row = conn.execute(f"SELECT * FROM {contract.source_table} WHERE {pk}::text = %s", (key,)).fetchone()  # noqa: S608
        sources.append(f"postgres: {contract.source_table}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"postgres: {exc}")
    mutation = None
    try:
        mutation = ctx.store.latest_mutation(table, key)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"control db: {exc}")

    kafka_events = [e for e in ctx.tracer.events_for(contract.topic, key) if e.get("lsn") is not None]
    if kafka_events:
        sources.append("API Kafka tail")

    bronze_rows, dlq_rows, batches, silver_row, trino_ms = [], [], {}, None, None
    try:
        rows, _ = ctx.clients.trino(
            f"SELECT {BRONZE_COLUMNS} FROM lakehouse.bronze.cdc_events "  # noqa: S608 - fixed column list
            "WHERE source_table = ? AND primary_key = ? ORDER BY source_lsn",
            [contract.source_table, key],
        )
        for row in rows:
            row["before"], row["after"] = _json(row.pop("before_json")), _json(row.pop("after_json"))
        bronze_rows = rows
        dlq_rows, _ = ctx.clients.trino(
            "SELECT dlq_id, error_code, error_detail, violations, kafka_topic, kafka_partition, kafka_offset, kafka_ts, "
            "first_seen_at, status, injection_id FROM lakehouse.ops.dlq_events "
            "WHERE source_topic = ? AND strpos(coalesce(raw_key, ''), ?) > 0 ORDER BY first_seen_at DESC LIMIT 20",
            [contract.topic, key],
        )
        batch_ids = sorted({r["batch_id"] for r in bronze_rows})
        if batch_ids:
            placeholders = ", ".join("?" for _ in batch_ids)
            records, _ = ctx.clients.trino(
                "SELECT stream_epoch, batch_id, attempts, committed_at, snapshot_ids, duration_ms "  # noqa: S608
                f"FROM lakehouse.ops.batch_commits WHERE batch_id IN ({placeholders})",
                batch_ids,
            )
            for record in records:
                record["snapshot_ids"] = _json(record.get("snapshot_ids")) or {}
                batches[(record["stream_epoch"], record["batch_id"])] = record
        silver_rows, trino_ms = ctx.clients.trino(
            f"SELECT * FROM lakehouse.{contract.target_table} WHERE {pk} = ?",  # noqa: S608
            [key],
        )
        silver_row = silver_rows[0] if silver_rows else None
        sources.append("Trino: bronze.cdc_events, ops.dlq_events, ops.batch_commits, " + contract.target_table)
    except Exception as exc:  # noqa: BLE001 - tables may not exist until the first batch commits
        errors.append(f"trino: {str(exc)[:200]}")

    committed = {}
    try:
        checkpoint = read_checkpoint(ctx.settings.checkpoint_dir)
        committed[checkpoint.epoch] = checkpoint.latest_committed_batch
        sources.append("Spark checkpoint commits/")
    except OSError as exc:
        errors.append(f"checkpoint: {exc}")

    result = assemble(
        table=table,
        key=key,
        mutation=_mutation(mutation),
        source_row=_plain_row(source_row),
        kafka_events=kafka_events,
        bronze_rows=bronze_rows,
        dlq_rows=dlq_rows,
        batches=batches,
        committed_batch=committed,
        silver_row=silver_row,
        trino_ms=trino_ms,
    )
    result["sources"] = sources + [f"unavailable: {e}" for e in errors]
    return result


def _mutation(row):
    if not row:
        return None
    return {"txid": row.get("txid"), "commit_lsn": row.get("commit_lsn"), "committed_at": _iso(row.get("at"))}


def _iso(value):
    return value.isoformat() if isinstance(value, datetime) else value


def _plain_row(row):
    if row is None:
        return None
    return {
        k: (
            v.isoformat()
            if isinstance(v, datetime)
            else (str(v) if not isinstance(v, (int, float, bool, type(None))) else v)
        )
        for k, v in row.items()
    }


def list_events(ctx, table=None, op=None, outcome=None, key=None, minutes=60, limit=100) -> dict:
    where, params = [f"committed_at > current_timestamp - INTERVAL '{int(minutes)}' MINUTE"], []
    if table:
        where.append("source_table = ?")
        params.append(contract_for(ctx, table).source_table)
    for column, value in (("op", op), ("apply_outcome", outcome), ("primary_key", key)):
        if value:
            where.append(f"{column} = ?")
            params.append(value)
    rows, _ = ctx.clients.trino(
        "SELECT event_id, source_table, primary_key, op, source_lsn, source_tx_id, source_ts, kafka_topic, "  # noqa: S608
        "kafka_partition, kafka_offset, batch_id, apply_outcome, is_late, contract_version, drift_fields, injection_id, "
        f"replay_of, committed_at, before_json, after_json FROM lakehouse.bronze.cdc_events WHERE {' AND '.join(where)} "
        f"ORDER BY committed_at DESC, source_lsn DESC LIMIT {int(limit)}",
        params,
    )
    for row in rows:
        row["before"], row["after"] = _json(row.pop("before_json")), _json(row.pop("after_json"))
    return {
        "events": rows,
        "count": len(rows),
        "as_of": datetime.now(timezone.utc),
        "source": "Trino: bronze.cdc_events",
    }


def recent_batches(ctx, limit=50) -> dict:
    rows, _ = ctx.clients.trino(
        "SELECT pipeline, stream_epoch, batch_id, attempts, started_at, committed_at, duration_ms, input_rows, valid_rows, "  # noqa: S608
        "dlq_rows, tombstones, duplicates, applied, superseded, stale, late_rows, commit_retries, kafka_offsets, "
        "watermark_ms, freshness_p50_ms, freshness_p95_ms, freshness_max_ms, contract_versions, snapshot_ids, "
        f"added_data_files, added_files_bytes, stage_ms FROM lakehouse.ops.batch_commits ORDER BY committed_at DESC LIMIT {int(limit)}"
    )
    for row in rows:
        for column in ("kafka_offsets", "contract_versions", "snapshot_ids"):
            row[column] = _json(row.get(column))
    return {"batches": rows, "as_of": datetime.now(timezone.utc), "source": "Trino: ops.batch_commits"}
