"""Assemble a record-level trace (PostgreSQL -> ... -> Trino) from evidence gathered by the services.

Every stage is "done" only when there is a real identifier for it. Otherwise it is "pending", or "failed" when
the event was rejected. Stages are never inferred from elapsed time.
"""

from __future__ import annotations

from datetime import datetime, timezone

from lakeflow_core.lsn import int_to_lsn

STAGES = (
    ("postgres", "PostgreSQL WAL"),
    ("debezium", "Debezium"),
    ("kafka", "Kafka"),
    ("spark", "Spark micro-batch"),
    ("checkpoint", "Checkpoint commit"),
    ("iceberg", "Iceberg snapshot"),
    ("trino", "Trino query"),
)


def _ts(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def _stage(stage: str, status: str, at: str | None = None, **details) -> dict:
    label = dict(STAGES)[stage]
    return {"stage": stage, "label": label, "status": status, "at": at, "details": details}


def assemble(
    *,
    table: str,
    key: str,
    mutation: dict | None,
    source_row: dict | None,
    kafka_events: list[dict],
    bronze_rows: list[dict],
    dlq_rows: list[dict],
    batches: dict[tuple[str, int], dict],
    committed_batch: dict[str, int | None],
    silver_row: dict | None,
    trino_ms: float | None,
    now: datetime | None = None,
) -> dict:
    """Build the trace for the newest change of (table, key).

    kafka_events: live events seen by the API tracer ({topic, partition, offset, kafka_ts_ms, lsn, tx_id, op,
    source_ts_ms, debezium_ts_ms, snapshot}); bronze_rows: rows from bronze.cdc_events; batches: batch records
    keyed by (stream_epoch, batch_id); committed_batch: stream_epoch -> latest checkpoint-committed batch id.
    """
    now = now or datetime.now(timezone.utc)
    by_lsn: dict[int, dict] = {}
    for event in kafka_events:
        by_lsn.setdefault(event["lsn"], {}).update({"kafka": event})
    for row in bronze_rows:
        by_lsn.setdefault(row["source_lsn"], {}).update({"bronze": row})
    latest_lsn = max(by_lsn) if by_lsn else None
    latest = by_lsn.get(latest_lsn, {})
    kafka = latest.get("kafka")
    bronze = latest.get("bronze")
    rejected = next(iter(sorted(dlq_rows, key=lambda r: r.get("kafka_offset") or 0, reverse=True)), None)

    stages = []
    if mutation or source_row or latest:
        at = _ts(kafka["source_ts_ms"]) if kafka else (bronze and _iso(bronze.get("source_ts")))
        at = at or (mutation and mutation.get("committed_at"))
        stages.append(_stage(
            "postgres", "done", at,
            tx_id=(kafka or {}).get("tx_id") or (bronze or {}).get("source_tx_id") or (mutation or {}).get("txid"),
            wal_lsn_after_commit=(mutation or {}).get("commit_lsn"),
            row_present=source_row is not None,
        ))
    else:
        stages.append(_stage("postgres", "pending"))

    if kafka or bronze:
        lsn = latest_lsn
        stages.append(_stage(
            "debezium", "done", _ts((kafka or {}).get("debezium_ts_ms")) or _iso((bronze or {}).get("debezium_ts")),
            source_lsn=lsn, lsn=int_to_lsn(lsn) if lsn is not None else None, op=(kafka or bronze).get("op"),
            snapshot=(kafka or {}).get("snapshot") or (bronze or {}).get("source_snapshot"),
        ))
        coords = kafka or {"topic": bronze["kafka_topic"], "partition": bronze["kafka_partition"],
                           "offset": bronze["kafka_offset"], "kafka_ts_ms": None}
        stages.append(_stage(
            "kafka", "done", _ts(coords.get("kafka_ts_ms")) or _iso((bronze or {}).get("kafka_ts")),
            topic=coords["topic"], partition=coords["partition"], offset=coords["offset"],
        ))
    elif rejected:
        stages.append(_stage("debezium", "done", None))
        stages.append(_stage("kafka", "done", _iso(rejected.get("kafka_ts")), topic=rejected.get("kafka_topic"),
                             partition=rejected.get("kafka_partition"), offset=rejected.get("kafka_offset")))
    else:
        stages.append(_stage("debezium", "pending"))
        stages.append(_stage("kafka", "pending"))

    batch = None
    if bronze:
        batch = batches.get((bronze["stream_epoch"], bronze["batch_id"]))
        stages.append(_stage(
            "spark", "done", _iso(bronze.get("committed_at")), batch_id=bronze["batch_id"],
            outcome=bronze["apply_outcome"], is_late=bronze["is_late"], contract_version=bronze["contract_version"],
            attempts=(batch or {}).get("attempts"),
        ))
        committed = committed_batch.get(bronze["stream_epoch"])
        if committed is not None and committed >= bronze["batch_id"]:
            stages.append(_stage("checkpoint", "done", None, batch_id=bronze["batch_id"], committed_through=committed))
        else:
            stages.append(_stage("checkpoint", "pending", None, batch_id=bronze["batch_id"],
                                 note="Iceberg committed; Spark has not written commits/N yet (or is replaying)"))
    elif rejected and not kafka:
        stages.append(_stage("spark", "failed", _iso(rejected.get("first_seen_at")), outcome="dlq",
                             error_code=rejected.get("error_code"), dlq_id=rejected.get("dlq_id")))
        stages.append(_stage("checkpoint", "skipped"))
    else:
        stages.append(_stage("spark", "pending"))
        stages.append(_stage("checkpoint", "pending"))

    target = f"silver.{table}"
    snapshot_id = None
    if batch:
        snapshot_id = (batch.get("snapshot_ids") or {}).get(target)
    applied = bronze is not None and bronze["apply_outcome"] == "applied"
    if bronze and not applied:
        stages.append(_stage("iceberg", "skipped", None, reason=f"event {bronze['apply_outcome']}: silver unchanged"))
    elif applied and batch:
        stages.append(_stage("iceberg", "done", _iso(batch.get("committed_at")), table=target, snapshot_id=snapshot_id))
    else:
        stages.append(_stage("iceberg", "pending"))

    visible = silver_row is not None and latest_lsn is not None and (silver_row.get("_source_lsn") or -1) >= latest_lsn
    if visible:
        stages.append(_stage("trino", "done", now.isoformat(), query_ms=None if trino_ms is None else round(trino_ms, 1),
                             is_deleted=silver_row.get("is_deleted")))
    elif bronze and not applied and silver_row is not None:
        stages.append(_stage("trino", "done", now.isoformat(), query_ms=trino_ms, note="current state unchanged"))
    else:
        stages.append(_stage("trino", "pending"))

    freshness = None
    source_ts_ms = (kafka or {}).get("source_ts_ms")
    if source_ts_ms is None and bronze and bronze.get("source_ts"):
        source_ts_ms = _ms(bronze["source_ts"])
    if applied and batch and batch.get("committed_at") and source_ts_ms is not None:
        freshness = _ms(batch["committed_at"]) - source_ts_ms

    events = []
    for lsn in sorted(by_lsn):
        entry = by_lsn[lsn]
        row, live = entry.get("bronze") or {}, entry.get("kafka") or {}
        rec = batches.get((row.get("stream_epoch"), row.get("batch_id"))) if row else None
        events.append({
            "event_id": row.get("event_id"),
            "op": row.get("op") or live.get("op"),
            "source_lsn": lsn,
            "lsn": int_to_lsn(lsn),
            "tx_id": row.get("source_tx_id") or live.get("tx_id"),
            "source_ts": _iso(row.get("source_ts")) or _ts(live.get("source_ts_ms")),
            "kafka_topic": row.get("kafka_topic") or live.get("topic"),
            "kafka_partition": row.get("kafka_partition") if row else live.get("partition"),
            "kafka_offset": row.get("kafka_offset") if row else live.get("offset"),
            "batch_id": row.get("batch_id"),
            "apply_outcome": row.get("apply_outcome"),
            "is_late": row.get("is_late"),
            "contract_version": row.get("contract_version"),
            "before": row.get("before"),
            "after": row.get("after"),
            "committed_at": _iso(row.get("committed_at")),
            "snapshot_id": ((rec or {}).get("snapshot_ids") or {}).get(target),
            "injection_id": row.get("injection_id") or live.get("injection_id"),
        })
    return {
        "table": table,
        "key": key,
        "stages": stages,
        "events": events,
        "rejected": dlq_rows,
        "final_state": silver_row,
        "source_state": source_row,
        "freshness_ms": freshness,
        "schema_version": (bronze or {}).get("contract_version"),
        "as_of": now.isoformat(),
    }


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).isoformat()
    return str(value)


def _ms(value) -> int:
    if isinstance(value, datetime):
        value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
