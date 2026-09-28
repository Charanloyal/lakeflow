"""Overview metrics and live topology. Values come from probes, Kafka, the checkpoint and Trino; never constants."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from ..monitor import COMPONENTS, overall

log = logging.getLogger("lakeflow.overview")
STALE_AFTER = timedelta(seconds=45)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def metric(name, label, value, unit, as_of, source, window=None, note=None) -> dict:
    stale = as_of is None or (_now() - as_of) > STALE_AFTER
    return {
        "name": name,
        "label": label,
        "value": value,
        "unit": unit,
        "as_of": as_of,
        "source": source,
        "window": window,
        "stale": stale,
        "note": note,
    }


def _trino_window_stats(ctx) -> dict:
    """p95 freshness and error rate over the last 15 minutes, from bronze and batch records (cached 10 s)."""

    def produce():
        out = {"as_of": _now(), "error": None}
        try:
            rows, _ = ctx.clients.trino(
                "SELECT approx_percentile(to_unixtime(committed_at) - to_unixtime(source_ts), 0.95) AS p95, "
                "approx_percentile(to_unixtime(committed_at) - to_unixtime(source_ts), 0.5) AS p50, count(*) AS n "
                "FROM lakehouse.bronze.cdc_events WHERE apply_outcome = 'applied' AND source_table = 'shop.orders' "
                "AND committed_at > current_timestamp - INTERVAL '15' MINUTE"
            )
            out.update(rows[0])
            rows, _ = ctx.clients.trino(
                "SELECT coalesce(sum(input_rows), 0) AS input_rows, coalesce(sum(dlq_rows), 0) AS dlq_rows, "
                "coalesce(sum(duplicates), 0) AS duplicates, coalesce(sum(applied), 0) AS applied, count(*) AS batches, "
                "max(committed_at) AS last_commit FROM lakehouse.ops.batch_commits "
                "WHERE committed_at > current_timestamp - INTERVAL '15' MINUTE"
            )
            out.update(rows[0])
        except Exception as exc:  # noqa: BLE001 - reported as a stale metric, not an API error
            out["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        return out

    return ctx.cached("overview.trino", 10, produce)


def overview(ctx) -> dict:
    probes = ctx.monitor.snapshot()
    now = _now()
    stats = _trino_window_stats(ctx)
    trino_as_of = None if stats.get("error") else stats["as_of"]
    rate, count = ctx.tracer.rate(60)
    tracer_as_of = now if ctx.tracer.running else None
    lag_total = sum(ctx.monitor.lag.values()) if ctx.monitor.lag else None
    input_rows = stats.get("input_rows") or 0
    slot = ctx.monitor.slot or {}
    metrics = [
        metric(
            "event_throughput",
            "Change events / s (Kafka, 1 min)",
            round(rate, 2),
            "events/s",
            tracer_as_of,
            "API Kafka tail on lakeflow.shop.*",
            "60s",
            None if ctx.tracer.running else ctx.tracer.error,
        ),
        metric(
            "freshness_p95",
            "p95 end-to-end freshness (orders)",
            _round(stats.get("p95")),
            "s",
            trino_as_of,
            "Trino: bronze.cdc_events (commit - source ts)",
            "15m",
            stats.get("error") or _no_data(stats.get("n")),
        ),
        metric(
            "freshness_p50",
            "p50 end-to-end freshness (orders)",
            _round(stats.get("p50")),
            "s",
            trino_as_of,
            "Trino: bronze.cdc_events (commit - source ts)",
            "15m",
            stats.get("error") or _no_data(stats.get("n")),
        ),
        metric(
            "consumer_lag",
            "Consumer lag",
            lag_total,
            "messages",
            ctx.monitor.lag_as_of,
            "Kafka end offsets - Spark checkpoint commits",
        ),
        metric(
            "slot_lag",
            "Replication slot lag",
            slot.get("lag_bytes"),
            "bytes",
            slot.get("as_of"),
            "pg_replication_slots.confirmed_flush_lsn",
        ),
        metric(
            "error_rate",
            "DLQ rate",
            round(100.0 * (stats.get("dlq_rows") or 0) / input_rows, 3) if input_rows else None,
            "%",
            trino_as_of,
            "Trino: ops.batch_commits (dlq_rows / input_rows)",
            "15m",
            stats.get("error") or (None if input_rows else "no records processed in window"),
        ),
        metric(
            "duplicates",
            "Duplicates absorbed",
            stats.get("duplicates"),
            "events",
            trino_as_of,
            "Trino: ops.batch_commits",
            "15m",
            stats.get("error"),
        ),
        metric("events_seen", "Events seen by tracer", count, "events", tracer_as_of, "API Kafka tail", "60s"),
    ]
    contract = ctx.registry.current("orders")
    target = float(contract.freshness_sla["p95_seconds"])
    observed = _round(stats.get("p95"))
    sla = [
        {
            "contract": "orders",
            "p95_target_seconds": target,
            "p95_observed_seconds": observed,
            "status": "no_data" if observed is None else ("met" if observed <= target else "breached"),
            "as_of": trino_as_of,
            "source": "Trino: bronze.cdc_events (shop.orders, 15 min)",
        }
    ]
    try:
        incidents = ctx.store.incidents(10)
    except Exception:  # noqa: BLE001
        incidents = []
    return {
        "environment": ctx.settings.environment,
        "overall": overall(probes),
        "metrics": metrics,
        "sla": sla,
        "incidents": incidents,
        "as_of": now,
    }


def _round(value, digits=2):
    return None if value is None else round(float(value), digits)


def _no_data(n):
    return "no applied events in window" if not n else None


def components(ctx) -> dict:
    probes = ctx.monitor.snapshot()
    items = [
        probes.get(name)
        or {
            "component": name,
            "label": label,
            "status": "unknown",
            "checked_at": None,
            "detail": "not probed yet",
            "source": "monitor",
            "data": {},
        }
        for name, label in COMPONENTS
    ]
    return {"overall": overall(probes), "components": items, "as_of": _now()}


def topology(ctx) -> dict:
    probes = ctx.monitor.snapshot()
    now = _now()

    def node(component, label, metrics):
        probe = probes.get(component, {})
        return {
            "id": component,
            "label": label,
            "status": probe.get("status", "unknown"),
            "detail": probe.get("detail", "not probed yet"),
            "metrics": metrics,
        }

    checked = {k: v.get("checked_at") for k, v in probes.items()}
    slot = ctx.monitor.slot or {}
    spark = probes.get("spark", {}).get("data", {})
    debezium = probes.get("debezium", {}).get("data", {})
    offsets = ctx.monitor.end_offsets
    nodes = [
        node(
            "postgres",
            "PostgreSQL WAL",
            [
                metric(
                    "current_lsn",
                    "Current WAL LSN",
                    slot.get("current_lsn"),
                    "lsn",
                    slot.get("as_of"),
                    "pg_current_wal_lsn()",
                ),
                metric(
                    "confirmed_flush_lsn",
                    "Slot confirmed LSN",
                    slot.get("confirmed_flush_lsn"),
                    "lsn",
                    slot.get("as_of"),
                    "pg_replication_slots",
                ),
            ],
        ),
        node(
            "debezium",
            "Debezium",
            [
                metric(
                    "heartbeat_age",
                    "Heartbeat age",
                    debezium.get("heartbeat_age_s"),
                    "s",
                    checked.get("debezium"),
                    "__debezium-heartbeat.lakeflow",
                ),
            ],
        ),
        node(
            "kafka",
            "Kafka",
            [
                metric(
                    f"end_offset_{t}",
                    f"{t} end offsets",
                    sum(p.values()),
                    "messages",
                    checked.get("kafka"),
                    "Kafka watermark offsets",
                )
                for t, p in sorted(offsets.items())
            ],
        ),
        node(
            "spark",
            "Spark",
            [
                metric(
                    "last_batch_id",
                    "Last batch",
                    spark.get("last_batch_id"),
                    "batch",
                    checked.get("spark"),
                    "Spark /metrics",
                ),
                metric(
                    "last_batch_age",
                    "Last batch age",
                    spark.get("last_batch_age_s"),
                    "s",
                    checked.get("spark"),
                    "Spark /metrics",
                ),
                metric(
                    "checkpoint_batch",
                    "Checkpoint committed batch",
                    spark.get("checkpoint_committed_batch"),
                    "batch",
                    checked.get("spark"),
                    "checkpoint commits/",
                ),
            ],
        ),
        node(
            "iceberg",
            "Iceberg",
            [
                metric(
                    "latest_snapshot",
                    "Latest silver.orders snapshot",
                    probes.get("iceberg", {}).get("data", {}).get("snapshot_id"),
                    "id",
                    checked.get("iceberg"),
                    'Trino: "orders$snapshots"',
                ),
            ],
        ),
        node("trino", "Trino", []),
    ]
    lag_total = sum(ctx.monitor.lag.values()) if ctx.monitor.lag else None
    edges = [
        {
            "source": "postgres",
            "target": "debezium",
            "label": "logical replication (pgoutput)",
            "lag": metric(
                "slot_lag", "Slot lag", slot.get("lag_bytes"), "bytes", slot.get("as_of"), "pg_replication_slots"
            ),
        },
        {"source": "debezium", "target": "kafka", "label": "change events (key = PK)", "lag": None},
        {
            "source": "kafka",
            "target": "spark",
            "label": "micro-batches",
            "lag": metric(
                "consumer_lag", "Consumer lag", lag_total, "messages", ctx.monitor.lag_as_of, "end offsets - checkpoint"
            ),
        },
        {"source": "spark", "target": "iceberg", "label": "idempotent MERGE / append", "lag": None},
        {"source": "iceberg", "target": "trino", "label": "REST catalog snapshots", "lag": None},
    ]
    return {"nodes": nodes, "edges": edges, "as_of": now}
