"""Background health monitor: probes every component, computes lag, records incidents on state changes."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from prometheus_client import Gauge

from .domain import promtext
from .domain.checkpoint import CheckpointState, consumer_lag, read_checkpoint

log = logging.getLogger("lakeflow.monitor")

COMPONENTS = (
    ("postgres", "PostgreSQL 16 (source)"),
    ("debezium", "Debezium connector"),
    ("kafka", "Kafka (KRaft)"),
    ("spark", "Spark Structured Streaming"),
    ("iceberg", "Iceberg (REST catalog + MinIO)"),
    ("trino", "Trino"),
)
COMPONENT_UP = Gauge("lakeflow_component_up", "1 if the component probe is healthy", ["component"])
CONSUMER_LAG = Gauge("lakeflow_consumer_lag_messages", "Kafka end offset minus Spark committed offset", ["topic"])
SLOT_LAG = Gauge("lakeflow_replication_slot_lag_bytes", "WAL bytes retained for the Debezium slot")
HEARTBEAT_AGE = Gauge("lakeflow_debezium_heartbeat_age_seconds", "Seconds since the last Debezium heartbeat")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Monitor:
    def __init__(self, settings, clients, tracer, store, interval_s: float = 10.0):
        self.settings, self.clients, self.tracer, self.store = settings, clients, tracer, store
        self.interval_s = interval_s
        self.results: dict[str, dict] = {}
        self.end_offsets: dict[str, dict[str, int]] = {}
        self.checkpoint: CheckpointState | None = None
        self.lag: dict[str, int] = {}
        self.lag_as_of: datetime | None = None
        self.slot: dict = {}
        self.spark: list = []
        self._lock = threading.Lock()

    def start(self) -> None:
        threading.Thread(target=self._loop, name="lakeflow-monitor", daemon=True).start()

    def _loop(self) -> None:
        while True:
            try:
                self.probe_all()
            except Exception:  # noqa: BLE001 - the monitor must keep running
                log.exception("monitor cycle failed")
            time.sleep(self.interval_s)

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return dict(self.results)

    def probe_all(self) -> None:
        results = {}
        for name, label in COMPONENTS:
            started = time.perf_counter()
            try:
                status, detail, data = getattr(self, f"_probe_{name}")()
            except Exception as exc:  # noqa: BLE001 - a failed probe means "down"
                status, detail, data = "down", f"{type(exc).__name__}: {str(exc)[:300]}", {}
            results[name] = {
                "component": name, "label": label, "status": status, "checked_at": _now(),
                "latency_ms": round((time.perf_counter() - started) * 1000, 1), "detail": detail,
                "source": PROBE_SOURCES[name], "data": data,
            }
            COMPONENT_UP.labels(component=name).set(1 if status == "healthy" else 0)
        previous = self.snapshot()
        with self._lock:
            self.results = results
        for name, result in results.items():
            before = previous.get(name, {}).get("status")
            if result["status"] in ("down", "degraded") and before != result["status"]:
                self._safe(self.store.open_incident, name, "critical" if result["status"] == "down" else "warning",
                           f"{result['label']} {result['status']}", result["detail"], result["source"])
            elif result["status"] == "healthy" and before in ("down", "degraded"):
                self._safe(self.store.resolve_incidents, name)

    @staticmethod
    def _safe(fn, *args) -> None:
        try:
            fn(*args)
        except Exception:  # noqa: BLE001 - the control DB being down must not break probing
            log.warning("could not record incident", exc_info=True)

    # ------------------------------------------------------------------------------------------ probes
    def _probe_postgres(self):
        with self.clients.source() as conn:
            row = conn.execute(
                "SELECT slot_name, active, confirmed_flush_lsn::text AS confirmed_flush_lsn, "
                "pg_current_wal_lsn()::text AS current_lsn, "
                "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag_bytes, "
                "wal_status FROM pg_replication_slots WHERE slot_name = 'lakeflow_cdc'"
            ).fetchone()
        if row is None:
            return "degraded", "replication slot lakeflow_cdc does not exist yet (connector not started)", {}
        self.slot = {**row, "as_of": _now()}
        SLOT_LAG.set(row["lag_bytes"] or 0)
        if not row["active"]:
            return "degraded", f"slot inactive; {row['lag_bytes']} WAL bytes retained", dict(row)
        return "healthy", f"slot active, lag {row['lag_bytes']} bytes, wal_status={row['wal_status']}", dict(row)

    def _probe_debezium(self):
        status = self.clients.connector_status()
        connector = status.get("connector", {}).get("state")
        tasks = [t.get("state") for t in status.get("tasks", [])]
        heartbeat = self.tracer.last_heartbeat_ms if self.tracer else None
        age = None if heartbeat is None else max(time.time() - heartbeat / 1000, 0)
        if age is not None:
            HEARTBEAT_AGE.set(age)
        data = {"connector": connector, "tasks": tasks, "heartbeat_age_s": None if age is None else round(age, 1)}
        if connector == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            if age is not None and age > 60:
                return "degraded", f"running but no heartbeat for {age:.0f}s", data
            return "healthy", "connector and task RUNNING", data
        if "FAILED" in tasks or connector == "FAILED":
            return "down", f"connector {connector}, tasks {tasks}", data
        return "degraded", f"connector {connector}, tasks {tasks}", data

    def _probe_kafka(self):
        topics = self.settings.cdc_topics + (self.settings.replay_topic,)
        offsets = self.clients.end_offsets(topics)
        self.end_offsets = offsets
        missing = [t for t in topics if t not in offsets]
        if missing:
            return "degraded", f"missing topics {missing}", {"end_offsets": offsets}
        return "healthy", f"{sum(len(p) for p in offsets.values())} partitions reachable", {"end_offsets": offsets}

    def _probe_spark(self):
        text = self.clients.http.get(self.settings.spark_metrics_url).text
        samples = promtext.parse(text)
        self.spark = samples
        try:
            self.checkpoint = read_checkpoint(self.settings.checkpoint_dir)
        except OSError:
            self.checkpoint = None
        if self.checkpoint and self.end_offsets:
            self.lag = consumer_lag(
                {t: p for t, p in self.end_offsets.items() if t in self.settings.cdc_topics}, self.checkpoint.committed_offsets
            )
            self.lag_as_of = _now()
            for topic, lag in self.lag.items():
                CONSUMER_LAG.labels(topic=topic).set(lag)
        active = promtext.value(samples, "lakeflow_stream_query_active")
        last_ts = promtext.value(samples, "lakeflow_stream_last_batch_timestamp_seconds")
        data = {
            "last_batch_id": promtext.value(samples, "lakeflow_stream_last_batch_id"),
            "last_batch_age_s": None if not last_ts else round(time.time() - last_ts, 1),
            "checkpoint_committed_batch": self.checkpoint.latest_committed_batch if self.checkpoint else None,
            "checkpoint_in_flight": self.checkpoint.in_flight if self.checkpoint else None,
            "consumer_lag": self.lag,
        }
        if active != 1:
            return "down", "streaming query not active", data
        if sum(self.lag.values()) > 50_000:
            return "degraded", f"consumer lag {sum(self.lag.values())} messages", data
        return "healthy", f"query active, last batch {data['last_batch_id']}", data

    def _probe_iceberg(self):
        rows, _ = self.clients.trino(
            'SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."orders$snapshots" '
            "ORDER BY committed_at DESC LIMIT 1"
        )
        if not rows:
            return "degraded", "silver.orders has no snapshots yet", {}
        return "healthy", f"latest silver.orders snapshot {rows[0]['snapshot_id']}", rows[0]

    def _probe_trino(self):
        info = self.clients.http.get(f"http://{self.settings.trino_host}:{self.settings.trino_port}/v1/info").json()
        if info.get("starting"):
            return "degraded", "Trino is starting", info
        return "healthy", f"Trino {info.get('nodeVersion', {}).get('version')} ready", {"version": info.get("nodeVersion")}


PROBE_SOURCES = {
    "postgres": "pg_replication_slots",
    "debezium": "Kafka Connect REST /connectors/lakeflow-cdc/status + heartbeat topic",
    "kafka": "Kafka metadata + watermark offsets",
    "spark": "Spark driver /metrics + checkpoint offsets/commits",
    "iceberg": 'Trino: silver."orders$snapshots"',
    "trino": "Trino /v1/info",
}


def overall(results: dict[str, dict]) -> str:
    statuses = [r["status"] for r in results.values()]
    if not statuses:
        return "unknown"
    if "down" in statuses:
        return "down"
    if "degraded" in statuses or "unknown" in statuses:
        return "degraded"
    return "healthy"
