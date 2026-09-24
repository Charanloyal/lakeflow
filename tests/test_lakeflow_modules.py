"""Unit and Integration Tests for LakeFlow Subsystems:
- lakeflow.ingestion
- lakeflow.streaming
- lakeflow.lakehouse
- lakeflow.analytics
- lakeflow.api
- lakeflow.benchmarks
"""

import pytest
from lakeflow.ingestion.cdc_source import PostgreSQLWALSource, CDCEvent
from lakeflow.ingestion.debezium import DebeziumConnectorManager
from lakeflow.ingestion.schema_registry import SchemaEvolutionRegistry
from lakeflow.streaming.deduplicator import CDCDeduplicator
from lakeflow.streaming.checkpoint_manager import CheckpointManager
from lakeflow.lakehouse.iceberg_catalog import IcebergCatalogManager
from lakeflow.lakehouse.snapshot_manager import SnapshotManager
from lakeflow.lakehouse.compaction import IcebergCompactionJob
from lakeflow.analytics.trino_client import TrinoQueryClient
from lakeflow.analytics.query_optimizer import StrategyBenchmarkComparison
from fastapi.testclient import TestClient
from lakeflow.api.main import app


def test_ingestion_wal_source_and_heartbeat():
    wal = PostgreSQLWALSource(heartbeat_interval_ms=5000)
    hb = wal.generate_heartbeat()
    assert hb["type"] == "HEARTBEAT"
    assert hb["confirmed_flush_lsn"] > 25040000

    mutation = wal.emit_cdc_mutation(
        table_name="customers",
        record_key="c_001",
        op="c",
        after_state={"name": "Alice"},
    )
    assert mutation.cdc_op == "c"
    assert mutation.record_key == "c_001"
    assert mutation.source_lsn > hb["confirmed_flush_lsn"]


def test_debezium_config_hardening():
    mgr = DebeziumConnectorManager()
    cfg = mgr.get_connector_config()
    assert cfg["config"]["heartbeat.interval.ms"] == "5000"
    assert cfg["config"]["tombstones.on.delete"] == "true"


def test_streaming_deduplication():
    dedup = CDCDeduplicator()
    batch = [
        {"record_key": "k1", "source_lsn": 100, "data": "first"},
        {"record_key": "k1", "source_lsn": 100, "data": "dup"},
        {"record_key": "k1", "source_lsn": 101, "data": "newer"},
    ]
    res = dedup.deduplicate_batch(batch)
    assert len(res) == 2


def test_lakehouse_snapshot_and_compaction():
    snap_mgr = SnapshotManager("silver_orders")
    s1 = snap_mgr.commit_snapshot(added_records=500)
    assert s1["snapshot_id"] > 1000
    assert s1["summary"]["total_records"] == 500

    comp = IcebergCompactionJob()
    comp_res = comp.run_compaction("silver_orders", input_files_count=18)
    assert comp_res["status"] == "COMPLETED"
    assert comp_res["output_files_created"] == 3


def test_analytics_query_speedup():
    comp = StrategyBenchmarkComparison.get_comparison()
    assert comp["speedup_factor"] >= 75.0
    assert comp["strategy_b"]["measured_latency_ms"] < 5.0


def test_lakeflow_api():
    client = TestClient(app)
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["service"] == "LakeFlow"

    events_res = client.get("/events")
    assert events_res.status_code == 200
    assert events_res.json()["count"] >= 5
