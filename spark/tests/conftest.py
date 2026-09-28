import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from lakeflow_core import tables as T
from lakeflow_core.contracts import load_registry
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakeflow_stream.config import Settings
from lakeflow_stream.metrics import PipelineMetrics
from lakeflow_stream.session import build_local_test_session
from lakeflow_stream.sink import SinkContext, ensure_tables

ROOT = Path(__file__).resolve().parents[2]
ICEBERG_PACKAGES = os.environ.get("ICEBERG_PACKAGES", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2")
KAFKA_SCHEMA = StructType([
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("timestamp", TimestampType()),
    StructField("timestampType", IntegerType()),
    StructField("headers", ArrayType(StructType([StructField("key", StringType()), StructField("value", BinaryType())]))),
])
ORDERS_TOPIC = "lakeflow.shop.orders"
CUSTOMERS_TOPIC = "lakeflow.shop.customers"
REPLAY_TOPIC = "lakeflow.replay.cdc"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = build_local_test_session(str(tmp_path_factory.mktemp("warehouse")), packages=ICEBERG_PACKAGES)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def ctx(spark, tmp_path):
    registry = load_registry(ROOT / "contracts")
    for name in [T.BRONZE_TABLE, T.DLQ_TABLE, T.BATCH_TABLE] + [registry.current(n).target_table for n in registry.names]:
        spark.sql(f"DROP TABLE IF EXISTS lakehouse.{name} PURGE")
    ensure_tables(spark, registry, "lakehouse")
    settings = Settings(contracts_dir=str(ROOT / "contracts"), pii_key=b"spark-test-key", allowed_lateness_ms=60_000)
    return SinkContext(spark=spark, settings=settings, registry=registry, epoch="testepoch", app_id=spark.sparkContext.applicationId,
                       metrics=PipelineMetrics(enabled=False))


def order(order_id, status="PENDING", amount="10.00", **extra):
    row = {"order_id": order_id, "customer_id": "0b7e7c1e-2f7a-4c55-8f0e-6a1d2c3b4a59", "status": status, "amount": amount,
           "currency": "USD", "created_at": "2026-09-25T10:00:00Z", "updated_at": "2026-09-25T10:00:00Z"}
    row.update(extra)
    return row


def envelope(op, lsn, before=None, after=None, table="orders", ts_ms=None, tx=1):
    source = {"version": "2.7.3.Final", "connector": "postgresql", "name": "lakeflow", "db": "lakeflow", "schema": "shop",
              "table": table, "lsn": lsn, "txId": tx, "ts_ms": ts_ms if ts_ms is not None else 1_790_000_000_000 + lsn,
              "snapshot": "false"}
    return json.dumps({"before": before, "after": after, "source": source, "op": op, "ts_ms": 1_790_000_000_000}).encode()


class Batch:
    """Builds a DataFrame shaped exactly like the Spark Kafka source (includeHeaders=true)."""

    def __init__(self, spark):
        self.spark = spark
        self.rows = []
        self.next_offset = {}

    def add(self, value, key=None, topic=ORDERS_TOPIC, partition=0, headers=None, offset=None):
        if offset is None:
            offset = self.next_offset.get((topic, partition), 0)
        self.next_offset[(topic, partition)] = offset + 1
        hdrs = [(k, v.encode()) for k, v in (headers or {}).items()] or None
        self.rows.append((key, value, topic, partition, offset, datetime.now(timezone.utc), 0, hdrs))
        return offset

    def change(self, op, pk, lsn, image=None, table="orders", ts_ms=None, **kw):
        before = image if op == "d" else None
        after = None if op == "d" else image
        pk_name = "order_id" if table == "orders" else "customer_id"
        key = json.dumps({pk_name: pk}).encode()
        topic = ORDERS_TOPIC if table == "orders" else CUSTOMERS_TOPIC
        return self.add(envelope(op, lsn, before, after, table=table, ts_ms=ts_ms), key=key, topic=topic, **kw)

    def df(self):
        return self.spark.createDataFrame(self.rows, KAFKA_SCHEMA)
