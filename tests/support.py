"""Helpers for integration/e2e suites running on the compose network (tools container)."""

from __future__ import annotations

import base64
import json
import os
import time
import uuid

import httpx
import psycopg
import trino
from confluent_kafka import Consumer, Producer, TopicPartition
from psycopg.rows import dict_row

PG_DSN = os.environ.get("LAKEFLOW_PG_DSN", "")
KAFKA = os.environ.get("LAKEFLOW_KAFKA_BOOTSTRAP", "kafka:9092")
CONNECT = os.environ.get("LAKEFLOW_CONNECT_URL", "http://connect:8083")
API = os.environ.get("LAKEFLOW_API_URL", "http://api:8000")
CONTROL_DIR = os.environ.get("LAKEFLOW_CONTROL_DIR", "/control")
ORDERS_TOPIC = "lakeflow.shop.orders"
ADMIN = (os.environ.get("LAKEFLOW_ADMIN_USER", "admin"), os.environ.get("LAKEFLOW_ADMIN_PASSWORD", ""))
VIEWER = (os.environ.get("LAKEFLOW_VIEWER_USER", "viewer"), os.environ.get("LAKEFLOW_VIEWER_PASSWORD", ""))


def eventually(fn, timeout: float = 240, interval: float = 2, what: str = "condition"):
    deadline, last_error = time.time() + timeout, None
    while time.time() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001 - keep polling, report the last error on timeout
            last_error = exc
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}; last error: {last_error!r}")


def trino_rows(sql: str, params=None) -> list[dict]:
    conn = trino.dbapi.connect(
        host=os.environ.get("LAKEFLOW_TRINO_HOST", "trino"),
        port=int(os.environ.get("LAKEFLOW_TRINO_PORT", "8080")),
        user="lakeflow-tests",
        catalog="lakehouse",
        schema="silver",
    )
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()  # also drives DML/DDL to completion
        names = [d[0] for d in cursor.description or []]
        return [dict(zip(names, row)) for row in rows] if names else []
    finally:
        conn.close()


def pg():
    return psycopg.connect(PG_DSN, row_factory=dict_row)


def insert_order(status="PENDING", amount="19.99", currency="USD", channel=None) -> dict:
    with pg() as conn:
        columns = "customer_id, status, amount, currency" + (", channel" if channel else "")
        values = [status, amount, currency] + ([channel] if channel else [])
        placeholders = ", ".join(["%s"] * (len(values)))
        row = conn.execute(
            f"INSERT INTO shop.orders ({columns}) SELECT customer_id, {placeholders} FROM shop.customers "  # noqa: S608
            "ORDER BY customer_id LIMIT 1 RETURNING order_id::text",
            values,
        ).fetchone()
        txid = conn.execute("SELECT (pg_current_xact_id()::text::bigint & 4294967295) AS txid").fetchone()["txid"]
    return {"order_id": row["order_id"], "txid": txid}


def silver_order(order_id: str) -> dict | None:
    rows = trino_rows("SELECT * FROM lakehouse.silver.orders WHERE order_id = ?", [order_id])
    return rows[0] if rows else None


def bronze_events(order_id: str) -> list[dict]:
    return trino_rows(
        "SELECT * FROM lakehouse.bronze.cdc_events WHERE source_table = 'shop.orders' AND primary_key = ? "
        "ORDER BY source_lsn",
        [order_id],
    )


def wait_visible(order_id: str, predicate=lambda row: True, timeout: float = 240) -> dict:
    """Silver commits before bronze within a batch, so also wait for the batch record (its last write)."""
    row = eventually(
        lambda: (row := silver_order(order_id)) and predicate(row) and row,
        timeout=timeout,
        what=f"order {order_id} in silver",
    )
    eventually(
        lambda: trino_rows(
            "SELECT 1 AS ok FROM lakehouse.ops.batch_commits WHERE stream_epoch = ? AND batch_id = ?",
            [row["_stream_epoch"], row["_batch_id"]],
        ),
        timeout=60,
        what=f"batch {row['_batch_id']} commit record",
    )
    return row


def marker(timeout: float = 240) -> str:
    """A fresh real change: once it is visible, every record produced before it has been processed."""
    order_id = insert_order(status="PAID")["order_id"]
    wait_visible(order_id, timeout=timeout)
    return order_id


def produce(value: bytes | None, key: bytes | None, headers: dict[str, str], topic: str = ORDERS_TOPIC) -> None:
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    producer.produce(topic, key=key, value=value, headers=[(k, v.encode()) for k, v in headers.items()])
    assert producer.flush(15) == 0


def fetch(topic: str, partition: int, offset: int) -> tuple[bytes, bytes]:
    consumer = Consumer(
        {"bootstrap.servers": KAFKA, "group.id": f"tests-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False}
    )
    try:
        consumer.assign([TopicPartition(topic, partition, offset)])
        deadline = time.time() + 20
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is not None and not message.error() and message.offset() == offset:
                return message.key(), message.value()
        raise AssertionError(f"could not read {topic}/{partition}/{offset}")
    finally:
        consumer.close()


def envelope(op, lsn, ts_ms, before=None, after=None) -> bytes:
    source = {
        "version": "tests",
        "connector": "postgresql",
        "name": "lakeflow",
        "db": "lakeflow",
        "schema": "shop",
        "table": "orders",
        "lsn": lsn,
        "txId": 1,
        "ts_ms": ts_ms,
        "snapshot": "false",
    }
    return json.dumps({"before": before, "after": after, "source": source, "op": op, "ts_ms": ts_ms}).encode()


def request_control(action: str) -> str:
    request_id = f"test-{uuid.uuid4().hex[:10]}"
    path = os.path.join(CONTROL_DIR, "requests", f"{request_id}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"id": request_id, "action": action, "requested_by": "tests"}, fh)
    os.replace(tmp, path)
    return request_id


def ack(request_id: str) -> dict | None:
    path = os.path.join(CONTROL_DIR, "acks", f"{request_id}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return None


def api(auth=ADMIN) -> httpx.Client:
    token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
    return httpx.Client(base_url=API, headers={"Authorization": f"Basic {token}"}, timeout=60)
