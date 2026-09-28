"""Data-plane integration tests against the running stack (no mocks): PostgreSQL -> Debezium -> Kafka -> Spark ->
Iceberg -> Trino. Run with `make integration-test` (tools container on the compose network)."""

import json
import time
from pathlib import Path

import httpx
import pytest
from support import (
    CONNECT,
    ORDERS_TOPIC,
    ack,
    bronze_events,
    envelope,
    eventually,
    fetch,
    insert_order,
    marker,
    pg,
    produce,
    request_control,
    silver_order,
    trino_rows,
    wait_visible,
)

from lakeflow_core.contracts import load_registry
from lakeflow_core.quality import build_checks

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def check_value(check_id: str):
    """Run one contract-generated quality check (same SQL as the Airflow DAG and the API)."""
    sql = {c.check_id: c for c in build_checks(load_registry(ROOT / "contracts"))}[check_id].sql
    return next(iter(trino_rows(sql)[0].values()))


def test_01_initial_snapshot_reconciles_with_source():
    with pg() as conn:
        source_orders = conn.execute("SELECT count(*) AS n FROM shop.orders").fetchone()["n"]
    eventually(
        lambda: (
            trino_rows("SELECT count(*) AS n FROM lakehouse.silver.orders WHERE NOT is_deleted")[0]["n"]
            >= source_orders
        ),
        timeout=420,
        what="initial Debezium snapshot in silver.orders",
    )
    assert check_value("orders.source_reconciliation") == 0
    assert check_value("customers.source_reconciliation") == 0
    assert check_value("customers.required_not_null") == 0


def test_02_insert_update_delete_carry_real_identifiers():
    created = insert_order()
    order_id = created["order_id"]
    row = wait_visible(order_id)
    assert row["status"] == "PENDING" and not row["is_deleted"]
    [event] = bronze_events(order_id)
    assert event["op"] == "c" and event["source_tx_id"] == created["txid"], (
        "Debezium txId must equal the PostgreSQL xid"
    )
    assert event["kafka_topic"] == ORDERS_TOPIC and event["kafka_offset"] >= 0 and event["apply_outcome"] == "applied"
    batch = trino_rows(
        "SELECT * FROM lakehouse.ops.batch_commits WHERE stream_epoch = ? AND batch_id = ?",
        [event["stream_epoch"], event["batch_id"]],
    )[0]
    assert json.loads(batch["snapshot_ids"])["silver.orders"] > 0

    with pg() as conn:
        conn.execute("UPDATE shop.orders SET status = 'PAID' WHERE order_id = %s", (order_id,))
    wait_visible(order_id, lambda r: r["status"] == "PAID" and r["_source_lsn"] > row["_source_lsn"])
    with pg() as conn:
        conn.execute("DELETE FROM shop.orders WHERE order_id = %s", (order_id,))
    wait_visible(order_id, lambda r: r["is_deleted"])
    events = bronze_events(order_id)
    assert [e["op"] for e in events] == ["c", "u", "d"]
    assert len({e["event_id"] for e in events}) == 3
    assert len(trino_rows("SELECT order_id FROM lakehouse.silver.orders WHERE order_id = ?", [order_id])) == 1


def test_03_redelivered_events_are_absorbed():
    order_id = insert_order()["order_id"]
    wait_visible(order_id)
    [event] = bronze_events(order_id)
    key, value = fetch(event["kafka_topic"], event["kafka_partition"], event["kafka_offset"])
    before = trino_rows("SELECT coalesce(sum(duplicates), 0) AS d FROM lakehouse.ops.batch_commits")[0]["d"]
    for _ in range(2):
        produce(value, key, {"lakeflow-injection-id": "it-dup", "lakeflow-injection-kind": "duplicate"})
    marker()
    after = trino_rows("SELECT coalesce(sum(duplicates), 0) AS d FROM lakehouse.ops.batch_commits")[0]["d"]
    assert after - before >= 2
    assert len(bronze_events(order_id)) == 1
    assert check_value("bronze.event_id_unique") == 0


def test_04_out_of_order_update_is_stale_and_late():
    order_id = insert_order(status="SHIPPED")["order_id"]
    row = wait_visible(order_id)
    image = {k: row[k] for k in ("order_id", "customer_id", "currency")}
    image.update(
        status="PENDING",
        amount=str(row["amount"]),
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
    )
    stale_lsn = row["_source_lsn"] - 1
    produce(
        envelope("u", stale_lsn, int(time.time() * 1000) - 3 * 3600 * 1000, after=image),
        json.dumps({"order_id": order_id}).encode(),
        {"lakeflow-injection-id": "it-late"},
    )
    marker()
    stale = [e for e in bronze_events(order_id) if e["source_lsn"] == stale_lsn]
    assert stale and stale[0]["apply_outcome"] == "stale" and stale[0]["is_late"]
    assert silver_order(order_id)["status"] == "SHIPPED"


def test_05_malformed_records_go_to_dlq_and_stream_continues():
    produce(b"{this is not json", b'{"order_id":"x"}', {"lakeflow-injection-id": "it-bad-json"})
    bad = envelope(
        "c",
        1,
        int(time.time() * 1000),
        after={
            "order_id": "00000000-0000-4000-8000-000000000123",
            "customer_id": "00000000-0000-4000-8000-000000000124",
            "status": "SHIPPED_TO_MARS",
            "amount": "-1.00",
            "currency": "USD",
            "created_at": "2026-09-25T00:00:00Z",
            "updated_at": "2026-09-25T00:00:00Z",
        },
    )
    produce(bad, b'{"order_id":"00000000-0000-4000-8000-000000000123"}', {"lakeflow-injection-id": "it-contract"})
    marker()
    rows = {
        r["injection_id"]: r
        for r in trino_rows(
            "SELECT injection_id, error_code, violations, status FROM lakehouse.ops.dlq_events WHERE injection_id IN ('it-bad-json', 'it-contract')"
        )
    }
    assert rows["it-bad-json"]["error_code"] == "MALFORMED_JSON"
    assert rows["it-contract"]["error_code"] == "CONTRACT_VIOLATION"
    assert set(rows["it-contract"]["violations"]) == {"status.enum", "amount.pattern"}


def test_06_crash_after_iceberg_commit_replays_without_duplicates():
    request_id = request_control("crash_after_commit")
    eventually(lambda: ack(request_id), timeout=30, what="crash request armed")
    order_id = insert_order()["order_id"]
    fired = eventually(
        lambda: (a := ack(request_id)) and a.get("batch_id") is not None and a, timeout=240, what="crash after commit"
    )
    marker(timeout=420)
    [event] = bronze_events(order_id)
    assert len(trino_rows("SELECT order_id FROM lakehouse.silver.orders WHERE order_id = ?", [order_id])) == 1
    batch = trino_rows(
        "SELECT attempts FROM lakehouse.ops.batch_commits WHERE stream_epoch = ? AND batch_id = ?",
        [event["stream_epoch"], fired["batch_id"]],
    )
    assert batch and batch[0]["attempts"] >= 2, "the crashed batch must have been replayed"
    assert check_value("orders.primary_key_unique") == 0
    assert check_value("bronze.event_id_unique") == 0


def test_07_connector_restart_resumes_from_its_offset():
    response = httpx.post(f"{CONNECT}/connectors/lakeflow-cdc/restart", params={"includeTasks": "true"}, timeout=30)
    assert response.status_code in (200, 202, 204)
    eventually(
        lambda: (
            httpx.get(f"{CONNECT}/connectors/lakeflow-cdc/status", timeout=10).json()["tasks"][0]["state"] == "RUNNING"
        ),
        timeout=120,
        what="connector RUNNING after restart",
    )
    marker(timeout=300)
    assert check_value("bronze.event_id_unique") == 0


def test_08_schema_evolution_to_contract_v2():
    with pg() as conn:
        conn.execute((ROOT / "platform" / "postgres" / "migrations" / "001_orders_channel.sql").read_text())
    order_id = insert_order(channel="web")["order_id"]
    row = wait_visible(order_id)
    assert row["channel"] == "web" and row["_contract_version"] == 2


def test_09_contract_drift_dlq_then_promotion_and_replay():
    """JPY is accepted by the source but not by contract v2; promoting proposed v3 and replaying fixes it."""
    order_id = insert_order(currency="JPY")["order_id"]
    rejected = eventually(
        lambda: trino_rows(
            "SELECT dlq_id FROM lakehouse.ops.dlq_events WHERE error_code = 'CONTRACT_VIOLATION' AND strpos(raw_key, ?) > 0",
            [order_id],
        ),
        what="JPY order in the DLQ",
    )
    assert silver_order(order_id) is None
    folder = ROOT / "contracts" / "orders"
    backup = {p: p.read_text(encoding="utf-8") for p in folder.glob("v*.schema.json")}
    proposed = folder / "proposed" / "v3.schema.json"
    proposed_text = proposed.read_text(encoding="utf-8")
    try:
        for path, text in backup.items():
            doc = json.loads(text)
            if doc["x-lakeflow"]["status"] == "current":
                doc["x-lakeflow"]["status"] = "superseded"
                path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        doc = json.loads(proposed_text)
        doc["x-lakeflow"]["status"] = "current"
        (folder / "v3.schema.json").write_text(json.dumps(doc, indent=2), encoding="utf-8")
        row = trino_rows(
            "SELECT dlq_id, source_topic, raw_key, raw_value FROM lakehouse.ops.dlq_events WHERE dlq_id = ?",
            [rejected[0]["dlq_id"]],
        )[0]
        produce(
            row["raw_value"].encode(),
            row["raw_key"].encode(),
            {"lakeflow-replay-of": row["dlq_id"], "lakeflow-original-topic": row["source_topic"]},
            topic="lakeflow.replay.cdc",
        )
        replayed = wait_visible(order_id)
        assert replayed["currency"] == "JPY" and replayed["_contract_version"] == 3
        status = trino_rows(
            "SELECT status, replay_attempts FROM lakehouse.ops.dlq_events WHERE dlq_id = ?", [row["dlq_id"]]
        )[0]
        assert status["status"] == "replayed" and status["replay_attempts"] == 1
    finally:
        (folder / "v3.schema.json").unlink(missing_ok=True)
        for path, text in backup.items():
            path.write_text(text, encoding="utf-8")


def test_10_final_reconciliation_and_uniqueness():
    marker()
    for check_id in (
        "orders.primary_key_unique",
        "customers.primary_key_unique",
        "bronze.event_id_unique",
        "orders.status_enum",
        "orders.required_not_null",
    ):
        assert check_value(check_id) == 0, check_id
