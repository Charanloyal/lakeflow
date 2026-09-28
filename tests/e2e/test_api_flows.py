"""End-to-end scenarios through the control-plane API against the running stack (tools container)."""

import pytest
from support import ADMIN, VIEWER, api, eventually

pytestmark = pytest.mark.e2e


def trace(client, order_id):
    response = client.get(f"/api/trace/orders/{order_id}")
    assert response.status_code == 200, response.text
    return response.json()


def stages(body):
    return {s["stage"]: s for s in body["stages"]}


def test_overview_metrics_have_sources_and_timestamps():
    with api(VIEWER) as client:
        body = client.get("/api/metrics/overview").json()
    assert body["metrics"]
    for metric in body["metrics"]:
        assert metric["source"]
        assert metric["as_of"] is not None or metric["stale"], metric


def test_viewer_is_read_only():
    with api(VIEWER) as client:
        assert client.post("/api/demo/orders", json={"amount": "1.00"}).status_code == 403
        assert client.post("/api/recovery/actions", json={"action": "crash_now"}).status_code == 403


def test_insert_update_delete_trace_with_real_identifiers():
    with api(ADMIN) as client:
        created = client.post("/api/demo/orders", json={"amount": "42.00", "currency": "EUR", "status": "PENDING"})
        assert created.status_code == 201, created.text
        order_id = created.json()["order_id"]
        hops = ("postgres", "debezium", "kafka", "spark", "iceberg", "trino")
        body = eventually(
            lambda: (t := trace(client, order_id)) and all(stages(t)[h]["status"] == "done" for h in hops) and t,
            what="insert traced through every hop",
        )
        s = stages(body)
        assert s["kafka"]["details"]["topic"] == "lakeflow.shop.orders"
        assert isinstance(s["kafka"]["details"]["offset"], int)
        assert isinstance(s["iceberg"]["details"]["snapshot_id"], int)
        assert s["postgres"]["details"]["tx_id"] == created.json()["txid"]
        assert body["freshness_ms"] is not None and body["freshness_ms"] >= 0
        lsn = body["final_state"]["_source_lsn"]

        assert client.patch(f"/api/demo/orders/{order_id}", json={"status": "PAID"}).status_code == 200
        eventually(lambda: trace(client, order_id)["final_state"]["_source_lsn"] > lsn, what="update visible")
        assert client.delete(f"/api/demo/orders/{order_id}").status_code == 200
        final = eventually(
            lambda: (t := trace(client, order_id))["final_state"]["is_deleted"] and t, what="delete visible"
        )
        assert [e["op"] for e in final["events"]] == ["c", "u", "d"]
        eventually(lambda: stages(trace(client, order_id))["checkpoint"]["status"] == "done", what="checkpoint commit")


def test_recovery_lab_duplicates_malformed_and_late():
    with api(ADMIN) as client:
        seed = client.post("/api/demo/orders", json={"amount": "7.00"}).json()["order_id"]
        eventually(lambda: stages(trace(client, seed))["trino"]["status"] == "done", what="seed order visible")
        for action in (
            {"action": "inject_duplicates", "count": 2},
            {"action": "inject_malformed", "count": 2, "kind": "contract_violation"},
            {"action": "inject_late", "count": 1},
        ):
            response = client.post("/api/recovery/actions", json=action)
            assert response.status_code == 202, response.text
        marker = client.post("/api/demo/orders", json={"amount": "8.00"}).json()["order_id"]
        eventually(lambda: stages(trace(client, marker))["trino"]["status"] == "done", what="marker after injections")
        eventually(
            lambda: any(
                r["injection_id"] and r["error_code"] == "CONTRACT_VIOLATION"
                for r in client.get("/api/quality/rejected?limit=50").json()["records"]
            ),
            what="injected contract violations in the DLQ",
        )
        eventually(
            lambda: any(
                e["injection_id"] and e["is_late"]
                for e in client.get("/api/events?outcome=stale&minutes=30").json()["events"]
            ),
            what="injected late event stored as stale",
        )
        results = client.post(
            "/api/quality/run", json={"checks": ["bronze.event_id_unique", "orders.primary_key_unique"]}
        )
        assert all(r["status"] == "pass" for r in results.json()["results"]), results.text


def test_crash_after_commit_is_recovered_without_duplicates():
    with api(ADMIN) as client:
        armed = client.post("/api/recovery/actions", json={"action": "crash_after_commit"})
        assert armed.status_code == 202 and armed.json()["status"] == "requested"
        request_id = armed.json()["id"]
        order_id = client.post("/api/demo/orders", json={"amount": "13.00"}).json()["order_id"]
        ack = eventually(
            lambda: next(
                (
                    a
                    for a in client.get("/api/recovery/actions").json()["acks"]
                    if a.get("id") == request_id and a.get("batch_id") is not None
                ),
                None,
            ),
            timeout=300,
            what="Spark to crash after an Iceberg commit",
        )
        replayed = eventually(
            lambda: next(
                (
                    b
                    for b in client.get("/api/metrics/batches?limit=100").json()["batches"]
                    if b["batch_id"] == ack["batch_id"] and b["attempts"] >= 2
                ),
                None,
            ),
            timeout=420,
            what=f"batch {ack['batch_id']} replayed after the restart",
        )
        assert replayed["attempts"] >= 2
        body = eventually(
            lambda: (t := trace(client, order_id)) and stages(t)["checkpoint"]["status"] == "done" and t,
            timeout=300,
            what="order committed after the restart",
        )
        assert len([e for e in body["events"] if e["op"] == "c"]) == 1
        results = client.post(
            "/api/quality/run",
            json={"checks": ["bronze.event_id_unique", "orders.primary_key_unique", "orders.source_reconciliation"]},
        )
        assert all(r["status"] == "pass" for r in results.json()["results"]), results.text


def test_contracts_lineage_benchmarks_and_adrs_are_served():
    with api(VIEWER) as client:
        schema = client.get("/api/quality/schema").json()
        assert {c["version"] for c in schema["contracts"] if c["contract"] == "orders"} >= {1, 2, 3}
        impact = client.get("/api/lineage/impact", params={"dataset": "postgres.shop.orders"}).json()
        assert "lakehouse.silver.orders" in impact["affected_datasets"]
        assert client.get("/api/benchmarks/runs").status_code == 200
        assert len(client.get("/api/architecture/adrs").json()["adrs"]) >= 5
        assert client.get("/api/topology").json()["nodes"]
