"""Table maintenance and backfill against the running data plane; the stream keeps writing during compaction."""

import threading
import time
from pathlib import Path

from support import bronze_events, insert_order, marker, pg, silver_order, trino_rows, wait_visible

from lakeflow_core.backfill import SIGNAL_INSERT, snapshot_signal
from lakeflow_core.contracts import load_registry
from lakeflow_core.maintenance import maintain_table, trino_query

ROOT = Path(__file__).resolve().parents[2]


def test_compaction_while_streaming_preserves_content():
    for _ in range(4):  # separate micro-batches -> several small files per bucket
        marker()
    stop, written = threading.Event(), []

    def keep_writing():
        while not stop.is_set():
            written.append(insert_order(status="PAID")["order_id"])
            time.sleep(1)

    writer = threading.Thread(target=keep_writing, daemon=True)
    writer.start()
    try:
        record = maintain_table(trino_query("trino", 8080, user="lakeflow-tests"), "silver.orders", runner="it")
    finally:
        stop.set()
        writer.join(timeout=30)
    assert record["status"] == "succeeded", record
    assert record["rewrite_snapshot_id"] is not None, "several small files must have been compacted"
    assert record["fingerprint_match"] is True
    summary = trino_rows(
        'SELECT summary FROM lakehouse.silver."orders$snapshots" WHERE snapshot_id = ?', [record["rewrite_snapshot_id"]]
    )[0]["summary"]
    assert int(summary["added-data-files"]) < int(summary["deleted-data-files"]), summary

    for order_id in written:  # rows written concurrently with the rewrite all arrive
        wait_visible(order_id)
    marker()
    assert trino_rows("SELECT count(*) - count(DISTINCT order_id) AS d FROM lakehouse.silver.orders")[0]["d"] == 0
    stored = trino_rows(
        "SELECT status, fingerprint_match FROM lakehouse.ops.maintenance_runs WHERE run_id = ?", [record["run_id"]]
    )
    assert stored == [{"status": "succeeded", "fingerprint_match": True}]


def test_backfill_restores_a_row_lost_from_the_lakehouse():
    order_id = insert_order(status="SHIPPED", amount="31.00")["order_id"]
    original = wait_visible(order_id)
    trino_rows("DELETE FROM lakehouse.silver.orders WHERE order_id = ?", [order_id])  # simulated lakehouse data loss
    assert silver_order(order_id) is None

    signal = snapshot_signal(load_registry(ROOT / "contracts"), "orders", keys=[order_id])
    with pg() as conn:
        conn.execute(SIGNAL_INSERT, signal)
    restored = wait_visible(order_id, timeout=240)
    assert restored["_source_op"] == "r" and not restored["is_deleted"]
    assert (restored["status"], str(restored["amount"])) == ("SHIPPED", "31.00")
    assert restored["order_id"] == original["order_id"]
    snapshot_events = [e for e in bronze_events(order_id) if e["op"] == "r"]
    assert len(snapshot_events) == 1, bronze_events(order_id)
