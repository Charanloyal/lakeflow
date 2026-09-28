import json
import random

from conftest import ORDERS_TOPIC, REPLAY_TOPIC, Batch, envelope, order

from lakeflow_core.semantics import ChangeEvent, apply_plan, plan_batch
from lakeflow_stream.sink import process_batch

A = "3f2b8f7e-5d1a-4c9b-9f60-1a2b3c4d5e6f"
B = "9b2f1c3d-7e4a-4b5c-8d6e-0f1a2b3c4d5e"


def silver(spark, table="silver.orders"):
    return {
        r["order_id" if "orders" in table else "customer_id"]: r.asDict()
        for r in spark.table(f"lakehouse.{table}").collect()
    }


def bronze(spark):
    return {r["event_id"]: r.asDict() for r in spark.table("lakehouse.bronze.cdc_events").collect()}


def batch_record(spark, batch_id):
    return spark.sql(f"SELECT * FROM lakehouse.ops.batch_commits WHERE batch_id = {batch_id}").collect()[0].asDict()


def test_insert_update_delete_round_trip(ctx, spark):
    b = Batch(spark)
    b.change("c", A, 100, order(A))
    b.change("u", A, 101, order(A, status="PAID"))
    b.change("c", B, 102, order(B))
    record = process_batch(ctx, b.df(), 0)
    assert (record["applied"], record["superseded"], record["valid_rows"]) == (2, 1, 3)
    rows = silver(spark)
    assert rows[A]["status"] == "PAID" and not rows[A]["is_deleted"]
    assert str(rows[A]["amount"]) == "10.00"

    b = Batch(spark)
    b.change("d", A, 103, order(A, status="PAID"), offset=10)
    process_batch(ctx, b.df(), 1)
    rows = silver(spark)
    assert rows[A]["is_deleted"] and rows[A]["_source_lsn"] == 103 and rows[A]["_prev_source_lsn"] == 101
    assert not rows[B]["is_deleted"]
    assert len(bronze(spark)) == 4
    assert json.loads(batch_record(spark, 1)["snapshot_ids"])["silver.orders"]


def test_duplicates_are_stored_once(ctx, spark):
    b = Batch(spark)
    value = envelope("c", 100, after=order(A))
    b.add(value, key=json.dumps({"order_id": A}).encode())
    b.add(value, key=json.dumps({"order_id": A}).encode(), partition=1)
    first = process_batch(ctx, b.df(), 0)
    assert first["duplicates"] == 1
    b = Batch(spark)
    b.add(value, key=json.dumps({"order_id": A}).encode(), offset=50)
    second = process_batch(ctx, b.df(), 1)
    assert second["duplicates"] == 1 and second["applied"] == 0
    assert len(bronze(spark)) == 1
    assert len(silver(spark)) == 1


def test_replayed_batch_after_crash_is_idempotent(ctx, spark):
    """Spark re-runs a batch when Iceberg committed but the checkpoint commit marker was never written."""
    b = Batch(spark)
    b.change("c", A, 100, order(A))
    b.change("u", A, 101, order(A, status="SHIPPED"))
    b.change("c", B, 102, order(B))
    b.add(b"{broken", key=b"x")
    df = b.df()
    first = process_batch(ctx, df, 7)
    silver_before, bronze_before = silver(spark), bronze(spark)
    dlq_before = spark.table("lakehouse.ops.dlq_events").count()
    replay = process_batch(ctx, df, 7)
    assert silver(spark) == silver_before
    assert bronze(spark) == bronze_before
    assert spark.table("lakehouse.ops.dlq_events").count() == dlq_before == 1
    assert (replay["applied"], replay["superseded"]) == (first["applied"], first["superseded"])
    assert batch_record(spark, 7)["attempts"] == 2


def test_out_of_order_event_is_stale(ctx, spark):
    b = Batch(spark)
    b.change("u", A, 200, order(A, status="DELIVERED"))
    process_batch(ctx, b.df(), 0)
    b = Batch(spark)
    b.change("u", A, 150, order(A, status="PENDING"), ts_ms=1_000, offset=5)
    record = process_batch(ctx, b.df(), 1)
    assert record["stale"] == 1 and record["late_rows"] == 1
    assert silver(spark)[A]["status"] == "DELIVERED"
    late = [r for r in bronze(spark).values() if r["source_lsn"] == 150][0]
    assert late["apply_outcome"] == "stale" and late["is_late"]


def test_invalid_records_go_to_dlq_and_replay_resolves_them(ctx, spark):
    b = Batch(spark)
    b.add(b"{not json", key=b"k")
    bad_offset = b.add(envelope("c", 300, after=order(A, amount="-5.00")), key=json.dumps({"order_id": A}).encode())
    b.add(envelope("t", 301), key=None)
    b.add(None, key=json.dumps({"order_id": A}).encode())
    record = process_batch(ctx, b.df(), 0)
    assert (record["dlq_rows"], record["tombstones"], record["valid_rows"]) == (3, 1, 0)
    dlq = {r["error_code"]: r.asDict() for r in spark.table("lakehouse.ops.dlq_events").collect()}
    assert set(dlq) == {"MALFORMED_JSON", "CONTRACT_VIOLATION", "UNSUPPORTED_OP"}
    violation = dlq["CONTRACT_VIOLATION"]
    assert violation["violations"] == ["amount.pattern"] and violation["kafka_offset"] == bad_offset

    b = Batch(spark)
    headers = {"lakeflow-replay-of": violation["dlq_id"], "lakeflow-original-topic": "lakeflow.shop.orders"}
    b.add(
        envelope("c", 300, after=order(A, amount="5.00")),
        key=json.dumps({"order_id": A}).encode(),
        topic=REPLAY_TOPIC,
        headers=headers,
    )
    process_batch(ctx, b.df(), 1)
    resolved = [r for r in spark.table("lakehouse.ops.dlq_events").collect() if r["dlq_id"] == violation["dlq_id"]][0]
    assert (resolved["status"], resolved["replay_attempts"], resolved["resolved_batch_id"]) == ("replayed", 1, 1)
    assert silver(spark)[A]["amount"] is not None


def test_contract_v2_and_pii_handling(ctx, spark):
    b = Batch(spark)
    b.change("c", A, 100, order(A, channel="mobile"))
    customer = {
        "customer_id": B,
        "email": "Ada@Example.com",
        "full_name": "Ada",
        "country": "GB",
        "created_at": "2026-09-01T00:00:00Z",
        "updated_at": "2026-09-01T00:00:00Z",
    }
    b.change("c", B, 101, customer, table="customers")
    process_batch(ctx, b.df(), 0)
    orders = silver(spark)
    assert orders[A]["channel"] == "mobile" and orders[A]["_contract_version"] == 2
    customers = silver(spark, "silver.customers")
    assert len(customers[B]["email_hmac"]) == 64
    assert "full_name" not in customers[B] and "email" not in customers[B]
    assert all("Ada" not in (r["after_json"] or "") for r in bronze(spark).values())


def test_spark_matches_reference_model(ctx, spark):
    """Random interleavings with redelivery: Spark results must equal lakeflow_core.semantics batch by batch."""
    rng = random.Random(7)
    keys = [f"00000000-0000-4000-8000-00000000000{i}" for i in range(4)]
    events = []
    for lsn in range(1001, 1031):
        pk = rng.choice(keys)
        events.append(
            (rng.choice(["c", "u", "u", "d"]), pk, lsn, order(pk, status=rng.choice(["PENDING", "PAID", "SHIPPED"])))
        )
    deliveries = events + rng.sample(events, 6)
    rng.shuffle(deliveries)

    oracle_silver, oracle_bronze, watermark, offset = {}, {}, 0, 0
    for batch_id, start in enumerate(range(0, len(deliveries), 6)):
        b = Batch(spark)
        model_events = []
        for op, pk, lsn, image in deliveries[start : start + 6]:
            b.change(op, pk, lsn, image, offset=offset)
            model_events.append(
                ChangeEvent(f"{pk}-{lsn}", "shop.orders", pk, op, lsn, 1_790_000_000_000 + lsn, ORDERS_TOPIC, 0, offset)
            )
            offset += 1
        record = process_batch(ctx, b.df(), batch_id)
        model = plan_batch(
            batch_id, model_events, oracle_silver, oracle_bronze, watermark, ctx.settings.allowed_lateness_ms
        )
        apply_plan(model, oracle_silver)
        oracle_bronze.update({e: batch_id for e in model.bronze_appends})
        watermark = model.watermark_after_ms
        expected = {o: sum(1 for v in model.outcomes.values() if v == o) for o in ("applied", "superseded", "stale")}
        assert {o: record[o] for o in expected} == expected, f"batch {batch_id}"
        assert record["duplicates"] == model.in_batch_duplicates + len(model.prior_duplicates), f"batch {batch_id}"

    actual = {pk: (r["_source_lsn"], r["is_deleted"]) for pk, r in silver(spark).items()}
    assert actual == {key[1]: (row.source_lsn, row.is_deleted) for key, row in oracle_silver.items()}
    assert len(bronze(spark)) == len(oracle_bronze) == len(events)
