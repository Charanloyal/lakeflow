"""Controlled source mutations on shop.orders (the only table the demo may change)."""

from __future__ import annotations

import logging
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from fastapi import HTTPException

log = logging.getLogger("lakeflow.demo")
TXID_SQL = "SELECT (pg_current_xact_id()::text::bigint & 4294967295) AS txid"
UPDATABLE = ("status", "amount", "currency", "channel")


def has_channel(conn) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = 'orders' "
        "AND column_name = 'channel'"
    ).fetchone()
    return row is not None


def _customer(conn, customer_id):
    if customer_id is not None:
        row = conn.execute("SELECT customer_id FROM shop.customers WHERE customer_id = %s", (customer_id,)).fetchone()
        if row is None:
            raise HTTPException(422, f"customer {customer_id} does not exist")
        return row["customer_id"]
    row = conn.execute("SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1").fetchone()
    if row is None:
        raise HTTPException(503, "no customers in the source database (seed data missing)")
    return row["customer_id"]


def _result(order_id, op, txid, lsn, note) -> dict:
    return {
        "order_id": str(order_id),
        "op": op,
        "txid": txid,
        "commit_lsn": lsn,
        "committed_at": datetime.now(timezone.utc),
        "note": note,
    }


def _finish(ctx, conn, actor, order_id, op, txid, detail):
    lsn = conn.execute("SELECT pg_current_wal_lsn()::text AS lsn").fetchone()["lsn"]
    try:
        ctx.store.record_mutation(actor, "orders", str(order_id), op, txid, lsn, detail)
    except Exception:  # noqa: BLE001 - the trace still works from Kafka/bronze evidence
        log.warning("could not record mutation %s %s in the control DB", op, order_id, exc_info=True)
    return _result(
        order_id,
        op,
        txid,
        lsn,
        "commit_lsn is the WAL position after commit; Debezium's source.lsn is the change record's own LSN",
    )


def create_order(ctx, body, actor: str) -> dict:
    try:
        with ctx.clients.source() as conn:
            with conn.transaction():
                customer = _customer(conn, body.customer_id)
                columns, values = (
                    ["customer_id", "status", "amount", "currency"],
                    [customer, body.status, body.amount, body.currency],
                )
                if body.channel is not None:
                    if not has_channel(conn):
                        raise HTTPException(
                            409, "channel needs contract v2: run Recovery Lab 'apply_schema_migration' first"
                        )
                    columns.append("channel")
                    values.append(body.channel)
                row = conn.execute(
                    f"INSERT INTO shop.orders ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))}) "  # noqa: S608
                    "RETURNING order_id",
                    values,
                ).fetchone()
                txid = conn.execute(TXID_SQL).fetchone()["txid"]
            return _finish(ctx, conn, actor, row["order_id"], "c", txid, body.model_dump(mode="json"))
    except psycopg.errors.CheckViolation as exc:
        raise HTTPException(422, f"rejected by source CHECK constraint: {exc.diag.message_primary}") from exc


def update_order(ctx, order_id: uuid.UUID, body, actor: str) -> dict:
    changes = {name: getattr(body, name) for name in body.model_fields_set if name in UPDATABLE}
    try:
        with ctx.clients.source() as conn:
            with conn.transaction():
                if "channel" in changes and not has_channel(conn):
                    raise HTTPException(
                        409, "channel needs contract v2: run Recovery Lab 'apply_schema_migration' first"
                    )
                assignments = ", ".join(f"{name} = %s" for name in changes)
                row = conn.execute(
                    f"UPDATE shop.orders SET {assignments} WHERE order_id = %s RETURNING order_id",  # noqa: S608 - allowlisted
                    [*changes.values(), order_id],
                ).fetchone()
                if row is None:
                    raise HTTPException(404, f"order {order_id} not found in PostgreSQL")
                txid = conn.execute(TXID_SQL).fetchone()["txid"]
            return _finish(ctx, conn, actor, order_id, "u", txid, {k: str(v) for k, v in changes.items()})
    except psycopg.errors.CheckViolation as exc:
        raise HTTPException(422, f"rejected by source CHECK constraint: {exc.diag.message_primary}") from exc


def delete_order(ctx, order_id: uuid.UUID, actor: str) -> dict:
    with ctx.clients.source() as conn:
        with conn.transaction():
            row = conn.execute("DELETE FROM shop.orders WHERE order_id = %s RETURNING order_id", (order_id,)).fetchone()
            if row is None:
                raise HTTPException(404, f"order {order_id} not found in PostgreSQL")
            txid = conn.execute(TXID_SQL).fetchone()["txid"]
        return _finish(ctx, conn, actor, order_id, "d", txid, {})


def generate(ctx, count: int, seed: int, actor: str) -> dict:
    """Seeded mix (60 % insert / 30 % update / 10 % delete), one transaction per mutation like a real app."""
    rng = random.Random(seed)
    started = time.perf_counter()
    created: list = []
    counts = {"c": 0, "u": 0, "d": 0}
    statuses = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
    with ctx.clients.source() as conn:
        customers = [
            r["customer_id"] for r in conn.execute("SELECT customer_id FROM shop.customers ORDER BY customer_id")
        ]
        if not customers:
            raise HTTPException(503, "no customers in the source database")
        conn.commit()
        for _ in range(count):
            roll = rng.random()
            with conn.transaction():
                if roll < 0.6 or not created:
                    row = conn.execute(
                        "INSERT INTO shop.orders (customer_id, status, amount, currency) VALUES (%s, %s, %s, %s) RETURNING order_id",
                        (
                            rng.choice(customers),
                            "PENDING",
                            Decimal(rng.randint(100, 500_000)) / 100,
                            rng.choice(["USD", "EUR", "GBP", "INR"]),
                        ),
                    ).fetchone()
                    created.append(row["order_id"])
                    counts["c"] += 1
                elif roll < 0.9:
                    conn.execute(
                        "UPDATE shop.orders SET status = %s WHERE order_id = %s",
                        (rng.choice(statuses), rng.choice(created)),
                    )
                    counts["u"] += 1
                else:
                    victim = created.pop(rng.randrange(len(created)))
                    conn.execute("DELETE FROM shop.orders WHERE order_id = %s", (victim,))
                    counts["d"] += 1
    try:
        ctx.store.audit(actor, "demo.generate", {"count": count, "seed": seed, **counts})
    except Exception:  # noqa: BLE001 - auditing is best effort; the mutations are committed
        log.warning("could not audit demo.generate", exc_info=True)
    return {
        "seed": seed,
        "inserted": counts["c"],
        "updated": counts["u"],
        "deleted": counts["d"],
        "order_ids": [str(o) for o in created][:50],
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
    }
