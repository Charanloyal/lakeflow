"""Recovery Lab: bounded, audited fault injection. Every injected record carries a lakeflow-injection-id header."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

ORDERS_TOPIC = "lakeflow.shop.orders"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def envelope(op: str, lsn: int, ts_ms: int, before=None, after=None, table="orders", tx_id=0) -> bytes:
    source = {
        "version": "lakeflow-recovery-lab",
        "connector": "postgresql",
        "name": "lakeflow",
        "db": "lakeflow",
        "schema": "shop",
        "table": table,
        "lsn": lsn,
        "txId": tx_id,
        "ts_ms": ts_ms,
        "snapshot": "false",
    }
    return json.dumps(
        {"before": before, "after": after, "source": source, "op": op, "ts_ms": int(time.time() * 1000)}
    ).encode()


def _headers(action_id: str, kind: str) -> dict[str, str]:
    return {"lakeflow-injection-id": action_id, "lakeflow-injection-kind": kind}


def inject_duplicates(ctx, action_id: str, count: int) -> dict:
    """Redeliver real events byte-for-byte, as Debezium does after a connector restart."""
    candidates = [
        e
        for e in ctx.tracer.recent(500, topic=ORDERS_TOPIC)
        if not e.get("injection_id") and not e.get("tombstone") and e.get("lsn") is not None
    ][:count]
    if not candidates:
        rows, _ = ctx.clients.trino(
            "SELECT kafka_topic AS topic, kafka_partition AS partition, kafka_offset AS offset FROM lakehouse.bronze.cdc_events "  # noqa: S608
            f"WHERE kafka_topic = ? AND injection_id IS NULL ORDER BY committed_at DESC LIMIT {int(count)}",
            [ORDERS_TOPIC],
        )
        candidates = rows
    if not candidates:
        raise HTTPException(409, "no order events to duplicate yet; create an order first")
    redelivered = []
    for event in candidates:
        key, value = ctx.clients.fetch_record(event["topic"], int(event["partition"]), int(event["offset"]))
        ctx.clients.produce(ORDERS_TOPIC, key, value, _headers(action_id, "duplicate"))
        redelivered.append({"partition": event["partition"], "offset": event["offset"]})
    return {"redelivered": redelivered, "expected": "counted as duplicates; bronze and silver unchanged"}


def inject_malformed(ctx, action_id: str, count: int, kind: str) -> dict:
    produced = []
    for _ in range(count):
        order_id = str(uuid.uuid4())
        key = json.dumps({"order_id": order_id}).encode()
        now_ms = int(time.time() * 1000)
        row = {
            "order_id": order_id,
            "customer_id": str(uuid.uuid4()),
            "status": "PENDING",
            "amount": "10.00",
            "currency": "USD",
            "created_at": _now().isoformat(),
            "updated_at": _now().isoformat(),
        }
        if kind == "invalid_json":
            value = b'{"op": "c", "source": {"lsn": 1, '
        elif kind == "contract_violation":
            value = envelope("c", 1, now_ms, after={**row, "amount": "-1.00", "status": "SHIPPED_TO_MARS"})
        elif kind == "unsupported_op":
            value = envelope("t", 1, now_ms)
        else:
            value = envelope("c", 1, now_ms, after=row)
            key = json.dumps({"order_id": str(uuid.uuid4())}).encode()
        ctx.clients.produce(ORDERS_TOPIC, key, value, _headers(action_id, f"malformed:{kind}"))
        produced.append(order_id)
    expected = {
        "invalid_json": "MALFORMED_JSON",
        "contract_violation": "CONTRACT_VIOLATION",
        "unsupported_op": "UNSUPPORTED_OP",
        "key_mismatch": "KEY_MISMATCH",
    }[kind]
    return {"produced": len(produced), "expected": f"DLQ with error_code {expected}; stream keeps running"}


def inject_late(ctx, action_id: str, count: int) -> dict:
    """Out-of-order updates: an older LSN and a 2 h old source timestamp for live orders."""
    rows, _ = ctx.clients.trino(
        "SELECT order_id, customer_id, status, amount, currency, created_at, updated_at, _source_lsn "  # noqa: S608
        f"FROM lakehouse.silver.orders WHERE NOT is_deleted ORDER BY _ingested_at DESC LIMIT {int(count)}"
    )
    if not rows:
        raise HTTPException(409, "no live orders in silver yet")
    old_ms = int(time.time() * 1000) - 2 * 3600 * 1000
    injected = []
    for row in rows:
        lsn = int(row.pop("_source_lsn")) - 1
        after = {**row, "status": "PENDING", "amount": str(row["amount"])}
        key = json.dumps({"order_id": row["order_id"]}).encode()
        ctx.clients.produce(ORDERS_TOPIC, key, envelope("u", lsn, old_ms, after=after), _headers(action_id, "late"))
        injected.append({"order_id": row["order_id"], "lsn": lsn})
    return {"injected": injected, "expected": "bronze apply_outcome=stale and is_late=true; silver unchanged"}


def request_control(ctx, action_id: str, action: str, actor: str) -> dict:
    requests = Path(ctx.settings.control_dir) / "requests"
    requests.mkdir(parents=True, exist_ok=True)
    payload = {"id": action_id, "action": action, "requested_by": actor, "requested_at": _now().isoformat()}
    tmp = requests / f".{action_id}.tmp"
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, requests / f"{action_id}.json")
    notes = {
        "crash_now": "Spark exits immediately; Docker restarts it and it resumes from the checkpoint",
        "crash_after_commit": "armed: after the next non-empty batch commits to Iceberg, Spark exits before the "
        "checkpoint commit; the replayed batch must not create duplicates",
    }
    return {"control_file": f"requests/{action_id}.json", "expected": notes[action]}


def apply_migration(ctx) -> dict:
    applied = []
    with ctx.clients.source() as conn:
        for path in sorted(Path(ctx.settings.migrations_dir).glob("*.sql")):
            conn.execute(path.read_text(encoding="utf-8"))
            applied.append(path.name)
    return {"applied": applied, "expected": "new order events carry `channel` and are stamped contract v2"}


def run(ctx, body, actor: str) -> dict:
    if not ctx.settings.recovery_lab_enabled:
        raise HTTPException(403, "Recovery Lab is disabled (LAKEFLOW_RECOVERY_LAB_ENABLED=false)")
    allowed, retry = ctx.recovery_limiter.allow(actor)
    if not allowed:
        raise HTTPException(
            429, f"Recovery Lab rate limit; retry in {retry:.0f}s", headers={"Retry-After": str(int(retry) + 1)}
        )
    action_id = f"{body.action[:8]}-{uuid.uuid4().hex[:10]}"
    status = "done"
    if body.action == "inject_duplicates":
        detail = inject_duplicates(ctx, action_id, body.count)
    elif body.action == "inject_malformed":
        detail = inject_malformed(ctx, action_id, body.count, body.kind)
    elif body.action == "inject_late":
        detail = inject_late(ctx, action_id, body.count)
    elif body.action in ("crash_now", "crash_after_commit"):
        detail, status = request_control(ctx, action_id, body.action, actor), "requested"
    elif body.action == "restart_connector":
        ctx.clients.restart_connector()
        detail = {"expected": "Debezium resumes from its last flushed LSN; re-emitted events are counted as duplicates"}
    else:
        detail = apply_migration(ctx)
    record = {
        "id": action_id,
        "action": body.action,
        "actor": actor,
        "requested_at": _now(),
        "status": status,
        "detail": {**detail, "count": body.count, "kind": body.kind if body.action == "inject_malformed" else None},
    }
    try:
        ctx.store.audit(actor, f"recovery.{body.action}", record["detail"], action_id=action_id, status=status)
        ctx.store.open_incident(
            "recovery-lab",
            "info",
            f"Operator action: {body.action}",
            json.dumps(record["detail"], default=str),
            f"Recovery Lab ({actor})",
        )
    except Exception:  # noqa: BLE001 - the action already happened; auditing failure is logged by the store
        pass
    return record


def history(ctx) -> dict:
    actions = []
    try:
        for row in ctx.store.audit_entries("recovery.", 30):
            actions.append(
                {
                    "id": row.get("action_id") or str(row["id"]),
                    "action": row["action"].removeprefix("recovery."),
                    "actor": row["actor"],
                    "requested_at": row["at"],
                    "status": row.get("status") or "done",
                    "detail": row["detail"],
                }
            )
    except Exception:  # noqa: BLE001
        pass
    acks = []
    ack_dir = Path(ctx.settings.control_dir) / "acks"
    if ack_dir.is_dir():
        for path in sorted(ack_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:20]:
            try:
                acks.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                continue
    return {"enabled": ctx.settings.recovery_lab_enabled, "actions": actions, "acks": acks}
