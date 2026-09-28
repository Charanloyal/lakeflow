"""Data quality: contract checks through Trino, DLQ triage and replay, schema history, freshness history."""

from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

from lakeflow_core.contracts import check_backward_compatible, parse_contract
from lakeflow_core.quality import build_checks, evaluate

RESULTS_DDL = (
    "CREATE TABLE IF NOT EXISTS lakehouse.ops.quality_results (run_id varchar, run_at timestamp(6) with time zone, "
    "check_id varchar, dataset varchar, category varchar, severity varchar, status varchar, value double, "
    "threshold double, unit varchar, error varchar, runner varchar) WITH (partitioning = ARRAY['day(run_at)'])"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def run(ctx, check_ids: list[str] | None, runner: str = "api") -> dict:
    checks = build_checks(ctx.registry)
    if check_ids:
        known = {c.check_id for c in checks}
        unknown = sorted(set(check_ids) - known)
        if unknown:
            raise HTTPException(422, f"unknown checks {unknown}")
        checks = [c for c in checks if c.check_id in set(check_ids)]
    run_at, run_id = _now(), uuid.uuid4().hex[:12]
    results = []
    for check in checks:
        try:
            rows, _ = ctx.clients.trino(check.sql, timeout_s=60)
            value = next(iter(rows[0].values())) if rows else None
            results.append({**evaluate(check, value), "run_at": run_at})
        except Exception as exc:  # noqa: BLE001 - one failing check must not hide the others
            results.append({**evaluate(check, None, error=f"{type(exc).__name__}: {str(exc)[:300]}"), "run_at": run_at})
    persisted = False
    try:
        ctx.clients.trino(RESULTS_DDL)
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(results))
        params = []
        for r in results:
            params += [
                run_id,
                run_at,
                r["check_id"],
                r["dataset"],
                r["category"],
                r["severity"],
                r["status"],
                r["value"],
                r["threshold"],
                r["unit"],
                r["error"],
                runner,
            ]
        ctx.clients.trino(f"INSERT INTO lakehouse.ops.quality_results VALUES {placeholders}", params)  # noqa: S608
        persisted = True
    except Exception:  # noqa: BLE001 - results are still returned to the caller
        persisted = False
    return {"results": results, "run_at": run_at, "source": "Trino (live queries)", "persisted": persisted}


def summary(ctx) -> dict:
    try:
        rows, _ = ctx.clients.trino(
            "SELECT check_id, dataset, category, severity, status, value, threshold, unit, error, run_at FROM ("
            "SELECT *, row_number() OVER (PARTITION BY check_id ORDER BY run_at DESC) AS rn "
            "FROM lakehouse.ops.quality_results) WHERE rn = 1 ORDER BY check_id"
        )
    except Exception:  # noqa: BLE001 - table appears after the first run
        rows = []
    descriptions = {c.check_id: c.description for c in build_checks(ctx.registry)}
    results = [{**r, "description": descriptions.get(r["check_id"], "")} for r in rows]
    try:
        dlq_rows, _ = ctx.clients.trino("SELECT count(*) AS n FROM lakehouse.ops.dlq_events WHERE status = 'open'")
        dlq_open = dlq_rows[0]["n"]
    except Exception:  # noqa: BLE001
        dlq_open = None
    return {
        "results": results,
        "dlq_open": dlq_open,
        "as_of": _now(),
        "source": "Trino: ops.quality_results (latest per check)",
    }


def rejected(ctx, status: str | None, limit: int) -> dict:
    where, params = "", []
    if status:
        where, params = "WHERE status = ?", [status]
    rows, _ = ctx.clients.trino(
        "SELECT dlq_id, first_seen_at, last_seen_at, kafka_topic, kafka_partition, kafka_offset, source_topic, "  # noqa: S608
        "contract_name, error_code, error_detail, violations, injection_id, status, replay_attempts, resolved_at, "
        f"substr(raw_value, 1, 2000) AS raw_value FROM lakehouse.ops.dlq_events {where} "
        f"ORDER BY first_seen_at DESC LIMIT {int(limit)}",
        params,
    )
    return {"records": rows, "as_of": _now(), "source": "Trino: ops.dlq_events"}


def _raw(text: str | None) -> bytes | None:
    if text is None:
        return None
    if text.startswith("base64:"):
        return base64.b64decode(text[7:])
    return text.encode("utf-8")


def replay(ctx, dlq_ids: list[str], actor: str) -> dict:
    placeholders = ", ".join("?" for _ in dlq_ids)
    rows, _ = ctx.clients.trino(
        f"SELECT dlq_id, source_topic, raw_key, raw_value FROM lakehouse.ops.dlq_events WHERE dlq_id IN ({placeholders}) "  # noqa: S608
        "AND status = 'open'",
        dlq_ids,
    )
    replayed = []
    for row in rows:
        headers = {
            "lakeflow-replay-of": row["dlq_id"],
            "lakeflow-original-topic": row["source_topic"],
            "lakeflow-replay-requested-by": actor,
        }
        ctx.clients.produce(ctx.settings.replay_topic, _raw(row["raw_key"]), _raw(row["raw_value"]), headers)
        replayed.append(row["dlq_id"])
    ctx.store.audit(actor, "quality.dlq_replay", {"replayed": replayed})
    return {"replayed": replayed, "missing": sorted(set(dlq_ids) - set(replayed)), "topic": ctx.settings.replay_topic}


def _describe(contract, status: str, problems: list[str]) -> dict:
    return {
        "contract": contract.name,
        "version": contract.version,
        "status": status,
        "owner": contract.owner,
        "classification": contract.classification,
        "compatibility": contract.compatibility,
        "freshness_sla": contract.freshness_sla,
        "required": list(contract.required),
        "fields": [
            {
                "name": f.name,
                "type": f.logical_type,
                "nullable": f.nullable,
                "pii": f.pii,
                "pii_handling": f.pii_handling,
                "enum": list(f.enum) if f.enum else None,
            }
            for f in contract.fields.values()
        ],
        "compatibility_problems": problems,
    }


def schema(ctx) -> dict:
    registry = ctx.registry
    contracts = []
    for name in registry.names:
        versions = registry.versions(name)
        for index, contract in enumerate(versions):
            problems = check_backward_compatible(versions[index - 1], contract) if index else []
            contracts.append(_describe(contract, contract.status, problems))
        for path in sorted(Path(ctx.settings.contracts_dir, name, "proposed").glob("v*.schema.json")):
            proposed = parse_contract(json.loads(path.read_text(encoding="utf-8")))
            contracts.append(
                _describe(proposed, "proposed", check_backward_compatible(registry.current(name), proposed))
            )
    observed, drift = [], []
    try:
        observed, _ = ctx.clients.trino(
            "SELECT contract_name, contract_version, count(*) AS events, min(committed_at) AS first_seen, "
            "max(committed_at) AS last_seen FROM lakehouse.bronze.cdc_events GROUP BY 1, 2 ORDER BY 1, 2"
        )
        drift, _ = ctx.clients.trino(
            "SELECT contract_name, field, count(*) AS events, max(committed_at) AS last_seen "
            "FROM lakehouse.bronze.cdc_events CROSS JOIN UNNEST(drift_fields) AS t(field) GROUP BY 1, 2 ORDER BY 3 DESC"
        )
    except Exception:  # noqa: BLE001 - bronze appears after the first batch
        pass
    return {
        "contracts": contracts,
        "observed_versions": observed,
        "drift": drift,
        "as_of": _now(),
        "source": "contracts/ + Trino: bronze.cdc_events",
    }


def freshness(ctx, minutes: int) -> dict:
    rows, _ = ctx.clients.trino(
        "SELECT batch_id, committed_at, freshness_p50_ms, freshness_p95_ms, freshness_max_ms, applied, input_rows, "  # noqa: S608
        f"duration_ms FROM lakehouse.ops.batch_commits WHERE committed_at > current_timestamp - INTERVAL '{int(minutes)}' MINUTE "
        "ORDER BY committed_at"
    )
    sla = float(ctx.registry.current("orders").freshness_sla["p95_seconds"])
    return {"points": rows, "sla_p95_seconds": sla, "as_of": _now(), "source": "Trino: ops.batch_commits"}
