"""Deterministic end-to-end benchmark against the running stack (ADR-0005). Runs in the tools container.

python benchmarks/run.py --profile ci --iterations 3 --events 300 --seed 42

Writes benchmarks/results/<timestamp>-<profile>-<run_id>.json (schema v1, every latency sample) and exits 1 when a
threshold from benchmarks/thresholds.json[<profile>] fails. Nothing is simulated: mutations are real PostgreSQL
transactions and every number is read back from Iceberg through Trino.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
import trino

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "libs"))

from lakeflow_core import benchmark  # noqa: E402
from lakeflow_core.maintenance import maintain_table, trino_query  # noqa: E402

STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
QUERY = (
    "SELECT currency, status, count(*) AS orders, sum(amount) AS revenue "
    "FROM lakehouse.silver.orders WHERE NOT is_deleted GROUP BY currency, status ORDER BY currency, status"
)


def trino_rows(sql: str, params=None, stats: dict | None = None) -> list[dict]:
    conn = trino.dbapi.connect(
        host=os.environ.get("LAKEFLOW_TRINO_HOST", "trino"),
        port=int(os.environ.get("LAKEFLOW_TRINO_PORT", "8080")),
        user="lakeflow-benchmark",
        catalog="lakehouse",
    )
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        if stats is not None:
            stats.update(cursor.stats or {})
        names = [d[0] for d in cursor.description or []]
        return [dict(zip(names, row, strict=True)) for row in rows]
    finally:
        conn.close()


def run_workload(conn, rng: random.Random, customer_id: str, events: int, tx_size: int) -> int:
    """60 % insert / 30 % update / 10 % delete, grouped into transactions of tx_size changes."""
    live: list[str] = []
    done = 0
    while done < events:
        with conn.transaction():
            for _ in range(min(tx_size, events - done)):
                roll = rng.random()
                if roll < 0.6 or not live:
                    amount = Decimal(rng.randint(100, 500_000)) / 100
                    row = conn.execute(
                        "INSERT INTO shop.orders (customer_id, status, amount, currency) VALUES (%s, %s, %s, %s) "
                        "RETURNING order_id::text",
                        (customer_id, "PENDING", amount, rng.choice(CURRENCIES)),
                    ).fetchone()
                    live.append(row[0])
                elif roll < 0.9:
                    conn.execute(
                        "UPDATE shop.orders SET status = %s WHERE order_id = %s",
                        (rng.choice(STATUSES), rng.choice(live)),
                    )
                else:
                    victim = live.pop(rng.randrange(len(live)))
                    conn.execute("DELETE FROM shop.orders WHERE order_id = %s", (victim,))
                done += 1
    return done


def observe(customer_id: str, expected: int, timeout_s: float) -> list[dict]:
    sql = (
        "SELECT event_id, to_unixtime(source_ts) * 1000 AS source_ms, to_unixtime(committed_at) * 1000 AS commit_ms, "
        "length(coalesce(after_json, before_json)) AS payload_bytes FROM lakehouse.bronze.cdc_events "
        "WHERE source_table = 'shop.orders' AND json_extract_scalar(coalesce(after_json, before_json), '$.customer_id') = ?"
    )
    deadline, rows = time.time() + timeout_s, []
    while time.time() < deadline:
        rows = trino_rows(sql, [customer_id])
        if len({r["event_id"] for r in rows}) >= expected:
            break
        time.sleep(2)
    return rows


def mismatches(customer_id: str) -> int:
    sql = (
        "SELECT count(*) AS n FROM (SELECT * FROM postgres.shop.orders WHERE customer_id = CAST(? AS uuid)) s "
        "FULL OUTER JOIN (SELECT * FROM lakehouse.silver.orders WHERE customer_id = ? AND NOT is_deleted) l "
        "ON CAST(s.order_id AS varchar) = l.order_id WHERE s.order_id IS NULL OR l.order_id IS NULL "
        "OR s.status <> l.status OR s.amount <> l.amount OR CAST(s.currency AS varchar) <> l.currency"
    )
    deadline, count = time.time() + 120, None
    while time.time() < deadline:
        count = trino_rows(sql, [customer_id, customer_id])[0]["n"]
        if count == 0:
            break
        time.sleep(3)
    return int(count)


def query_benchmark(runs: int) -> dict:
    def phase(name: str) -> dict:
        files = trino_rows(
            'SELECT count_if(content = 0) AS data_files, count_if(content <> 0) AS delete_files '
            'FROM lakehouse.silver."orders$files"'
        )[0]
        samples, digest = [], None
        for index in range(runs):
            stats: dict = {}
            started = time.perf_counter()
            rows = trino_rows(QUERY, stats=stats)
            samples.append(
                {
                    "run": index + 1,
                    "kind": "cold" if index == 0 else "warm",
                    "wall_ms": round((time.perf_counter() - started) * 1000, 2),
                    "trino_elapsed_ms": stats.get("elapsedTimeMillis"),
                    "trino_cpu_ms": stats.get("cpuTimeMillis"),
                    "processed_rows": stats.get("processedRows"),
                    "processed_bytes": stats.get("processedBytes"),
                }
            )
            digest = hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()
        return {"phase": name, **files, "result_sha256": digest, "runs": samples}

    before = phase("before_optimize")
    record = maintain_table(trino_query(os.environ.get("LAKEFLOW_TRINO_HOST", "trino"), 8080), "silver.orders",
                            tasks=["optimize"], runner="benchmark")
    after = phase("after_optimize")
    return {
        "query_id": "silver_orders_revenue_by_currency_status",
        "sql": QUERY,
        "note": "same query and same data before/after; cold = first run after the table changed (metadata + JIT)",
        "same_result": before["result_sha256"] == after["result_sha256"],
        "optimize": {k: record[k] for k in ("status", "rewrite_snapshot_id", "fingerprint_match", "error")},
        "phases": [before, after],
    }


def environment() -> dict:
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), platform.processor()) if Path("/proc/cpuinfo").exists() else ""
    mem = next((int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith("MemTotal")), None) if Path("/proc/meminfo").exists() else None
    return {
        "cpu_model": cpu,
        "cpu_count": os.cpu_count(),
        "memory_bytes": mem,
        "os": platform.platform(),
        "python": platform.python_version(),
        "ci_run_url": os.environ.get("CI_RUN_URL") or None,
        "runner": "github-actions" if os.environ.get("CI_RUN_URL") else "local",
    }


def config(profile: str, resource_profile: str, args) -> dict:
    limits = {}
    path = ROOT / "infra" / "profiles" / f"{resource_profile}.env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                limits[key] = value
    partitions = {}
    for line in (ROOT / "platform" / "kafka" / "topics.conf").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            name, count, *_ = line.split()
            partitions[name] = int(count)
    properties = {r["key"]: r["value"] for r in trino_rows('SELECT * FROM lakehouse.silver."orders$properties"')}
    return {
        "profile": profile,
        "resource_profile": resource_profile,
        "trigger_interval_seconds": limits.get("TRIGGER_INTERVAL_SECONDS"),
        "max_offsets_per_trigger": limits.get("MAX_OFFSETS_PER_TRIGGER"),
        "container_limits": {k: v for k, v in limits.items() if k.endswith(("_MEM", "_HEAP", "_CPUS", "_CORES"))},
        "topic_partitions": partitions,
        "iceberg_properties": properties,
        "transaction_size": args.tx_size,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default="laptop", help="threshold profile in benchmarks/thresholds.json")
    parser.add_argument("--resource-profile", default=os.environ.get("LAKEFLOW_PROFILE", "8gb"))
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--events", type=int, default=2000, help="source changes per iteration")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--tx-size", type=int, default=10)
    parser.add_argument("--query-runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--output", default=str(ROOT / "benchmarks" / "results"))
    args = parser.parse_args()

    run_id, created = uuid.uuid4().hex[:12], datetime.now(timezone.utc)
    iterations = []
    with psycopg.connect(os.environ["LAKEFLOW_PG_DSN"], autocommit=True) as conn:
        for index in range(args.iterations):
            rng = random.Random(args.seed + index)
            customer_id = conn.execute(
                "INSERT INTO shop.customers (email, full_name, country) VALUES (%s, %s, 'DE') RETURNING customer_id::text",
                (f"bench-{run_id}-{index}@example.invalid", f"Benchmark {run_id} #{index}"),
            ).fetchone()[0]
            started = time.time()
            expected = run_workload(conn, rng, customer_id, args.events, args.tx_size)
            rows = observe(customer_id, expected, args.timeout)
            unique = {r["event_id"]: r for r in rows}
            dlq = trino_rows(
                "SELECT count(*) AS n FROM lakehouse.ops.dlq_events WHERE strpos(raw_value, ?) > 0", [customer_id]
            )[0]["n"]
            iterations.append(
                {
                    "index": index,
                    "seed": args.seed + index,
                    "customer_id": customer_id,
                    "events_expected": expected,
                    "events_observed": len(unique),
                    "duplicates_observed": len(rows) - len(unique),
                    "dlq_events": int(dlq),
                    "reconciliation_mismatches": mismatches(customer_id),
                    "first_source_ms": min((r["source_ms"] for r in unique.values()), default=started * 1000),
                    "last_commit_ms": max((r["commit_ms"] for r in unique.values()), default=time.time() * 1000),
                    "payload_bytes_avg": round(sum(r["payload_bytes"] or 0 for r in unique.values()) / max(len(unique), 1)),
                    "latency_ms": sorted(round(r["commit_ms"] - r["source_ms"], 1) for r in unique.values()),
                }
            )
            print(f"iteration {index}: {len(unique)}/{expected} events observed", flush=True)

    doc = {
        "schema_version": benchmark.RESULT_SCHEMA_VERSION,
        "run_id": run_id,
        "created_at": created.isoformat(),
        "git_sha": os.environ.get("GIT_SHA", "unknown"),
        "environment": environment(),
        "config": config(args.profile, args.resource_profile, args),
        "workload": {"events_per_iteration": args.events, "iterations": args.iterations, "seed": args.seed,
                     "mix": {"insert": 0.6, "update": 0.3, "delete": 0.1}},
        "iterations": iterations,
        "query_benchmarks": [query_benchmark(args.query_runs)],
    }
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{created.strftime('%Y%m%dT%H%M%SZ')}-{args.profile}-{run_id}.json"
    path.write_text(json.dumps(doc, indent=1, default=str) + "\n", encoding="utf-8")

    summary = benchmark.summarize_result(doc)
    thresholds = json.loads((ROOT / "benchmarks" / "thresholds.json").read_text(encoding="utf-8")).get(args.profile, {})
    checks = benchmark.evaluate_thresholds(summary, thresholds) if thresholds else []
    print(json.dumps({"file": str(path.relative_to(ROOT)), "latency_ms": summary["latency_ms"],
                      "throughput_eps": summary["throughput_eps"], "loss_rate": summary["loss_rate"],
                      "duplicate_rate": summary["duplicate_rate"], "checks": checks}, indent=1, default=str))
    return 1 if any(not c["passed"] for c in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
