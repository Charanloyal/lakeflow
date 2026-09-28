"""Lineage (with live stats), benchmark results (recomputed from raw JSON) and ADRs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException
from lakeflow_core import benchmark, lineage
from lakeflow_core.adr import load_adrs


def _now() -> datetime:
    return datetime.now(timezone.utc)


def lineage_graph(ctx) -> dict:
    spec = lineage.load_spec(Path(ctx.settings.contracts_dir) / "lineage.json")

    def stats():
        live = {}
        for dataset in spec["datasets"]:
            parts = dataset["id"].split(".")
            try:
                if parts[0] == "lakehouse" and len(parts) == 3:
                    rows, _ = ctx.clients.trino(
                        f'SELECT summary[\'total-records\'] AS records, committed_at FROM lakehouse.{parts[1]}."{parts[2]}$snapshots" '  # noqa: S608
                        "ORDER BY committed_at DESC LIMIT 1"
                    )
                    if rows:
                        live[dataset["id"]] = {"records": rows[0]["records"], "last_commit": rows[0]["committed_at"],
                                               "source": "Iceberg snapshot summary"}
                elif parts[0] == "kafka":
                    offsets = ctx.monitor.end_offsets.get(".".join(parts[1:]))
                    if offsets is not None:
                        live[dataset["id"]] = {"records": sum(offsets.values()), "source": "Kafka end offsets"}
            except Exception:  # noqa: BLE001 - a dataset without data yet simply has no stats
                continue
        return live

    live = ctx.cached("lineage.stats", 30, stats)
    datasets = [{**d, "live": live.get(d["id"])} for d in spec["datasets"]]
    return {"datasets": datasets, "jobs": spec["jobs"], "as_of": _now()}


def impact(ctx, dataset: str) -> dict:
    spec = lineage.load_spec(Path(ctx.settings.contracts_dir) / "lineage.json")
    try:
        return lineage.impact(spec, dataset)
    except lineage.LineageError as exc:
        raise HTTPException(404, str(exc)) from exc


def _thresholds(ctx) -> dict:
    path = Path(ctx.settings.benchmark_dir).parent / "thresholds.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def benchmark_runs(ctx) -> dict:
    thresholds = _thresholds(ctx)
    runs = []
    for doc in benchmark.load_results(ctx.settings.benchmark_dir):
        summary = benchmark.summarize_result(doc)
        profile = doc["config"].get("profile")
        limits = thresholds.get(profile or "", {})
        runs.append({
            "run_id": doc["run_id"], "file": doc["_file"], "created_at": doc["created_at"], "git_sha": doc["git_sha"],
            "profile": profile, "environment": doc["environment"], "config": doc["config"], "workload": doc["workload"],
            "summary": summary, "thresholds": benchmark.evaluate_thresholds(summary, limits) if limits else [],
        })
    runs.sort(key=lambda r: r["created_at"], reverse=True)
    return {"runs": runs, "source": "benchmarks/results/*.json (summaries recomputed from raw samples)"}


def benchmark_file(ctx, run_id: str) -> Path:
    for path in Path(ctx.settings.benchmark_dir).glob("*.json"):
        try:
            if json.loads(path.read_text(encoding="utf-8")).get("run_id") == run_id:
                return path
        except ValueError:
            continue
    raise HTTPException(404, f"benchmark run {run_id} not found")


def adrs(ctx) -> dict:
    return {"adrs": load_adrs(ctx.settings.adr_dir)}
