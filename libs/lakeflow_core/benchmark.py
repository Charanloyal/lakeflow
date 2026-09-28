"""Benchmark result schema helpers: summaries are always recomputed from raw per-event samples."""

from __future__ import annotations

import json
from pathlib import Path

from .stats import rate, summarize

RESULT_SCHEMA_VERSION = 1
REQUIRED_KEYS = ("schema_version", "run_id", "created_at", "git_sha", "environment", "config", "workload", "iterations")


class BenchmarkResultError(ValueError):
    pass


def validate_result(doc: dict) -> None:
    missing = [k for k in REQUIRED_KEYS if k not in doc]
    if missing:
        raise BenchmarkResultError(f"benchmark result is missing {missing}")
    if doc["schema_version"] != RESULT_SCHEMA_VERSION:
        raise BenchmarkResultError(f"unsupported schema_version {doc['schema_version']}")
    for index, it in enumerate(doc["iterations"]):
        for key in ("events_expected", "events_observed", "duplicates_observed", "dlq_events", "latency_ms"):
            if key not in it:
                raise BenchmarkResultError(f"iteration {index} is missing {key}")


def iteration_summary(it: dict) -> dict:
    latency = summarize(it["latency_ms"])
    elapsed_s = max((it["last_commit_ms"] - it["first_source_ms"]) / 1000.0, 1e-9)
    expected = it["events_expected"]
    return {
        "events_expected": expected,
        "events_observed": it["events_observed"],
        "throughput_eps": it["events_observed"] / elapsed_s,
        "elapsed_s": elapsed_s,
        "latency_ms": latency,
        "duplicate_rate": rate(it["duplicates_observed"], max(it["events_observed"], 1)),
        "loss_rate": rate(max(expected - it["events_observed"], 0), max(expected, 1)),
        "error_rate": rate(it["dlq_events"], max(expected, 1)),
        "reconciliation_mismatches": it.get("reconciliation_mismatches"),
    }


def summarize_result(doc: dict) -> dict:
    """Pooled latency percentiles plus per-iteration throughput distribution."""
    validate_result(doc)
    per_iteration = [iteration_summary(it) for it in doc["iterations"]]
    pooled = [v for it in doc["iterations"] for v in it["latency_ms"]]
    expected = sum(it["events_expected"] for it in doc["iterations"])
    observed = sum(it["events_observed"] for it in doc["iterations"])
    return {
        "iterations": len(per_iteration),
        "events_expected": expected,
        "events_observed": observed,
        "latency_ms": summarize(pooled),
        "throughput_eps": summarize(p["throughput_eps"] for p in per_iteration),
        "duplicate_rate": rate(sum(it["duplicates_observed"] for it in doc["iterations"]), max(observed, 1)),
        "loss_rate": rate(max(expected - observed, 0), max(expected, 1)),
        "error_rate": rate(sum(it["dlq_events"] for it in doc["iterations"]), max(expected, 1)),
        "reconciliation_mismatches": sum(int(it.get("reconciliation_mismatches") or 0) for it in doc["iterations"]),
        "per_iteration": per_iteration,
        "query_benchmarks": doc.get("query_benchmarks", []),
    }


def evaluate_thresholds(summary: dict, thresholds: dict) -> list[dict]:
    """thresholds: {"max_loss_rate": 0, "max_p95_latency_ms": 60000, "min_throughput_eps": 50, ...}."""
    measured = {
        "loss_rate": summary["loss_rate"],
        "duplicate_rate": summary["duplicate_rate"],
        "error_rate": summary["error_rate"],
        "reconciliation_mismatches": summary["reconciliation_mismatches"],
        "p95_latency_ms": summary["latency_ms"]["p95"],
        "p99_latency_ms": summary["latency_ms"]["p99"],
        "throughput_eps": summary["throughput_eps"]["mean"],
    }
    checks = []
    for name, limit in sorted(thresholds.items()):
        kind, _, metric = name.partition("_")
        if kind not in ("max", "min") or metric not in measured:
            raise BenchmarkResultError(f"unknown threshold {name}")
        value = measured[metric]
        passed = value is not None and (value <= limit if kind == "max" else value >= limit)
        checks.append({"name": name, "metric": metric, "value": value, "limit": limit, "passed": passed})
    return checks


def load_results(directory: str | Path) -> list[dict]:
    docs = []
    for path in sorted(Path(directory).glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        validate_result(doc)
        doc["_file"] = path.name
        docs.append(doc)
    return docs
