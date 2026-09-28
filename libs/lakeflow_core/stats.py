"""Descriptive statistics used by benchmarks, the API and the quality checks (no numpy dependency)."""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable


def percentile(values: Iterable[float], q: float) -> float | None:
    """Linear interpolation between closest ranks (numpy's default 'linear' method); q in [0, 100]."""
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return None
    if not 0 <= q <= 100:
        raise ValueError("q must be within [0, 100]")
    rank = (len(ordered) - 1) * q / 100.0
    low = math.floor(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def summarize(values: Iterable[float]) -> dict[str, float | int | None]:
    data = [float(v) for v in values]
    if not data:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "stddev": None,
            "p50": None,
            "p95": None,
            "p99": None,
        }
    return {
        "count": len(data),
        "min": min(data),
        "max": max(data),
        "mean": statistics.fmean(data),
        "stddev": statistics.stdev(data) if len(data) > 1 else 0.0,
        "p50": percentile(data, 50),
        "p95": percentile(data, 95),
        "p99": percentile(data, 99),
    }


def rate(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else numerator / denominator
