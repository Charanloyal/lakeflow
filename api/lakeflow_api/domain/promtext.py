"""Minimal Prometheus text-format parser (for the Spark driver's /metrics)."""

from __future__ import annotations

import re

_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|[+-]Inf)")
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse(text: str) -> list[tuple[str, dict[str, str], float]]:
    samples = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = _LINE.match(line)
        if not match:
            continue
        name, labels, value = match.groups()
        samples.append((name, dict(_LABEL.findall(labels or "")), float(value)))
    return samples


def value(samples, name: str, **labels) -> float | None:
    for sample_name, sample_labels, sample_value in samples:
        if sample_name == name and all(sample_labels.get(k) == v for k, v in labels.items()):
            return sample_value
    return None


def total(samples, name: str, **labels) -> float:
    return sum(v for n, lab, v in samples if n == name and all(lab.get(k) == x for k, x in labels.items()))
