"""Dataset lineage graph and impact analysis over the declarative spec in lineage/lineage.json."""

from __future__ import annotations

import json
from collections import deque
from pathlib import Path


class LineageError(ValueError):
    pass


def load_spec(path: str | Path) -> dict:
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_spec(spec)
    return spec


def validate_spec(spec: dict) -> None:
    """Ids must be unique and every job reference must resolve.

    Cycles are allowed because DLQ replay is an intentional feedback loop. Traversal is cycle-safe.
    """
    datasets = {d["id"] for d in spec.get("datasets", [])}
    jobs = {j["id"] for j in spec.get("jobs", [])}
    if len(datasets) != len(spec.get("datasets", [])) or len(jobs) != len(spec.get("jobs", [])):
        raise LineageError("dataset and job ids must be unique")
    if datasets & jobs:
        raise LineageError(f"ids used for both datasets and jobs: {sorted(datasets & jobs)}")
    for job in spec.get("jobs", []):
        for ref in list(job.get("inputs", [])) + list(job.get("outputs", [])):
            if ref not in datasets:
                raise LineageError(f"job {job['id']} references unknown dataset {ref}")


def edges(spec: dict) -> dict[str, list[str]]:
    """Adjacency list: dataset -> job -> dataset."""
    graph: dict[str, list[str]] = {}
    for job in spec.get("jobs", []):
        for source in job.get("inputs", []):
            graph.setdefault(source, []).append(job["id"])
        graph.setdefault(job["id"], []).extend(job.get("outputs", []))
    return graph


def _reverse(graph: dict[str, list[str]]) -> dict[str, list[str]]:
    reverse: dict[str, list[str]] = {}
    for node, children in graph.items():
        for child in children:
            reverse.setdefault(child, []).append(node)
    return reverse


def _walk(graph: dict[str, list[str]], start: str) -> list[dict]:
    seen = {start}
    queue = deque([(start, 0)])
    out: list[dict] = []
    while queue:
        node, depth = queue.popleft()
        for child in graph.get(node, []):
            if child not in seen:
                seen.add(child)
                out.append({"id": child, "depth": depth + 1})
                queue.append((child, depth + 1))
    return out


def downstream(spec: dict, node_id: str) -> list[dict]:
    _require(spec, node_id)
    return _walk(edges(spec), node_id)


def upstream(spec: dict, node_id: str) -> list[dict]:
    _require(spec, node_id)
    return _walk(_reverse(edges(spec)), node_id)


def impact(spec: dict, dataset_id: str) -> dict:
    """What breaks, and who to notify, if `dataset_id` is late, wrong, or changes schema."""
    nodes = {d["id"]: {**d, "type": "dataset"} for d in spec.get("datasets", [])}
    nodes.update({j["id"]: {**j, "type": "job"} for j in spec.get("jobs", [])})
    affected = [{**nodes[item["id"]], "depth": item["depth"]} for item in downstream(spec, dataset_id)]
    owners = sorted({n.get("owner") for n in affected if n.get("owner")})
    return {
        "dataset": dataset_id,
        "affected": affected,
        "affected_datasets": [a["id"] for a in affected if a["type"] == "dataset"],
        "affected_jobs": [a["id"] for a in affected if a["type"] == "job"],
        "owners_to_notify": owners,
    }


def _require(spec: dict, node_id: str) -> None:
    ids = {d["id"] for d in spec.get("datasets", [])} | {j["id"] for j in spec.get("jobs", [])}
    if node_id not in ids:
        raise LineageError(f"unknown lineage node {node_id}")
