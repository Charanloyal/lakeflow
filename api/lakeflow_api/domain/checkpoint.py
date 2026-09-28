"""Read Spark Structured Streaming checkpoint files (offsets/N, commits/N) from the shared volume."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CheckpointState:
    epoch: str | None
    latest_offsets_batch: int | None
    latest_committed_batch: int | None
    committed_offsets: dict[str, dict[str, int]]
    planned_offsets: dict[str, dict[str, int]]

    @property
    def in_flight(self) -> bool:
        return self.latest_offsets_batch is not None and self.latest_offsets_batch != self.latest_committed_batch

    def is_committed(self, batch_id: int) -> bool:
        return self.latest_committed_batch is not None and batch_id <= self.latest_committed_batch


def _batch_ids(directory: Path) -> list[int]:
    if not directory.is_dir():
        return []
    return sorted(int(p.name) for p in directory.iterdir() if p.name.isdigit())


def parse_offsets_file(text: str) -> dict[str, dict[str, int]]:
    """Line 1 'v1', line 2 batch metadata JSON, then one JSON line per source (Kafka: {topic: {partition: offset}})."""
    lines = [line for line in text.splitlines() if line.strip()]
    offsets: dict[str, dict[str, int]] = {}
    for line in lines[2:]:
        if line.strip() == "-":
            continue
        doc = json.loads(line)
        for topic, partitions in doc.items():
            offsets[topic] = {str(p): int(o) for p, o in partitions.items()}
    return offsets


def read_checkpoint(root: str | Path) -> CheckpointState:
    root = Path(root)
    query = root / "query"
    epoch_file = root / "stream-epoch"
    epoch = epoch_file.read_text(encoding="utf-8").strip() if epoch_file.exists() else None
    offsets_ids = _batch_ids(query / "offsets")
    commit_ids = _batch_ids(query / "commits")
    latest_offsets = offsets_ids[-1] if offsets_ids else None
    latest_commit = commit_ids[-1] if commit_ids else None
    planned = (
        parse_offsets_file((query / "offsets" / str(latest_offsets)).read_text()) if latest_offsets is not None else {}
    )
    committed = (
        parse_offsets_file((query / "offsets" / str(latest_commit)).read_text())
        if latest_commit is not None and (query / "offsets" / str(latest_commit)).exists()
        else {}
    )
    return CheckpointState(epoch, latest_offsets, latest_commit, committed, planned)


def consumer_lag(end_offsets: dict[str, dict[str, int]], committed: dict[str, dict[str, int]]) -> dict[str, int]:
    """Messages not yet covered by a committed batch, per topic (Spark offsets are 'next offset to read')."""
    lag: dict[str, int] = {}
    for topic, partitions in end_offsets.items():
        done = committed.get(topic, {})
        lag[topic] = sum(max(end - done.get(p, 0), 0) for p, end in partitions.items())
    return lag
