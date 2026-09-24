"""Structured Streaming Checkpoint & State Recovery Manager for LakeFlow."""

from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional


class CheckpointManager:
    """Manages streaming micro-batch offset commits and state restoration."""

    def __init__(self, checkpoint_dir: str = "s3a://lakeflow/checkpoints/"):
        self.checkpoint_dir = checkpoint_dir
        self._offsets: Dict[str, int] = {}
        self._batch_id: int = 0

    def commit_offset(self, topic: str, partition: int, offset: int) -> Dict[str, Any]:
        key = f"{topic}:{partition}"
        self._offsets[key] = offset
        self._batch_id += 1
        return {
            "batch_id": self._batch_id,
            "partition_key": key,
            "committed_offset": offset,
            "timestamp": time.time(),
        }

    def get_latest_offsets(self) -> Dict[str, int]:
        return dict(self._offsets)

    def recover_from_checkpoint(self, state_dict: Dict[str, Any]) -> None:
        self._offsets = state_dict.get("offsets", {})
        self._batch_id = state_dict.get("batch_id", 0)
