"""Apache Iceberg ACID Snapshot Lifecycle & Time Travel Manager."""

from __future__ import annotations
import time
from typing import Any, Dict, List, Optional


class SnapshotManager:
    """Tracks atomic Iceberg table snapshot commits, manifests, and time travel lineage."""

    def __init__(self, table_name: str = "silver_orders"):
        self.table_name = table_name
        self.snapshots: List[Dict[str, Any]] = []
        self._current_snapshot_id: int = 1000

    def commit_snapshot(
        self,
        added_records: int,
        deleted_records: int = 0,
        operation: str = "append",
    ) -> Dict[str, Any]:
        """Commits an atomic Iceberg snapshot and returns its metadata manifest summary."""
        self._current_snapshot_id += 1
        snapshot = {
            "snapshot_id": self._current_snapshot_id,
            "timestamp_ms": int(time.time() * 1000),
            "operation": operation,
            "summary": {
                "added_records": added_records,
                "deleted_records": deleted_records,
                "total_records": sum(s["summary"]["added_records"] for s in self.snapshots) + added_records - deleted_records,
            },
        }
        self.snapshots.append(snapshot)
        return snapshot

    def get_snapshot(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        for s in self.snapshots:
            if s["snapshot_id"] == snapshot_id:
                return s
        return None

    def get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        return self.snapshots[-1] if self.snapshots else None
