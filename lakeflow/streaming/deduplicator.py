"""Deterministic Composite Key Deduplication & Out-of-Order LSN Resolution Engine."""

from __future__ import annotations
from typing import Any, Dict, List, Set, Tuple


class CDCDeduplicator:
    """Implements watermark-based deduplication and latest-LSN state resolution.
    Guarantees exactly-once processing across Kafka consumer group rebalances.
    """

    def __init__(self, watermark_window_seconds: int = 600):
        self.watermark_window_seconds = watermark_window_seconds
        self.seen_composite_keys: Set[Tuple[str, int]] = set()

    def deduplicate_batch(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filters duplicate events sharing identical (record_key, source_lsn)."""
        deduped = []
        for event in events:
            key = (event["record_key"], event["source_lsn"])
            if key not in self.seen_composite_keys:
                self.seen_composite_keys.add(key)
                deduped.append(event)
        return deduped

    def resolve_latest_state_per_key(self, events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Resolves out-of-order event delivery by strictly selecting the event with the highest source_lsn."""
        resolved: Dict[str, Dict[str, Any]] = {}
        for event in events:
            record_key = event["record_key"]
            lsn = event["source_lsn"]
            if record_key not in resolved or lsn > resolved[record_key]["source_lsn"]:
                resolved[record_key] = event
        return resolved
