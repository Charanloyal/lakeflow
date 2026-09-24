"""PostgreSQL Logical WAL Replication & CDC Source Management for LakeFlow.
Handles CDC mutation framing, WAL LSN tracking, and heartbeat generation to prevent WAL bloat.
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


@dataclass
class CDCEvent:
    """Represents a structured Change Data Capture event originating from PostgreSQL WAL."""
    record_key: str
    table_name: str
    cdc_op: str  # c: create/insert, u: update, d: delete, r: snapshot read
    source_lsn: int
    source_timestamp: float
    tx_id: int
    before_state: Optional[Dict[str, Any]] = None
    after_state: Optional[Dict[str, Any]] = None
    source_topic: str = "lakeflow.platform.telemetry_events"
    received_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def is_tombstone(self) -> bool:
        return self.cdc_op.lower() == "d" or (self.after_state is None and self.before_state is not None)


class PostgreSQLWALSource:
    """Simulates and tracks PostgreSQL logical replication slots and automated heartbeat advancement."""

    def __init__(
        self,
        slot_name: str = "lakeflow_cdc_slot",
        publication_name: str = "lakeflow_publication",
        heartbeat_interval_ms: int = 5000,
    ):
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.confirmed_flush_lsn: int = 25040000
        self.last_heartbeat_time: float = time.time()
        self.events_emitted_count: int = 0

    def generate_heartbeat(self) -> Dict[str, Any]:
        """Emits an automated heartbeat to advance LSN during quiet periods, preventing WAL disk bloat."""
        now = time.time()
        self.confirmed_flush_lsn += 64  # Advance WAL byte offset
        self.last_heartbeat_time = now
        return {
            "type": "HEARTBEAT",
            "slot_name": self.slot_name,
            "confirmed_flush_lsn": self.confirmed_flush_lsn,
            "timestamp": now,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
        }

    def emit_cdc_mutation(
        self,
        table_name: str,
        record_key: str,
        op: str,
        after_state: Optional[Dict[str, Any]] = None,
        before_state: Optional[Dict[str, Any]] = None,
    ) -> CDCEvent:
        """Constructs and emits a validated CDC event with strictly monotonically increasing LSN."""
        self.confirmed_flush_lsn += 128
        self.events_emitted_count += 1
        now = time.time()

        return CDCEvent(
            record_key=record_key,
            table_name=table_name,
            cdc_op=op,
            source_lsn=self.confirmed_flush_lsn,
            source_timestamp=now,
            tx_id=1000 + self.events_emitted_count,
            before_state=before_state,
            after_state=after_state,
            source_topic=f"lakeflow.platform.{table_name}",
        )
