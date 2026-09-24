"""Feature Metadata & Schema Definitions for FeatureHub."""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class FeatureType(str, Enum):
    FLOAT = "float"
    INT = "int"
    STRING = "string"
    BOOL = "bool"
    TIMESTAMP = "timestamp"
    VECTOR = "vector"


@dataclass
class EntityDefinition:
    """Represents an entity / primary key in the feature store."""
    name: str
    join_key: str
    description: str
    owner: str = "data-platform-team@lakeflow.io"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FeatureDefinition:
    """Master specification for every feature in FeatureHub.
    
    Guarantees the 8 mandatory attributes:
    1. name
    2. type
    3. description
    4. entity
    5. source
    6. timestamp
    7. owner
    8. version
    """
    name: str
    type: str
    description: str
    entity: str
    source: str
    timestamp: str
    owner: str
    version: str
    tags: List[str] = field(default_factory=list)
    freshness_sla_seconds: int = 3600  # Default 1 hour freshness SLA
    default_value: Any = None
    is_active: bool = True

    def __post_init__(self):
        # Validate that all 8 mandatory fields are non-empty
        mandatory_fields = [
            ("name", self.name),
            ("type", self.type),
            ("description", self.description),
            ("entity", self.entity),
            ("source", self.source),
            ("timestamp", self.timestamp),
            ("owner", self.owner),
            ("version", self.version),
        ]
        for field_name, value in mandatory_fields:
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Feature metadata violation: '{field_name}' must be non-empty.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def is_fresh(self, current_time: Optional[datetime] = None) -> bool:
        """Evaluates whether the feature computed timestamp satisfies the freshness SLA."""
        now = current_time or datetime.now(timezone.utc)
        try:
            # Handle ISO string with or without Z / offset
            ts_str = self.timestamp.replace("Z", "+00:00")
            feature_dt = datetime.fromisoformat(ts_str)
            if feature_dt.tzinfo is None:
                feature_dt = feature_dt.replace(tzinfo=timezone.utc)
            delta_seconds = (now - feature_dt).total_seconds()
            return delta_seconds <= self.freshness_sla_seconds
        except Exception:
            return False
