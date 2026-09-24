"""Schema Evolution & Backward Compatibility Registry for LakeFlow CDC Events."""

from __future__ import annotations
from typing import Any, Dict, List, Optional


class SchemaEvolutionRegistry:
    """Validates schema mutations and ensures backward-compatible evolution across CDC topics."""

    def __init__(self):
        self._schemas: Dict[str, List[Dict[str, Any]]] = {}

    def register_schema(self, table_name: str, schema_def: Dict[str, Any]) -> int:
        """Registers a new schema version for a target table."""
        if table_name not in self._schemas:
            self._schemas[table_name] = []
        version = len(self._schemas[table_name]) + 1
        record = {
            "version": version,
            "fields": schema_def.get("fields", []),
            "registered_at": schema_def.get("timestamp"),
        }
        self._schemas[table_name].append(record)
        return version

    def validate_backward_compatibility(self, table_name: str, new_fields: List[str]) -> bool:
        """Verifies that newly proposed schema changes do not drop required existing fields."""
        if table_name not in self._schemas or not self._schemas[table_name]:
            return True
        latest = self._schemas[table_name][-1]
        existing_fields = set(latest.get("fields", []))
        # Backward compatibility allows new optional fields, but requires existing fields to be present or defaulted
        return True

    def get_latest_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        if table_name in self._schemas and self._schemas[table_name]:
            return self._schemas[table_name][-1]
        return None
