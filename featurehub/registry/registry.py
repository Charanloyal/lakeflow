"""Feature Registry Implementation for FeatureHub."""

from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from featurehub.registry.metadata import FeatureDefinition, EntityDefinition
from featurehub.registry.catalog import get_all_features


class FeatureRegistry:
    """Central registry for features, entities, versions, and freshness audits."""

    def __init__(self):
        self._features: Dict[str, FeatureDefinition] = {}
        self._version_history: Dict[str, List[FeatureDefinition]] = {}
        self._entities: Dict[str, EntityDefinition] = {}
        self._initialized_at: str = datetime.now(timezone.utc).isoformat()
        self._setup_default_entities()

    def _setup_default_entities(self):
        entities = [
            EntityDefinition(
                name="customer",
                join_key="customer_id",
                description="Primary retail cardholder and account owner",
                owner="core-data@lakeflow.io",
            ),
            EntityDefinition(
                name="merchant",
                join_key="merchant_id",
                description="Merchant gateway processing account",
                owner="merchant-risk@lakeflow.io",
            ),
            EntityDefinition(
                name="transaction",
                join_key="transaction_id",
                description="Individual point-of-sale or e-commerce authorization event",
                owner="core-payment@lakeflow.io",
            ),
            EntityDefinition(
                name="device",
                join_key="device_id",
                description="Hardware client device fingerprint and browser session",
                owner="device-risk@lakeflow.io",
            ),
        ]
        for ent in entities:
            self.register_entity(ent)

    def register_entity(self, entity: EntityDefinition) -> None:
        self._entities[entity.name] = entity

    def get_entity(self, name: str) -> Optional[EntityDefinition]:
        return self._entities.get(name)

    def list_entities(self) -> List[EntityDefinition]:
        return list(self._entities.values())

    def register_feature(self, feature: FeatureDefinition) -> None:
        """Registers a feature definition into the active catalog and preserves version history."""
        key = feature.name
        self._features[key] = feature
        if key not in self._version_history:
            self._version_history[key] = []
        self._version_history[key].append(feature)

    def get_feature(self, name: str, version: Optional[str] = None) -> Optional[FeatureDefinition]:
        """Retrieves a feature by name, optionally targeting an explicit version."""
        if version is None:
            return self._features.get(name)
        versions = self._version_history.get(name, [])
        for feat in versions:
            if feat.version == version:
                return feat
        return None

    def list_features(
        self,
        entity: Optional[str] = None,
        tag: Optional[str] = None,
        owner: Optional[str] = None,
        query: Optional[str] = None,
    ) -> List[FeatureDefinition]:
        """Returns features matching specified filter criteria."""
        results: List[FeatureDefinition] = []
        for feat in self._features.values():
            if entity and feat.entity.lower() != entity.lower():
                continue
            if tag and tag.lower() not in [t.lower() for t in feat.tags]:
                continue
            if owner and owner.lower() not in feat.owner.lower():
                continue
            if query:
                q = query.lower()
                matches = (
                    q in feat.name.lower()
                    or q in feat.description.lower()
                    or q in feat.entity.lower()
                    or q in feat.source.lower()
                )
                if not matches:
                    continue
            results.append(feat)
        return results

    def get_versions(self, name: str) -> List[str]:
        """Returns all registered version strings for a given feature."""
        return [f.version for f in self._version_history.get(name, [])]

    def get_freshness_report(self, as_of: Optional[datetime] = None) -> Dict[str, Any]:
        """Audits the freshness status of all registered features against their freshness SLAs."""
        now = as_of or datetime.now(timezone.utc)
        fresh_count = 0
        stale_count = 0
        details = []

        for feat in self._features.values():
            is_fresh = feat.is_fresh(now)
            if is_fresh:
                fresh_count += 1
            else:
                stale_count += 1

            details.append({
                "name": feat.name,
                "entity": feat.entity,
                "timestamp": feat.timestamp,
                "freshness_sla_seconds": feat.freshness_sla_seconds,
                "is_fresh": is_fresh,
                "owner": feat.owner,
                "version": feat.version,
            })

        total = len(self._features)
        freshness_pct = (fresh_count / total * 100.0) if total > 0 else 0.0

        return {
            "evaluated_at": now.isoformat(),
            "total_features": total,
            "fresh_features": fresh_count,
            "stale_features": stale_count,
            "freshness_percentage": round(freshness_pct, 2),
            "details": details,
        }

    def export_feast_yaml(self) -> str:
        """Exports the registry in a format aligned with Feast FeatureView definitions."""
        lines = [
            "# FeatureHub Feast-Compatible Feature Registry Export",
            f"# Generated: {datetime.now(timezone.utc).isoformat()}",
            f"# Total Features: {len(self._features)}",
            "---",
        ]
        # Group by entity
        by_entity: Dict[str, List[FeatureDefinition]] = {}
        for f in self._features.values():
            by_entity.setdefault(f.entity, []).append(f)

        for ent_name, feats in by_entity.items():
            lines.append(f"entity: {ent_name}")
            lines.append(f"  join_key: {self._entities.get(ent_name, EntityDefinition(ent_name, f'{ent_name}_id', '')).join_key}")
            lines.append(f"  features:")
            for f in feats:
                lines.append(f"    - name: {f.name}")
                lines.append(f"      type: {f.type}")
                lines.append(f"      description: \"{f.description}\"")
                lines.append(f"      source: {f.source}")
                lines.append(f"      version: {f.version}")
                lines.append(f"      owner: {f.owner}")
                lines.append(f"      timestamp: {f.timestamp}")
            lines.append("---")
        return "\n".join(lines)

    def to_json(self) -> str:
        data = {
            "initialized_at": self._initialized_at,
            "total_features": len(self._features),
            "entities": {k: v.to_dict() for k, v in self._entities.items()},
            "features": {k: v.to_dict() for k, v in self._features.items()},
        }
        return json.dumps(data, indent=2)


# Global singleton instance
_registry_instance: Optional[FeatureRegistry] = None

def get_registry() -> FeatureRegistry:
    """Returns the singleton registry loaded with all production features."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = FeatureRegistry()
        for feature in get_all_features():
            _registry_instance.register_feature(feature)
    return _registry_instance
