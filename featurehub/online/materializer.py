"""Materialization Engine for FeatureHub.
Pushes batch/streaming feature computations from PostgreSQL / Offline Store into the Redis Online Store.
"""

from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd
from featurehub.offline.offline_store import OfflineFeatureStore
from featurehub.online.redis_store import RedisOnlineStore, get_online_store
from featurehub.registry.registry import get_registry


class MaterializationEngine:
    """Orchestrates syncing feature state from offline storage to low-latency Redis online store."""

    def __init__(
        self,
        offline_store: Optional[OfflineFeatureStore] = None,
        online_store: Optional[RedisOnlineStore] = None,
    ):
        self.offline_store = offline_store or OfflineFeatureStore()
        self.online_store = online_store or get_online_store()
        self.registry = get_registry()

    def materialize_entity(
        self,
        entity_name: str,
        features_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """Materializes latest computed feature state for a specific entity into Redis.

        Args:
            entity_name: Target entity ('customer', 'merchant', 'device').
            features_df: Optional DataFrame with entity features. If None, loaded from offline store.

        Returns:
            Dict[str, Any]: Summary metrics for the materialization job.
        """
        start_time = time.time()
        entity_def = self.registry.get_entity(entity_name)
        join_key = entity_def.join_key if entity_def else f"{entity_name}_id"

        df = features_df if features_df is not None else self.offline_store.get_feature_table(entity_name)
        if df.empty:
            return {"entity": entity_name, "status": "skipped", "records_written": 0}

        # Keep the latest snapshot per entity ID
        if "feature_timestamp" in df.columns:
            df = df.sort_values(by="feature_timestamp").groupby(join_key).last().reset_index()
        else:
            df = df.groupby(join_key).last().reset_index()

        # Columns to write (excluding timestamp and key itself)
        feature_cols = [c for c in df.columns if c not in [join_key, "feature_timestamp"]]

        records_written = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for _, row in df.iterrows():
            entity_id = str(row[join_key])
            feature_dict = {col: row[col] for col in feature_cols if pd.notna(row[col])}
            self.online_store.write_features(
                entity_name=entity_name,
                entity_id=entity_id,
                features=feature_dict,
            )
            records_written += 1

        duration_ms = (time.time() - start_time) * 1000.0

        # Update registry timestamps to reflect fresh materialization
        for feat_name in feature_cols:
            feat_def = self.registry.get_feature(feat_name)
            if feat_def:
                feat_def.timestamp = now_iso

        return {
            "entity": entity_name,
            "status": "success",
            "records_written": records_written,
            "features_materialized": len(feature_cols),
            "duration_ms": round(duration_ms, 2),
            "completed_at": now_iso,
        }

    def materialize_all(self) -> Dict[str, Any]:
        """Materializes all supported entities into the online store."""
        start_time = time.time()
        results = []
        for entity_def in self.registry.list_entities():
            ent_name = entity_def.name
            if ent_name == "transaction":
                # Transactions are dynamic events, not stored entities
                continue
            try:
                res = self.materialize_entity(ent_name)
                results.append(res)
            except Exception as e:
                results.append({
                    "entity": ent_name,
                    "status": "error",
                    "error": str(e),
                })

        total_records = sum(r.get("records_written", 0) for r in results)
        total_duration = (time.time() - start_time) * 1000.0

        return {
            "status": "completed",
            "total_entities_processed": len(results),
            "total_records_written": total_records,
            "total_duration_ms": round(total_duration, 2),
            "entities": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
