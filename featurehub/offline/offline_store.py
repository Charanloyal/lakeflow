"""Offline Feature Store for FeatureHub.
Coordinates historical feature retrieval and multi-entity Point-In-Time (ASOF) joins.
"""

from __future__ import annotations
import pandas as pd
from typing import Dict, List, Optional
from featurehub.offline.asof_join import point_in_time_join
from featurehub.offline.data_generator import generate_historical_dataset
from featurehub.registry.registry import get_registry


class OfflineFeatureStore:
    """Manages historical tables and point-in-time feature extraction for ML training."""

    def __init__(self, feature_tables: Optional[Dict[str, pd.DataFrame]] = None):
        self.registry = get_registry()
        if feature_tables is not None:
            self._feature_tables = feature_tables
        else:
            _, self._feature_tables = generate_historical_dataset()

    def get_feature_table(self, entity_name: str) -> pd.DataFrame:
        if entity_name not in self._feature_tables:
            raise KeyError(f"Entity table '{entity_name}' not found in offline store.")
        return self._feature_tables[entity_name]

    def set_feature_table(self, entity_name: str, df: pd.DataFrame) -> None:
        self._feature_tables[entity_name] = df

    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        feature_names: List[str],
        timestamp_col: str = "timestamp",
    ) -> pd.DataFrame:
        """Performs Point-In-Time joins to retrieve historical feature values without data leakage.

        Args:
            entity_df: Observation dataframe containing entity IDs and timestamps (e.g. transactions).
            feature_names: List of requested feature names from the registry.
            timestamp_col: Name of the event observation timestamp column.

        Returns:
            pd.DataFrame: Original entity_df enriched with point-in-time feature columns.
        """
        result_df = entity_df.copy()
        result_df[timestamp_col] = pd.to_datetime(result_df[timestamp_col], utc=True)

        # Categorize requested features by entity
        features_by_entity: Dict[str, List[str]] = {}
        for feat_name in feature_names:
            feat_def = self.registry.get_feature(feat_name)
            if feat_def is None:
                # If not registered, check if it's already an event column
                if feat_name in result_df.columns:
                    continue
                raise ValueError(f"Unknown feature requested: '{feat_name}'")
            features_by_entity.setdefault(feat_def.entity, []).append(feat_name)

        # Join features for each external entity
        for entity_name, feats in features_by_entity.items():
            if entity_name == "transaction":
                # Transaction features are already present in observation records
                continue

            entity_def = self.registry.get_entity(entity_name)
            join_key = entity_def.join_key if entity_def else f"{entity_name}_id"

            if join_key not in result_df.columns:
                raise ValueError(
                    f"Observation DataFrame missing required join key '{join_key}' for entity '{entity_name}'"
                )

            if entity_name not in self._feature_tables:
                continue

            feature_table = self._feature_tables[entity_name]
            result_df = point_in_time_join(
                events_df=result_df,
                features_df=feature_table,
                entity_key=join_key,
                event_timestamp_col=timestamp_col,
                feature_timestamp_col="feature_timestamp",
                feature_cols=feats,
            )

            # Drop temporary join artifact feature_timestamp column
            if "feature_timestamp" in result_df.columns:
                result_df = result_df.drop(columns=["feature_timestamp"])

        return result_df
