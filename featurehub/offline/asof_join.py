"""Point-In-Time (ASOF) Join Engine for FeatureHub.
Guarantees zero feature leakage / lookahead bias by strictly matching
feature states where feature_timestamp <= observation_timestamp.
"""

from __future__ import annotations
import pandas as pd
from typing import List, Optional


class PointInTimeJoinError(Exception):
    """Raised when point-in-time integrity constraints are violated."""
    pass


def point_in_time_join(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
    entity_key: str,
    event_timestamp_col: str = "timestamp",
    feature_timestamp_col: str = "feature_timestamp",
    feature_cols: Optional[List[str]] = None,
    tolerance: Optional[pd.Timedelta] = None,
) -> pd.DataFrame:
    """Executes a strict backward Point-In-Time (ASOF) join between observation events and historical feature snapshots.

    Args:
        events_df: DataFrame containing events with observation timestamps and entity keys.
        features_df: DataFrame containing feature values computed over time.
        entity_key: Primary key column to partition by (e.g. 'customer_id', 'merchant_id').
        event_timestamp_col: Timestamp column name in events_df.
        feature_timestamp_col: Timestamp column name in features_df.
        feature_cols: Subset of feature columns to join. If None, joins all feature columns.
        tolerance: Maximum time difference allowed between event and feature (e.g. pd.Timedelta('30 days')).

    Returns:
        pd.DataFrame: Events joined with point-in-time feature values. Guaranteed no future data leakage.
    """
    if events_df.empty:
        return events_df.copy()

    left = events_df.copy()
    right = features_df.copy()

    # Convert timestamps to datetime with UTC timezone
    left[event_timestamp_col] = pd.to_datetime(left[event_timestamp_col], utc=True)
    right[feature_timestamp_col] = pd.to_datetime(right[feature_timestamp_col], utc=True)

    # Sort both datasets chronologically (strict requirement for ASOF joins)
    left = left.sort_values(by=event_timestamp_col).reset_index(drop=True)
    right = right.sort_values(by=feature_timestamp_col).reset_index(drop=True)

    # Ensure entity keys match types
    left[entity_key] = left[entity_key].astype(str)
    right[entity_key] = right[entity_key].astype(str)

    # Determine columns to select from right
    if feature_cols:
        select_cols = [entity_key, feature_timestamp_col] + [c for c in feature_cols if c in right.columns]
        right = right[select_cols]

    # Perform strict backward merge_asof: matches latest right row where feature_ts <= event_ts
    joined = pd.merge_asof(
        left,
        right,
        left_on=event_timestamp_col,
        right_on=feature_timestamp_col,
        by=entity_key,
        direction="backward",
        tolerance=tolerance,
    )

    # Verify that no future feature leaked into any event row
    leakage_mask = (joined[feature_timestamp_col] > joined[event_timestamp_col])
    if leakage_mask.any():
        leaked_count = int(leakage_mask.sum())
        raise PointInTimeJoinError(
            f"CRITICAL LEAKAGE DETECTED: {leaked_count} records received future feature states!"
        )

    return joined
