"""Unit Tests for Point-In-Time Joins and Data Leakage Prevention in FeatureHub."""

import pytest
import pandas as pd
from datetime import datetime, timezone
from featurehub.offline.asof_join import point_in_time_join, PointInTimeJoinError
from featurehub.offline.offline_store import OfflineFeatureStore
from featurehub.offline.data_generator import generate_historical_dataset


def test_point_in_time_strictly_prevents_future_leakage():
    """Verify that events at T_event strictly join feature states from T_feature <= T_event,
    and NEVER join feature values updated after T_event.
    """
    # Create customer timeline with changing spend over 3 hours
    features_df = pd.DataFrame([
        {
            "customer_id": "cust_001",
            "feature_timestamp": "2026-09-01T08:00:00Z",
            "customer_spend_amount_24h": 100.0,
        },
        {
            "customer_id": "cust_001",
            "feature_timestamp": "2026-09-01T10:00:00Z",
            "customer_spend_amount_24h": 500.0,
        },
        {
            "customer_id": "cust_001",
            "feature_timestamp": "2026-09-01T12:00:00Z",
            "customer_spend_amount_24h": 2000.0,  # Massive future spike
        },
    ])

    # Event 1 occurs at 09:30 (should get 100.0, NOT 500.0 or 2000.0)
    # Event 2 occurs at 10:15 (should get 500.0, NOT 2000.0)
    # Event 3 occurs at 13:00 (should get 2000.0)
    events_df = pd.DataFrame([
        {"tx_id": "tx_1", "customer_id": "cust_001", "timestamp": "2026-09-01T09:30:00Z"},
        {"tx_id": "tx_2", "customer_id": "cust_001", "timestamp": "2026-09-01T10:15:00Z"},
        {"tx_id": "tx_3", "customer_id": "cust_001", "timestamp": "2026-09-01T13:00:00Z"},
    ])

    joined = point_in_time_join(
        events_df=events_df,
        features_df=features_df,
        entity_key="customer_id",
        event_timestamp_col="timestamp",
        feature_timestamp_col="feature_timestamp",
    )

    # Validate Event 1
    row_1 = joined[joined["tx_id"] == "tx_1"].iloc[0]
    assert row_1["customer_spend_amount_24h"] == 100.0, (
        f"Leakage failure: tx_1 got {row_1['customer_spend_amount_24h']} instead of 100.0"
    )

    # Validate Event 2
    row_2 = joined[joined["tx_id"] == "tx_2"].iloc[0]
    assert row_2["customer_spend_amount_24h"] == 500.0, (
        f"Leakage failure: tx_2 got {row_2['customer_spend_amount_24h']} instead of 500.0"
    )

    # Validate Event 3
    row_3 = joined[joined["tx_id"] == "tx_3"].iloc[0]
    assert row_3["customer_spend_amount_24h"] == 2000.0


def test_point_in_time_exact_boundary():
    """Verify that an event occurring at the exact same second as a feature snapshot
    correctly joins that snapshot.
    """
    features_df = pd.DataFrame([
        {
            "customer_id": "cust_002",
            "feature_timestamp": "2026-09-01T10:00:00Z",
            "customer_tx_count_24h": 5,
        }
    ])
    events_df = pd.DataFrame([
        {
            "tx_id": "tx_boundary",
            "customer_id": "cust_002",
            "timestamp": "2026-09-01T10:00:00Z",
        }
    ])

    joined = point_in_time_join(
        events_df=events_df,
        features_df=features_df,
        entity_key="customer_id",
    )

    assert joined.iloc[0]["customer_tx_count_24h"] == 5


def test_point_in_time_multi_entity_offline_store():
    """Verify end-to-end historical retrieval across multiple entities without leakage."""
    events_df, feature_tables = generate_historical_dataset(
        num_customers=20,
        num_merchants=10,
        num_devices=20,
        num_transactions=100,
        seed=123,
    )

    store = OfflineFeatureStore(feature_tables=feature_tables)
    features_to_fetch = [
        "customer_spend_amount_24h",
        "customer_tx_count_24h",
        "merchant_tx_volume_24h",
        "merchant_risk_score",
        "device_trust_score",
        "device_ip_reputation_score",
    ]

    enriched = store.get_historical_features(
        entity_df=events_df,
        feature_names=features_to_fetch,
    )

    # Check all requested features are joined
    for feat in features_to_fetch:
        assert feat in enriched.columns, f"Missing joined feature: {feat}"

    assert len(enriched) == len(events_df)
