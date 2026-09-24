"""Unit tests for Redis Online Store and Materialization."""

import pytest
from featurehub.online.redis_store import RedisOnlineStore
from featurehub.online.materializer import MaterializationEngine


def test_online_store_write_and_read():
    store = RedisOnlineStore()
    features = {
        "customer_tx_count_24h": 5,
        "customer_spend_amount_24h": 320.50,
        "customer_is_vip": True,
    }
    store.write_features("customer", "test_cust_001", features)
    retrieved = store.read_entity_features("customer", "test_cust_001")

    assert retrieved["customer_tx_count_24h"] == 5
    assert abs(retrieved["customer_spend_amount_24h"] - 320.50) < 1e-4
    assert retrieved["customer_is_vip"] is True


def test_multi_entity_online_retrieval():
    store = RedisOnlineStore()
    store.write_features("customer", "cust_test", {"cust_feat": 10})
    store.write_features("merchant", "merch_test", {"merch_feat": 20})
    store.write_features("device", "dev_test", {"dev_feat": 30})

    batch = store.get_online_features(
        entity_keys={"customer": "cust_test", "merchant": "merch_test", "device": "dev_test"},
        feature_names=["cust_feat", "merch_feat", "dev_feat"],
    )

    assert batch["cust_feat"] == 10
    assert batch["merch_feat"] == 20
    assert batch["dev_feat"] == 30


def test_materializer_end_to_end():
    engine = MaterializationEngine()
    summary = engine.materialize_all()

    assert summary["status"] == "completed"
    assert summary["total_records_written"] > 0
    assert summary["total_entities_processed"] == 3
