"""Integration tests for FastAPI Real-Time Prediction API."""

import pytest
from fastapi.testclient import TestClient
from featurehub.api.main import app

client = TestClient(app)


def test_api_health():
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["features_registered"] >= 120
    assert data["model_loaded"] is True


def test_api_registry_endpoints():
    res = client.get("/registry/features")
    assert res.status_code == 200
    data = res.json()
    assert data["total_matches"] >= 120

    res_fresh = client.get("/registry/freshness")
    assert res_fresh.status_code == 200
    assert "freshness_percentage" in res_fresh.json()


def test_api_realtime_prediction():
    payload = {
        "transaction_id": "tx_test_integration",
        "customer_id": "cust_0001",
        "merchant_id": "merch_0001",
        "device_id": "dev_0001",
        "amount": 55.0,
        "distance_from_home_km": 5.0,
        "speed_from_last_tx_kmh": 20.0,
        "is_foreign_country": False,
        "cvv_matched": True,
        "threeds_authenticated": True,
        "retry_attempt_count": 0,
    }
    res = client.post("/predict/fraud", json=payload)
    assert res.status_code == 200
    data = res.json()

    assert data["transaction_id"] == "tx_test_integration"
    assert 0.0 <= data["fraud_probability"] <= 1.0
    assert data["decision"] in ["APPROVE", "REVIEW", "DECLINE"]
    assert data["retrieval_latency_ms"] >= 0.0
    assert data["total_latency_ms"] < 25.0  # Ultra-fast real-time SLA
    assert len(data["top_explanations"]) > 0
