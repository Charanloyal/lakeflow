"""FastAPI Real-Time Prediction & Feature Serving Engine for FeatureHub."""

from __future__ import annotations
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Ensure repo root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from featurehub.registry.registry import get_registry
from featurehub.online.redis_store import get_online_store
from featurehub.online.materializer import MaterializationEngine
from featurehub.models.model import FraudClassifier

app = FastAPI(
    title="FeatureHub Prediction & Feature Store API",
    description="Real-time multi-entity feature retrieval, freshness monitoring, and low-latency fraud prediction.",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load registry, online store, and trained model
registry = get_registry()
online_store = get_online_store()
materializer = MaterializationEngine()

# Ensure online store has initial materialized data
materializer.materialize_all()

# Load saved model artifact
MODEL_PATH = Path(__file__).resolve().parent.parent / "models" / "artifacts" / "fraud_detector.joblib"
model: Optional[FraudClassifier] = None

if MODEL_PATH.exists():
    try:
        model = joblib.load(MODEL_PATH)
    except Exception as e:
        print(f"Warning: Failed to load model from {MODEL_PATH}: {e}")

if model is None:
    # Fallback to train on-the-fly if artifact not loaded
    from featurehub.models.train import train_fraud_model
    model = train_fraud_model(save_artifacts=True)


# =============================================================================
# Request & Response Schemas
# =============================================================================

class TransactionPredictionRequest(BaseModel):
    transaction_id: str = Field(..., example="tx_998124")
    customer_id: str = Field(..., example="cust_0001")
    merchant_id: str = Field(..., example="merch_0002")
    device_id: str = Field(..., example="dev_0003")
    amount: float = Field(..., example=145.50)
    hour_of_day: Optional[int] = Field(None, example=14)
    distance_from_home_km: Optional[float] = Field(None, example=12.4)
    speed_from_last_tx_kmh: Optional[float] = Field(None, example=35.0)
    is_foreign_country: Optional[bool] = Field(False, example=False)
    cvv_matched: Optional[bool] = Field(True, example=True)
    threeds_authenticated: Optional[bool] = Field(True, example=True)
    retry_attempt_count: Optional[int] = Field(0, example=0)
    override_features: Optional[Dict[str, Any]] = Field(default_factory=dict)


class FeatureContribution(BaseModel):
    feature: str
    value: float
    weight: float
    impact: float
    direction: str


class PredictionResponse(BaseModel):
    transaction_id: str
    fraud_probability: float
    decision: str  # APPROVE, REVIEW, DECLINE
    risk_tier: str  # LOW, MEDIUM, HIGH, CRITICAL
    retrieval_latency_ms: float
    inference_latency_ms: float
    total_latency_ms: float
    features_used_count: int
    top_explanations: List[FeatureContribution]
    features_snapshot: Dict[str, Any]
    timestamp: str


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/health", tags=["System"])
def health_check():
    store_stats = online_store.get_stats()
    return {
        "status": "healthy",
        "service": "FeatureHub",
        "version": "2.1.0",
        "model_loaded": model is not None,
        "online_store": store_stats,
        "features_registered": len(registry.list_features()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/registry/features", tags=["Registry"])
def list_registered_features(
    entity: Optional[str] = None,
    tag: Optional[str] = None,
    owner: Optional[str] = None,
    query: Optional[str] = None,
):
    features = registry.list_features(entity=entity, tag=tag, owner=owner, query=query)
    return {
        "total_matches": len(features),
        "features": [f.to_dict() for f in features],
    }


@app.get("/registry/freshness", tags=["Registry"])
def get_feature_freshness():
    return registry.get_freshness_report()


@app.get("/registry/entities", tags=["Registry"])
def list_entities():
    return {"entities": [e.to_dict() for e in registry.list_entities()]}


@app.get("/features/online/{entity_name}/{entity_id}", tags=["Online Store"])
def get_online_entity_features(entity_name: str, entity_id: str):
    features = online_store.read_entity_features(entity_name, entity_id)
    if not features:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entity '{entity_id}' of type '{entity_name}' not found in online store.",
        )
    return {
        "entity": entity_name,
        "entity_id": entity_id,
        "features_count": len(features),
        "features": features,
    }


@app.post("/features/materialize", tags=["Materialization"])
def trigger_materialization():
    """Triggers materialization job to refresh all online features from offline storage."""
    summary = materializer.materialize_all()
    return summary


@app.post("/predict/fraud", response_model=PredictionResponse, tags=["Real-Time Prediction"])
def predict_fraud(req: TransactionPredictionRequest):
    """Real-time fraud scoring endpoint:
    1. Fetches online features for customer, merchant, and device from Redis.
    2. Blends transaction payload features.
    3. Feeds feature vector into actual trained ML model.
    4. Returns score, decision, latency breakdown, and feature explanations.
    """
    start_total = time.perf_counter()

    # Step 1: Online Feature Retrieval
    t0_retrieval = time.perf_counter()
    entity_keys = {
        "customer": req.customer_id,
        "merchant": req.merchant_id,
        "device": req.device_id,
    }
    online_features = online_store.get_online_features(entity_keys=entity_keys, feature_names=[])
    retrieval_latency_ms = (time.perf_counter() - t0_retrieval) * 1000.0

    # Step 2: Combine with incoming transaction payload
    now = datetime.now(timezone.utc)
    hour = req.hour_of_day if req.hour_of_day is not None else now.hour
    is_night = (hour >= 23 or hour <= 5)

    combined_features: Dict[str, Any] = {
        # Defaults if not present in online store
        "customer_spend_amount_24h": float(online_features.get("customer_spend_amount_24h", 120.0)),
        "customer_spend_amount_7d": float(online_features.get("customer_spend_amount_7d", 600.0)),
        "customer_tx_count_24h": float(online_features.get("customer_tx_count_24h", 3.0)),
        "customer_avg_spend_amount_30d": float(online_features.get("customer_avg_spend_amount_30d", 55.0)),
        "customer_failed_tx_count_24h": float(online_features.get("customer_failed_tx_count_24h", 0.0)),
        "customer_credit_score": float(online_features.get("customer_credit_score", 710.0)),
        "customer_risk_tier": float(online_features.get("customer_risk_tier", 2.0)),
        "merchant_tx_volume_24h": float(online_features.get("merchant_tx_volume_24h", 15000.0)),
        "merchant_fraud_rate_30d": float(online_features.get("merchant_fraud_rate_30d", 0.002)),
        "merchant_chargeback_ratio_30d": float(online_features.get("merchant_chargeback_ratio_30d", 0.003)),
        "merchant_risk_score": float(online_features.get("merchant_risk_score", 0.15)),
        "merchant_category_risk_index": float(online_features.get("merchant_category_risk_index", 0.25)),
        "device_ip_reputation_score": float(online_features.get("device_ip_reputation_score", 8.0)),
        "device_trust_score": float(online_features.get("device_trust_score", 0.92)),
        "device_distinct_cards_24h": float(online_features.get("device_distinct_cards_24h", 1.0)),
        "device_tcp_rtt_ms": float(online_features.get("device_tcp_rtt_ms", 45.0)),
        # Dynamic Transaction attributes
        "tx_amount": float(req.amount),
        "tx_amount_to_customer_avg_ratio": float(req.amount / max(1.0, float(online_features.get("customer_avg_spend_amount_30d", 55.0)))),
        "tx_distance_from_home_km": float(req.distance_from_home_km if req.distance_from_home_km is not None else 8.5),
        "tx_speed_from_last_tx_kmh": float(req.speed_from_last_tx_kmh if req.speed_from_last_tx_kmh is not None else 25.0),
        "tx_is_night": 1.0 if is_night else 0.0,
        "tx_is_impossible_travel": 1.0 if (req.speed_from_last_tx_kmh or 0) > 800.0 else 0.0,
        "tx_is_foreign_country": 1.0 if req.is_foreign_country else 0.0,
        "tx_cvv_matched": 1.0 if req.cvv_matched else 0.0,
        "tx_3ds_authenticated": 1.0 if req.threeds_authenticated else 0.0,
        "tx_retry_attempt_count": float(req.retry_attempt_count or 0),
    }

    # Allow arbitrary feature overrides for what-if scenario testing
    if req.override_features:
        for k, v in req.override_features.items():
            combined_features[k] = float(v)

    # Step 3: Model Inference
    t0_inference = time.perf_counter()
    feature_vector = np.array([[combined_features.get(f, 0.0) for f in model.feature_names]], dtype=float)
    fraud_prob = float(model.predict_proba(feature_vector)[0])
    inference_latency_ms = (time.perf_counter() - t0_inference) * 1000.0

    # Step 4: Decision & Risk Tier Assignment
    if fraud_prob >= 0.70:
        decision = "DECLINE"
        risk_tier = "CRITICAL"
    elif fraud_prob >= 0.38:
        decision = "REVIEW"
        risk_tier = "HIGH"
    elif fraud_prob >= 0.15:
        decision = "APPROVE"
        risk_tier = "MEDIUM"
    else:
        decision = "APPROVE"
        risk_tier = "LOW"

    # Step 5: Feature Attributions
    raw_explanations = model.explain_prediction(combined_features, top_k=5)
    explanations = [FeatureContribution(**exp) for exp in raw_explanations]

    total_latency_ms = (time.perf_counter() - start_total) * 1000.0

    return PredictionResponse(
        transaction_id=req.transaction_id,
        fraud_probability=round(fraud_prob, 4),
        decision=decision,
        risk_tier=risk_tier,
        retrieval_latency_ms=round(retrieval_latency_ms, 3),
        inference_latency_ms=round(inference_latency_ms, 3),
        total_latency_ms=round(total_latency_ms, 3),
        features_used_count=len(model.feature_names),
        top_explanations=explanations,
        features_snapshot={k: round(v, 4) if isinstance(v, float) else v for k, v in combined_features.items()},
        timestamp=now.isoformat(),
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("featurehub.api.main:app", host="0.0.0.0", port=8000, reload=False)
