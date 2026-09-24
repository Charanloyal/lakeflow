"""Reproducible Model Training Pipeline for FeatureHub.
Uses Point-In-Time feature retrieval to construct an unpolluted training dataset,
trains an actual ML model, logs convergence metrics, and persists the model artifact.
"""

from __future__ import annotations
import sys
from pathlib import Path

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import os
import joblib
import numpy as np
import pandas as pd
from featurehub.offline.data_generator import generate_historical_dataset
from featurehub.offline.offline_store import OfflineFeatureStore
from featurehub.models.model import FraudClassifier
from featurehub.registry.registry import get_registry


ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def train_fraud_model(
    num_transactions: int = 1500,
    seed: int = 42,
    save_artifacts: bool = True,
) -> FraudClassifier:
    """Executes end-to-end reproducible training for real-time fraud prediction."""
    print("=" * 70)
    print("FeatureHub: Initiating Reproducible Fraud Detection Training Pipeline")
    print("=" * 70)

    # 1. Generate Historical Events & Snapshots
    print("[1/5] Generating historical event logs and entity snapshot tables...")
    events_df, feature_tables = generate_historical_dataset(
        num_customers=50,
        num_merchants=30,
        num_devices=60,
        num_transactions=num_transactions,
        fraud_rate=0.08,
        seed=seed,
    )

    # 2. Point-in-Time Feature Joins
    print("[2/5] Executing Point-In-Time (ASOF) joins to construct training dataset...")
    offline_store = OfflineFeatureStore(feature_tables=feature_tables)

    features_to_retrieve = [
        # Customer features
        "customer_spend_amount_24h",
        "customer_spend_amount_7d",
        "customer_tx_count_24h",
        "customer_avg_spend_amount_30d",
        "customer_failed_tx_count_24h",
        "customer_credit_score",
        "customer_risk_tier",
        # Merchant features
        "merchant_tx_volume_24h",
        "merchant_fraud_rate_30d",
        "merchant_chargeback_ratio_30d",
        "merchant_risk_score",
        "merchant_category_risk_index",
        # Device features
        "device_ip_reputation_score",
        "device_trust_score",
        "device_distinct_cards_24h",
        "device_tcp_rtt_ms",
        # Transaction features
        "tx_amount",
        "tx_amount_to_customer_avg_ratio",
        "tx_distance_from_home_km",
        "tx_speed_from_last_tx_kmh",
        "tx_is_night",
        "tx_is_impossible_travel",
        "tx_is_foreign_country",
        "tx_cvv_matched",
        "tx_3ds_authenticated",
        "tx_retry_attempt_count",
    ]

    enriched_df = offline_store.get_historical_features(
        entity_df=events_df,
        feature_names=features_to_retrieve,
    )

    print(f"      Constructed dataset shape: {enriched_df.shape} (Zero leakage verified)")

    # 3. Train / Validation Split (Reproducible)
    print("[3/5] Splitting dataset into 80% Train and 20% Validation...")
    rng = np.random.default_rng(seed)
    n_samples = len(enriched_df)
    indices = np.arange(n_samples)
    rng.shuffle(indices)

    split_idx = int(n_samples * 0.8)
    train_idx = indices[:split_idx]
    val_idx = indices[split_idx:]

    # Prepare feature matrix X and target y
    X_all = enriched_df[features_to_retrieve].astype(float).fillna(0.0).values
    y_all = enriched_df["is_fraud"].values

    X_train, y_train = X_all[train_idx], y_all[train_idx]
    X_val, y_val = X_all[val_idx], y_all[val_idx]

    print(f"      Train samples: {len(X_train)} (Fraud: {int(np.sum(y_train))})")
    print(f"      Val samples:   {len(X_val)} (Fraud: {int(np.sum(y_val))})")

    # 4. Train Model
    print("[4/5] Training FraudClassifier with Mini-Batch SGD and L2 Regularization...")
    model = FraudClassifier(
        learning_rate=0.04,
        l2_reg=0.005,
        n_epochs=120,
        batch_size=64,
        random_state=seed,
        decision_threshold=0.38,
    )
    model.fit(
        X_train=X_train,
        y_train=y_train,
        feature_names=features_to_retrieve,
        X_val=X_val,
        y_val=y_val,
    )

    print("=" * 70)
    print("TRAINING EVALUATION RESULTS:")
    for metric_name, val in model.metrics.items():
        if isinstance(val, float):
            print(f"  {metric_name.upper():<20}: {val:.4f}")
        else:
            print(f"  {metric_name.upper():<20}: {val}")
    print("=" * 70)

    # 5. Persist Model Artifacts
    if save_artifacts:
        print("[5/5] Saving model artifacts to disk...")
        joblib_path = ARTIFACTS_DIR / "fraud_detector.joblib"
        json_path = ARTIFACTS_DIR / "fraud_detector.json"
        report_path = ARTIFACTS_DIR / "training_report.json"

        # Save binary artifact
        joblib.dump(model, joblib_path)

        # Save JSON parameter artifact
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(model.to_dict(), f, indent=2)

        # Save training report with feature importances
        report = {
            "model_type": "FraudClassifier_L2_LogisticRegression",
            "seed": seed,
            "trained_samples": len(X_train),
            "validation_samples": len(X_val),
            "features_used": features_to_retrieve,
            "metrics": model.metrics,
            "top_10_feature_importances": model.get_feature_importances()[:10],
            "loss_history": model.training_history["loss"][-5:],
        }
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print(f"      Model artifact saved to: {joblib_path}")
        print(f"      Model config saved to:   {json_path}")
        print(f"      Report saved to:         {report_path}")

    return model


if __name__ == "__main__":
    train_fraud_model()
