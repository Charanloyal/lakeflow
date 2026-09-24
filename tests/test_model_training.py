"""Unit tests for ML model training, reproducible evaluation, and persistence."""

import pytest
import numpy as np
from featurehub.models.model import FraudClassifier
from featurehub.models.train import train_fraud_model


def test_fraud_classifier_fit_and_convergence():
    rng = np.random.default_rng(42)
    # Generate simple separable synthetic data
    X = rng.normal(size=(200, 5))
    # Target correlated with features 0 and 1
    logits = 2.0 * X[:, 0] - 1.5 * X[:, 1]
    probs = 1.0 / (1.0 + np.exp(-logits))
    y = (probs > 0.5).astype(int)

    feature_names = [f"f_{i}" for i in range(5)]
    model = FraudClassifier(learning_rate=0.1, n_epochs=50, random_state=42)
    model.fit(X, y, feature_names=feature_names)

    # Verify loss decreases
    assert model.training_history["loss"][0] > model.training_history["loss"][-1]
    # Verify ROC-AUC is high
    assert model.metrics["roc_auc"] > 0.85

    # Test serialization and restoration
    data_dict = model.to_dict()
    restored = FraudClassifier.from_dict(data_dict)
    test_vec = np.array([[1.0, -1.0, 0.0, 0.0, 0.0]])
    assert abs(model.predict_proba(test_vec)[0] - restored.predict_proba(test_vec)[0]) < 1e-6


def test_reproducible_training_pipeline():
    model1 = train_fraud_model(num_transactions=300, seed=42, save_artifacts=False)
    model2 = train_fraud_model(num_transactions=300, seed=42, save_artifacts=False)

    # Identical random seed must yield identical weights
    np.testing.assert_allclose(model1.weights, model2.weights, rtol=1e-5, atol=1e-5)
    assert model1.metrics["roc_auc"] == model2.metrics["roc_auc"]
