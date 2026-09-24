"""Machine Learning Model Architecture for Real-Time Fraud Prediction in FeatureHub.
Implements an actual trainable classifier with reproducible gradient optimization,
L2 regularization, convergence tracking, and feature attribution.
"""

from __future__ import annotations
import json
import numpy as np
from typing import Any, Dict, List, Optional, Tuple


class FraudClassifier:
    """Production-grade regularized classification model for real-time fraud scoring.
    Trained using Mini-Batch Stochastic Gradient Descent with L2 weight decay.
    """

    def __init__(
        self,
        learning_rate: float = 0.05,
        l2_reg: float = 0.01,
        n_epochs: int = 150,
        batch_size: int = 64,
        random_state: int = 42,
        decision_threshold: float = 0.40,
    ):
        self.learning_rate = learning_rate
        self.l2_reg = l2_reg
        self.n_epochs = n_epochs
        self.batch_size = batch_size
        self.random_state = random_state
        self.decision_threshold = decision_threshold

        # Learned parameters
        self.weights: Optional[np.ndarray] = None
        self.bias: float = 0.0
        self.feature_names: List[str] = []
        self.feature_means: Optional[np.ndarray] = None
        self.feature_stds: Optional[np.ndarray] = None
        self.training_history: Dict[str, List[float]] = {"loss": [], "val_auc": []}
        self.metrics: Dict[str, float] = {}

    @staticmethod
    def _sigmoid(z: np.ndarray) -> np.ndarray:
        # Clip z to prevent numerical overflow in exp(-z)
        z_clipped = np.clip(z, -30.0, 30.0)
        return 1.0 / (1.0 + np.exp(-z_clipped))

    def _standardize(self, X: np.ndarray, fit: bool = False) -> np.ndarray:
        if fit:
            self.feature_means = np.mean(X, axis=0)
            self.feature_stds = np.std(X, axis=0)
            # Avoid division by zero for constant features
            self.feature_stds[self.feature_stds == 0.0] = 1.0
        return (X - self.feature_means) / self.feature_stds

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        feature_names: List[str],
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> FraudClassifier:
        """Trains the model using gradient descent with exact loss computation."""
        rng = np.random.default_rng(self.random_state)
        self.feature_names = list(feature_names)
        n_samples, n_features = X_train.shape

        # Standardize features
        X_norm = self._standardize(X_train, fit=True)
        if X_val is not None:
            X_val_norm = self._standardize(X_val, fit=False)

        # Initialize weights with Xavier/Glorot scaling
        limit = np.sqrt(6.0 / (n_features + 1))
        self.weights = rng.uniform(-limit, limit, size=n_features)
        self.bias = float(np.log(max(0.01, np.mean(y_train) / (1.0 - np.mean(y_train)))))

        # Training loop
        for epoch in range(self.n_epochs):
            # Shuffle training data
            indices = np.arange(n_samples)
            rng.shuffle(indices)
            X_shuffled = X_norm[indices]
            y_shuffled = y_train[indices]

            epoch_losses = []
            for start_idx in range(0, n_samples, self.batch_size):
                end_idx = min(start_idx + self.batch_size, n_samples)
                xb = X_shuffled[start_idx:end_idx]
                yb = y_shuffled[start_idx:end_idx]
                b_size = xb.shape[0]

                # Forward pass
                linear = np.dot(xb, self.weights) + self.bias
                y_pred = self._sigmoid(linear)

                # Loss: Binary Cross-Entropy with L2 regularization
                eps = 1e-15
                y_pred_safe = np.clip(y_pred, eps, 1.0 - eps)
                bce = -np.mean(yb * np.log(y_pred_safe) + (1.0 - yb) * np.log(1.0 - y_pred_safe))
                reg_loss = (self.l2_reg / (2.0 * b_size)) * np.sum(self.weights ** 2)
                epoch_losses.append(bce + reg_loss)

                # Gradients
                errors = y_pred - yb
                dw = (np.dot(xb.T, errors) / b_size) + (self.l2_reg * self.weights / b_size)
                db = float(np.mean(errors))

                # Weight update
                self.weights -= self.learning_rate * dw
                self.bias -= self.learning_rate * db

            avg_loss = float(np.mean(epoch_losses))
            self.training_history["loss"].append(avg_loss)

            if X_val is not None and y_val is not None:
                val_preds = self._sigmoid(np.dot(X_val_norm, self.weights) + self.bias)
                val_auc = self.compute_roc_auc(y_val, val_preds)
                self.training_history["val_auc"].append(val_auc)

        # Compute final evaluation metrics on validation set if provided
        eval_X = X_val if X_val is not None else X_train
        eval_y = y_val if y_val is not None else y_train
        probs = self.predict_proba(eval_X)
        preds = (probs >= self.decision_threshold).astype(int)

        self.metrics = {
            "accuracy": float(np.mean(preds == eval_y)),
            "roc_auc": float(self.compute_roc_auc(eval_y, probs)),
            "precision": float(self.compute_precision(eval_y, preds)),
            "recall": float(self.compute_recall(eval_y, preds)),
            "f1_score": float(self.compute_f1(eval_y, preds)),
            "threshold": self.decision_threshold,
            "n_features": len(self.feature_names),
            "n_training_samples": n_samples,
        }

        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Returns fraud probability scores in [0.0, 1.0]."""
        if self.weights is None:
            raise ValueError("Model is not fitted yet.")
        X_norm = self._standardize(X, fit=False)
        linear = np.dot(X_norm, self.weights) + self.bias
        return self._sigmoid(linear)

    def predict(self, X: np.ndarray, threshold: Optional[float] = None) -> np.ndarray:
        t = threshold if threshold is not None else self.decision_threshold
        probs = self.predict_proba(X)
        return (probs >= t).astype(int)

    def explain_prediction(self, feature_vector: Dict[str, Any], top_k: int = 5) -> List[Dict[str, Any]]:
        """Returns the top contributing features for an individual inference."""
        if self.weights is None:
            return []

        contributions = []
        for i, fname in enumerate(self.feature_names):
            val = float(feature_vector.get(fname, 0.0))
            mean_val = float(self.feature_means[i])
            std_val = float(self.feature_stds[i])
            z_score = (val - mean_val) / std_val
            weight = float(self.weights[i])
            impact = z_score * weight

            contributions.append({
                "feature": fname,
                "value": round(val, 4),
                "weight": round(weight, 4),
                "impact": round(impact, 4),
                "direction": "RISK_INCREASING" if impact > 0 else "RISK_DECREASING",
            })

        # Sort by absolute impact descending
        contributions.sort(key=lambda x: abs(x["impact"]), reverse=True)
        return contributions[:top_k]

    def get_feature_importances(self) -> List[Dict[str, Any]]:
        """Returns normalized global feature importances based on learned weights."""
        if self.weights is None:
            return []
        abs_weights = np.abs(self.weights)
        total = np.sum(abs_weights) or 1.0
        normalized = abs_weights / total

        importance_list = []
        for name, norm_wt, raw_wt in zip(self.feature_names, normalized, self.weights):
            importance_list.append({
                "feature": name,
                "importance": round(float(norm_wt), 4),
                "weight": round(float(raw_wt), 4),
            })
        importance_list.sort(key=lambda x: x["importance"], reverse=True)
        return importance_list

    @staticmethod
    def compute_roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
        """Computes ROC-AUC via rank-sum Wilcoxon-Mann-Whitney statistic."""
        pos_mask = (y_true == 1)
        neg_mask = (y_true == 0)
        n_pos = int(np.sum(pos_mask))
        n_neg = int(np.sum(neg_mask))
        if n_pos == 0 or n_neg == 0:
            return 0.5

        ranks = np.argsort(np.argsort(y_score)) + 1
        pos_rank_sum = np.sum(ranks[pos_mask])
        u_stat = pos_rank_sum - (n_pos * (n_pos + 1)) / 2.0
        return float(u_stat / (n_pos * n_neg))

    @staticmethod
    def compute_precision(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fp = np.sum((y_true == 0) & (y_pred == 1))
        return float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0

    @staticmethod
    def compute_recall(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fn = np.sum((y_true == 1) & (y_pred == 0))
        return float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0

    @staticmethod
    def compute_f1(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        prec = FraudClassifier.compute_precision(y_true, y_pred)
        rec = FraudClassifier.compute_recall(y_true, y_pred)
        return float(2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Serializes model parameters into dictionary."""
        return {
            "model_type": "FraudClassifier_L2_LogisticRegression",
            "version": "1.0.0",
            "feature_names": self.feature_names,
            "weights": self.weights.tolist() if self.weights is not None else [],
            "bias": float(self.bias),
            "feature_means": self.feature_means.tolist() if self.feature_means is not None else [],
            "feature_stds": self.feature_stds.tolist() if self.feature_stds is not None else [],
            "decision_threshold": self.decision_threshold,
            "metrics": self.metrics,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FraudClassifier:
        """Restores model from serialized dictionary."""
        model = cls(decision_threshold=data.get("decision_threshold", 0.40))
        model.feature_names = data["feature_names"]
        model.weights = np.array(data["weights"], dtype=np.float64)
        model.bias = float(data["bias"])
        model.feature_means = np.array(data["feature_means"], dtype=np.float64)
        model.feature_stds = np.array(data["feature_stds"], dtype=np.float64)
        model.metrics = data.get("metrics", {})
        return model
