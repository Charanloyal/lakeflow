"""Unit tests for FeatureHub Feature Registry."""

import pytest
from featurehub.registry.registry import get_registry
from featurehub.registry.metadata import FeatureDefinition


def test_registry_contains_at_least_120_features():
    registry = get_registry()
    features = registry.list_features()
    assert len(features) >= 120, f"Expected at least 120 features, but found {len(features)}"


def test_every_feature_has_all_8_mandatory_fields():
    registry = get_registry()
    features = registry.list_features()

    for feat in features:
        # 1. name
        assert feat.name and isinstance(feat.name, str), f"Missing name in {feat}"
        # 2. type
        assert feat.type and isinstance(feat.type, str), f"Missing type in {feat.name}"
        # 3. description
        assert feat.description and isinstance(feat.description, str), f"Missing description in {feat.name}"
        # 4. entity
        assert feat.entity in ["customer", "merchant", "transaction", "device"], f"Invalid entity in {feat.name}"
        # 5. source
        assert feat.source and isinstance(feat.source, str), f"Missing source in {feat.name}"
        # 6. timestamp
        assert feat.timestamp and isinstance(feat.timestamp, str), f"Missing timestamp in {feat.name}"
        # 7. owner
        assert "@" in feat.owner, f"Invalid owner format in {feat.name}"
        # 8. version
        assert feat.version and isinstance(feat.version, str), f"Missing version in {feat.name}"


def test_registry_filtering_and_search():
    registry = get_registry()

    customer_feats = registry.list_features(entity="customer")
    assert len(customer_feats) >= 30
    for f in customer_feats:
        assert f.entity == "customer"

    velocity_feats = registry.list_features(tag="velocity")
    assert len(velocity_feats) > 5

    search_feats = registry.list_features(query="spend")
    assert len(search_feats) > 0


def test_freshness_reporting():
    registry = get_registry()
    report = registry.get_freshness_report()
    assert "total_features" in report
    assert "freshness_percentage" in report
    assert report["total_features"] >= 120
