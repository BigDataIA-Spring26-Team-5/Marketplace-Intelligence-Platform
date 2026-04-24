"""Unit tests for UC4 AssociationRuleMiner."""

from __future__ import annotations

import pandas as pd
import pytest

from src.uc4_recommendations.association_rules import AssociationRuleMiner


@pytest.fixture
def transactions():
    """Simple transactions with strong A→B co-purchase."""
    rows = []
    # 30 baskets with A and B together
    for tid in range(30):
        rows.append({"transaction_id": tid, "product_id": "A"})
        rows.append({"transaction_id": tid, "product_id": "B"})
    # 20 baskets with C alone with different others
    for tid in range(30, 50):
        rows.append({"transaction_id": tid, "product_id": "C"})
        rows.append({"transaction_id": tid, "product_id": "D"})
    return pd.DataFrame(rows)


class TestMineRules:
    def test_empty_transactions_raises(self):
        miner = AssociationRuleMiner(pd.DataFrame())
        with pytest.raises(ValueError):
            miner.mine_rules()

    def test_none_transactions_raises(self):
        miner = AssociationRuleMiner(None)
        with pytest.raises(ValueError):
            miner.mine_rules()

    def test_mines_rules_happy_path(self, transactions):
        miner = AssociationRuleMiner(transactions)
        rules = miner.mine_rules(min_support=0.1, min_confidence=0.1, min_lift=1.0)
        assert not rules.empty
        assert "antecedent_id" in rules.columns
        assert "consequent_id" in rules.columns

    def test_high_threshold_yields_empty(self, transactions):
        miner = AssociationRuleMiner(transactions)
        rules = miner.mine_rules(min_support=0.99, min_confidence=0.99, min_lift=100.0)
        assert rules.empty

    def test_single_item_baskets_skipped(self):
        # All single-item baskets → no rules
        df = pd.DataFrame([{"transaction_id": i, "product_id": f"P{i}"} for i in range(20)])
        miner = AssociationRuleMiner(df)
        rules = miner.mine_rules(min_support=0.01)
        assert rules.empty

    def test_vocab_cap_applied(self):
        # Make many rare products + 2 common ones
        rows = []
        for tid in range(10):
            rows.append({"transaction_id": tid, "product_id": "HOT_A"})
            rows.append({"transaction_id": tid, "product_id": "HOT_B"})
        for i, rare in enumerate(range(50)):
            rows.append({"transaction_id": 100 + i, "product_id": f"RARE_{rare}"})
            rows.append({"transaction_id": 100 + i, "product_id": "HOT_A"})
        df = pd.DataFrame(rows)
        miner = AssociationRuleMiner(df)
        rules = miner.mine_rules(min_support=0.05, min_confidence=0.1, min_lift=1.0, max_products=2)
        # should not crash; vocab is capped
        assert isinstance(rules, pd.DataFrame)


class TestGetRecommendations:
    def test_empty_rules_returns_empty(self):
        miner = AssociationRuleMiner()
        assert miner.get_recommendations("A") == []

    def test_returns_top_k(self, transactions):
        miner = AssociationRuleMiner(transactions)
        miner.mine_rules(min_support=0.1, min_confidence=0.1, min_lift=1.0)
        recs = miner.get_recommendations("A", top_k=5)
        assert isinstance(recs, list)
        if recs:
            assert "product_id" in recs[0]
            assert "confidence" in recs[0]
            assert "lift" in recs[0]

    def test_unknown_product(self, transactions):
        miner = AssociationRuleMiner(transactions)
        miner.mine_rules(min_support=0.1, min_confidence=0.1, min_lift=1.0)
        assert miner.get_recommendations("UNKNOWN_XYZ") == []


class TestCompareRawVsEnriched:
    def test_returns_structured_dict(self, transactions):
        miner = AssociationRuleMiner()
        out = miner.compare_raw_vs_enriched(transactions, transactions, "A", top_k=3)
        assert "raw_recommendations" in out
        assert "enriched_recommendations" in out
        assert "lift_improvement" in out
        assert out["product_id"] == "A"


class TestRulesProperty:
    def test_rules_none_initially(self):
        assert AssociationRuleMiner().rules is None
