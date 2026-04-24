"""Unit tests for UC4 ProductGraph."""

from __future__ import annotations

import pandas as pd
import pytest

from src.uc4_recommendations.graph_store import ProductGraph


@pytest.fixture
def products_df():
    return pd.DataFrame([
        {"product_id": "P1", "product_name": "Oats", "brand_name": "Bob",
         "primary_category": "Cereal", "dietary_tags": "gluten-free",
         "allergens": "", "dq_score_post": 0.9},
        {"product_id": "P2", "product_name": "Milk", "brand_name": "Dairy",
         "primary_category": "Dairy", "dietary_tags": "",
         "allergens": "milk", "dq_score_post": 0.8},
        {"product_id": "P3", "product_name": "Honey", "brand_name": "Bee",
         "primary_category": "Sweetener", "dietary_tags": "",
         "allergens": "", "dq_score_post": 0.7},
    ])


@pytest.fixture
def rules_df():
    return pd.DataFrame([
        {"antecedent_id": "P1", "consequent_id": "P2", "lift": 3.0, "confidence": 0.7},
        {"antecedent_id": "P2", "consequent_id": "P3", "lift": 2.0, "confidence": 0.5},
        {"antecedent_id": "P1", "consequent_id": "UNKNOWN", "lift": 5.0, "confidence": 0.9},
    ])


class TestLoadProducts:
    def test_loads_nodes_and_categories(self, products_df):
        g = ProductGraph()
        n = g.load_products(products_df)
        assert n == 3
        stats = g.stats()
        assert stats["product_nodes"] == 3
        assert stats["category_nodes"] >= 3

    def test_uses_product_name_when_no_product_id(self, products_df):
        df = products_df.drop(columns=["product_id"])
        g = ProductGraph()
        n = g.load_products(df)
        assert n == 3

    def test_missing_optional_cols_filled(self):
        df = pd.DataFrame([{"product_id": "X", "product_name": "x"}])
        g = ProductGraph()
        assert g.load_products(df) == 1

    def test_empty_id_filtered(self):
        df = pd.DataFrame([
            {"product_id": "", "product_name": "x"},
            {"product_id": "A", "product_name": "a"},
        ])
        g = ProductGraph()
        assert g.load_products(df) == 1


class TestLoadRelationships:
    def test_adds_valid_edges_only(self, products_df, rules_df):
        g = ProductGraph()
        g.load_products(products_df)
        n = g.load_relationships(rules_df)
        # UNKNOWN consequent is filtered out
        assert n == 2

    def test_empty_rules(self, products_df):
        g = ProductGraph()
        g.load_products(products_df)
        assert g.load_relationships(pd.DataFrame(
            {"antecedent_id": [], "consequent_id": [], "lift": [], "confidence": []}
        )) == 0


class TestCrossCategory:
    def test_returns_cross_category(self, products_df, rules_df):
        g = ProductGraph()
        g.load_products(products_df)
        g.load_relationships(rules_df)
        recs = g.cross_category_recommendations("P1", max_hops=2, top_k=5)
        names = [r["product_id"] for r in recs]
        assert "P2" in names
        # Sorted by affinity
        if len(recs) > 1:
            assert recs[0]["affinity_score"] >= recs[1]["affinity_score"]

    def test_unknown_product_empty(self, products_df):
        g = ProductGraph()
        g.load_products(products_df)
        assert g.cross_category_recommendations("NOPE") == []

    def test_top_k_limits(self, products_df, rules_df):
        g = ProductGraph()
        g.load_products(products_df)
        g.load_relationships(rules_df)
        recs = g.cross_category_recommendations("P1", max_hops=3, top_k=1)
        assert len(recs) <= 1


class TestFindPath:
    def test_existing_path(self, products_df, rules_df):
        g = ProductGraph()
        g.load_products(products_df)
        g.load_relationships(rules_df)
        path = g.find_path("P1", "P3")
        assert path and path[0] == "P1" and path[-1] == "P3"

    def test_no_path(self, products_df):
        g = ProductGraph()
        g.load_products(products_df)
        assert g.find_path("P1", "P3") == []

    def test_missing_node(self, products_df):
        g = ProductGraph()
        g.load_products(products_df)
        assert g.find_path("NOPE", "P1") == []


class TestStats:
    def test_empty_graph(self):
        g = ProductGraph()
        s = g.stats()
        assert s["product_nodes"] == 0
        assert s["total_nodes"] == 0
