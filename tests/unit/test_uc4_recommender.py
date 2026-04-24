"""Unit tests for UC4 ProductRecommender."""

from __future__ import annotations

import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.uc4_recommendations.recommender import ProductRecommender


@pytest.fixture
def enriched_df():
    return pd.DataFrame([
        {"product_id": "P1", "product_name": "Organic Oats", "brand_name": "Bob",
         "primary_category": "Cereal", "dietary_tags": "gluten-free", "allergens": "",
         "dq_score_post": 0.9, "is_recalled": False, "recall_class": ""},
        {"product_id": "P2", "product_name": "Whole Milk", "brand_name": "Dairy",
         "primary_category": "Dairy", "dietary_tags": "", "allergens": "milk",
         "dq_score_post": 0.8, "is_recalled": False, "recall_class": ""},
        {"product_id": "P3", "product_name": "Bad Product", "brand_name": "X",
         "primary_category": "Other", "dietary_tags": "", "allergens": "",
         "dq_score_post": 0.1, "is_recalled": True, "recall_class": "Class I"},
    ])


@pytest.fixture
def transactions_df():
    rows = []
    for tid in range(30):
        rows.append({"transaction_id": tid, "product_id": "P1", "product_name": "Organic Oats"})
        rows.append({"transaction_id": tid, "product_id": "P2", "product_name": "Whole Milk"})
    return pd.DataFrame(rows)


class TestInit:
    def test_creates_empty(self):
        rec = ProductRecommender()
        assert rec._products is None
        assert rec.is_ready() is False


class TestBuild:
    def test_builds_successfully(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        stats = rec.build(enriched_df, transactions_df, safety_filter=False)
        assert "products_indexed" in stats
        assert stats["products_indexed"] >= 2

    def test_safety_filter_removes_recalled(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=True)
        assert "P3" not in rec._products["product_id"].values

    def test_adds_product_id_if_missing(self, transactions_df):
        df = pd.DataFrame([
            {"product_name": "A", "brand_name": ""},
            {"product_name": "B", "brand_name": ""},
        ])
        # Build transactions referencing the auto-generated IDs (index strings)
        tx = pd.DataFrame([
            {"transaction_id": i, "product_id": "0"} for i in range(3)
        ] + [
            {"transaction_id": i, "product_id": "1"} for i in range(3)
        ])
        rec = ProductRecommender()
        rec.build(df, tx, safety_filter=False)
        assert "product_id" in rec._products.columns


class TestFindProduct:
    def test_none_when_not_built(self):
        assert ProductRecommender().find_product("x") is None

    def test_exact_id_match(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        assert rec.find_product("P1") == "P1"

    def test_substring_name_match(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        assert rec.find_product("oats") == "P1"

    def test_no_match(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        assert rec.find_product("zzzneverexists") is None


class TestGetProductName:
    def test_returns_pid_when_none(self):
        assert ProductRecommender()._get_product_name("P1") == "P1"

    def test_returns_name_when_found(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        assert rec._get_product_name("P1") == "Organic Oats"

    def test_returns_pid_if_not_found(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        assert rec._get_product_name("NOPE") == "NOPE"


class TestTopAntecedents:
    def test_empty_when_no_rules(self):
        assert ProductRecommender().top_antecedents() == []

    def test_returns_summary(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec.top_antecedents(n=5)
        assert isinstance(out, list)
        if out:
            assert "product_id" in out[0]
            assert "product_name" in out[0]


class TestRecommendations:
    def test_also_bought(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec.also_bought("P1", top_k=5)
        assert isinstance(out, list)

    def test_also_bought_with_name_query(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec.also_bought("oats", top_k=5)
        assert isinstance(out, list)

    def test_you_might_like(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec.you_might_like("P1", top_k=5)
        assert isinstance(out, list)


class TestStats:
    def test_empty(self):
        s = ProductRecommender().stats()
        assert s["products"] == 0
        assert s["rules"] == 0

    def test_after_build(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        s = rec.stats()
        assert s["products"] >= 2


class TestEnrichRec:
    def test_no_products(self):
        rec = ProductRecommender()
        r = {"product_id": "X"}
        assert rec._enrich_rec(r) == r

    def test_adds_metadata(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec._enrich_rec({"product_id": "P1"})
        assert out["product_name"] == "Organic Oats"
        assert out["primary_category"] == "Cereal"


class TestPersistence:
    def test_is_saved_false_when_missing(self, tmp_path):
        assert ProductRecommender.is_saved(tmp_path) is False

    def test_save_and_load(self, enriched_df, transactions_df, tmp_path):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        rec.save(tmp_path)
        assert (tmp_path / "products.parquet").exists()
        assert (tmp_path / "graph.pkl").exists()
        assert (tmp_path / "meta.json").exists()
        assert ProductRecommender.is_saved(tmp_path) is True

        rec2 = ProductRecommender.load(tmp_path)
        assert rec2._products is not None
        assert len(rec2._products) == len(rec._products)

    def test_load_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ProductRecommender.load(tmp_path)


class TestDemoComparison:
    def test_returns_structured_dict(self, enriched_df, transactions_df):
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df, safety_filter=False)
        out = rec.demo_comparison(transactions_df, "P1", top_k=3)
        assert "product_id" in out
        assert "max_lift_raw" in out
        assert "max_lift_enriched" in out
        assert "signal_consolidation_ratio" in out


class TestLoadFromBigQuery:
    def test_import_error(self, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "google.cloud", None)
        with pytest.raises(ImportError):
            ProductRecommender.load_from_bigquery()

    def test_runs_with_mocked_bq(self):
        fake_bq = MagicMock()
        client = MagicMock()
        tx_df = pd.DataFrame([{"transaction_id": 1, "product_id": "A", "product_name": "a"}])
        prod_df = pd.DataFrame([{"product_id": "A", "product_name": "a", "aisle_id": 1,
                                  "department_id": 1, "primary_category": "X"}])
        query_job = MagicMock()
        query_job.to_dataframe.side_effect = [tx_df, prod_df]
        client.query.return_value = query_job
        fake_bq.Client.return_value = client
        fake_google_cloud = MagicMock()
        fake_google_cloud.bigquery = fake_bq
        fake_google = MagicMock()
        fake_google.cloud = fake_google_cloud
        with patch.dict("sys.modules", {"google": fake_google, "google.cloud": fake_google_cloud,
                                          "google.cloud.bigquery": fake_bq}):
            tx, p = ProductRecommender.load_from_bigquery(sample_orders=10)
        assert len(tx) == 1
        assert len(p) == 1
