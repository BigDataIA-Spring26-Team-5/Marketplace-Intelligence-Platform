"""Unit tests for embedding_enrich (Strategy 2 KNN)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.enrichment import embedding as emb_mod


def _df(n=3):
    return pd.DataFrame({
        "product_name": [f"Product {i}" for i in range(n)],
        "primary_category": [None] * n,
    })


class TestEmbeddingEnrich:
    def test_returns_early_if_primary_category_not_requested(self):
        df = _df(2)
        mask = pd.Series([True, True])
        out_df, out_mask, stats = emb_mod.embedding_enrich(df, ["allergens"], mask)
        assert stats == {"resolved": 0}

    def test_returns_early_if_mask_empty(self):
        df = _df(2)
        df["primary_category"] = ["Dairy", "Meat"]  # no nulls
        mask = pd.Series([True, True])
        out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 0

    def test_chroma_unavailable_returns_zero(self):
        df = _df(2)
        mask = pd.Series([True, True])
        with patch.object(emb_mod, "load_corpus", return_value=(None, [])):
            out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 0
        assert "_knn_neighbors" in out_df.columns

    def test_corpus_too_small_skips(self):
        df = _df(2)
        mask = pd.Series([True, True])
        fake_index = MagicMock()
        fake_index.count.return_value = 5  # < MIN_ENRICHMENT_CORPUS
        with patch.object(emb_mod, "load_corpus", return_value=(fake_index, [])), \
             patch.object(emb_mod, "evict_corpus"), \
             patch.object(emb_mod, "augment_from_df", return_value=0):
            out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats.get("skipped") == "corpus_too_small"

    def test_batch_search_success(self, monkeypatch):
        df = _df(2)
        mask = pd.Series([True, True])
        fake_index = MagicMock()
        fake_index.count.return_value = 5000
        neighbors = [{"category": "Dairy", "product_name": "Milk", "similarity": 0.9}]
        with patch.object(emb_mod, "load_corpus", return_value=(fake_index, [])), \
             patch.object(emb_mod, "evict_corpus"), \
             patch.object(emb_mod, "augment_from_df", return_value=0), \
             patch.object(emb_mod, "knn_search_batch", return_value=[("Dairy", 0.9, neighbors), (None, 0.0, [])]), \
             patch.object(emb_mod, "add_to_corpus"), \
             patch.object(emb_mod, "save_corpus"), \
             monkeypatch.context() as m:
            m.setattr(emb_mod, "MIN_ENRICHMENT_CORPUS", 100)
            out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 1
        assert out_df.at[0, "primary_category"] == "Dairy"

    def test_batch_search_exception(self, monkeypatch):
        df = _df(2)
        mask = pd.Series([True, True])
        fake_index = MagicMock()
        fake_index.count.return_value = 5000
        with patch.object(emb_mod, "load_corpus", return_value=(fake_index, [])), \
             patch.object(emb_mod, "evict_corpus"), \
             patch.object(emb_mod, "augment_from_df", return_value=0), \
             patch.object(emb_mod, "knn_search_batch", side_effect=RuntimeError("boom")), \
             monkeypatch.context() as m:
            m.setattr(emb_mod, "MIN_ENRICHMENT_CORPUS", 100)
            out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 0

    def test_import_error_skips(self, monkeypatch):
        df = _df(1)
        mask = pd.Series([True])
        fake_index = MagicMock()
        fake_index.count.return_value = 5000
        with patch.object(emb_mod, "load_corpus", return_value=(fake_index, [])), \
             patch.object(emb_mod, "evict_corpus"), \
             patch.object(emb_mod, "augment_from_df", return_value=0), \
             patch.object(emb_mod, "knn_search_batch", side_effect=ImportError("no faiss")), \
             monkeypatch.context() as m:
            m.setattr(emb_mod, "MIN_ENRICHMENT_CORPUS", 100)
            out_df, _, stats = emb_mod.embedding_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 0
