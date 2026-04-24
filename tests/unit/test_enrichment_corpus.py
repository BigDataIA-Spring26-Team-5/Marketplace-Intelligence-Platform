"""Unit tests for enrichment corpus (ChromaDB-backed)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.enrichment import corpus as corpus_mod
from src.enrichment.corpus import (
    _build_row_text,
    _compute_embedding_key,
    _make_vector_id,
    _score_from_neighbors,
    add_to_corpus,
    augment_from_df,
    build_seed_corpus,
    evict_corpus,
    knn_search,
    knn_search_batch,
    load_corpus,
    save_corpus,
)


class TestHelpers:
    def test_build_row_text_joins(self):
        row = pd.Series({
            "product_name": "Milk",
            "brand_name": "BrandX",
            "ingredients": "milk, salt",
            "category": "Dairy",
        })
        text = _build_row_text(row)
        assert "Milk" in text and "BrandX" in text

    def test_build_row_text_skips_nan(self):
        row = pd.Series({"product_name": "X", "brand_name": None})
        assert _build_row_text(row).strip() == "X"

    def test_embedding_key_deterministic(self):
        k1 = _compute_embedding_key("m", "t")
        k2 = _compute_embedding_key("m", "t")
        assert k1 == k2
        assert len(k1) == 16

    def test_vector_id_deterministic(self):
        a = _make_vector_id("milk", "Dairy")
        b = _make_vector_id("milk", "Dairy")
        assert a == b


class TestScoreFromNeighbors:
    def test_empty_votes(self):
        cat, conf, top = _score_from_neighbors([])
        assert cat is None and conf == 0.0

    def test_below_threshold(self):
        neighbors = [{"category": "D", "product_name": "x", "similarity": 0.3}]
        cat, _, _ = _score_from_neighbors(neighbors)
        assert cat is None

    def test_majority_vote(self):
        neighbors = [
            {"category": "Dairy", "product_name": "a", "similarity": 0.9},
            {"category": "Dairy", "product_name": "b", "similarity": 0.85},
            {"category": "Meat", "product_name": "c", "similarity": 0.5},
        ]
        cat, conf, top = _score_from_neighbors(neighbors)
        assert cat == "Dairy"
        assert conf >= 0.6

    def test_low_confidence_returns_none(self):
        neighbors = [
            {"category": "Dairy", "product_name": "a", "similarity": 0.5},
        ]
        cat, conf, _ = _score_from_neighbors(neighbors)
        assert cat is None


class TestLoadSaveCorpus:
    def test_load_corpus_success(self):
        fake = MagicMock()
        fake.count.return_value = 100
        with patch.object(corpus_mod, "_get_collection", return_value=fake):
            col, meta = load_corpus()
        assert col is fake
        assert meta == []

    def test_load_corpus_failure(self):
        with patch.object(corpus_mod, "_get_collection", side_effect=RuntimeError("down")):
            col, meta = load_corpus()
        assert col is None

    def test_save_corpus_noop_none(self):
        save_corpus(None, [])

    def test_save_corpus_logs_count(self):
        fake = MagicMock()
        fake.count.return_value = 5
        save_corpus(fake, [])


class TestEvict:
    def test_evict_no_stale(self):
        fake = MagicMock()
        fake.get.return_value = {"ids": [], "metadatas": []}
        fake.count.return_value = 10
        evict_corpus(fake)
        fake.delete.assert_not_called()

    def test_evict_stale(self):
        fake = MagicMock()
        fake.get.return_value = {"ids": ["a", "b"], "metadatas": [{}, {}]}
        fake.count.return_value = 10
        evict_corpus(fake)
        fake.delete.assert_called()

    def test_evict_handles_exception(self):
        fake = MagicMock()
        fake.get.side_effect = RuntimeError("x")
        fake.count.return_value = 10
        evict_corpus(fake)  # should not raise


class TestAugment:
    def test_no_labeled_rows(self):
        fake = MagicMock()
        fake.count.return_value = 0
        df = pd.DataFrame({"primary_category": [None, None]})
        n = augment_from_df(df, fake, unresolved_count=100)
        assert n == 0

    def test_ratio_threshold_skips(self):
        fake = MagicMock()
        fake.count.return_value = 50
        df = pd.DataFrame({"primary_category": ["Dairy"]})
        n = augment_from_df(df, fake, unresolved_count=100, force_ratio_threshold=0.25)
        assert n == 0  # 50/100 = 0.5 >= 0.25

    def test_upsert_when_below_threshold(self):
        fake = MagicMock()
        fake.count.return_value = 5
        df = pd.DataFrame({
            "primary_category": ["Dairy", "Meat"],
            "product_name": ["Milk", "Beef"],
        })
        model = MagicMock()
        model.encode.return_value = np.array([[0.1] * 8, [0.2] * 8], dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            n = augment_from_df(df, fake, unresolved_count=100)
        assert n == 2
        fake.upsert.assert_called()

    def test_model_none(self):
        fake = MagicMock()
        fake.count.return_value = 0
        df = pd.DataFrame({"primary_category": ["Dairy"], "product_name": ["a"]})
        with patch.object(corpus_mod, "_get_model", return_value=None):
            n = augment_from_df(df, fake, unresolved_count=100)
        assert n == 0


class TestBuildSeed:
    def test_too_few_rows_skips(self):
        df = pd.DataFrame({"primary_category": ["Dairy"], "product_name": ["a"]})
        with patch.object(corpus_mod, "_get_model", return_value=MagicMock()):
            build_seed_corpus(df)  # no raise

    def test_model_none_returns(self):
        df = pd.DataFrame({"primary_category": ["Dairy"] * 20, "product_name": ["a"] * 20})
        with patch.object(corpus_mod, "_get_model", return_value=None):
            build_seed_corpus(df)

    def test_chroma_unavailable(self):
        df = pd.DataFrame({"primary_category": ["Dairy"] * 20, "product_name": ["a"] * 20})
        model = MagicMock()
        model.encode.return_value = np.zeros((20, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model), \
             patch.object(corpus_mod, "_get_collection", side_effect=RuntimeError("down")):
            build_seed_corpus(df)

    def test_full_seed(self):
        df = pd.DataFrame({
            "primary_category": ["Dairy"] * 20,
            "product_name": ["P" + str(i) for i in range(20)],
        })
        model = MagicMock()
        model.encode.return_value = np.zeros((20, 8), dtype=np.float32)
        fake_col = MagicMock()
        with patch.object(corpus_mod, "_get_model", return_value=model), \
             patch.object(corpus_mod, "_get_collection", return_value=fake_col):
            build_seed_corpus(df)
        fake_col.upsert.assert_called()


class TestKnnSearch:
    def test_model_or_index_none(self):
        row = pd.Series({"product_name": "Milk"})
        result = knn_search(row, None, [])
        assert result == (None, 0.0, [])

    def test_corpus_too_small(self):
        fake = MagicMock()
        fake.count.return_value = 2
        with patch.object(corpus_mod, "_get_model", return_value=MagicMock()):
            result = knn_search(pd.Series({"product_name": "x"}), fake, [])
        assert result == (None, 0.0, [])

    def test_empty_text_returns_none(self):
        fake = MagicMock()
        fake.count.return_value = 100
        model = MagicMock()
        with patch.object(corpus_mod, "_get_model", return_value=model):
            result = knn_search(pd.Series({"product_name": None}), fake, [])
        assert result == (None, 0.0, [])

    def test_query_failure(self):
        fake = MagicMock()
        fake.count.return_value = 100
        fake.query.side_effect = RuntimeError("boom")
        model = MagicMock()
        model.encode.return_value = np.zeros((1, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            result = knn_search(pd.Series({"product_name": "Milk"}), fake, [])
        assert result == (None, 0.0, [])

    def test_successful_query(self):
        fake = MagicMock()
        fake.count.return_value = 100
        fake.query.return_value = {
            "metadatas": [[
                {"category": "Dairy", "product_name": "Milk"},
                {"category": "Dairy", "product_name": "Cheese"},
            ]],
            "distances": [[0.1, 0.15]],
        }
        model = MagicMock()
        model.encode.return_value = np.zeros((1, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            cat, conf, _ = knn_search(pd.Series({"product_name": "Milk"}), fake, [])
        assert cat == "Dairy"


class TestKnnBatch:
    def test_empty_rows_fallback(self):
        fake = MagicMock()
        fake.count.return_value = 100
        model = MagicMock()
        model.get_sentence_embedding_dimension.return_value = 8
        model.encode.return_value = np.zeros((0, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            rows = [pd.Series({"product_name": None}), pd.Series({"product_name": None})]
            results = knn_search_batch(rows, fake, [])
        assert all(r == (None, 0.0, []) for r in results)

    def test_model_none(self):
        rows = [pd.Series({"product_name": "x"})]
        with patch.object(corpus_mod, "_get_model", return_value=None):
            r = knn_search_batch(rows, MagicMock(), [])
        assert r == [(None, 0.0, [])]


class TestAddToCorpus:
    def test_skip_when_none(self):
        add_to_corpus(pd.Series({"product_name": "X"}), "Dairy", None, [])

    def test_skip_when_empty_text(self):
        fake = MagicMock()
        model = MagicMock()
        with patch.object(corpus_mod, "_get_model", return_value=model):
            add_to_corpus(pd.Series({"product_name": None}), "Dairy", fake, [])
        fake.upsert.assert_not_called()

    def test_upsert(self):
        fake = MagicMock()
        model = MagicMock()
        model.encode.return_value = np.zeros((1, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            add_to_corpus(pd.Series({"product_name": "Milk"}), "Dairy", fake, [])
        fake.upsert.assert_called_once()

    def test_upsert_error_swallowed(self):
        fake = MagicMock()
        fake.upsert.side_effect = RuntimeError("x")
        model = MagicMock()
        model.encode.return_value = np.zeros((1, 8), dtype=np.float32)
        with patch.object(corpus_mod, "_get_model", return_value=model):
            add_to_corpus(pd.Series({"product_name": "Milk"}), "Dairy", fake, [])
