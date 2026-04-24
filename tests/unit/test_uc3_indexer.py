"""Unit tests for UC3 ProductIndexer."""

from __future__ import annotations

import pickle
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_chroma():
    with patch("src.uc3_search.indexer.chromadb") as mod:
        client = MagicMock()
        mod.HttpClient.return_value = client
        yield client


@pytest.fixture
def sample_df():
    return pd.DataFrame([
        {"product_name": "Organic Oats", "brand_name": "Bob", "primary_category": "cereal",
         "ingredients": "oats", "dietary_tags": "gluten-free", "allergens": "",
         "is_organic": "True", "dq_score_post": 0.9, "data_source": "usda"},
        {"product_name": "Corn Flakes", "brand_name": "Kellogg", "primary_category": "cereal",
         "ingredients": "corn", "dietary_tags": "", "allergens": "",
         "is_organic": "False", "dq_score_post": 0.8, "data_source": "usda"},
        {"product_name": None, "brand_name": "X", "primary_category": "",
         "ingredients": "", "dietary_tags": "", "allergens": "",
         "is_organic": "", "dq_score_post": 0.0, "data_source": ""},
    ])


class TestHelpers:
    def test_build_text_concatenates_fields(self):
        from src.uc3_search.indexer import _build_text
        out = _build_text({"product_name": "Oats", "brand_name": "Bob", "ingredients": "oat"})
        assert "oats" in out and "bob" in out

    def test_build_text_handles_none(self):
        from src.uc3_search.indexer import _build_text
        out = _build_text({"product_name": None, "brand_name": "X"})
        assert "x" in out

    def test_build_text_empty(self):
        from src.uc3_search.indexer import _build_text
        assert _build_text({}) == ""

    def test_tokenize(self):
        from src.uc3_search.indexer import _tokenize
        assert _tokenize("Hello, World!") == ["hello", "world"]


class TestProductIndexer:
    def test_init_creates_client(self, mock_chroma):
        from src.uc3_search.indexer import ProductIndexer
        idx = ProductIndexer()
        assert idx._chroma is mock_chroma

    def test_clean_drops_nulls_and_dupes(self, mock_chroma, sample_df):
        from src.uc3_search.indexer import ProductIndexer
        idx = ProductIndexer()
        df2 = pd.concat([sample_df, sample_df.iloc[[0]]])
        cleaned = idx._clean(df2)
        assert cleaned["product_name"].notna().all()
        assert len(cleaned) == 2

    def test_collection_gets_or_creates(self, mock_chroma):
        from src.uc3_search.indexer import ProductIndexer
        idx = ProductIndexer()
        idx.collection()
        mock_chroma.get_or_create_collection.assert_called_once()

    def test_build_runs_full_pipeline(self, mock_chroma, sample_df, tmp_path):
        from src.uc3_search import indexer as mod
        col = MagicMock()
        col.count.return_value = 2
        mock_chroma.create_collection.return_value = col
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "bm25.pkl"):
            idx = mod.ProductIndexer()
            n = idx.build(sample_df)
        assert n == 2
        col.add.assert_called()

    def test_build_batches(self, mock_chroma, tmp_path):
        from src.uc3_search import indexer as mod
        col = MagicMock()
        col.count.return_value = 3
        mock_chroma.create_collection.return_value = col
        df = pd.DataFrame([
            {"product_name": f"p{i}", "brand_name": f"b{i}"} for i in range(3)
        ])
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "bm25.pkl"):
            idx = mod.ProductIndexer()
            idx.build(df, batch_size=2)
        assert col.add.call_count >= 2

    def test_build_delete_exception_swallowed(self, mock_chroma, sample_df, tmp_path):
        from src.uc3_search import indexer as mod
        mock_chroma.delete_collection.side_effect = Exception("no such collection")
        col = MagicMock()
        col.count.return_value = 2
        mock_chroma.create_collection.return_value = col
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "bm25.pkl"):
            mod.ProductIndexer().build(sample_df)

    def test_load_bm25_missing_raises(self, mock_chroma, tmp_path):
        from src.uc3_search import indexer as mod
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "nope.pkl"):
            with pytest.raises(FileNotFoundError):
                mod.ProductIndexer().load_bm25()

    def test_load_bm25_returns_tuple(self, mock_chroma, tmp_path):
        from src.uc3_search import indexer as mod
        path = tmp_path / "bm25.pkl"
        with open(path, "wb") as f:
            pickle.dump(("bm25", ["a"], [{"x": 1}]), f)
        with patch.object(mod, "BM25_INDEX_PATH", path):
            res = mod.ProductIndexer().load_bm25()
        assert res[0] == "bm25"

    def test_stats_success(self, mock_chroma, tmp_path):
        from src.uc3_search import indexer as mod
        col = MagicMock()
        col.count.return_value = 5
        mock_chroma.get_or_create_collection.return_value = col
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "nope.pkl"):
            stats = mod.ProductIndexer().stats()
        assert stats["chroma_docs"] == 5
        assert stats["bm25_index"] is False

    def test_stats_chroma_fail(self, mock_chroma, tmp_path):
        from src.uc3_search import indexer as mod
        mock_chroma.get_or_create_collection.side_effect = Exception("down")
        with patch.object(mod, "BM25_INDEX_PATH", tmp_path / "nope.pkl"):
            stats = mod.ProductIndexer().stats()
        assert stats["chroma_docs"] == 0

    def test_build_bm25_import_error(self, mock_chroma, sample_df, tmp_path, monkeypatch):
        from src.uc3_search import indexer as mod
        import sys
        monkeypatch.setitem(sys.modules, "rank_bm25", None)
        idx = mod.ProductIndexer()
        # Should log warning and return without raising
        idx._build_bm25(idx._clean(sample_df))
