"""Unit tests for UC3 HybridSearch."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_deps():
    """Mock chromadb + ProductIndexer.load_bm25."""
    with patch("src.uc3_search.hybrid_search.chromadb") as chroma_mod, \
         patch("src.uc3_search.hybrid_search.ProductIndexer") as idx_cls:
        chroma_client = MagicMock()
        chroma_mod.HttpClient.return_value = chroma_client
        idx_inst = MagicMock()
        idx_cls.return_value = idx_inst
        # Default: BM25 loads successfully
        bm25 = MagicMock()
        bm25.get_scores.return_value = [0.5, 0.9, 0.1]
        docs = [
            {"product_name": "p0", "brand_name": "b0"},
            {"product_name": "p1", "brand_name": "b1"},
            {"product_name": "p2", "brand_name": "b2"},
        ]
        idx_inst.load_bm25.return_value = (bm25, ["0", "1", "2"], docs)
        yield chroma_client, idx_inst, bm25


class TestHybridSearchInit:
    def test_loads_bm25(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        assert hs._bm25 is not None
        assert len(hs._bm25_docs) == 3

    def test_bm25_missing_file_handled(self):
        with patch("src.uc3_search.hybrid_search.chromadb"), \
             patch("src.uc3_search.hybrid_search.ProductIndexer") as idx_cls:
            idx_cls.return_value.load_bm25.side_effect = FileNotFoundError()
            from src.uc3_search.hybrid_search import HybridSearch
            hs = HybridSearch()
            assert hs._bm25 is None

    def test_bm25_generic_exception_handled(self):
        with patch("src.uc3_search.hybrid_search.chromadb"), \
             patch("src.uc3_search.hybrid_search.ProductIndexer") as idx_cls:
            idx_cls.return_value.load_bm25.side_effect = RuntimeError("bad pickle")
            from src.uc3_search.hybrid_search import HybridSearch
            hs = HybridSearch()
            assert hs._bm25 is None


class TestBM25Search:
    def test_returns_ranked_results(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        res = hs.bm25_search("query", top_k=2)
        assert len(res) == 2
        assert res[0]["mode"] == "bm25"
        assert res[0]["score"] >= res[1]["score"]
        assert res[0]["rank"] == 1

    def test_empty_when_no_bm25(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        hs._bm25 = None
        assert hs.bm25_search("query") == []


class TestSemanticSearch:
    def test_returns_ranked_results(self, mock_deps):
        chroma_client, _, _ = mock_deps
        col = MagicMock()
        col.count.return_value = 5
        col.query.return_value = {
            "metadatas": [[{"product_name": "A"}, {"product_name": "B"}]],
            "distances": [[0.1, 0.3]],
        }
        chroma_client.get_collection.return_value = col
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        res = hs.semantic_search("q", top_k=5)
        assert len(res) == 2
        assert res[0]["mode"] == "semantic"
        assert abs(res[0]["score"] - 0.9) < 1e-6

    def test_chroma_failure_returns_empty(self, mock_deps):
        chroma_client, _, _ = mock_deps
        chroma_client.get_collection.side_effect = Exception("down")
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        assert hs.semantic_search("q") == []


class TestRRF:
    def test_combines_and_ranks(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        bm25 = [{"product_name": "A", "brand_name": "x"}, {"product_name": "B", "brand_name": "y"}]
        sem = [{"product_name": "A", "brand_name": "x"}, {"product_name": "C", "brand_name": "z"}]
        res = hs.reciprocal_rank_fusion(bm25, sem, top_k=5)
        # A appears in both → highest
        assert res[0]["product_name"] == "A"
        assert all(r["mode"] == "hybrid" for r in res)

    def test_top_k_limits(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        bm25 = [{"product_name": f"P{i}", "brand_name": ""} for i in range(5)]
        res = hs.reciprocal_rank_fusion(bm25, [], top_k=2)
        assert len(res) == 2


class TestSearchDispatch:
    def test_mode_bm25(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        res = hs.search("q", top_k=1, mode="bm25")
        assert res[0]["mode"] == "bm25"

    def test_mode_semantic(self, mock_deps):
        chroma_client, _, _ = mock_deps
        col = MagicMock()
        col.count.return_value = 1
        col.query.return_value = {"metadatas": [[{"product_name": "A"}]], "distances": [[0.2]]}
        chroma_client.get_collection.return_value = col
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        res = hs.search("q", top_k=1, mode="semantic")
        assert res[0]["mode"] == "semantic"

    def test_mode_hybrid(self, mock_deps):
        chroma_client, _, _ = mock_deps
        col = MagicMock()
        col.count.return_value = 1
        col.query.return_value = {"metadatas": [[{"product_name": "p1", "brand_name": "b1"}]],
                                   "distances": [[0.2]]}
        chroma_client.get_collection.return_value = col
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        res = hs.search("q", top_k=3, mode="hybrid")
        assert all(r["mode"] == "hybrid" for r in res)

    def test_suppress_recalled(self, mock_deps):
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        hs._bm25_docs = [
            {"product_name": "safe", "brand_name": "", "is_recalled": "False", "recall_class": ""},
            {"product_name": "bad", "brand_name": "", "is_recalled": "True", "recall_class": "Class I"},
        ]
        # Force get_scores to return values
        hs._bm25.get_scores.return_value = [0.5, 0.9]
        res = hs.search("q", top_k=5, mode="bm25", suppress_recalled=True)
        names = [r["product_name"] for r in res]
        assert "bad" not in names


class TestIsReady:
    def test_ready_both_up(self, mock_deps):
        chroma_client, _, _ = mock_deps
        col = MagicMock()
        col.count.return_value = 10
        chroma_client.get_collection.return_value = col
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        assert hs.is_ready() is True

    def test_not_ready_no_chroma(self, mock_deps):
        chroma_client, _, _ = mock_deps
        chroma_client.get_collection.side_effect = Exception()
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        assert hs.is_ready() is False


def test_doc_key():
    from src.uc3_search.hybrid_search import HybridSearch
    assert HybridSearch._doc_key({"product_name": "A", "brand_name": "B"}) == "A::B"
