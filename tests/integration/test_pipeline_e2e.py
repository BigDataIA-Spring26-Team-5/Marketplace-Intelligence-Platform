"""End-to-end integration tests for the ETL pipeline.

Exercises multiple components together with minimal mocking.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Integration: blocks chain together correctly
# ---------------------------------------------------------------------------

class TestBlocksChain:
    """Chain multiple transform blocks and verify data flows correctly."""

    def test_whitespace_then_lowercase_then_punctuation(self):
        from src.blocks.strip_whitespace import StripWhitespaceBlock
        from src.blocks.lowercase_brand import LowercaseBrandBlock
        from src.blocks.strip_punctuation import StripPunctuationBlock

        df = pd.DataFrame({
            "product_name": ["  Apple Juice!  ", "Banana-Milk "],
            "brand_name":   ["  TROPICANA  ", "  ALMOND BREEZE  "],
        })
        df = StripWhitespaceBlock().run(df)
        df = LowercaseBrandBlock().run(df)
        df = StripPunctuationBlock().run(df)

        assert df["brand_name"].iloc[0] == "tropicana"
        assert df["brand_name"].iloc[1] == "almond breeze"
        assert "!" not in df["product_name"].iloc[0]


# ---------------------------------------------------------------------------
# Integration: registry lookups + block instantiation
# ---------------------------------------------------------------------------

class TestRegistryBlockExecution:
    def test_registry_lists_blocks(self):
        from src.registry.block_registry import BlockRegistry

        reg = BlockRegistry()
        names = reg.list_blocks()
        assert isinstance(names, list)
        assert len(names) > 0

    def test_core_blocks_instantiate(self):
        from src.registry.block_registry import BlockRegistry

        reg = BlockRegistry()
        for name in ("strip_whitespace", "lowercase_brand", "strip_punctuation"):
            block = reg.get(name)
            assert block is not None


# ---------------------------------------------------------------------------
# Integration: schema analyzer + cache roundtrip
# ---------------------------------------------------------------------------

class TestSchemaCacheRoundtrip:
    def test_profile_and_cache_schema_fingerprint(self, tmp_path):
        from src.schema.analyzer import profile_dataframe
        from src.cache.client import CacheClient

        df = pd.DataFrame({
            "name":  ["A", "B", "C"],
            "brand": ["X", "Y", "Z"],
            "price": [1.0, 2.5, 3.0],
        })
        profile = profile_dataframe(df)
        assert "name" in profile and "brand" in profile

        client = CacheClient(no_cache=True)
        stored = client.set("yaml", ["fp1"], b"payload", ttl=60)
        assert stored is False


# ---------------------------------------------------------------------------
# Integration: UC3 indexer + hybrid search interact correctly via mocks
# ---------------------------------------------------------------------------

class TestUC3IndexHybridSearch:
    def test_indexer_stats_reflects_chroma_and_bm25_presence(self, tmp_path, monkeypatch):
        from src.uc3_search import indexer as idx_mod

        monkeypatch.setattr(idx_mod, "BM25_INDEX_PATH", tmp_path / "bm25.pkl")

        fake_client = MagicMock()
        fake_collection = MagicMock()
        fake_collection.count.return_value = 42
        fake_client.get_or_create_collection.return_value = fake_collection

        with patch("src.uc3_search.indexer.chromadb.HttpClient", return_value=fake_client):
            inst = idx_mod.ProductIndexer()
            stats = inst.stats()

        assert stats["chroma_docs"] == 42
        assert stats["bm25_index"] is False


# ---------------------------------------------------------------------------
# Integration: UC4 miner + graph builder
# ---------------------------------------------------------------------------

class TestUC4MinerGraphChain:
    def test_mine_rules_then_load_into_graph(self):
        from src.uc4_recommendations.association_rules import AssociationRuleMiner as RuleMiner
        from src.uc4_recommendations.graph_store import ProductGraph

        tx = pd.DataFrame({
            "transaction_id": [1, 1, 2, 2, 3, 3, 4, 4],
            "product_id":     ["A", "B", "A", "B", "A", "B", "A", "B"],
        })
        miner = RuleMiner(tx)
        rules = miner.mine_rules(min_support=0.1, min_lift=1.0, min_confidence=0.1)
        assert len(rules) >= 1

        products = pd.DataFrame({
            "product_id":       ["A", "B"],
            "product_name":     ["Apple", "Banana"],
            "primary_category": ["Fruit", "Fruit"],
        })
        graph = ProductGraph()
        graph.load_products(products)
        stats = graph.stats()
        assert stats["product_nodes"] == 2


# ---------------------------------------------------------------------------
# Integration: metrics exporter + collector both push without error
# ---------------------------------------------------------------------------

class TestMetricsPipeline:
    def test_exporter_and_collector_both_silent_on_network_failure(self):
        from src.uc2_observability.metrics_exporter import MetricsExporter
        from src.uc2_observability.metrics_collector import MetricsCollector

        with patch("src.uc2_observability.metrics_exporter.push_to_gateway",
                   side_effect=ConnectionError("no gateway")):
            exp = MetricsExporter()
            result = exp.push({
                "run_id": "r1", "source_name": "test", "rows_in": 100,
                "rows_out": 95, "dq_score_pre": 50.0, "dq_score_post": 80.0,
                "duration_seconds": 1.2, "status": "success",
            })
            assert result is False

        with patch("src.uc2_observability.metrics_collector.push_to_gateway",
                   side_effect=ConnectionError("no gateway")):
            coll = MetricsCollector()
            coll.push_block_dq("r1", "test", "block1", 0, 0.8, 100)
