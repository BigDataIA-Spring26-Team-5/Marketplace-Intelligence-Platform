"""Unit tests for LLMEnrichBlock — mocks deterministic/embedding/llm tiers."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from src.blocks.llm_enrich import LLMEnrichBlock, ENRICHMENT_COLUMNS


def _mock_tier(resolved=0):
    """Return a tier function that marks N rows resolved."""
    def tier_fn(df, enrich_cols, needs, **kwargs):
        stats = {"resolved": resolved, "corpus_augmented": 0, "corpus_size_after": 0}
        return df, needs, stats
    return tier_fn


class TestLLMEnrich:
    @patch("src.blocks.llm_enrich.llm_enrich")
    @patch("src.blocks.llm_enrich.embedding_enrich")
    @patch("src.blocks.llm_enrich.deterministic_enrich")
    def test_all_tiers_called(self, mock_det, mock_emb, mock_llm):
        mock_det.side_effect = _mock_tier(1)
        mock_emb.side_effect = _mock_tier(0)
        mock_llm.side_effect = _mock_tier(0)
        df = pd.DataFrame({"product_name": ["x"], "ingredients": ["milk"]})
        out = LLMEnrichBlock().run(df)
        assert mock_det.called
        assert mock_emb.called
        assert mock_llm.called
        assert "enriched_by_llm" in out.columns

    @patch("src.blocks.llm_enrich.llm_enrich")
    @patch("src.blocks.llm_enrich.embedding_enrich")
    @patch("src.blocks.llm_enrich.deterministic_enrich")
    def test_adds_enrichment_columns(self, mock_det, mock_emb, mock_llm):
        mock_det.side_effect = _mock_tier(0)
        mock_emb.side_effect = _mock_tier(0)
        mock_llm.side_effect = _mock_tier(0)
        df = pd.DataFrame({"product_name": ["x"], "ingredients": ["y"]})
        out = LLMEnrichBlock().run(df)
        for col in ENRICHMENT_COLUMNS:
            assert col in out.columns

    @patch("src.blocks.llm_enrich.llm_enrich")
    @patch("src.blocks.llm_enrich.embedding_enrich")
    @patch("src.blocks.llm_enrich.deterministic_enrich")
    def test_stats_recorded(self, mock_det, mock_emb, mock_llm):
        mock_det.side_effect = _mock_tier(2)
        mock_emb.side_effect = _mock_tier(1)
        mock_llm.side_effect = _mock_tier(0)
        df = pd.DataFrame({"product_name": ["a", "b"], "ingredients": ["x", "y"]})
        LLMEnrichBlock().run(df)
        stats = LLMEnrichBlock.last_enrichment_stats
        assert stats["deterministic"] == 2
        assert stats["embedding"] == 1

    @patch("src.blocks.llm_enrich.llm_enrich")
    @patch("src.blocks.llm_enrich.embedding_enrich")
    @patch("src.blocks.llm_enrich.deterministic_enrich")
    def test_drops_knn_neighbors(self, mock_det, mock_emb, mock_llm):
        def tier_with_knn(df, enrich_cols, needs, **kwargs):
            df["_knn_neighbors"] = "[]"
            return df, needs, {"resolved": 0, "corpus_augmented": 0, "corpus_size_after": 0}
        mock_det.side_effect = _mock_tier(0)
        mock_emb.side_effect = tier_with_knn
        mock_llm.side_effect = _mock_tier(0)
        df = pd.DataFrame({"product_name": ["x"], "ingredients": ["y"]})
        out = LLMEnrichBlock().run(df)
        assert "_knn_neighbors" not in out.columns

    @patch("src.blocks.llm_enrich.llm_enrich")
    @patch("src.blocks.llm_enrich.embedding_enrich")
    @patch("src.blocks.llm_enrich.deterministic_enrich")
    def test_custom_enrich_cols_from_config(self, mock_det, mock_emb, mock_llm):
        mock_det.side_effect = _mock_tier(0)
        mock_emb.side_effect = _mock_tier(0)
        mock_llm.side_effect = _mock_tier(0)
        df = pd.DataFrame({"product_name": ["x"]})
        out = LLMEnrichBlock().run(df, config={"enrichment_columns": ["primary_category"]})
        assert "primary_category" in out.columns
