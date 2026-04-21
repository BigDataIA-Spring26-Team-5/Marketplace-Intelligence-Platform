"""
UC4 Recommendations — Unified Product Recommender

Combines association rules (also-bought) and graph traversal (cross-category)
into a single interface.  Also provides the before/after demo comparison
that is the core deliverable: raw fragmented IDs → enriched canonical IDs
shows 3-4x lift improvement.

Depends on UC1 output:
    enriched_df  — unified product catalog with canonical IDs
    transactions — transaction log with product_id column mapped to canonical IDs
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.uc4_recommendations.association_rules import AssociationRuleMiner
from src.uc4_recommendations.graph_store import ProductGraph

logger = logging.getLogger(__name__)


class ProductRecommender:
    """
    Unified recommender for UC4.

    Usage:
        rec = ProductRecommender()
        rec.build(enriched_df, transactions_df)
        print(rec.also_bought("B001234"))
        print(rec.you_might_like("B001234"))
        print(rec.demo_comparison(raw_tx_df, enriched_tx_df, "B001234"))
    """

    def __init__(self):
        self._miner = AssociationRuleMiner()
        self._graph = ProductGraph()
        self._products: pd.DataFrame | None = None

    # ── build ──────────────────────────────────────────────────────────────────

    def build(self, enriched_df: pd.DataFrame, transactions_df: pd.DataFrame) -> dict:
        """
        Full build from UC1 output.

        enriched_df:     UC1 unified catalog (product_id or product_name, brand_name,
                         primary_category, dietary_tags, allergens, dq_score_post)
        transactions_df: [transaction_id, product_id] — IDs must match enriched_df

        Returns build stats dict.
        """
        self._products = enriched_df.copy()

        # Add product_id if not present — use index
        if "product_id" not in self._products.columns:
            self._products["product_id"] = self._products.index.astype(str)

        # Mine rules
        self._miner = AssociationRuleMiner(transactions_df)
        rules = self._miner.mine_rules()

        # Build graph
        self._graph = ProductGraph()
        n_nodes = self._graph.load_products(self._products)
        n_edges = self._graph.load_relationships(rules) if not rules.empty else 0

        stats = {
            "products_indexed":  n_nodes,
            "rules_mined":       len(rules),
            "graph_edges":       n_edges,
            **self._graph.stats(),
        }
        logger.info("UC4 recommender built: %s", stats)
        return stats

    # ── recommendations ────────────────────────────────────────────────────────

    def also_bought(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        "Customers who bought this also bought" — direct co-purchase from rules.
        Returns [{product_id, product_name, confidence, lift}]
        """
        raw_recs = self._miner.get_recommendations(product_id, top_k)
        return [self._enrich_rec(r) for r in raw_recs]

    def you_might_like(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        "You might also like" — cross-category via graph traversal.
        Returns [{product_id, product_name, primary_category, affinity_score, hops}]
        """
        return self._graph.cross_category_recommendations(product_id, max_hops=2, top_k=top_k)

    def demo_comparison(
        self,
        raw_transactions_df: pd.DataFrame,
        enriched_transactions_df: pd.DataFrame,
        product_id: str,
        top_k: int = 5,
    ) -> dict:
        """
        Side-by-side comparison: raw fragmented IDs vs UC1-enriched canonical IDs.
        This is the core deliverable for UC4 demo.

        Returns:
        {
          "product_id": str,
          "raw_recommendations":       [{product_id, confidence, lift}],
          "enriched_recommendations":  [{product_id, confidence, lift}],
          "max_lift_raw":      float,
          "max_lift_enriched": float,
          "lift_improvement":  float,         # enriched - raw
          "raw_unique_ids":    int,            # how fragmented raw catalog is
          "enriched_unique_ids": int,          # how consolidated enriched catalog is
          "signal_consolidation_ratio": float, # raw/enriched — higher = more consolidation
        }
        """
        result = self._miner.compare_raw_vs_enriched(
            raw_transactions_df, enriched_transactions_df, product_id, top_k
        )

        raw_unique      = raw_transactions_df["product_id"].nunique()
        enriched_unique = enriched_transactions_df["product_id"].nunique()

        result["raw_unique_ids"]             = raw_unique
        result["enriched_unique_ids"]        = enriched_unique
        result["signal_consolidation_ratio"] = (
            round(raw_unique / enriched_unique, 2) if enriched_unique > 0 else 0.0
        )
        return result

    def is_ready(self) -> bool:
        return (
            self._products is not None
            and self._miner.rules is not None
            and not self._miner.rules.empty
        )

    def stats(self) -> dict:
        return {
            "products":  len(self._products) if self._products is not None else 0,
            "rules":     len(self._miner.rules) if self._miner.rules is not None else 0,
            "graph":     self._graph.stats(),
        }

    # ── internals ──────────────────────────────────────────────────────────────

    def _enrich_rec(self, rec: dict) -> dict:
        """Attach product metadata to a raw association-rule recommendation."""
        if self._products is None:
            return rec
        pid = rec.get("product_id", "")
        match = self._products[self._products.get("product_id", pd.Series()) == pid]
        if match.empty and "product_name" in self._products.columns:
            match = self._products[self._products["product_name"] == pid]
        if not match.empty:
            row = match.iloc[0]
            rec["product_name"]     = str(row.get("product_name", ""))
            rec["primary_category"] = str(row.get("primary_category", ""))
            rec["brand_name"]       = str(row.get("brand_name", ""))
        return rec
