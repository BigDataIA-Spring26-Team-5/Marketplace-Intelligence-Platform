"""
UC3 Hybrid Search — LLM-as-Judge Evaluation

Evaluates search quality using Amazon ESCI benchmark (already in bronze_raw.esci).

ESCI relevance labels:
    E (Exact)       → 3
    S (Substitute)  → 2
    C (Complement)  → 1
    I (Irrelevant)  → 0

Metrics computed:
    nDCG@10  — Normalized Discounted Cumulative Gain
    MRR      — Mean Reciprocal Rank
    relevance_distribution — {E, S, C, I} counts

Produces before/after comparison to show UC1 enrichment improves search quality.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

ESCI_LABEL_SCORES = {"E": 3, "S": 2, "C": 1, "I": 0}

JUDGE_PROMPT = """You are a search relevance judge for a food product catalog.

Query: {query}

Product:
  Name: {product_name}
  Brand: {brand_name}
  Category: {primary_category}
  Dietary tags: {dietary_tags}
  Allergens: {allergens}

Rate the relevance of this product to the query using exactly one label:
  E  — Exact match: directly satisfies the query
  S  — Substitute: similar product, partially satisfies
  C  — Complement: related product, often paired with query
  I  — Irrelevant: unrelated

Respond with only the single letter: E, S, C, or I"""


class SearchEvaluator:
    """
    LLM-as-Judge evaluation for UC3 search quality.

    Usage:
        evaluator = SearchEvaluator(esci_df)
        report = evaluator.run(search_fn_before, search_fn_after, n_queries=100)
        # report["before"]["ndcg"], report["after"]["ndcg"], report["delta"]
    """

    def __init__(self, esci_df: pd.DataFrame | None = None):
        """
        esci_df: DataFrame from bronze_raw.esci with columns:
                 query, product_title, esci_label (E/S/C/I)
        """
        self._esci = esci_df
        self._llm  = None

    # ── public API ─────────────────────────────────────────────────────────────

    def run(
        self,
        search_fn_before,
        search_fn_after,
        n_queries: int = 100,
        top_k: int = 10,
    ) -> dict:
        """
        Run full before/after evaluation.

        search_fn_before / search_fn_after: callable(query, top_k) → list[dict]
        Returns:
            {
              "before":  {"ndcg": float, "mrr": float, "relevance_distribution": dict},
              "after":   {"ndcg": float, "mrr": float, "relevance_distribution": dict},
              "delta":   {"ndcg": float, "mrr": float},
              "n_queries": int,
            }
        """
        queries = self._sample_queries(n_queries)
        before_scores, after_scores = [], []

        for query in queries:
            b_results = search_fn_before(query, top_k)
            a_results = search_fn_after(query, top_k)

            b_rel = [self.judge_relevance(query, r) for r in b_results]
            a_rel = [self.judge_relevance(query, r) for r in a_results]

            before_scores.append(b_rel)
            after_scores.append(a_rel)

        before_metrics = self._aggregate(before_scores, top_k)
        after_metrics  = self._aggregate(after_scores, top_k)

        return {
            "before":    before_metrics,
            "after":     after_metrics,
            "delta":     {
                "ndcg": round(after_metrics["ndcg"] - before_metrics["ndcg"], 4),
                "mrr":  round(after_metrics["mrr"]  - before_metrics["mrr"],  4),
            },
            "n_queries": len(queries),
        }

    def judge_relevance(self, query: str, product: dict) -> int:
        """
        Use Claude to judge relevance of a product to a query.
        Returns integer relevance score: 3 (E), 2 (S), 1 (C), 0 (I).
        Falls back to ESCI ground truth if available.
        """
        # Try ground-truth ESCI label first (faster, free)
        gt = self._ground_truth_label(query, product.get("product_name", ""))
        if gt is not None:
            return gt

        # Fall back to LLM judge
        label = self._llm_judge(query, product)
        return ESCI_LABEL_SCORES.get(label, 0)

    def compute_ndcg(self, relevance_scores: list[int], k: int = 10) -> float:
        """Normalized Discounted Cumulative Gain at k."""
        scores = relevance_scores[:k]
        if not scores:
            return 0.0

        dcg  = sum(s / math.log2(i + 2) for i, s in enumerate(scores))
        ideal = sorted(scores, reverse=True)
        idcg = sum(s / math.log2(i + 2) for i, s in enumerate(ideal))

        return round(dcg / idcg, 4) if idcg > 0 else 0.0

    def compute_mrr(self, relevance_scores: list[int]) -> float:
        """Mean Reciprocal Rank — first relevant result position."""
        for rank, score in enumerate(relevance_scores, start=1):
            if score >= 2:   # Exact or Substitute counts as relevant
                return round(1.0 / rank, 4)
        return 0.0

    # ── internals ──────────────────────────────────────────────────────────────

    def _sample_queries(self, n: int) -> list[str]:
        if self._esci is None or self._esci.empty:
            return []
        col = "query" if "query" in self._esci.columns else self._esci.columns[0]
        queries = self._esci[col].dropna().unique().tolist()
        return queries[:n]

    def _ground_truth_label(self, query: str, product_name: str) -> int | None:
        if self._esci is None:
            return None
        mask = (
            self._esci.get("query", pd.Series()).str.lower() == query.lower()
        ) & (
            self._esci.get("product_title", pd.Series()).str.lower()
            == product_name.lower()
        )
        match = self._esci[mask]
        if match.empty:
            return None
        label = match.iloc[0].get("esci_label", "I")
        return ESCI_LABEL_SCORES.get(label, 0)

    def _llm_judge(self, query: str, product: dict) -> str:
        try:
            import anthropic
            if self._llm is None:
                self._llm = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

            prompt = JUDGE_PROMPT.format(
                query=query,
                product_name=product.get("product_name", ""),
                brand_name=product.get("brand_name", ""),
                primary_category=product.get("primary_category", ""),
                dietary_tags=product.get("dietary_tags", ""),
                allergens=product.get("allergens", ""),
            )
            msg = self._llm.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=5,
                messages=[{"role": "user", "content": prompt}],
            )
            label = msg.content[0].text.strip().upper()
            return label if label in ESCI_LABEL_SCORES else "I"
        except Exception as exc:
            logger.warning("LLM judge failed: %s", exc)
            return "I"

    def _aggregate(self, all_rel_scores: list[list[int]], k: int) -> dict:
        ndcg_scores = [self.compute_ndcg(r, k) for r in all_rel_scores]
        mrr_scores  = [self.compute_mrr(r) for r in all_rel_scores]

        flat = [s for rel in all_rel_scores for s in rel]
        inv  = {v: k for k, v in ESCI_LABEL_SCORES.items()}
        dist = {label: 0 for label in "ESCI"}
        for s in flat:
            dist[inv.get(s, "I")] += 1

        return {
            "ndcg": round(sum(ndcg_scores) / len(ndcg_scores), 4) if ndcg_scores else 0.0,
            "mrr":  round(sum(mrr_scores)  / len(mrr_scores),  4) if mrr_scores  else 0.0,
            "relevance_distribution": dist,
            "n_queries": len(all_rel_scores),
        }
