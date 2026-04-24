"""
UC4 Recommendations — Association Rule Mining

Uses FP-Growth (mlxtend) on transaction baskets to compute:
    support, confidence, lift per product pair

Key demo: run on raw product IDs (fragmented by UC1 dedup not applied)
          vs enriched canonical IDs (after UC1 dedup) — lift improves 3-4x
          because variant names collapse into one canonical ID.

Transaction data format expected (one row per transaction):
    transaction_id | product_id | product_name
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

MIN_SUPPORT    = 0.005   # 0.5% — safe with max_len=2; combinatorial explosion needs unbounded max_len, not low support
MIN_CONFIDENCE = 0.10
MIN_LIFT       = 1.2     # only keep rules that beat random baseline
MAX_VOCAB      = 15_000  # cap unique products before encoding; dense matrix = n_baskets × vocab


class AssociationRuleMiner:
    """
    Mine co-purchase rules from transaction data.

    Usage:
        miner = AssociationRuleMiner(transactions_df)
        rules_df = miner.mine_rules()
        recs = miner.get_recommendations("B001234", top_k=5)
    """

    def __init__(self, transactions_df: pd.DataFrame | None = None):
        """
        transactions_df: must have columns [transaction_id, product_id]
        """
        self._transactions = transactions_df
        self._rules: pd.DataFrame | None = None

    # ── public API ─────────────────────────────────────────────────────────────

    def mine_rules(
        self,
        min_support: float = MIN_SUPPORT,
        min_confidence: float = MIN_CONFIDENCE,
        min_lift: float = MIN_LIFT,
        max_products: int = MAX_VOCAB,
    ) -> pd.DataFrame:
        """
        Run FP-Growth and extract association rules.
        Returns DataFrame: antecedents, consequents, support, confidence, lift
        """
        try:
            from mlxtend.frequent_patterns import fpgrowth, association_rules
            from mlxtend.preprocessing import TransactionEncoder
        except ImportError:
            raise ImportError("pip install mlxtend")

        if self._transactions is None or self._transactions.empty:
            raise ValueError("No transaction data loaded")

        # Cap vocabulary to top-N most purchased products.
        # Dense basket matrix = n_baskets × n_unique_products; 50K orders × 49K products
        # = ~2.3 GB bool array before any FP-Growth work — OOMs small VMs.
        product_counts = self._transactions["product_id"].value_counts()
        if len(product_counts) > max_products:
            top_products = set(product_counts.head(max_products).index)
            tx = self._transactions[self._transactions["product_id"].isin(top_products)]
            logger.info(
                "Vocab capped: %d → %d unique products (dropped tail)", len(product_counts), max_products
            )
        else:
            tx = self._transactions

        # Build basket matrix
        baskets = (
            tx
            .groupby("transaction_id")["product_id"]
            .apply(list)
            .tolist()
        )
        # Remove single-item baskets — they can't produce rules and inflate support counts
        baskets = [b for b in baskets if len(b) > 1]
        if not baskets:
            logger.warning("No multi-item baskets after vocab cap — lowering max_products or min_support may help")
            self._rules = pd.DataFrame()
            return self._rules

        te = TransactionEncoder()
        te_array = te.fit(baskets).transform(baskets)
        basket_df = pd.DataFrame(te_array, columns=te.columns_)
        del te_array  # free dense bool array before FP-Growth allocates its own structures

        # max_len=2: only mine pairs. Without this, fpgrowth enumerates all k-itemsets
        # (pairs, triplets, quadruplets...) — combinatorial explosion that blows past 31 GB RAM.
        # Association rules for "also-bought" only need antecedent=1 item → consequent=1 item anyway.
        frequent_items = fpgrowth(
            basket_df, min_support=min_support, use_colnames=True, max_len=2
        )
        if frequent_items.empty:
            logger.warning("No frequent itemsets found — lower min_support")
            self._rules = pd.DataFrame()
            return self._rules

        rules = association_rules(
            frequent_items, metric="confidence", min_threshold=min_confidence
        )
        rules = rules[rules["lift"] >= min_lift].sort_values("lift", ascending=False)

        # Flatten frozensets to single product IDs for easier lookup
        rules = rules[
            rules["antecedents"].apply(len) == 1
        ].copy()
        rules["antecedent_id"]  = rules["antecedents"].apply(lambda x: next(iter(x)))
        rules["consequent_id"]  = rules["consequents"].apply(lambda x: next(iter(x)))

        self._rules = rules.reset_index(drop=True)
        logger.info("Mined %d association rules", len(self._rules))
        return self._rules

    def get_recommendations(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        Co-purchase recommendations for a product_id.
        Returns [{product_id, confidence, lift, support}]
        """
        if self._rules is None or self._rules.empty:
            return []

        # normalize to string to handle float antecedent_id (parquet loads ints as float64)
        pid_str = str(product_id)
        try:
            pid_norm = str(int(float(pid_str)))
        except (ValueError, OverflowError):
            pid_norm = pid_str
        ant_str = self._rules["antecedent_id"].astype(str)
        mask = ant_str.isin({pid_str, pid_norm})
        # also try int-normalised column
        if not mask.any():
            try:
                ant_norm = self._rules["antecedent_id"].apply(
                    lambda x: str(int(float(x))) if str(x).replace('.','',1).isdigit() else str(x)
                )
                mask = ant_norm.isin({pid_str, pid_norm})
            except Exception:
                pass
        matches = self._rules[mask].nlargest(top_k, "lift")

        return [
            {
                "product_id": row["consequent_id"],
                "confidence": round(float(row["confidence"]), 4),
                "lift":       round(float(row["lift"]), 4),
                "support":    round(float(row["support"]), 6),
            }
            for _, row in matches.iterrows()
        ]

    def compare_raw_vs_enriched(
        self,
        raw_transactions_df: pd.DataFrame,
        enriched_transactions_df: pd.DataFrame,
        product_id: str,
        top_k: int = 5,
    ) -> dict:
        """
        Before/after comparison showing lift improvement from UC1 dedup.

        raw_transactions_df:      product_id = raw noisy names (many variants)
        enriched_transactions_df: product_id = canonical IDs  (consolidated)
        """
        raw_miner = AssociationRuleMiner(raw_transactions_df)
        raw_miner.mine_rules()
        raw_recs = raw_miner.get_recommendations(product_id, top_k)

        enriched_miner = AssociationRuleMiner(enriched_transactions_df)
        enriched_miner.mine_rules()
        enriched_recs = enriched_miner.get_recommendations(product_id, top_k)

        raw_lift      = max((r["lift"] for r in raw_recs),      default=0.0)
        enriched_lift = max((r["lift"] for r in enriched_recs), default=0.0)

        return {
            "product_id":              product_id,
            "raw_recommendations":     raw_recs,
            "enriched_recommendations": enriched_recs,
            "max_lift_raw":            round(raw_lift, 4),
            "max_lift_enriched":       round(enriched_lift, 4),
            "lift_improvement":        round(enriched_lift - raw_lift, 4),
            "raw_rule_count":          len(raw_miner._rules) if raw_miner._rules is not None else 0,
            "enriched_rule_count":     len(enriched_miner._rules) if enriched_miner._rules is not None else 0,
        }

    @property
    def rules(self) -> pd.DataFrame | None:
        return self._rules
