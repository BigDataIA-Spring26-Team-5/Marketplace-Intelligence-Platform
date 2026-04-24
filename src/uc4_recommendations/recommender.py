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

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import pandas as pd

from src.uc4_recommendations.association_rules import AssociationRuleMiner
from src.uc4_recommendations.graph_store import ProductGraph

GCP_PROJECT   = "mip-platform-2024"
BQ_DATASET    = "instacart"
TX_VIEW       = f"{GCP_PROJECT}.{BQ_DATASET}.transactions_with_names"
PRODUCTS_TBL  = f"{GCP_PROJECT}.{BQ_DATASET}.products"
DEPTS_TBL     = f"{GCP_PROJECT}.{BQ_DATASET}.departments"

# Absolute path so both `aq` (scripts/) and Streamlit (app.py) resolve the same dir
UC4_SAVE_DIR = Path(__file__).resolve().parent.parent.parent / "output" / "uc4"

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

    def build(
        self,
        enriched_df: pd.DataFrame,
        transactions_df: pd.DataFrame,
        safety_filter: bool = True,
    ) -> dict:
        """
        Full build from UC1 output.

        enriched_df:     UC1 unified catalog (product_id or product_name, brand_name,
                         primary_category, dietary_tags, allergens, dq_score_post)
        transactions_df: [transaction_id, product_id] — IDs must match enriched_df
        safety_filter:   if True, remove Class I recalled products before building indexes.

        Returns build stats dict.
        """
        self._products = enriched_df.copy()

        # Add product_id if not present — use index
        if "product_id" not in self._products.columns:
            self._products["product_id"] = self._products.index.astype(str)

        if safety_filter and "is_recalled" in self._products.columns:
            class1_mask = (
                (self._products["is_recalled"] == True)  # noqa: E712
                & self._products["recall_class"].fillna("").str.upper().str.startswith("CLASS I")
            )
            n_removed = class1_mask.sum()
            if n_removed > 0:
                removed_ids = set(self._products.loc[class1_mask, "product_id"])
                self._products = self._products[~class1_mask].reset_index(drop=True)
                transactions_df = transactions_df[
                    ~transactions_df["product_id"].isin(removed_ids)
                ].reset_index(drop=True)
                logger.warning(
                    "Safety filter removed %d Class I recalled product(s) from UC4 catalog",
                    n_removed,
                )

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

    # ── BigQuery loader ────────────────────────────────────────────────────────

    @classmethod
    def load_from_bigquery(
        cls,
        sample_orders: int = 100_000,
        project: str = GCP_PROJECT,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load Instacart data from BigQuery and return (transactions_df, products_df).

        transactions_df: [transaction_id, product_id, product_name]
        products_df:     [product_id, product_name, aisle_id, department_id]

        sample_orders: number of orders to sample (default 100k — enough for FP-Growth
                       without running out of memory on 32M row full table).
        """
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError("google-cloud-bigquery required: pip install google-cloud-bigquery")

        client = bigquery.Client(project=project)

        # Sample using MOD on order_id for fast pseudo-random sampling (avoids ORDER BY RAND() full scan)
        tx_query = f"""
            SELECT t.transaction_id, t.product_id, t.product_name
            FROM `{TX_VIEW}` t
            WHERE t.transaction_id IN (
                SELECT DISTINCT order_id
                FROM `{GCP_PROJECT}.{BQ_DATASET}.order_products_prior`
                WHERE MOD(order_id, CAST(CEIL(3214874 / {sample_orders}) AS INT64)) = 0
                LIMIT {sample_orders}
            )
        """
        logger.info("Loading %d sampled orders from BigQuery...", sample_orders)
        transactions_df = client.query(tx_query).to_dataframe()
        logger.info("Loaded %d transaction rows", len(transactions_df))

        products_query = f"""
            SELECT p.product_id, p.product_name, p.aisle_id, p.department_id,
                   COALESCE(d.department, CAST(p.department_id AS STRING)) AS primary_category
            FROM `{PRODUCTS_TBL}` p
            LEFT JOIN `{DEPTS_TBL}` d USING (department_id)
        """
        products_df = client.query(products_query).to_dataframe()
        logger.info("Loaded %d products", len(products_df))

        return transactions_df, products_df

    # ── recommendations ────────────────────────────────────────────────────────

    def find_product(self, query: str) -> str | None:
        """
        Resolve a product name or ID string to a canonical product_id string.
        Tries exact ID match first, then case-insensitive name substring match.
        Returns None if not found.
        """
        if self._products is None:
            return None
        pid_col = self._products["product_id"].astype(str)
        if query in pid_col.values:
            return query
        mask = self._products["product_name"].str.lower().str.contains(
            query.lower(), na=False, regex=False
        )
        matches = self._products[mask]
        if not matches.empty:
            return str(matches.iloc[0]["product_id"])
        return None

    def _get_product_name(self, pid: str) -> str:
        if self._products is None:
            return pid
        match = self._products[self._products["product_id"].astype(str) == pid]
        if not match.empty and "product_name" in match.columns:
            return str(match.iloc[0]["product_name"])
        return pid

    def top_antecedents(self, n: int = 8) -> list[dict]:
        """Top-n products that appear as rule antecedents — used to populate UI examples."""
        if self._miner.rules is None or self._miner.rules.empty or self._products is None:
            return []
        summary = (
            self._miner.rules
            .groupby("antecedent_id")
            .agg(rule_count=("lift", "count"), max_lift=("lift", "max"))
            .sort_values("max_lift", ascending=False)
            .head(n)
            .reset_index()
        )
        result = []
        for row in summary.to_dict("records"):
            pid = str(row["antecedent_id"])
            result.append({
                "product_id":   pid,
                "product_name": self._get_product_name(pid),
                "max_lift":     round(row["max_lift"], 2),
                "rule_count":   int(row["rule_count"]),
            })
        return result

    def also_bought(self, query: str, top_k: int = 5) -> list[dict]:
        """
        "Customers who bought this also bought" — direct co-purchase from rules.
        query: product_id string or product name (case-insensitive substring match).
        Returns [{product_id, product_name, confidence, lift}]
        """
        pid = self.find_product(query) or query
        raw_recs = self._miner.get_recommendations(pid, top_k)
        return [self._enrich_rec(r) for r in raw_recs]

    def you_might_like(self, query: str, top_k: int = 5) -> list[dict]:
        """
        "You might also like" — cross-category via graph traversal.
        query: product_id string or product name (case-insensitive substring match).
        Returns [{product_id, product_name, primary_category, affinity_score, hops}]
        """
        pid = self.find_product(query) or query
        return self._graph.cross_category_recommendations(pid, max_hops=2, top_k=top_k)

    def demo_comparison(
        self,
        tx_df: pd.DataFrame,
        product_query: str,
        top_k: int = 5,
    ) -> dict:
        """
        Before/after comparison showing lift improvement from UC1 dedup.

        Uses same transaction sample but two different ID encodings:
          raw:      product_name as ID  — text variants = fragmented signal
          enriched: product_id as ID    — canonical integer = consolidated signal

        tx_df must have columns: transaction_id, product_id, product_name
        product_query: product name or ID — resolved via find_product()

        Returns:
        {
          "product_id", "product_name",
          "raw_recommendations", "enriched_recommendations",
          "max_lift_raw", "max_lift_enriched", "lift_improvement",
          "raw_unique_ids", "enriched_unique_ids", "signal_consolidation_ratio"
        }
        """
        pid   = self.find_product(product_query) or product_query
        pname = self._get_product_name(pid)

        # Subsample to 10K orders for interactive speed (~130K rows, ~20s FP-Growth)
        all_orders = tx_df["transaction_id"].drop_duplicates()
        sample_size = min(10_000, len(all_orders))
        sampled_orders = all_orders.sample(sample_size, random_state=42)
        sample_tx = tx_df[tx_df["transaction_id"].isin(sampled_orders)]

        # Raw: product_name as ID (text fragmentation)
        raw_tx = sample_tx[["transaction_id", "product_name"]].rename(
            columns={"product_name": "product_id"}
        )
        raw_miner = AssociationRuleMiner(raw_tx)
        raw_miner.mine_rules()
        raw_recs = raw_miner.get_recommendations(pname, top_k)

        # Enriched: product_id as ID (canonical int, same subsample)
        enriched_tx = sample_tx[["transaction_id", "product_id"]].copy()
        enriched_tx["product_id"] = enriched_tx["product_id"].astype(str)
        enriched_miner = AssociationRuleMiner(enriched_tx)
        enriched_miner.mine_rules()
        enriched_recs = enriched_miner.get_recommendations(pid, top_k)
        enriched_recs = [self._enrich_rec(r) for r in enriched_recs]

        raw_lift      = max((r["lift"] for r in raw_recs),      default=0.0)
        enriched_lift = max((r["lift"] for r in enriched_recs), default=0.0)

        raw_unique      = raw_tx["product_id"].nunique()
        enriched_unique = enriched_tx["product_id"].nunique()

        return {
            "product_id":               pid,
            "product_name":             pname,
            "raw_recommendations":      raw_recs,
            "enriched_recommendations": enriched_recs,
            "max_lift_raw":             round(raw_lift, 4),
            "max_lift_enriched":        round(enriched_lift, 4),
            "lift_improvement":         round(enriched_lift - raw_lift, 4),
            "raw_unique_ids":           raw_unique,
            "enriched_unique_ids":      enriched_unique,
            "signal_consolidation_ratio": round(raw_unique / enriched_unique, 2) if enriched_unique > 0 else 0.0,
        }

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

    # ── persistence ────────────────────────────────────────────────────────────

    def save(self, directory: Path | str = UC4_SAVE_DIR) -> Path:
        """Persist recommender to disk (products parquet + rules parquet + graph pickle)."""
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)

        if self._products is not None:
            self._products.to_parquet(d / "products.parquet", index=False)

        if self._miner.rules is not None and not self._miner.rules.empty:
            rules = self._miner.rules.copy()
            # Frozensets aren't parquet-serializable — drop them
            for col in ("antecedents", "consequents"):
                if col in rules.columns:
                    rules = rules.drop(columns=[col])
            rules.to_parquet(d / "rules.parquet", index=False)

        with open(d / "graph.pkl", "wb") as f:
            pickle.dump(self._graph._G, f)

        meta = {
            "products": len(self._products) if self._products is not None else 0,
            "rules":    len(self._miner.rules) if self._miner.rules is not None else 0,
            "graph":    self._graph.stats(),
        }
        (d / "meta.json").write_text(json.dumps(meta, indent=2))
        logger.info("UC4 recommender saved to %s", d)
        return d

    @classmethod
    def load(cls, directory: Path | str = UC4_SAVE_DIR) -> "ProductRecommender":
        """Load a previously saved recommender from disk."""
        d = Path(directory)
        products_path = d / "products.parquet"
        rules_path    = d / "rules.parquet"
        graph_path    = d / "graph.pkl"

        if not products_path.exists():
            raise FileNotFoundError(f"No saved UC4 recommender found at {d}")

        rec = cls()
        rec._products = pd.read_parquet(products_path)

        if rules_path.exists():
            rules_df = pd.read_parquet(rules_path)
            rec._miner._rules = rules_df

        if graph_path.exists():
            import networkx as nx
            with open(graph_path, "rb") as f:
                rec._graph._G = pickle.load(f)

        logger.info(
            "UC4 recommender loaded from %s (%d products, %d rules)",
            d,
            len(rec._products),
            len(rec._miner.rules) if rec._miner.rules is not None else 0,
        )
        return rec

    @staticmethod
    def is_saved(directory: Path | str = UC4_SAVE_DIR) -> bool:
        return (Path(directory) / "products.parquet").exists()

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
