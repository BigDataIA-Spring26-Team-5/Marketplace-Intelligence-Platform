"""
UC4 Recommendations — Product Relationship Graph (NetworkX)

Nodes: products and categories
Edges: co-purchase relationships (weight = lift score)

Used for cross-category "you might also like" recommendations via
graph traversal — finds non-obvious product affinities across categories.

Uses NetworkX (no new service required).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ProductGraph:
    """
    In-memory product relationship graph backed by NetworkX.

    Usage:
        graph = ProductGraph()
        graph.load_products(enriched_df)
        graph.load_relationships(rules_df)
        recs = graph.cross_category_recommendations("B001234", max_hops=2)
    """

    def __init__(self):
        try:
            import networkx as nx
            self._G = nx.DiGraph()
        except ImportError:
            raise ImportError("pip install networkx")

    # ── public API ─────────────────────────────────────────────────────────────

    def load_products(self, df: pd.DataFrame) -> int:
        """
        Load product nodes. Each product gets attributes from UC1 output.
        Returns number of nodes added.
        """
        import networkx as nx

        count = 0
        for _, row in df.iterrows():
            pid = str(row.get("product_id") or row.get("product_name", ""))
            if not pid:
                continue
            self._G.add_node(pid, **{
                "type":             "product",
                "product_name":     str(row.get("product_name") or ""),
                "brand_name":       str(row.get("brand_name") or ""),
                "primary_category": str(row.get("primary_category") or ""),
                "dietary_tags":     str(row.get("dietary_tags") or ""),
                "allergens":        str(row.get("allergens") or ""),
                "dq_score_post":    float(row.get("dq_score_post") or 0.0),
            })

            # Also add category node + product→category edge
            cat = str(row.get("primary_category") or "Unknown")
            self._G.add_node(cat, type="category")
            self._G.add_edge(pid, cat, weight=1.0, edge_type="belongs_to")
            count += 1

        logger.info("Loaded %d product nodes, %d total nodes", count, self._G.number_of_nodes())
        return count

    def load_relationships(self, rules_df: pd.DataFrame) -> int:
        """
        Load co-purchase edges from association rules DataFrame.
        rules_df must have: antecedent_id, consequent_id, lift, confidence
        Returns number of edges added.
        """
        count = 0
        for _, row in rules_df.iterrows():
            src = str(row["antecedent_id"])
            dst = str(row["consequent_id"])
            if not self._G.has_node(src) or not self._G.has_node(dst):
                continue
            self._G.add_edge(src, dst,
                weight=float(row.get("lift", 1.0)),
                confidence=float(row.get("confidence", 0.0)),
                edge_type="co_purchase",
            )
            count += 1

        logger.info("Loaded %d co-purchase edges", count)
        return count

    def cross_category_recommendations(
        self, product_id: str, max_hops: int = 2, top_k: int = 5
    ) -> list[dict]:
        """
        Find cross-category product recommendations via graph traversal.
        Explores up to max_hops away, returns products in different categories.
        """
        import networkx as nx

        if product_id not in self._G:
            return []

        src_category = self._G.nodes[product_id].get("primary_category", "")

        # BFS up to max_hops, collect product nodes in other categories
        visited  = {product_id}
        frontier = {product_id}
        results: list[dict] = []

        for hop in range(max_hops):
            next_frontier = set()
            for node in frontier:
                for neighbor in self._G.successors(node):
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                    node_data = self._G.nodes[neighbor]
                    if node_data.get("type") != "product":
                        continue
                    neighbor_cat = node_data.get("primary_category", "")
                    if neighbor_cat and neighbor_cat != src_category:
                        edge_data = self._G.get_edge_data(node, neighbor, {})
                        results.append({
                            "product_id":       neighbor,
                            "product_name":     node_data.get("product_name", neighbor),
                            "primary_category": neighbor_cat,
                            "hops":             hop + 1,
                            "affinity_score":   round(float(edge_data.get("weight", 1.0)), 4),
                        })
                    next_frontier.add(neighbor)
            frontier = next_frontier

        results.sort(key=lambda x: x["affinity_score"], reverse=True)
        return results[:top_k]

    def find_path(self, product_a: str, product_b: str) -> list[str]:
        """Shortest path between two products through the graph."""
        import networkx as nx
        try:
            return nx.shortest_path(self._G, product_a, product_b, weight=None)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    def stats(self) -> dict:
        product_nodes  = sum(1 for _, d in self._G.nodes(data=True) if d.get("type") == "product")
        category_nodes = sum(1 for _, d in self._G.nodes(data=True) if d.get("type") == "category")
        copurchase_edges = sum(
            1 for _, _, d in self._G.edges(data=True) if d.get("edge_type") == "co_purchase"
        )
        return {
            "product_nodes":    product_nodes,
            "category_nodes":   category_nodes,
            "copurchase_edges": copurchase_edges,
            "total_nodes":      self._G.number_of_nodes(),
            "total_edges":      self._G.number_of_edges(),
        }
