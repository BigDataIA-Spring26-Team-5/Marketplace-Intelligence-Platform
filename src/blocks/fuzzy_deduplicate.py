"""Blocking + rapidfuzz scoring + union-find clustering for deduplication."""

from __future__ import annotations

import logging

import pandas as pd
from rapidfuzz import fuzz
from src.blocks.base import Block

logger = logging.getLogger(__name__)


class UnionFind:
    """Union-Find (Disjoint Set) for transitive closure of duplicate pairs."""

    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


class FuzzyDeduplicateBlock(Block):
    name = "fuzzy_deduplicate"
    domain = "all"
    description = "Cluster near-duplicate rows using blocking + rapidfuzz scoring"
    inputs = ["product_name", "brand_name"]
    outputs = ["duplicate_group_id", "canonical"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        config = config or {}
        threshold = config.get("dedup_threshold", 85)
        name_weight = config.get("name_weight", 0.5)
        brand_weight = config.get("brand_weight", 0.2)
        combined_weight = config.get("combined_weight", 0.3)

        df = df.copy().reset_index(drop=True)
        n = len(df)

        # Build blocking key: first 3 chars of product_name (lowered)
        if "product_name" not in df.columns:
            df["duplicate_group_id"] = range(n)
            df["canonical"] = True
            return df

        names = df["product_name"].fillna("").astype(str).str.lower()
        brands = df["brand_name"].fillna("").astype(str).str.lower() if "brand_name" in df.columns else pd.Series([""] * n)

        blocks: dict[str, list[int]] = {}
        for idx, name in enumerate(names):
            key = name[:3].strip()
            if key:
                blocks.setdefault(key, []).append(idx)

        # Pairwise comparison within blocks
        uf = UnionFind(n)
        for block_indices in blocks.values():
            if len(block_indices) < 2:
                continue
            for i in range(len(block_indices)):
                for j in range(i + 1, len(block_indices)):
                    a, b = block_indices[i], block_indices[j]
                    name_score = fuzz.token_sort_ratio(names.iloc[a], names.iloc[b])
                    brand_score = fuzz.ratio(brands.iloc[a], brands.iloc[b])
                    combined_a = f"{names.iloc[a]} {brands.iloc[a]}"
                    combined_b = f"{names.iloc[b]} {brands.iloc[b]}"
                    combined_score = fuzz.token_sort_ratio(combined_a, combined_b)

                    weighted = (
                        name_score * name_weight
                        + brand_score * brand_weight
                        + combined_score * combined_weight
                    )
                    if weighted >= threshold:
                        uf.union(a, b)

        # Assign cluster IDs
        cluster_map: dict[int, int] = {}
        cluster_id = 0
        group_ids = []
        for idx in range(n):
            root = uf.find(idx)
            if root not in cluster_map:
                cluster_map[root] = cluster_id
                cluster_id += 1
            group_ids.append(cluster_map[root])

        df["duplicate_group_id"] = group_ids

        # Mark canonical (first in each group)
        seen_groups: set[int] = set()
        canonical = []
        for gid in group_ids:
            canonical.append(gid not in seen_groups)
            seen_groups.add(gid)
        df["canonical"] = canonical

        unique_clusters = cluster_id
        dup_rows = n - unique_clusters
        dedup_rate = (dup_rows / n * 100) if n > 0 else 0
        logger.info(f"Dedup: {n} rows → {unique_clusters} clusters ({dedup_rate:.1f}% duplicate rate)")

        # Log largest clusters
        from collections import Counter
        cluster_sizes = Counter(group_ids)
        largest = cluster_sizes.most_common(3)
        for cid, size in largest:
            if size > 1:
                logger.info(f"  Largest cluster #{cid}: {size} rows")

        return df
