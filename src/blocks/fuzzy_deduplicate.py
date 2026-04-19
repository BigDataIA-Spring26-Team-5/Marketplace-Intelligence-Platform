"""Blocking + rapidfuzz scoring + union-find clustering for deduplication."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz.process import cdist as rf_cdist
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

        # Rows with no usable name stay as singletons — exclude from blocking to avoid
        # collapsing all null-name rows into one massive cluster via the "" key
        valid_name_mask = names.str.len() > 0

        blocks: dict[str, list[int]] = {}
        for idx in names[valid_name_mask].index:
            key = names.iloc[idx][:3].strip()
            if key:
                blocks.setdefault(key, []).append(idx)

        # Vectorized comparison within blocks via rapidfuzz cdist
        uf = UnionFind(n)
        for block_indices in blocks.values():
            if len(block_indices) < 2:
                continue
            block_names = [names.iloc[i] for i in block_indices]
            block_brands = [brands.iloc[i] for i in block_indices]
            block_combined = [f"{nm} {br}" for nm, br in zip(block_names, block_brands)]

            name_mat = rf_cdist(block_names, block_names, scorer=fuzz.token_sort_ratio, workers=-1) / 100.0
            brand_mat = rf_cdist(block_brands, block_brands, scorer=fuzz.ratio, workers=-1) / 100.0
            combined_mat = rf_cdist(block_combined, block_combined, scorer=fuzz.token_sort_ratio, workers=-1) / 100.0

            weighted_mat = (
                name_mat * name_weight
                + brand_mat * brand_weight
                + combined_mat * combined_weight
            )

            pairs = np.argwhere(weighted_mat >= threshold / 100.0)
            for i_local, j_local in pairs:
                if i_local < j_local:
                    uf.union(block_indices[i_local], block_indices[j_local])

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
