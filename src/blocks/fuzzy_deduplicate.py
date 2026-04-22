"""Blocking + rapidfuzz scoring + union-find clustering for deduplication."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz.process import cdist as rf_cdist
from src.blocks.base import Block

logger = logging.getLogger(__name__)

_NOISE_RE = re.compile(r"\b(the|a|an|and|or|of|in|for|with|by)\b", re.IGNORECASE)


def _normalize_name(name: str) -> str:
    """Lowercase, strip noise words and extra whitespace."""
    n = name.lower().strip()
    n = _NOISE_RE.sub(" ", n)
    return re.sub(r"\s+", " ", n).strip()


def _compute_dedup_key(normalized_name: str) -> str:
    """SHA-256-16 of normalized product name."""
    return hashlib.sha256(normalized_name.encode()).hexdigest()[:16]


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

    last_clusters: list[dict] = []
    last_dedup_rate: float = 0.0

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        self.last_clusters = []
        self.last_dedup_rate = 0.0
        config = config or {}
        threshold = config.get("dedup_threshold", 85)
        name_weight = config.get("name_weight", 0.5)
        brand_weight = config.get("brand_weight", 0.2)
        combined_weight = config.get("combined_weight", 0.3)
        cache_client = config.get("cache_client")

        df = df.copy().reset_index(drop=True)
        n = len(df)

        # Build blocking key: first 3 chars of product_name (lowered)
        if "product_name" not in df.columns:
            df["duplicate_group_id"] = range(n)
            df["canonical"] = True
            return df

        names = df["product_name"].fillna("").astype(str).str.lower()
        brands = df["brand_name"].fillna("").astype(str).str.lower() if "brand_name" in df.columns else pd.Series([""] * n)

        # Check dedup cluster cache — pre-assign rows with known cluster IDs
        from src.cache.client import CACHE_TTL_DEDUP
        pre_assigned: dict[int, int] = {}  # df index → cluster_id from cache
        if cache_client is not None:
            for idx in range(n):
                raw_name = df["product_name"].iloc[idx] if "product_name" in df.columns else ""
                norm = _normalize_name(str(raw_name) if pd.notna(raw_name) else "")
                if not norm:
                    continue
                cached = cache_client.get("dedup", norm)
                if cached is not None:
                    try:
                        pre_assigned[idx] = json.loads(cached.decode())["cluster_id"]
                    except Exception:
                        pass

        # If all rows have cached cluster IDs, skip similarity computation
        uncached_indices = [i for i in range(n) if i not in pre_assigned]

        # Rows with no usable name stay as singletons — exclude from blocking to avoid
        # collapsing all null-name rows into one massive cluster via the "" key
        valid_name_mask = names.str.len() > 0

        uf = UnionFind(n)

        if uncached_indices:
            blocks: dict[str, list[int]] = {}
            for idx in names[valid_name_mask].index:
                if idx in uncached_indices:
                    key = names.iloc[idx][:3].strip()
                    if key:
                        blocks.setdefault(key, []).append(idx)

            # Vectorized comparison within blocks via rapidfuzz cdist.
            # For large blocks (>= OOM_BLOCK_THRESHOLD) fall back to lazy pair-by-pair
            # scoring to avoid allocating O(n²) matrices that can exhaust RAM.
            OOM_BLOCK_THRESHOLD = int(os.environ.get("DEDUP_BLOCK_OOM_THRESHOLD", "2000"))

            for block_indices in blocks.values():
                if len(block_indices) < 2:
                    continue
                block_names = [names.iloc[i] for i in block_indices]
                block_brands = [brands.iloc[i] for i in block_indices]
                block_combined = [f"{nm} {br}" for nm, br in zip(block_names, block_brands)]

                if len(block_indices) >= OOM_BLOCK_THRESHOLD:
                    # Lazy path: compare each pair once without materialising full matrices
                    logger.warning(
                        "Block size %d >= OOM threshold %d; using lazy pair comparison",
                        len(block_indices), OOM_BLOCK_THRESHOLD,
                    )
                    thresh_score = threshold / 100.0
                    for li in range(len(block_indices)):
                        for lj in range(li + 1, len(block_indices)):
                            name_score  = fuzz.token_sort_ratio(block_names[li], block_names[lj]) / 100.0
                            brand_score = fuzz.ratio(block_brands[li], block_brands[lj]) / 100.0
                            comb_score  = fuzz.token_sort_ratio(block_combined[li], block_combined[lj]) / 100.0
                            weighted = (
                                name_score * name_weight
                                + brand_score * brand_weight
                                + comb_score * combined_weight
                            )
                            if weighted >= thresh_score:
                                uf.union(block_indices[li], block_indices[lj])
                else:
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

        # Assign cluster IDs — pre-assigned rows get their cached IDs
        cluster_map: dict[int, int] = {}
        cluster_id = 0
        group_ids = []

        # Seed cluster_map with pre-assigned IDs to maintain cross-partition consistency
        for idx, cid in pre_assigned.items():
            root = uf.find(idx)
            if root not in cluster_map:
                cluster_map[root] = cid
                cluster_id = max(cluster_id, cid + 1)

        for idx in range(n):
            root = uf.find(idx)
            if root not in cluster_map:
                cluster_map[root] = cluster_id
                cluster_id += 1
            group_ids.append(cluster_map[root])

        # Cache new cluster assignments
        if cache_client is not None:
            for idx in uncached_indices:
                raw_name = df["product_name"].iloc[idx] if "product_name" in df.columns else ""
                norm = _normalize_name(str(raw_name) if pd.notna(raw_name) else "")
                if not norm:
                    continue
                try:
                    cache_client.set("dedup", norm, json.dumps({"cluster_id": group_ids[idx]}).encode(), ttl=CACHE_TTL_DEDUP)
                except Exception:
                    pass

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

        # UC2: populate last_clusters and last_dedup_rate
        self.last_dedup_rate = (n - unique_clusters) / n if n > 0 else 0.0

        from collections import Counter
        cluster_sizes = Counter(group_ids)
        largest = cluster_sizes.most_common(3)
        for cid, size in largest:
            if size > 1:
                logger.info(f"  Largest cluster #{cid}: {size} rows")

        # Build last_clusters — one dict per multi-member cluster
        built_clusters: list[dict] = []
        for cid, size in cluster_sizes.items():
            if size <= 1:
                continue
            member_indices = [i for i, g in enumerate(group_ids) if g == cid]
            member_names = [
                str(df["product_name"].iloc[i]) if "product_name" in df.columns else ""
                for i in member_indices
            ]
            canonical_idx = member_indices[0]
            canonical_name = str(df["product_name"].iloc[canonical_idx]) if "product_name" in df.columns else ""
            canonical_brand = str(df["brand_name"].iloc[canonical_idx]) if "brand_name" in df.columns else ""
            norm_name = _normalize_name(canonical_name)
            built_clusters.append({
                "cluster_id": cid,
                "member_product_names": member_names,
                "canonical_product_name": canonical_name,
                "canonical_brand_name": canonical_brand,
                "size": size,
                "dedup_key": norm_name[:3] if norm_name else "",
            })
        self.last_clusters = built_clusters

        return df
