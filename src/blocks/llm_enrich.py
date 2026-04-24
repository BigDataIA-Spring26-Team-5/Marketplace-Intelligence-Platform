"""3-strategy enrichment block — S1: deterministic extraction -> S2: KNN corpus -> S3: LLM."""

from __future__ import annotations

import logging

import pandas as pd
from src.blocks.base import Block
from src.enrichment.deterministic import deterministic_enrich
from src.enrichment.embedding import embedding_enrich
from src.enrichment.llm_tier import llm_enrich
from src.enrichment.rules_loader import EnrichmentRulesLoader

logger = logging.getLogger(__name__)

ENRICHMENT_COLUMNS = ["primary_category", "dietary_tags", "is_organic", "allergens"]

# Safety fields that must never be modified by S2 or S3
_SAFETY_FIELDS = ["allergens", "is_organic", "dietary_tags"]


class LLMEnrichBlock(Block):
    name = "llm_enrich"
    domain = "all"
    description = "3-strategy enrichment (deterministic → KNN → LLM) for category, dietary tags, organic flag"
    inputs = ["product_name", "ingredients"]
    outputs = ["primary_category", "dietary_tags", "is_organic", "enriched_by_llm"]

    # Stores enrichment stats from the last run (read by the UI / graph node)
    last_enrichment_stats: dict = {}

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        config = config or {}
        domain = config.get("domain", "nutrition")
        enrich_cols = config.get("enrichment_columns", ENRICHMENT_COLUMNS)

        df = df.copy()

        # Load enrichment rules for this domain
        rules_loader = EnrichmentRulesLoader(domain)

        # Ensure enrichment columns exist
        for col in enrich_cols:
            if col not in df.columns:
                df[col] = pd.NA

        # Track which rows still need enrichment (True = needs enrichment)
        needs_enrichment = df[enrich_cols].isna().any(axis=1)

        stats = {"deterministic": 0, "embedding": 0, "llm": 0, "unresolved": 0, "corpus_augmented": 0, "corpus_size_after": 0}
        initial_missing = int(needs_enrichment.sum())
        logger.info(f"Enrichment: {initial_missing}/{len(df)} rows need enrichment")

        # Strategy 1: Deterministic extraction using domain rules
        df, needs_enrichment, s1_stats = deterministic_enrich(
            df, enrich_cols, needs_enrichment, rules=rules_loader.s1_fields
        )
        stats["deterministic"] = s1_stats["resolved"]
        logger.info(f"  S1 (deterministic extraction): resolved {stats['deterministic']} rows")

        # Capture safety field values after S1 to verify S2/S3 do not modify them
        safety_field_names = rules_loader.safety_field_names() or _SAFETY_FIELDS
        safety_snapshot = {
            col: df[col].copy()
            for col in safety_field_names
            if col in df.columns
        }

        # Strategy 2: KNN corpus search (primary_category only)
        df, needs_enrichment, s2_stats = embedding_enrich(df, enrich_cols, needs_enrichment, cache_client=config.get("cache_client"))
        stats["embedding"] = s2_stats["resolved"]
        stats["corpus_augmented"] = s2_stats.get("corpus_augmented", 0)
        stats["corpus_size_after"] = s2_stats.get("corpus_size_after", 0)
        logger.info(f"  S2 (KNN corpus): resolved {stats['embedding']} rows")

        # Capture which rows are unresolved before S3 to identify S3-resolved rows
        unresolved_before_s3 = needs_enrichment.copy()

        # Strategy 3: RAG-augmented LLM (primary_category only)
        df, needs_enrichment, s3_stats = llm_enrich(df, enrich_cols, needs_enrichment, cache_client=config.get("cache_client"))
        stats["llm"] = s3_stats["resolved"]
        stats["unresolved"] = int(df["primary_category"].isna().sum()) if "primary_category" in df.columns else 0
        logger.info(f"  S3 (LLM): resolved {stats['llm']} rows")
        logger.info(f"  Unresolved: {stats['unresolved']} rows")

        # Drop pipeline-internal column before output
        if "_knn_neighbors" in df.columns:
            df = df.drop(columns=["_knn_neighbors"])

        # Tag rows resolved by S3 only
        df["enriched_by_llm"] = False
        s3_resolved = unresolved_before_s3 & ~needs_enrichment
        if s3_resolved.any():
            df.loc[s3_resolved, "enriched_by_llm"] = True

        # Safety assertion: S2 and S3 must not have modified safety fields
        for col, before in safety_snapshot.items():
            after = df[col]
            s3_llm_rows = df.index[df["enriched_by_llm"]]
            # Check only rows marked as enriched_by_llm (S3-resolved)
            changed = s3_llm_rows[
                after.loc[s3_llm_rows].fillna("__null__")
                != before.loc[s3_llm_rows].fillna("__null__")
            ]
            if len(changed) > 0:
                logger.warning(
                    f"Safety assertion failed: '{col}' was modified by S2/S3 "
                    f"for {len(changed)} rows. These rows: {changed.tolist()[:10]}"
                )

        # Store stats for access by the UI/graph
        LLMEnrichBlock.last_enrichment_stats = stats

        return df
