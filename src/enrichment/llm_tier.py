"""Strategy 3: RAG-augmented LLM — primary_category assignment only.

Uses top-3 KNN neighbors from Strategy 2 as RAG context injected into the
LLM prompt. The model does analogy completion anchored to real examples rather
than cold inference from a sparse row. This dramatically reduces hallucination.

allergens, is_organic, and dietary_tags are NEVER passed to this strategy.
Those fields are handled exclusively by Strategy 1 (deterministic extraction).
If S1 extraction fails, those fields stay null — they are not inferred here.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Optional

import pandas as pd


def _safe_text(v) -> str:
    """Return str(v) or '' — safe for pd.NA, None, float NaN."""
    try:
        return "" if pd.isna(v) else str(v)
    except (TypeError, ValueError):
        return str(v) if v is not None else ""

import asyncio

from src.models.llm import async_call_llm_json, call_llm_json, get_enrichment_llm
from src.enrichment.corpus import add_to_corpus, load_corpus, save_corpus

logger = logging.getLogger(__name__)

CATEGORIES = (
    "Breakfast Cereals, Dairy, Meat & Poultry, Seafood, Bakery, "
    "Confectionery, Snacks, Beverages, Condiments, Frozen Foods, Fruits, "
    "Vegetables, Pasta & Grains, Soups, Baby Food, Supplements, "
    "Canned Foods, Deli, Pet Food, Other"
)

SYSTEM_PROMPT = (
    "You are a product categorization assistant. Assign exactly one category "
    "from the list below. Return ONLY a JSON object with one key: "
    '{{"primary_category": "<category>"}}. If you cannot determine the category '
    'with confidence, return {{"primary_category": null}}.\n\n'
    f"CATEGORIES: {CATEGORIES}"
)

BATCH_SYSTEM_PROMPT = (
    "You are a product categorization assistant. Categorize each product below. "
    "Return ONLY a JSON object: "
    '{"results": [{"idx": 0, "primary_category": "<category>"}, ...]}. '
    "Use null for primary_category if unsure. Include ALL indices in results.\n\n"
    f"CATEGORIES: {CATEGORIES}"
)

_LLM_BATCH_SIZE   = int(__import__("os").environ.get("LLM_ENRICH_BATCH_SIZE",  "50"))
_LLM_CONCURRENCY  = int(__import__("os").environ.get("LLM_ENRICH_CONCURRENCY", "5"))


def _compute_content_hash(product_name: str, description: str, enrich_cols: list[str]) -> str:
    """SHA-256-16 of (product_name, description, sorted enrich_cols)."""
    raw = json.dumps({
        "name": (product_name or "").strip().lower(),
        "desc": (description or "").strip(),
        "cols": sorted(enrich_cols),
    })
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _build_rag_prompt(row: pd.Series, neighbors: list[dict]) -> str:
    """Build a RAG-augmented prompt for primary_category assignment."""
    lines = []

    if neighbors:
        lines.append("Similar products already categorized:")
        for n in neighbors:
            sim = n.get("similarity", 0.0)
            pname = n.get("product_name", "")
            cat = n.get("category", "")
            lines.append(f"  - {pname} → {cat} (similarity: {sim:.2f})")
        lines.append("")

    lines.append("Product to categorize:")
    for field, label in [
        ("product_name", "Name"),
        ("brand_name", "Brand"),
        ("ingredients", "Ingredients"),
        ("category", "Source category"),
    ]:
        val = row.get(field)
        if pd.notna(val) and str(val).strip():
            lines.append(f"  {label}: {val}")

    lines.append("")
    lines.append("What is the primary_category?")
    return "\n".join(lines)


def _build_batch_rag_prompt(rows: list[pd.Series], neighbors_list: list[list[dict]]) -> str:
    """Build a batch RAG prompt for multiple rows."""
    lines = ["Products to categorize (return results for ALL indices):"]
    for i, (row, neighbors) in enumerate(zip(rows, neighbors_list)):
        lines.append(f"\n[{i}]")
        for field, label in [
            ("product_name", "Name"),
            ("brand_name", "Brand"),
            ("ingredients", "Ingredients"),
            ("category", "Source category"),
        ]:
            val = row.get(field)
            if pd.notna(val) and str(val).strip():
                lines.append(f"  {label}: {str(val)[:200]}")
        if neighbors:
            lines.append("  Similar:")
            for n in neighbors[:2]:
                lines.append(f"    - {n.get('product_name', '')} → {n.get('category', '')} ({n.get('similarity', 0):.2f})")
    return "\n".join(lines)


async def _call_one_batch(
    miss_rows: list,
    batch_neighbors: list,
    model: str,
    semaphore: asyncio.Semaphore,
    batch_label: str,
    max_retries: int = 4,
) -> dict | Exception:
    """Fire one LLM batch call with exponential backoff on rate limits. Returns result dict or Exception."""
    prompt = _build_batch_rag_prompt(miss_rows, batch_neighbors)
    last_exc: Exception = Exception("no attempts made")
    for attempt in range(max_retries):
        try:
            async with semaphore:
                return await async_call_llm_json(
                    model=model,
                    messages=[
                        {"role": "system", "content": BATCH_SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.1,
                )
        except Exception as e:
            last_exc = e
            is_rate_limit = (
                "ratelimit" in type(e).__name__.lower()
                or "rate limit" in str(e).lower()
                or "429" in str(e)
            )
            if is_rate_limit and attempt < max_retries - 1:
                wait = 30 * (2 ** attempt)  # 30s, 60s, 120s
                logger.warning(
                    "S3 RAG-LLM: %s rate limited (attempt %d/%d), retrying in %ds",
                    batch_label, attempt + 1, max_retries, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.warning("S3 RAG-LLM: %s failed: %s", batch_label, e)
                return e
    return last_exc


def llm_enrich(
    df: pd.DataFrame,
    enrich_cols: list[str],
    needs_enrichment: pd.Series,
    cache_client=None,
) -> tuple[pd.DataFrame, pd.Series, dict]:
    """
    Call LLM with RAG context for rows where primary_category is still null.

    Only operates on primary_category. allergens, is_organic, and dietary_tags
    are never sent to the LLM — they are extraction-only fields handled by S1.

    Batches are fired concurrently (up to LLM_ENRICH_CONCURRENCY) via asyncio.
    df mutations are applied sequentially after all batches complete.

    Returns (modified_df, updated_needs_enrichment_mask, stats).
    """
    if "primary_category" not in enrich_cols:
        return df, needs_enrichment, {"resolved": 0}

    mask = needs_enrichment & df["primary_category"].isna()
    if not mask.any():
        return df, needs_enrichment, {"resolved": 0}

    model = get_enrichment_llm()
    rows_to_enrich = df.index[mask].tolist()
    logger.info(
        f"S3 RAG-LLM: {len(rows_to_enrich)} rows need primary_category "
        f"(batch_size={_LLM_BATCH_SIZE}, concurrency={_LLM_CONCURRENCY})"
    )

    index, metadata = load_corpus()
    resolved = 0

    from src.cache.client import CACHE_TTL_LLM

    # ── Phase 1: Cache lookup (synchronous) ──────────────────────────
    # Collect per-batch data: cache hits applied immediately, misses queued for LLM
    pending_batches: list[tuple[list[int], list, list]] = []  # (miss_indices, miss_rows, neighbors)

    for batch_start in range(0, len(rows_to_enrich), _LLM_BATCH_SIZE):
        batch_indices = rows_to_enrich[batch_start:batch_start + _LLM_BATCH_SIZE]
        batch_rows = [df.loc[idx] for idx in batch_indices]

        cache_hit_map: dict[int, dict] = {}
        miss_indices: list[int] = []
        miss_rows: list[pd.Series] = []

        if cache_client is not None:
            for idx, row in zip(batch_indices, batch_rows):
                product_name = _safe_text(row.get("product_name"))
                description = _safe_text(row.get("ingredients")) or _safe_text(row.get("description"))
                content_hash = _compute_content_hash(product_name, description, enrich_cols)
                cached = cache_client.get("llm", content_hash)
                if cached is not None:
                    try:
                        cache_hit_map[idx] = json.loads(cached.decode())
                    except Exception:
                        miss_indices.append(idx)
                        miss_rows.append(row)
                else:
                    miss_indices.append(idx)
                    miss_rows.append(row)
        else:
            miss_indices = list(batch_indices)
            miss_rows = batch_rows

        for real_idx, cached_result in cache_hit_map.items():
            for col, val in cached_result.items():
                if col in df.columns and val is not None:
                    df.at[real_idx, col] = val
            resolved += 1

        if not miss_indices:
            continue

        batch_neighbors: list[list[dict]] = []
        for row in miss_rows:
            raw = row.get("_knn_neighbors")
            try:
                neighbors = json.loads(raw) if pd.notna(raw) and raw else []
            except (json.JSONDecodeError, TypeError):
                neighbors = []
            batch_neighbors.append(neighbors)

        pending_batches.append((miss_indices, miss_rows, batch_neighbors))

    if not pending_batches:
        needs_enrichment = df[enrich_cols].isna().any(axis=1)
        return df, needs_enrichment, {"resolved": resolved}

    # ── Phase 2: Fire all LLM batches concurrently ───────────────────
    # Always runs in a fresh thread+loop — safe in both CLI and Streamlit contexts.
    async def _gather_all():
        semaphore = asyncio.Semaphore(_LLM_CONCURRENCY)
        tasks = [
            _call_one_batch(
                miss_rows=miss_rows,
                batch_neighbors=neighbors,
                model=model,
                semaphore=semaphore,
                batch_label=f"batch[{i * _LLM_BATCH_SIZE}:{(i + 1) * _LLM_BATCH_SIZE}]",
            )
            for i, (_, miss_rows, neighbors) in enumerate(pending_batches)
        ]
        return await asyncio.gather(*tasks)

    import concurrent.futures

    def _run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_gather_all())
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        llm_results = pool.submit(_run_in_thread).result()

    # ── Phase 3: Apply results sequentially ──────────────────────────
    corpus_updated = False
    for (miss_indices, miss_rows, _), result in zip(pending_batches, llm_results):
        if isinstance(result, Exception):
            continue
        items = result.get("results", []) if isinstance(result, dict) else []
        for item in items:
            local_idx = item.get("idx")
            category = item.get("primary_category")
            if local_idx is None or not isinstance(local_idx, int):
                continue
            if local_idx < 0 or local_idx >= len(miss_indices):
                continue
            real_idx = miss_indices[local_idx]
            if category is not None:
                df.at[real_idx, "primary_category"] = category
                resolved += 1
                if index is not None:
                    add_to_corpus(df.loc[real_idx], category, index, metadata)
                    corpus_updated = True
                if cache_client is not None:
                    try:
                        row = miss_rows[local_idx]
                        product_name = _safe_text(row.get("product_name"))
                        description = _safe_text(row.get("ingredients")) or _safe_text(row.get("description"))
                        content_hash = _compute_content_hash(product_name, description, enrich_cols)
                        row_result = {col: df.at[real_idx, col] for col in enrich_cols if col in df.columns}
                        cache_client.set("llm", content_hash, json.dumps(row_result).encode(), ttl=CACHE_TTL_LLM)
                    except Exception as _ce:
                        logger.debug(f"LLM cache write skipped: {_ce}")

    if corpus_updated:
        try:
            save_corpus(index, metadata)
        except Exception as e:
            logger.warning(f"S3 RAG-LLM: could not save corpus: {e}")

    needs_enrichment = df[enrich_cols].isna().any(axis=1)
    return df, needs_enrichment, {"resolved": resolved}
