"""
Ops router — /v1/ops/*

Cache stats/flush and domain schema inspection.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

from src.api.dependencies import get_cache_client
from src.api.models.ops import (
    CacheFlushRequest,
    CacheFlushResult,
    CacheStats,
    ColumnDef,
    SchemaResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()

_ENRICHMENT_COLS = {"allergens", "primary_category", "dietary_tags", "is_organic"}
_COMPUTED_COLS = {"dq_score_pre", "dq_score_post", "dq_delta"}
_SCHEMA_DIR = Path("config/schemas")


# ── GET /v1/ops/cache/stats ──────────────────────────────────────────────────

@router.get("/cache/stats", response_model=CacheStats)
def cache_stats():
    cache = get_cache_client()
    try:
        stats = cache.get_stats()
        # stats is a CacheStats dataclass from src.cache.stats — map to our Pydantic model
        return CacheStats(
            redis_connected=getattr(stats, "redis_connected", False),
            total_keys=getattr(stats, "total_keys", 0),
            by_prefix=getattr(stats, "by_prefix", {}),
            sqlite_fallback=getattr(stats, "sqlite_fallback", False),
            sqlite_key_count=getattr(stats, "sqlite_key_count", None),
        )
    except Exception as exc:
        logger.warning("Cache stats failed: %s", exc)
        return CacheStats(
            redis_connected=False,
            total_keys=0,
            by_prefix={},
            sqlite_fallback=True,
        )


# ── POST /v1/ops/cache/flush ─────────────────────────────────────────────────

@router.post("/cache/flush", response_model=CacheFlushResult)
def cache_flush(body: CacheFlushRequest):
    # confirm=False raises ValueError from validator → FastAPI returns 422 automatically
    cache = get_cache_client()
    deleted = 0

    try:
        if body.prefix is None and body.domain is None:
            deleted = cache.flush_all_prefixes()
        elif body.prefix:
            # Delete all keys with this prefix (use a dummy key pattern)
            # CacheClient.delete expects a specific key, so we flush by prefix
            deleted = _flush_by_prefix(cache, body.prefix)
        elif body.domain:
            # Flush yaml keys matching domain
            deleted = _flush_yaml_by_domain(cache, body.domain)
    except Exception as exc:
        logger.exception("Cache flush failed: %s", exc)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "detail": str(exc)})

    return CacheFlushResult(deleted_count=deleted, prefix=body.prefix, domain=body.domain)


def _flush_by_prefix(cache, prefix: str) -> int:
    """Flush all keys for a known prefix."""
    try:
        r = cache._redis  # type: ignore[attr-defined]
        if r is None:
            return 0
        pattern = f"etl:{prefix}:*"
        keys = list(r.scan_iter(pattern))
        if keys:
            r.delete(*keys)
        return len(keys)
    except Exception as exc:
        logger.warning("Redis prefix flush failed: %s", exc)
        return 0


def _flush_yaml_by_domain(cache, domain: str) -> int:
    """Flush yaml cache keys that contain the domain string."""
    try:
        r = cache._redis  # type: ignore[attr-defined]
        if r is None:
            return 0
        pattern = f"etl:yaml:*{domain}*"
        keys = list(r.scan_iter(pattern))
        if keys:
            r.delete(*keys)
        return len(keys)
    except Exception as exc:
        logger.warning("Redis domain flush failed: %s", exc)
        return 0


# ── GET /v1/ops/schema/{domain} ──────────────────────────────────────────────

@router.get("/schema/{domain}", response_model=SchemaResponse)
def get_schema(domain: str):
    schema_path = _SCHEMA_DIR / f"{domain}_schema.json"
    if not schema_path.exists():
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "detail": f"No schema file found for domain '{domain}'"},
        )

    try:
        raw = json.loads(schema_path.read_text())
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "detail": f"Failed to parse schema: {exc}"},
        )

    # raw is either a list of column dicts or a dict with a "columns" key
    if isinstance(raw, list):
        col_list = raw
    elif isinstance(raw, dict):
        col_list = raw.get("columns", [])
    else:
        col_list = []

    columns = []
    for col in col_list:
        if isinstance(col, str):
            name, dtype, required = col, "str", False
        elif isinstance(col, dict):
            name = col.get("name", "")
            dtype = col.get("dtype", col.get("type", "str"))
            required = col.get("required", False)
        else:
            continue

        columns.append(ColumnDef(
            name=name,
            dtype=str(dtype),
            required=bool(required),
            enrichment=name in _ENRICHMENT_COLS,
            computed=name in _COMPUTED_COLS,
        ))

    return SchemaResponse(
        domain=domain,
        columns=columns,
        source_file=str(schema_path),
    )
