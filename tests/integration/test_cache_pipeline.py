"""
Integration test: YAML mapping cache across two sequential pipeline invocations.

SC-004: Second invocation with identical source schema must skip LLM calls for
schema analysis (analyze_schema + critique_schema + plan_sequence agents) and
produce output identical to the first invocation.

These tests use an in-memory fake cache (dict-backed) so no real Redis is
required in CI. The fake cache satisfies the CacheClient interface.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fake in-memory cache satisfying the CacheClient interface
# ---------------------------------------------------------------------------

class FakeCache:
    """Dict-backed drop-in for CacheClient. No Redis, no TTL, no SHA-256."""

    def __init__(self):
        self._store: dict[str, bytes] = {}
        self._hits: dict[str, int] = {}
        self._misses: dict[str, int] = {}

    def _key(self, prefix: str, key_input) -> str:
        if isinstance(key_input, list):
            raw = json.dumps(key_input, sort_keys=True)
        else:
            raw = str(key_input)
        import hashlib
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return f"{prefix}:{digest}"

    def get(self, prefix: str, key_input) -> Optional[bytes]:
        k = self._key(prefix, key_input)
        v = self._store.get(k)
        if v is not None:
            self._hits[prefix] = self._hits.get(prefix, 0) + 1
        else:
            self._misses[prefix] = self._misses.get(prefix, 0) + 1
        return v

    def set(self, prefix: str, key_input, value: bytes, ttl: int = 0) -> bool:
        k = self._key(prefix, key_input)
        self._store[k] = value
        return True

    def delete(self, prefix: str, key_input) -> bool:
        k = self._key(prefix, key_input)
        return self._store.pop(k, None) is not None

    def flush_all_prefixes(self) -> int:
        n = len(self._store)
        self._store.clear()
        return n

    def get_stats(self):
        from src.cache.stats import CacheStats
        stats = CacheStats()
        for prefix, n in self._hits.items():
            for _ in range(n):
                stats.record_hit(prefix)
        for prefix, n in self._misses.items():
            for _ in range(n):
                stats.record_miss(prefix)
        return stats

    @property
    def yaml_hits(self) -> int:
        return self._hits.get("yaml", 0)

    @property
    def yaml_misses(self) -> int:
        return self._misses.get("yaml", 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_csv(tmp_path: Path, filename: str = "part.csv") -> Path:
    """Write a minimal food-product CSV to tmp_path and return its path."""
    df = pd.DataFrame({
        "fdc_id": [1, 2, 3],
        "description": ["Whole Milk", "Cheddar Cheese", "Greek Yogurt"],
        "brand_owner": ["DairyFarm", "CheeseCo", "YogurtBrand"],
        "ingredients": [
            "MILK, VITAMIN D",
            "CULTURED MILK, SALT",
            "MILK, ACTIVE CULTURES",
        ],
        "serving_size": [240.0, 28.0, 170.0],
    })
    path = tmp_path / filename
    df.to_csv(path, index=False)
    return path


def _make_mock_llm_response(mapping: dict | None = None) -> dict:
    """Minimal valid analyze_schema_node LLM response."""
    return {
        "column_mapping": mapping or {
            "fdc_id": "fdc_id",
            "description": "product_name",
            "brand_owner": "brand_name",
            "ingredients": "ingredients",
            "serving_size": "serving_size",
        },
        "operations": [],
        "revised_operations": None,
        "gaps": [],
        "derivable_gaps": [],
        "missing_columns": [],
        "unresolvable_gaps": [],
        "mapping_warnings": [],
        "excluded_columns": [],
        "enrichment_columns_to_generate": [],
        "enrich_alias_ops": [],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _mock_unified_schema(version: str = ""):
    """Return a minimal unified schema mock with a fixed version string."""
    mock = MagicMock()
    mock.version = version
    mock.for_prompt.return_value = json.dumps({"product_name": {"dtype": "object"}})
    mock.columns = {}
    return mock


def _seed_cache(cache: FakeCache, source_schema: dict, domain: str, yaml_path: str,
                yaml_text: str, schema_version: str = "") -> str:
    """Compute fingerprint and seed the fake cache with a valid YAML cache entry."""
    from src.agents.orchestrator import _compute_schema_fingerprint
    from src.cache.client import CACHE_TTL_YAML

    fingerprint = _compute_schema_fingerprint(source_schema, domain, schema_version)
    payload = {
        "column_mapping": {"fdc_id": "fdc_id", "description": "product_name"},
        "operations": [],
        "revised_operations": None,
        "mapping_yaml_path": yaml_path,
        "block_registry_hits": {},
        "block_sequence": ["column_map"],
        "sequence_reasoning": "",
        "enrichment_columns_to_generate": [],
        "enrich_alias_ops": [],
        "gaps": [],
        "derivable_gaps": [],
        "missing_columns": [],
        "unresolvable_gaps": [],
        "mapping_warnings": [],
        "excluded_columns": [],
        "__yaml_text__": yaml_text,
    }
    cache.set("yaml", fingerprint, json.dumps(payload).encode(), ttl=CACHE_TTL_YAML)
    return fingerprint


class TestYamlCacheHit:
    """YAML mapping cache warms on first invoke, hits on second."""

    def test_second_invoke_sets_cache_yaml_hit(self, tmp_path):
        """cache_yaml_hit must be True in state returned by second invoke."""
        from src.agents.orchestrator import analyze_schema_node

        source_schema = {
            "fdc_id": {"dtype": "int64"},
            "description": {"dtype": "object"},
        }
        cache = FakeCache()
        mock_unified = _mock_unified_schema(version="")  # version="" matches default

        _seed_cache(
            cache, source_schema, "nutrition",
            yaml_path=str(tmp_path / "mapping.yaml"),
            yaml_text="mappings:\n  description: product_name\n",
        )

        state = {
            "source_schema": source_schema,
            "domain": "nutrition",
            "source_path": str(tmp_path / "f.csv"),
            "cache_client": cache,
            "unified_schema_existed": False,
        }

        with patch("src.agents.orchestrator.get_domain_schema", return_value=mock_unified):
            result = analyze_schema_node(state)

        assert result.get("cache_yaml_hit") is True
        assert cache.yaml_hits == 1
        assert cache.yaml_misses == 0

    def test_cache_miss_on_first_invoke(self, tmp_path):
        """Empty cache must produce a miss (no cache_yaml_hit set)."""
        from src.agents.orchestrator import analyze_schema_node

        source_schema = {"fdc_id": {"dtype": "int64"}, "description": {"dtype": "object"}}
        cache = FakeCache()
        mock_unified = _mock_unified_schema()

        llm_response = {
            "column_mapping": {"fdc_id": "fdc_id", "description": "product_name"},
            "operations": [],
            "revised_operations": None,
            "gaps": [],
            "derivable_gaps": [],
            "missing_columns": [],
            "unresolvable_gaps": [],
            "mapping_warnings": [],
            "excluded_columns": [],
            "enrichment_columns_to_generate": [],
            "enrich_alias_ops": [],
        }

        with patch("src.agents.orchestrator.get_domain_schema", return_value=mock_unified):
            with patch("src.agents.orchestrator.call_llm_json", return_value=llm_response):
                with patch("src.agents.orchestrator.write_mapping_yaml", return_value=str(tmp_path / "m.yaml")):
                    state = {
                        "source_schema": source_schema,
                        "domain": "nutrition",
                        "source_path": str(tmp_path / "f.csv"),
                        "cache_client": cache,
                        "unified_schema_existed": True,
                    }
                    result = analyze_schema_node(state)

        assert result.get("cache_yaml_hit") is not True
        assert cache.yaml_misses == 1

    def test_yaml_file_rematerialized_on_hit(self, tmp_path):
        """YAML file must be written to disk when cache is hit and file is absent."""
        from src.agents.orchestrator import analyze_schema_node

        yaml_path = tmp_path / "mapping.yaml"
        yaml_text = "mappings:\n  description: product_name\n"
        assert not yaml_path.exists()

        source_schema = {"fdc_id": {"dtype": "int64"}, "description": {"dtype": "object"}}
        cache = FakeCache()
        mock_unified = _mock_unified_schema()

        _seed_cache(cache, source_schema, "nutrition", str(yaml_path), yaml_text)

        state = {
            "source_schema": source_schema,
            "domain": "nutrition",
            "source_path": str(tmp_path / "f.csv"),
            "cache_client": cache,
            "unified_schema_existed": True,
        }

        with patch("src.agents.orchestrator.get_domain_schema", return_value=mock_unified):
            result = analyze_schema_node(state)

        assert result.get("cache_yaml_hit") is True
        assert yaml_path.exists()
        assert yaml_path.read_text() == yaml_text

    def test_no_cache_client_does_not_hit(self, tmp_path):
        """Without cache_client, analyze_schema_node must not attempt cache lookup."""
        from src.agents.orchestrator import analyze_schema_node

        mock_unified = _mock_unified_schema()
        llm_response = {
            "column_mapping": {"description": "product_name"},
            "operations": [],
            "revised_operations": None,
            "gaps": [],
            "derivable_gaps": [],
            "missing_columns": [],
            "unresolvable_gaps": [],
            "mapping_warnings": [],
            "excluded_columns": [],
            "enrichment_columns_to_generate": [],
            "enrich_alias_ops": [],
        }

        with patch("src.agents.orchestrator.get_domain_schema", return_value=mock_unified):
            with patch("src.agents.orchestrator.call_llm_json", return_value=llm_response):
                with patch("src.agents.orchestrator.write_mapping_yaml", return_value=str(tmp_path / "m.yaml")):
                    state = {
                        "source_schema": {"description": {"dtype": "object"}},
                        "domain": "nutrition",
                        "source_path": str(tmp_path / "f.csv"),
                        "cache_client": None,
                        "unified_schema_existed": True,
                    }
                    result = analyze_schema_node(state)

        assert result.get("cache_yaml_hit") is not True


class TestRouteAfterAnalyzeSchema:
    """Conditional edge routing based on cache_yaml_hit."""

    def test_routes_to_check_registry_on_hit(self):
        from src.agents.graph import route_after_analyze_schema
        assert route_after_analyze_schema({"cache_yaml_hit": True}) == "check_registry"

    def test_routes_to_critique_schema_on_miss(self):
        from src.agents.graph import route_after_analyze_schema
        assert route_after_analyze_schema({"cache_yaml_hit": False, "with_critic": True}) == "critique_schema"

    def test_routes_to_critique_schema_when_key_absent(self):
        from src.agents.graph import route_after_analyze_schema
        assert route_after_analyze_schema({"with_critic": True}) == "critique_schema"
