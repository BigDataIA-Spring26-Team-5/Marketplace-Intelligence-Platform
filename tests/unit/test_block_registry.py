"""Unit tests for BlockRegistry domain pack loading (T017, T035)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from src.registry.block_registry import (
    FALLBACK_SEQUENCE,
    BlockNotFoundError,
    BlockRegistry,
)


def _reset_registry():
    """Force BlockRegistry to re-initialise on next .instance() call."""
    BlockRegistry._instance = None


# ── helpers ──────────────────────────────────────────────────────────────────


def _write_sequence_yaml(domain_packs_root: Path, domain: str, content: str) -> Path:
    pack_dir = domain_packs_root / domain
    pack_dir.mkdir(parents=True, exist_ok=True)
    p = pack_dir / "block_sequence.yaml"
    p.write_text(textwrap.dedent(content))
    return p


# ── domain sequence loading ───────────────────────────────────────────────────


class TestDomainPackSequenceLoad:
    def test_nutrition_sequence_loads(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("nutrition", {})
        assert isinstance(seq, list)
        assert "__generated__" in seq
        assert "dq_score_pre" in seq
        assert "dq_score_post" in seq

    def test_nutrition_custom_blocks_present(self):
        """T035: nutrition custom blocks are discovered and namespaced correctly."""
        _reset_registry()
        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("nutrition", {})
        assert "nutrition__extract_allergens" in seq
        assert "nutrition__extract_quantity_column" in seq

    def test_nutrition_silver_sequence(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        silver = reg.get_silver_sequence("nutrition")
        assert "schema_enforce" in silver
        assert "nutrition__extract_allergens" not in silver

    def test_nutrition_gold_sequence(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        gold = reg.get_gold_sequence("nutrition")
        assert "dq_score_post" in gold

    def test_missing_domain_returns_fallback(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("nonexistent_domain_xyz", {})
        assert seq == FALLBACK_SEQUENCE

    def test_pricing_sequence_loads(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("pricing", {})
        assert "keep_quantity_in_name" in seq

    def test_retail_inventory_sequence_loads(self):
        _reset_registry()
        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("retail_inventory", {})
        assert isinstance(seq, list)
        assert len(seq) > 0


# ── custom block discovery via importlib ─────────────────────────────────────


class TestCustomBlockDiscovery:
    def test_nutrition_custom_blocks_registered(self):
        """T035: nutrition__* blocks appear in registry after init."""
        _reset_registry()
        reg = BlockRegistry.instance()
        all_blocks = list(reg.blocks.keys())
        assert "nutrition__extract_allergens" in all_blocks
        assert "nutrition__extract_quantity_column" in all_blocks

    def test_custom_block_executable(self):
        """Custom block can be retrieved and has a run() method."""
        _reset_registry()
        reg = BlockRegistry.instance()
        block = reg.blocks.get("nutrition__extract_allergens")
        assert block is not None
        assert hasattr(block, "run")

    def test_src_blocks_deleted(self):
        """Original food-specific src/blocks files must be gone."""
        import importlib.util

        assert importlib.util.find_spec("src.blocks.extract_allergens") is None
        assert importlib.util.find_spec("src.blocks.extract_quantity_column") is None


# ── BlockNotFoundError ────────────────────────────────────────────────────────


class TestBlockNotFoundError:
    def test_unknown_block_in_yaml_raises(self, tmp_path, monkeypatch):
        """A block_sequence.yaml referencing an unknown block raises BlockNotFoundError."""
        import src.registry.block_registry as mod

        fake_packs = tmp_path / "domain_packs"
        _write_sequence_yaml(fake_packs, "bad_domain", """
            domain: bad_domain
            sequence:
              - dq_score_pre
              - this_block_does_not_exist
              - dq_score_post
        """)
        monkeypatch.setattr(mod, "DOMAIN_PACKS_DIR", fake_packs)
        _reset_registry()

        reg = BlockRegistry.instance()
        with pytest.raises(BlockNotFoundError):
            reg.get_default_sequence("bad_domain", {})

        _reset_registry()

    def test_valid_domain_pack_no_error(self, tmp_path, monkeypatch):
        import src.registry.block_registry as mod

        fake_packs = tmp_path / "domain_packs"
        _write_sequence_yaml(fake_packs, "ok_domain", """
            domain: ok_domain
            sequence:
              - dq_score_pre
              - __generated__
              - strip_whitespace
              - dq_score_post
        """)
        monkeypatch.setattr(mod, "DOMAIN_PACKS_DIR", fake_packs)
        _reset_registry()

        reg = BlockRegistry.instance()
        seq = reg.get_default_sequence("ok_domain", {})
        assert "strip_whitespace" in seq

        _reset_registry()
