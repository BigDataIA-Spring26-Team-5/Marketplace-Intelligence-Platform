"""Unit tests for BlockRegistry — singleton, sequences, discovery."""

from __future__ import annotations

import pandas as pd
import pytest

from src.registry.block_registry import BlockRegistry
from src.schema.models import UnifiedSchema, ColumnSpec


@pytest.fixture(autouse=True)
def reset_registry():
    BlockRegistry.reset()
    yield
    BlockRegistry.reset()


class TestSingleton:
    def test_same_instance(self):
        a = BlockRegistry.instance()
        b = BlockRegistry.instance()
        assert a is b

    def test_reset(self):
        a = BlockRegistry.instance()
        BlockRegistry.reset()
        b = BlockRegistry.instance()
        assert a is not b


class TestGetAndList:
    def test_get_known_block(self):
        r = BlockRegistry.instance()
        block = r.get("strip_whitespace")
        assert block.name == "strip_whitespace"

    def test_get_unknown_raises(self):
        r = BlockRegistry.instance()
        with pytest.raises(KeyError):
            r.get("nonexistent_block")

    def test_list_blocks_all(self):
        r = BlockRegistry.instance()
        names = r.list_blocks()
        assert "strip_whitespace" in names
        assert "llm_enrich" in names

    def test_list_blocks_filtered_by_domain(self):
        r = BlockRegistry.instance()
        pricing = r.list_blocks(domain="pricing")
        assert "keep_quantity_in_name" in pricing


class TestStages:
    def test_is_stage(self):
        r = BlockRegistry.instance()
        assert r.is_stage("dedup_stage") is True
        assert r.is_stage("strip_whitespace") is False

    def test_expand_stage(self):
        r = BlockRegistry.instance()
        blocks = r.expand_stage("dedup_stage")
        assert "fuzzy_deduplicate" in blocks
        assert "golden_record_select" in blocks

    def test_expand_unknown_returns_self(self):
        r = BlockRegistry.instance()
        assert r.expand_stage("foo") == ["foo"]


class TestDefaultSequence:
    def test_default_sequence_nutrition(self):
        r = BlockRegistry.instance()
        schema = UnifiedSchema(columns={
            "product_name": ColumnSpec(type="string"),
            "allergens": ColumnSpec(type="string", enrichment=True),
        })
        seq = r.get_default_sequence(domain="nutrition", unified_schema=schema)
        assert "dq_score_pre" in seq
        assert "__generated__" in seq
        assert "dedup_stage" in seq
        assert "enrich_stage" in seq
        assert seq[-1] == "dq_score_post"

    def test_default_sequence_pricing_has_keep_quantity(self):
        r = BlockRegistry.instance()
        schema = UnifiedSchema(columns={"x": ColumnSpec(type="string")})
        seq = r.get_default_sequence(domain="pricing", unified_schema=schema)
        assert "keep_quantity_in_name" in seq
        assert "extract_quantity_column" not in seq

    def test_default_sequence_no_enrichment(self):
        r = BlockRegistry.instance()
        schema = UnifiedSchema(columns={"x": ColumnSpec(type="string")})
        seq = r.get_default_sequence(domain="nutrition", unified_schema=schema, enable_enrichment=False)
        assert "enrich_stage" not in seq


class TestSilverSequence:
    def test_silver_has_schema_enforce(self):
        r = BlockRegistry.instance()
        seq = r.get_silver_sequence()
        assert "schema_enforce" in seq
        assert "dedup_stage" not in seq

    def test_silver_pricing(self):
        r = BlockRegistry.instance()
        seq = r.get_silver_sequence(domain="pricing")
        assert "keep_quantity_in_name" in seq


class TestGoldSequence:
    def test_gold_nutrition(self):
        r = BlockRegistry.instance()
        seq = r.get_gold_sequence(domain="nutrition")
        assert seq[0] == "dq_score_pre"
        assert "dedup_stage" in seq
        assert "enrich_stage" in seq
        assert seq[-1] == "dq_score_post"

    def test_gold_pricing_no_enrich(self):
        r = BlockRegistry.instance()
        seq = r.get_gold_sequence(domain="pricing")
        assert "enrich_stage" not in seq


class TestMetadata:
    def test_generated_sentinel_metadata(self):
        r = BlockRegistry.instance()
        md = r.get_blocks_with_metadata(["__generated__"])
        assert md[0]["name"] == "__generated__"

    def test_stage_expands(self):
        r = BlockRegistry.instance()
        md = r.get_blocks_with_metadata(["dedup_stage"])
        names = [m["name"] for m in md]
        assert "fuzzy_deduplicate" in names

    def test_regular_block(self):
        r = BlockRegistry.instance()
        md = r.get_blocks_with_metadata(["strip_whitespace"])
        assert md[0]["name"] == "strip_whitespace"
        assert "description" in md[0]

    def test_unknown_block_skipped(self):
        r = BlockRegistry.instance()
        md = r.get_blocks_with_metadata(["nonexistent"])
        assert md == []


class TestRegisterBlock:
    def test_register_custom(self):
        from src.blocks.base import Block

        class Dummy(Block):
            name = "dummy_test"
            def run(self, df, config=None):
                return df

        r = BlockRegistry.instance()
        r.register_block(Dummy())
        assert r.get("dummy_test").name == "dummy_test"

    def test_refresh(self):
        r = BlockRegistry.instance()
        r.refresh()  # should not error
