"""Tests for new EnrichmentRulesLoader properties (T008)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from src.enrichment.rules_loader import EnrichmentRulesLoader

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NUTRITION_DOMAIN = "nutrition"

HEALTHCARE_YAML = textwrap.dedent("""\
    domain: healthcare_test

    text_columns: [diagnosis_text, medications, procedures]

    fields:
      - name: icd10_codes
        strategy: deterministic
        output_type: multi
        patterns:
          - regex: "\\\\b([A-Z][0-9]{2}(?:\\\\.[0-9A-Z]{1,4})?)\\\\b"
            label: icd10

      - name: medication_names
        strategy: deterministic
        output_type: multi
        patterns:
          - regex: "\\\\b(metformin|lisinopril|atorvastatin)\\\\b"
            label: common_medication

      - name: diagnosis_category
        strategy: llm
        output_type: single
        classification_classes:
          - Cardiovascular
          - Diabetes
          - Respiratory
          - Neurological
          - Other
        rag_context_field: diagnosis_text
""")


@pytest.fixture
def healthcare_rules_path(tmp_path) -> Path:
    domain_dir = tmp_path / "healthcare_test"
    domain_dir.mkdir()
    rules_file = domain_dir / "enrichment_rules.yaml"
    rules_file.write_text(HEALTHCARE_YAML)
    return tmp_path


@pytest.fixture(autouse=False)
def patch_domain_packs_dir(healthcare_rules_path, monkeypatch):
    """Redirect DOMAIN_PACKS_DIR to tmp_path for healthcare fixture tests."""
    import src.enrichment.rules_loader as m
    monkeypatch.setattr(m, "DOMAIN_PACKS_DIR", healthcare_rules_path)


# ---------------------------------------------------------------------------
# Tests against nutrition domain (real domain_packs/)
# ---------------------------------------------------------------------------

def test_enrichment_column_names_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    names = loader.enrichment_column_names
    assert isinstance(names, list)
    assert len(names) > 0
    assert "allergens" in names
    assert "primary_category" in names


def test_text_columns_fallback_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    cols = loader.text_columns
    assert cols == ["product_name", "ingredients", "category"]


def test_llm_categories_string_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    cats = loader.llm_categories_string
    assert isinstance(cats, str)
    assert "Dairy" in cats
    assert "Snacks" in cats


def test_safety_field_names_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    safety = loader.safety_field_names()
    assert "allergens" in safety
    assert "is_organic" in safety
    assert "dietary_tags" in safety
    assert "primary_category" not in safety


def test_llm_rag_context_field_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    assert loader.llm_rag_context_field == "product_name"


# ---------------------------------------------------------------------------
# Tests against healthcare fixture
# ---------------------------------------------------------------------------

def test_enrichment_column_names_healthcare(patch_domain_packs_dir):
    loader = EnrichmentRulesLoader("healthcare_test")
    names = loader.enrichment_column_names
    assert "icd10_codes" in names
    assert "medication_names" in names
    assert "diagnosis_category" in names


def test_text_columns_explicit_healthcare(patch_domain_packs_dir):
    loader = EnrichmentRulesLoader("healthcare_test")
    assert loader.text_columns == ["diagnosis_text", "medications", "procedures"]


def test_text_columns_fallback_when_key_absent(tmp_path, monkeypatch):
    """When text_columns key is absent, fall back to food defaults."""
    import src.enrichment.rules_loader as m
    monkeypatch.setattr(m, "DOMAIN_PACKS_DIR", tmp_path)
    d = tmp_path / "minimal_domain"
    d.mkdir()
    (d / "enrichment_rules.yaml").write_text(
        "domain: minimal_domain\nfields: []\n"
    )
    loader = EnrichmentRulesLoader("minimal_domain")
    assert loader.text_columns == ["product_name", "ingredients", "category"]


def test_llm_categories_string_healthcare(patch_domain_packs_dir):
    loader = EnrichmentRulesLoader("healthcare_test")
    cats = loader.llm_categories_string
    assert "Cardiovascular" in cats
    assert "Diabetes" in cats
    assert "Allergens" not in cats


def test_safety_field_names_healthcare(patch_domain_packs_dir):
    loader = EnrichmentRulesLoader("healthcare_test")
    safety = loader.safety_field_names()
    assert "icd10_codes" in safety
    assert "medication_names" in safety
    assert "diagnosis_category" not in safety
    # must not contain food safety fields
    assert "allergens" not in safety


def test_llm_categories_string_empty_when_no_llm_fields(tmp_path, monkeypatch):
    import src.enrichment.rules_loader as m
    monkeypatch.setattr(m, "DOMAIN_PACKS_DIR", tmp_path)
    d = tmp_path / "det_only"
    d.mkdir()
    (d / "enrichment_rules.yaml").write_text(
        "domain: det_only\nfields:\n"
        "  - name: flag\n    strategy: deterministic\n"
        "    output_type: boolean\n    patterns: []\n"
    )
    loader = EnrichmentRulesLoader("det_only")
    assert loader.llm_categories_string == ""


def test_load_prompt_examples_nutrition():
    loader = EnrichmentRulesLoader(NUTRITION_DOMAIN)
    examples = loader.load_prompt_examples(NUTRITION_DOMAIN)
    assert isinstance(examples, list)
    assert len(examples) > 0
    assert any(e.get("source_col") for e in examples)


def test_load_prompt_examples_missing_domain_returns_empty(tmp_path, monkeypatch):
    import src.enrichment.rules_loader as m
    monkeypatch.setattr(m, "DOMAIN_PACKS_DIR", tmp_path)
    loader = EnrichmentRulesLoader.__new__(EnrichmentRulesLoader)
    loader.domain = "nonexistent"
    loader.all_fields = []
    loader._raw = {}
    examples = loader.load_prompt_examples("nonexistent")
    assert examples == []
