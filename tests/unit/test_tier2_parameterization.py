"""Tests confirming Tier 2 parameterization works for non-food domains (T009)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest

HEALTHCARE_YAML = textwrap.dedent("""\
    domain: healthcare_test

    text_columns: [diagnosis_text, medications]

    fields:
      - name: icd10_codes
        strategy: deterministic
        output_type: multi
        patterns:
          - regex: "\\\\b([A-Z][0-9]{2}(?:\\\\.[0-9A-Z]{1,4})?)\\\\b"
            label: icd10

      - name: diagnosis_category
        strategy: llm
        output_type: single
        classification_classes:
          - Cardiovascular
          - Diabetes
          - Respiratory
          - Other
        rag_context_field: diagnosis_text
""")


@pytest.fixture
def healthcare_domain_dir(tmp_path) -> Path:
    d = tmp_path / "healthcare_test"
    d.mkdir()
    (d / "enrichment_rules.yaml").write_text(HEALTHCARE_YAML)
    return tmp_path


@pytest.fixture(autouse=False)
def patch_dir(healthcare_domain_dir, monkeypatch):
    import src.enrichment.rules_loader as m
    monkeypatch.setattr(m, "DOMAIN_PACKS_DIR", healthcare_domain_dir)


# ---------------------------------------------------------------------------
# get_safety_columns / get_valid_categories via guardrails.py
# ---------------------------------------------------------------------------

def test_get_safety_columns_healthcare(patch_dir):
    from src.agents.guardrails import get_safety_columns
    cols = get_safety_columns("healthcare_test")
    assert "icd10_codes" in cols
    # Must NOT contain food safety fields
    assert "allergens" not in cols
    assert "is_organic" not in cols


def test_get_valid_categories_healthcare(patch_dir):
    from src.agents.guardrails import get_valid_categories
    cats = get_valid_categories("healthcare_test")
    assert "Cardiovascular" in cats
    assert "Diabetes" in cats
    # Must NOT contain food categories
    assert "Breakfast Cereals" not in cats
    assert "Dairy" not in cats


def test_get_safety_columns_fallback_unknown_domain():
    from src.agents.guardrails import SAFETY_COLUMNS, get_safety_columns
    cols = get_safety_columns("totally_unknown_domain_xyz")
    # Falls back to food defaults when domain not found
    assert cols == SAFETY_COLUMNS


def test_get_valid_categories_fallback_unknown_domain():
    from src.agents.guardrails import VALID_CATEGORIES, get_valid_categories
    cats = get_valid_categories("totally_unknown_domain_xyz")
    assert cats == VALID_CATEGORIES


# ---------------------------------------------------------------------------
# deterministic_enrich uses domain text_columns
# ---------------------------------------------------------------------------

def test_deterministic_enrich_uses_domain_text_cols(patch_dir):
    """With healthcare domain, deterministic_enrich should read diagnosis_text col."""
    from src.enrichment.rules_loader import EnrichmentRulesLoader
    from src.enrichment.deterministic import deterministic_enrich

    loader = EnrichmentRulesLoader("healthcare_test")
    rules = loader.s1_fields

    df = pd.DataFrame({
        "diagnosis_text": ["Patient has E11.9 and I10", "Normal checkup"],
        "medications": ["metformin 500mg", "None"],
        "icd10_codes": [pd.NA, pd.NA],
    })
    enrich_cols = ["icd10_codes"]
    needs_enrichment = df["icd10_codes"].isna()

    result_df, _, stats = deterministic_enrich(
        df, enrich_cols, needs_enrichment, rules=rules, domain="healthcare_test"
    )
    # At least the row with ICD codes should have been populated
    assert result_df["icd10_codes"].notna().any()


def test_deterministic_enrich_no_food_cols_referenced(patch_dir, monkeypatch):
    """With healthcare domain, food-only cols (product_name, ingredients) not required."""
    from src.enrichment.rules_loader import EnrichmentRulesLoader
    from src.enrichment.deterministic import deterministic_enrich

    loader = EnrichmentRulesLoader("healthcare_test")
    rules = loader.s1_fields

    # DataFrame has NO product_name or ingredients — healthcare domain should still work
    df = pd.DataFrame({
        "diagnosis_text": ["E11.9 Diabetes Type 2"],
        "icd10_codes": [pd.NA],
    })
    enrich_cols = ["icd10_codes"]
    needs_enrichment = df["icd10_codes"].isna()

    result_df, _, stats = deterministic_enrich(
        df, enrich_cols, needs_enrichment, rules=rules, domain="healthcare_test"
    )
    # Should not raise, and should attempt extraction from diagnosis_text
    assert isinstance(result_df, pd.DataFrame)
