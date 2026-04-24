"""Unit tests for validate_enrichment_rules_yaml() — all 5 deterministic checks."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.agents.domain_kit_graph import ValidationIssue, validate_enrichment_rules_yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _er(fields: list[dict] | None = None, domain: str = "test") -> dict:
    """Minimal valid enrichment_rules dict."""
    return {
        "domain": domain,
        "text_columns": ["description"],
        "fields": fields or [],
    }


def _bs(sequence: list[str] | None = None, domain: str = "test") -> dict:
    """Minimal valid block_sequence dict."""
    if sequence is None:
        sequence = ["dq_score_pre", "__generated__", "llm_enrich", "dq_score_post"]
    return {"domain": domain, "sequence": sequence}


# ---------------------------------------------------------------------------
# Check 4: enrichment field name matches CSV header → warning
# ---------------------------------------------------------------------------


def test_check4_enrichment_field_matches_csv_header():
    er = _er(fields=[{"name": "drug_class", "strategy": "deterministic", "patterns": []}])
    csv_headers = ["ndc_code", "drug_class", "dosage_form"]
    issues = validate_enrichment_rules_yaml(er, csv_headers)
    warnings = [i for i in issues if i["level"] == "warning" and "matches_csv_header" in i["check"]]
    assert len(warnings) == 1
    assert "drug_class" in warnings[0]["message"]


def test_check4_no_match_is_clean():
    er = _er(fields=[{"name": "active_ingredients", "strategy": "deterministic", "patterns": []}])
    csv_headers = ["ndc_code", "drug_name", "dosage_form"]
    issues = validate_enrichment_rules_yaml(er, csv_headers)
    header_warnings = [i for i in issues if "matches_csv_header" in i["check"]]
    assert header_warnings == []


def test_check4_case_insensitive():
    er = _er(fields=[{"name": "DrugClass", "strategy": "deterministic", "patterns": []}])
    csv_headers = ["drugclass", "ndc_code"]
    issues = validate_enrichment_rules_yaml(er, csv_headers)
    warnings = [i for i in issues if "matches_csv_header" in i["check"]]
    assert len(warnings) == 1


# ---------------------------------------------------------------------------
# Check 1: __generated__ sentinel absent → error
# ---------------------------------------------------------------------------


def test_check1_missing_generated_sentinel():
    er = _er()
    bs = _bs(sequence=["dq_score_pre", "llm_enrich", "dq_score_post"])  # no __generated__
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    errors = [i for i in issues if i["level"] == "error" and "sentinel" in i["check"]]
    assert len(errors) == 1


def test_check1_sentinel_present_is_clean():
    er = _er()
    bs = _bs()  # includes __generated__
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    sentinel_errors = [i for i in issues if "sentinel" in i["check"]]
    assert sentinel_errors == []


# ---------------------------------------------------------------------------
# Check 2: dq_score_pre not first or dq_score_post not last → warning
# ---------------------------------------------------------------------------


def test_check2_dq_score_pre_not_first():
    er = _er()
    bs = _bs(sequence=["__generated__", "dq_score_pre", "llm_enrich", "dq_score_post"])
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    warnings = [i for i in issues if "dq_score_pre_not_first" in i["check"]]
    assert len(warnings) == 1


def test_check2_dq_score_post_not_last():
    er = _er()
    bs = _bs(sequence=["dq_score_pre", "__generated__", "dq_score_post", "llm_enrich"])
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    warnings = [i for i in issues if "dq_score_post_not_last" in i["check"]]
    assert len(warnings) == 1


def test_check2_correct_order_is_clean():
    er = _er()
    bs = _bs()
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    order_warnings = [i for i in issues if "dq_score" in i["check"]]
    assert order_warnings == []


# ---------------------------------------------------------------------------
# Check 3: custom block in sequence has no matching .py file → error
# ---------------------------------------------------------------------------


def test_check3_missing_custom_block_file(tmp_path: Path):
    er = _er()
    bs = _bs(sequence=[
        "dq_score_pre", "__generated__", "pharma__extract_ndc", "llm_enrich", "dq_score_post"
    ])
    domain_dir = tmp_path / "pharma"
    domain_dir.mkdir()
    # custom_blocks/ exists but extract_ndc.py does not
    (domain_dir / "custom_blocks").mkdir()

    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs, domain_dir=domain_dir)
    errors = [i for i in issues if i["level"] == "error" and "missing_custom_block_file" in i["check"]]
    assert len(errors) == 1
    assert "extract_ndc" in errors[0]["message"]


def test_check3_custom_block_file_present(tmp_path: Path):
    er = _er()
    bs = _bs(sequence=[
        "dq_score_pre", "__generated__", "pharma__extract_ndc", "llm_enrich", "dq_score_post"
    ])
    domain_dir = tmp_path / "pharma"
    custom_dir = domain_dir / "custom_blocks"
    custom_dir.mkdir(parents=True)
    (custom_dir / "extract_ndc.py").write_text("# stub")

    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs, domain_dir=domain_dir)
    file_errors = [i for i in issues if "missing_custom_block_file" in i["check"]]
    assert file_errors == []


# ---------------------------------------------------------------------------
# Check 5: enrichment field matches custom block name in sequence → warning
# ---------------------------------------------------------------------------


def test_check5_double_extraction_anti_pattern():
    er = _er(fields=[{"name": "ndc_code", "strategy": "deterministic", "patterns": []}])
    bs = _bs(sequence=[
        "dq_score_pre", "__generated__", "pharma__extract_ndc_code", "llm_enrich", "dq_score_post"
    ])
    issues = validate_enrichment_rules_yaml(er, [], block_sequence_dict=bs)
    warnings = [i for i in issues if "double_extraction" in i["check"]]
    assert len(warnings) >= 1


# ---------------------------------------------------------------------------
# All-valid pack → empty issues
# ---------------------------------------------------------------------------


def test_all_valid_pack_returns_no_issues():
    er = _er(fields=[{"name": "active_ingredients", "strategy": "deterministic", "patterns": []}])
    bs = _bs()
    csv_headers = ["ndc_code", "drug_name", "dosage_form", "manufacturer"]
    issues = validate_enrichment_rules_yaml(
        er, csv_headers, block_sequence_dict=bs, domain_dir=None
    )
    assert issues == []
