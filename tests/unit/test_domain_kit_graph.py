"""Unit tests for DomainKitGraph and ScaffoldGraph node functions.

All LLM calls are mocked via unittest.mock.patch so no API key is required.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.agents.domain_kit_graph import (
    DomainKitState,
    ScaffoldState,
    _analyze_csv_node,
    _commit_to_disk_node,
    _generate_block_sequence_node,
    _generate_enrichment_rules_node,
    _generate_prompt_examples_node,
    _generate_scaffold_node,
    _hitl_review_node,
    _revise_enrichment_rules_node,
    _route_after_validate,
    _route_after_validate_syntax,
    _save_to_custom_blocks_node,
    _validate_enrichment_rules_node,
    _validate_syntax_node,
    run_kit_step,
    run_scaffold_step,
)

# ---------------------------------------------------------------------------
# analyze_csv
# ---------------------------------------------------------------------------


SAMPLE_CSV = "col_a,col_b,col_c\nval1,val2,val3\nval4,val5,val6\n"


def test_analyze_csv_extracts_headers():
    state = DomainKitState(csv_content=SAMPLE_CSV, domain_name="test", description="test")
    result = _analyze_csv_node(state)
    assert result["csv_headers"] == ["col_a", "col_b", "col_c"]


def test_analyze_csv_builds_table():
    state = DomainKitState(csv_content=SAMPLE_CSV, domain_name="test", description="test")
    result = _analyze_csv_node(state)
    assert "col_a" in result["csv_sample_table"]


def test_analyze_csv_empty_content():
    state = DomainKitState(csv_content="", domain_name="test", description="test")
    result = _analyze_csv_node(state)
    assert result["csv_headers"] == []


# ---------------------------------------------------------------------------
# validate_enrichment_rules — retry counter
# ---------------------------------------------------------------------------

_VALID_ER_YAML = yaml.dump({
    "domain": "test",
    "text_columns": ["description"],
    "fields": [{"name": "my_field", "strategy": "deterministic", "patterns": [{"regex": r"\b(foo)\b", "label": "foo"}]}],
})

_INVALID_ER_YAML = "not: valid: yaml: content: [missing bracket"


def test_validate_enrichment_rules_increments_retry_on_error():
    state = DomainKitState(
        domain_name="test",
        csv_headers=["col_a"],
        enrichment_rules_yaml=_INVALID_ER_YAML,
        retry_count=0,
        validation_errors=[],
    )
    result = _validate_enrichment_rules_node(state)
    assert result["retry_count"] == 1
    assert len(result["validation_errors"]) > 0


def test_validate_enrichment_rules_no_increment_on_success():
    state = DomainKitState(
        domain_name="test",
        csv_headers=["col_a"],
        enrichment_rules_yaml=_VALID_ER_YAML,
        retry_count=0,
        validation_errors=[],
    )
    result = _validate_enrichment_rules_node(state)
    assert result["retry_count"] == 0
    assert result["validation_errors"] == []


def test_validate_enrichment_rules_extracts_field_names():
    state = DomainKitState(
        domain_name="test",
        csv_headers=[],
        enrichment_rules_yaml=_VALID_ER_YAML,
        retry_count=0,
        validation_errors=[],
    )
    result = _validate_enrichment_rules_node(state)
    assert "my_field" in result["enrichment_fields"]


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_route_after_validate_goes_to_revise_when_errors_and_retry_lt_2():
    state = DomainKitState(validation_errors=["some error"], retry_count=1)
    assert _route_after_validate(state) == "revise_enrichment_rules"


def test_route_after_validate_goes_to_prompt_examples_when_no_errors():
    state = DomainKitState(validation_errors=[], retry_count=0)
    assert _route_after_validate(state) == "generate_prompt_examples"


def test_route_after_validate_goes_to_prompt_examples_when_retry_exhausted():
    state = DomainKitState(validation_errors=["still broken"], retry_count=2)
    assert _route_after_validate(state) == "generate_prompt_examples"


def test_route_after_validate_syntax_retries_when_invalid():
    state = ScaffoldState(syntax_valid=False, retry_count=0)
    assert _route_after_validate_syntax(state) == "fix_scaffold"


def test_route_after_validate_syntax_goes_to_hitl_when_valid():
    state = ScaffoldState(syntax_valid=True, retry_count=0)
    assert _route_after_validate_syntax(state) == "hitl_review"


def test_route_after_validate_syntax_goes_to_hitl_when_retries_exhausted():
    state = ScaffoldState(syntax_valid=False, retry_count=2)
    assert _route_after_validate_syntax(state) == "hitl_review"


# ---------------------------------------------------------------------------
# hitl_review — detects existing files
# ---------------------------------------------------------------------------


def test_hitl_review_sets_pending_review(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    state = DomainKitState(domain_name="newdomain")
    result = _hitl_review_node(state)
    assert result["pending_review"] is True
    assert result["existing_files"] == {}


def test_hitl_review_captures_existing_files(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    domain_dir = tmp_path / "mydomain"
    domain_dir.mkdir()
    (domain_dir / "enrichment_rules.yaml").write_text("domain: mydomain\n")

    state = DomainKitState(domain_name="mydomain")
    result = _hitl_review_node(state)
    assert "enrichment_rules.yaml" in result["existing_files"]
    assert "mydomain" in result["existing_files"]["enrichment_rules.yaml"]


# ---------------------------------------------------------------------------
# commit_to_disk — .bak files and audit log
# ---------------------------------------------------------------------------


def test_commit_to_disk_writes_files(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    state = DomainKitState(
        domain_name="testdomain",
        enrichment_rules_yaml="domain: testdomain\nfields: []\n",
        prompt_examples_yaml="domain: testdomain\ncolumn_mapping_examples: []\n",
        block_sequence_yaml="domain: testdomain\nsequence: [dq_score_pre]\n",
        existing_files={},
        user_edits={},
    )
    result = _commit_to_disk_node(state)
    assert result["committed"] is True
    assert (tmp_path / "testdomain" / "enrichment_rules.yaml").exists()


def test_commit_to_disk_writes_bak_for_existing(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    domain_dir = tmp_path / "testdomain"
    domain_dir.mkdir()
    old_content = "domain: testdomain\nfields: []\n"
    (domain_dir / "enrichment_rules.yaml").write_text(old_content)

    state = DomainKitState(
        domain_name="testdomain",
        enrichment_rules_yaml="domain: testdomain\nfields: [{name: new_field}]\n",
        prompt_examples_yaml="domain: testdomain\ncolumn_mapping_examples: []\n",
        block_sequence_yaml="domain: testdomain\nsequence: [dq_score_pre]\n",
        existing_files={"enrichment_rules.yaml": old_content},
        user_edits={},
    )
    result = _commit_to_disk_node(state)
    assert result["committed"] is True
    assert (domain_dir / "enrichment_rules.yaml.bak").exists()
    assert (domain_dir / "enrichment_rules.yaml.bak").read_text() == old_content


def test_commit_to_disk_uses_user_edits(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    state = DomainKitState(
        domain_name="editdomain",
        enrichment_rules_yaml="original: content\n",
        prompt_examples_yaml="orig: prompt\n",
        block_sequence_yaml="orig: sequence\n",
        existing_files={},
        user_edits={
            "enrichment_rules.yaml": "edited: content\n",
            "prompt_examples.yaml": "edited: prompt\n",
            "block_sequence.yaml": "edited: sequence\n",
        },
    )
    result = _commit_to_disk_node(state)
    assert result["committed"] is True
    written = (tmp_path / "editdomain" / "enrichment_rules.yaml").read_text()
    assert written == "edited: content\n"


def test_commit_to_disk_appends_audit_log(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    state = DomainKitState(
        domain_name="auditdomain",
        enrichment_rules_yaml="domain: auditdomain\n",
        prompt_examples_yaml="domain: auditdomain\n",
        block_sequence_yaml="domain: auditdomain\n",
        existing_files={},
        user_edits={},
    )
    _commit_to_disk_node(state)
    audit_file = tmp_path / "auditdomain" / ".audit.jsonl"
    assert audit_file.exists()
    import json
    entry = json.loads(audit_file.read_text().strip())
    assert entry["action"] == "generate"


# ---------------------------------------------------------------------------
# LLM-calling nodes — mocked
# ---------------------------------------------------------------------------


_MOCK_ER_YAML = "domain: test\ntext_columns: [desc]\nfields:\n  - name: drug_type\n    strategy: deterministic\n    patterns: []\n"
_MOCK_PE_YAML = "domain: test\ncolumn_mapping_examples:\n  - source_col: name\n    target_col: product_name\n    operation: RENAME\n"
_MOCK_BS_YAML = "domain: test\nsequence:\n  - dq_score_pre\n  - __generated__\n  - llm_enrich\n  - dq_score_post\n"


@patch("src.agents.domain_kit_graph.call_llm_json")
def test_generate_enrichment_rules_calls_llm(mock_llm):
    mock_llm.return_value = {"yaml": _MOCK_ER_YAML}
    state = DomainKitState(
        domain_name="test",
        description="test domain",
        csv_headers=["col_a"],
        csv_sample_table="| col_a |\n| --- |",
        validation_errors=[],
        retry_count=0,
    )
    result = _generate_enrichment_rules_node(state)
    assert (result.get("enrichment_rules_yaml") or "").strip() == _MOCK_ER_YAML.strip()
    mock_llm.assert_called_once()


@patch("src.agents.domain_kit_graph.call_llm_json")
def test_revise_enrichment_rules_uses_fix_prompt(mock_llm):
    mock_llm.return_value = {"yaml": _MOCK_ER_YAML}
    state = DomainKitState(
        domain_name="test",
        description="test domain",
        csv_headers=["col_a"],
        enrichment_rules_yaml="broken: yaml",
        validation_errors=["Missing 'domain' key"],
        retry_count=1,
    )
    result = _revise_enrichment_rules_node(state)
    assert (result.get("enrichment_rules_yaml") or "").strip() == _MOCK_ER_YAML.strip()
    call_args = mock_llm.call_args[1]["messages"][0]["content"]
    assert "Missing 'domain' key" in call_args or mock_llm.called


@patch("src.agents.domain_kit_graph.call_llm_json")
def test_generate_prompt_examples_receives_enrichment_fields(mock_llm):
    mock_llm.return_value = {"yaml": _MOCK_PE_YAML}
    state = DomainKitState(
        domain_name="test",
        description="test",
        csv_headers=["col_a"],
        csv_sample_table="table",
        enrichment_fields=["drug_type"],
    )
    result = _generate_prompt_examples_node(state)
    assert (result.get("prompt_examples_yaml") or "").strip() == _MOCK_PE_YAML.strip()
    # Prompt must mention enrichment fields to prevent phantom mappings
    prompt_content = mock_llm.call_args[1]["messages"][0]["content"]
    assert "drug_type" in prompt_content


@patch("src.agents.domain_kit_graph.call_llm_json")
def test_generate_block_sequence_passes_enrichment_fields(mock_llm):
    mock_llm.return_value = {"yaml": _MOCK_BS_YAML}
    state = DomainKitState(
        domain_name="test",
        description="test",
        enrichment_fields=["drug_type", "category"],
    )
    result = _generate_block_sequence_node(state)
    assert (result.get("block_sequence_yaml") or "").strip() == _MOCK_BS_YAML.strip()
    prompt_content = mock_llm.call_args[1]["messages"][0]["content"]
    assert "drug_type" in prompt_content


# ---------------------------------------------------------------------------
# Scaffold: validate_syntax
# ---------------------------------------------------------------------------


def test_validate_syntax_valid_python():
    source = "class Foo:\n    def bar(self):\n        pass\n"
    state = ScaffoldState(scaffold_source=source, retry_count=0)
    result = _validate_syntax_node(state)
    assert result["syntax_valid"] is True
    assert result["syntax_error"] == ""


def test_validate_syntax_invalid_python():
    source = "class Foo:\n    def bar(self):\n        pass\n    invalid syntax here {\n"
    state = ScaffoldState(scaffold_source=source, retry_count=0)
    result = _validate_syntax_node(state)
    assert result["syntax_valid"] is False
    assert result["syntax_error"] != ""
    assert result["retry_count"] == 1


# ---------------------------------------------------------------------------
# save_to_custom_blocks
# ---------------------------------------------------------------------------


def test_save_to_custom_blocks_writes_file(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "src.agents.domain_kit_graph.DOMAIN_PACKS_DIR", tmp_path
    )
    source = "class ExtractDrugTypeBlock:\n    name = 'pharma__extract_drug_type'\n"
    state = ScaffoldState(domain_name="pharma", scaffold_source=source, user_source="")
    result = _save_to_custom_blocks_node(state)
    assert result["committed"] is True
    files = list((tmp_path / "pharma" / "custom_blocks").iterdir())
    assert len(files) == 1
    assert files[0].suffix == ".py"


# ---------------------------------------------------------------------------
# run_kit_step / run_scaffold_step — unknown step raises
# ---------------------------------------------------------------------------


def test_run_kit_step_unknown_raises():
    with pytest.raises(KeyError, match="Unknown kit step"):
        run_kit_step("nonexistent_step", DomainKitState())


def test_run_scaffold_step_unknown_raises():
    with pytest.raises(KeyError, match="Unknown scaffold step"):
        run_scaffold_step("nonexistent_step", ScaffoldState())
