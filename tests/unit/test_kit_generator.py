"""Unit tests for kit_generator.py (T015)."""

from __future__ import annotations

import yaml
import pytest
from unittest.mock import patch, MagicMock


MOCK_VALID_ENRICHMENT = """domain: healthcare_test
text_columns: [diagnosis_text, medications]
fields:
  - name: icd10_codes
    strategy: deterministic
    output_type: multi
    patterns:
      - regex: "\\\\b([A-Z][0-9]{2})\\\\b"
        label: icd10
  - name: diagnosis_category
    strategy: llm
    output_type: single
    classification_classes:
      - Cardiovascular
      - Diabetes
      - Other
    rag_context_field: diagnosis_text
"""

MOCK_VALID_PROMPT_EXAMPLES = """domain: healthcare_test
column_mapping_examples:
  - source_col: patient_id
    target_col: data_source
    operation: CAST
    cast_to: string
  - source_col: diagnosis_text
    target_col: description
    operation: RENAME
"""

MOCK_VALID_BLOCK_SEQUENCE = """domain: healthcare_test
sequence:
  - dq_score_pre
  - __generated__
  - strip_whitespace
  - healthcare_test__extract_icd10
  - llm_enrich
  - dq_score_post
"""

MOCK_LLM_RESPONSE = {
    "enrichment_rules": MOCK_VALID_ENRICHMENT,
    "prompt_examples": MOCK_VALID_PROMPT_EXAMPLES,
    "block_sequence": MOCK_VALID_BLOCK_SEQUENCE,
}

SAMPLE_CSV = "patient_id,discharge_date,diagnosis_text,medications\n1,2024-01-01,E11.9 Diabetes,metformin\n2,2024-01-02,I10 Hypertension,lisinopril\n"


@patch("src.ui.kit_generator.call_llm_json", return_value=MOCK_LLM_RESPONSE)
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_generate_returns_all_three_files(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", SAMPLE_CSV)
    assert "enrichment_rules.yaml" in result
    assert "prompt_examples.yaml" in result
    assert "block_sequence.yaml" in result


@patch("src.ui.kit_generator.call_llm_json", return_value=MOCK_LLM_RESPONSE)
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_each_file_passes_yaml_safe_load(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", SAMPLE_CSV)
    for fname, content in result.items():
        assert not content.startswith('{"error"'), f"{fname} has error: {content}"
        parsed = yaml.safe_load(content)
        assert parsed is not None, f"{fname} parsed to None"


@patch("src.ui.kit_generator.call_llm_json", return_value=MOCK_LLM_RESPONSE)
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_block_sequence_contains_generated_sentinel(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", SAMPLE_CSV)
    bs_content = result.get("block_sequence.yaml", "")
    assert "__generated__" in bs_content, "block_sequence.yaml must contain __generated__ sentinel"


@patch("src.ui.kit_generator.call_llm_json", side_effect=Exception("LLM unavailable"))
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_llm_error_returns_partial_dict_no_exception(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", SAMPLE_CSV)
    assert isinstance(result, dict)
    assert "enrichment_rules.yaml" in result
    assert "prompt_examples.yaml" in result
    assert "block_sequence.yaml" in result
    for content in result.values():
        assert '"error"' in content, "Error path should include error key"


@patch("src.ui.kit_generator.call_llm_json", return_value={"enrichment_rules": "domain: x\nfields: []", "prompt_examples": "domain: x\ncolumn_mapping_examples: []", "block_sequence": "INVALID: [yaml: bad"})
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_yaml_parse_error_marks_file_as_error(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", SAMPLE_CSV)
    # block_sequence has invalid YAML
    bs = result.get("block_sequence.yaml", "")
    assert '"error"' in bs
    # other files should be fine
    er = result.get("enrichment_rules.yaml", "")
    assert '"error"' not in er


@patch("src.ui.kit_generator.call_llm_json", return_value=MOCK_LLM_RESPONSE)
@patch("src.ui.kit_generator.get_orchestrator_llm", return_value="mock-model")
def test_csv_parsing_handles_empty_csv(mock_model, mock_llm):
    from src.ui.kit_generator import generate_domain_kit
    result = generate_domain_kit("healthcare_test", "Healthcare domain", "")
    assert isinstance(result, dict)
