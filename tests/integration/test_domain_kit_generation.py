"""Integration tests for the Agentic Domain Kit Builder (T032).

Runs the full DomainKitGraph against 4 fixture CSVs with mocked HITL and LLM calls.
Asserts that generated YAML passes validate_enrichment_rules_yaml() with zero errors.

These tests DO NOT hit a real LLM — call_llm_json is patched with domain-appropriate
synthetic responses so we can verify graph wiring and validator integration end-to-end.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from src.agents.domain_kit_graph import (
    DomainKitState,
    build_kit_graph,
    run_kit_step,
    validate_enrichment_rules_yaml,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

# ---------------------------------------------------------------------------
# Synthetic LLM response factory
# ---------------------------------------------------------------------------

def _make_enrichment_rules_yaml(domain: str, extra_fields: list[str] | None = None) -> str:
    """Return a minimal valid enrichment_rules YAML for a given domain."""
    fields = extra_fields or []
    field_blocks = ""
    for f in fields:
        field_blocks += textwrap.dedent(f"""\
          - name: {f}
            strategy: s1_extract
            output_type: single
            patterns:
              - regex: "\\\\b({f})\\\\b"
                label: true
        """)
    return textwrap.dedent(f"""\
        __generated__: true
        domain: {domain}
        version: "1.0"
        fields:
        {field_blocks or "  []"}
    """).strip()


def _make_prompt_examples_yaml(domain: str) -> str:
    return textwrap.dedent(f"""\
        __generated__: true
        domain: {domain}
        examples:
          - input_column: raw_name
            output_column: product_name
            operation: RENAME
    """).strip()


def _make_block_sequence_yaml(domain: str) -> str:
    return textwrap.dedent(f"""\
        __generated__: true
        sequence:
          - dq_score_pre
          - __generated__
          - dq_score_post
    """).strip()


def _llm_side_effect_factory(domain: str, extra_enrichment_fields: list[str] | None = None):
    """Return a side_effect function for call_llm_json that yields domain-appropriate responses."""
    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        n = call_count["n"]
        # First call: enrichment rules
        if n == 1:
            return {"yaml": _make_enrichment_rules_yaml(domain, extra_enrichment_fields)}
        # Second call: prompt examples
        if n == 2:
            return {"yaml": _make_prompt_examples_yaml(domain)}
        # Third call: block sequence
        if n == 3:
            return {"yaml": _make_block_sequence_yaml(domain)}
        # Fallback (should not be reached in happy path)
        return {"yaml": ""}

    return side_effect


# ---------------------------------------------------------------------------
# Parametrized fixtures
# ---------------------------------------------------------------------------

FIXTURE_PARAMS = [
    pytest.param(
        "pharma",
        "pharma_sample.csv",
        "Pharmaceutical drug catalog with dosage and approval data",
        [],
        id="pharma",
    ),
    pytest.param(
        "nutrition",
        "nutrition_sample.csv",
        "Packaged food products with ingredients and categories",
        [],
        id="nutrition",
    ),
    pytest.param(
        "fda_recalls",
        "fda_recalls_sample.csv",
        "FDA food recall records with classification and distribution",
        [],
        id="fda_recalls",
    ),
    pytest.param(
        "healthcare",
        "healthcare_sample.csv",
        "Patient discharge records with diagnoses and medications",
        [],
        id="healthcare",
    ),
]


@pytest.mark.parametrize("domain,csv_filename,description,extra_fields", FIXTURE_PARAMS)
def test_full_kit_graph_zero_errors(
    domain: str,
    csv_filename: str,
    description: str,
    extra_fields: list[str],
    tmp_path,
):
    """Full DomainKitGraph run produces YAML that passes validate_enrichment_rules_yaml with zero errors."""
    csv_content = (FIXTURES_DIR / csv_filename).read_text()

    state: DomainKitState = DomainKitState(
        domain_name=domain,
        description=description,
        csv_content=csv_content,
    )

    side_effect = _llm_side_effect_factory(domain, extra_fields)

    with patch("src.agents.domain_kit_graph.call_llm_json", side_effect=side_effect):
        # Step through graph up to hitl_review (stop before commit_to_disk)
        state = run_kit_step("analyze_csv", state)
        assert "csv_headers" in state, "analyze_csv must populate csv_headers"
        assert len(state["csv_headers"]) > 0

        state = run_kit_step("generate_enrichment_rules", state)
        assert "enrichment_rules_yaml" in state

        state = run_kit_step("validate_enrichment_rules", state)
        assert "enrichment_fields" in state

        # With mock returning valid YAML on first call, retry_count should be 0
        # and no validation errors should be present (mock includes __generated__)
        # Skip revise loop — mock produces no errors

        state = run_kit_step("generate_prompt_examples", state)
        assert "prompt_examples_yaml" in state

        state = run_kit_step("generate_block_sequence", state)
        assert "block_sequence_yaml" in state

    # Now validate the generated enrichment rules with the block sequence
    er_dict = yaml.safe_load(state["enrichment_rules_yaml"])
    bs_dict = yaml.safe_load(state["block_sequence_yaml"])
    csv_headers = state["csv_headers"]

    issues = validate_enrichment_rules_yaml(
        er_dict,
        csv_headers,
        block_sequence_dict=bs_dict,
        domain_dir=None,  # no custom blocks dir in test
    )

    errors = [i for i in issues if i["level"] == "error"]
    assert errors == [], (
        f"validate_enrichment_rules_yaml() returned errors for {domain}:\n"
        + "\n".join(f"  [{i['check']}] {i['message']}" for i in errors)
    )


@pytest.mark.parametrize("domain,csv_filename,description,extra_fields", FIXTURE_PARAMS)
def test_analyze_csv_extracts_headers(domain: str, csv_filename: str, description: str, extra_fields: list[str]):
    """analyze_csv node correctly extracts headers from all fixture CSVs."""
    csv_content = (FIXTURES_DIR / csv_filename).read_text()

    state: DomainKitState = DomainKitState(
        domain_name=domain,
        description=description,
        csv_content=csv_content,
    )

    state = run_kit_step("analyze_csv", state)

    assert "csv_headers" in state
    assert len(state["csv_headers"]) > 0
    assert "csv_sample_table" in state
    # Sample table should be markdown with | separators
    assert "|" in state["csv_sample_table"]


@pytest.mark.parametrize("domain,csv_filename,description,extra_fields", FIXTURE_PARAMS)
def test_validate_enrichment_rules_no_false_positives(
    domain: str,
    csv_filename: str,
    description: str,
    extra_fields: list[str],
    tmp_path,
):
    """validate_enrichment_rules_yaml() on a clean mock pack produces zero errors."""
    csv_content = (FIXTURES_DIR / csv_filename).read_text()

    state: DomainKitState = DomainKitState(
        domain_name=domain,
        description=description,
        csv_content=csv_content,
    )

    state = run_kit_step("analyze_csv", state)
    csv_headers = state["csv_headers"]

    er_dict = yaml.safe_load(_make_enrichment_rules_yaml(domain))
    bs_dict = yaml.safe_load(_make_block_sequence_yaml(domain))

    issues = validate_enrichment_rules_yaml(
        er_dict,
        csv_headers,
        block_sequence_dict=bs_dict,
        domain_dir=None,
    )

    errors = [i for i in issues if i["level"] == "error"]
    assert errors == [], (
        f"False positive errors for {domain}: "
        + ", ".join(i["check"] for i in errors)
    )
