"""Tests for src/ui/kit_generator.py re-exports.

The module now delegates to src/agents/domain_kit_graph.py.
These tests verify the re-export surface is intact.
Full logic tests live in tests/unit/test_domain_kit_graph.py.
"""

from __future__ import annotations


def test_run_kit_step_importable():
    from src.ui.kit_generator import run_kit_step
    assert callable(run_kit_step)


def test_domain_kit_state_importable():
    from src.ui.kit_generator import DomainKitState
    # TypedDict — can instantiate with no args
    state = DomainKitState()
    assert isinstance(state, dict)


def test_validate_enrichment_rules_yaml_importable():
    from src.ui.kit_generator import validate_enrichment_rules_yaml
    assert callable(validate_enrichment_rules_yaml)


def test_validate_enrichment_rules_yaml_returns_list():
    from src.ui.kit_generator import validate_enrichment_rules_yaml
    issues = validate_enrichment_rules_yaml({}, [])
    assert isinstance(issues, list)
