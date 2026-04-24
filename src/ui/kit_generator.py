"""Thin adapter — delegates to the agentic DomainKitGraph.

The old single-shot generate_domain_kit() is replaced by the multi-step graph in
src/agents/domain_kit_graph.py. This module re-exports the step runner and state
type so existing callsites in domain_kits.py have a stable import path.
"""

from __future__ import annotations

from src.agents.domain_kit_graph import (  # noqa: F401 — re-exported for callsite convenience
    DomainKitState,
    run_kit_step,
    validate_enrichment_rules_yaml,
)
