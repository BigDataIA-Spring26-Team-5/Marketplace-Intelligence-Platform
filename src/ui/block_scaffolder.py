"""Thin adapter — delegates to the agentic ScaffoldGraph.

The old single-shot generate_block_scaffold() is replaced by the multi-step graph in
src/agents/domain_kit_graph.py. This module re-exports the step runner and state type
so existing callsites in domain_kits.py have a stable import path.
"""

from __future__ import annotations

from src.agents.domain_kit_graph import (  # noqa: F401 — re-exported for callsite convenience
    ScaffoldState,
    run_scaffold_step,
)
