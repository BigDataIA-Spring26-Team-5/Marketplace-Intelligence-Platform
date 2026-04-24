"""Tests for src/ui/block_scaffolder.py re-exports.

The module now delegates to src/agents/domain_kit_graph.py.
These tests verify the re-export surface is intact.
Full logic tests live in tests/unit/test_domain_kit_graph.py.
"""

from __future__ import annotations


def test_run_scaffold_step_importable():
    from src.ui.block_scaffolder import run_scaffold_step
    assert callable(run_scaffold_step)


def test_scaffold_state_importable():
    from src.ui.block_scaffolder import ScaffoldState
    state = ScaffoldState()
    assert isinstance(state, dict)


def test_run_scaffold_step_unknown_raises():
    from src.ui.block_scaffolder import run_scaffold_step, ScaffoldState
    import pytest
    with pytest.raises(KeyError):
        run_scaffold_step("nonexistent", ScaffoldState())
