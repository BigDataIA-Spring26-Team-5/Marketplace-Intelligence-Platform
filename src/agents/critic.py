"""Agent 2 — Gap Analysis Critic: validates and corrects Agent 1's operations list."""

from __future__ import annotations

import json
import logging

from src.agents.state import PipelineState
from src.agents.prompts import CRITIC_PROMPT
from src.models.llm import call_llm_json, get_critic_llm
from src.schema.analyzer import get_unified_schema

logger = logging.getLogger(__name__)


def critique_schema_node(state: PipelineState) -> dict:
    """
    Agent 2: Critic node that audits and corrects Agent 1's schema analysis.

    Invoked only during new schema ingestion — when Agent 1 produces a fresh
    operations list. Uses a more capable reasoning model to catch errors,
    omissions, and poor classifications.

    Reads from state:
        - operations: Agent 1's raw operations list
        - source_schema: full profile_dataframe() output
        - unified_schema: target schema

    Writes to state:
        - revised_operations: corrected operations list
        - critique_notes: audit trail of corrections made
    """
    # Guard: skip if already ran (prevents double-execution on Streamlit rerenders)
    if state.get("revised_operations") is not None:
        logger.info("Agent 2 already ran — skipping")
        return {}

    operations = state.get("operations", [])
    if not operations:
        logger.info("No operations from Agent 1 — skipping critique")
        return {}

    source_schema = state.get("source_schema", {})
    column_mapping = state.get("column_mapping", {})

    meta_block = source_schema.get("__meta__", {})
    columns_only = {k: v for k, v in source_schema.items() if k != "__meta__"}

    unified_for_prompt = get_unified_schema().for_prompt()

    model = get_critic_llm()
    logger.info(f"Agent 2 critique using model: {model}")

    result = call_llm_json(
        model=model,
        messages=[
            {
                "role": "user",
                "content": CRITIC_PROMPT.format(
                    source_profile=json.dumps(columns_only, indent=2),
                    source_meta=json.dumps(meta_block, indent=2),
                    unified_schema=json.dumps(unified_for_prompt, indent=2),
                    column_mapping=json.dumps(column_mapping, indent=2),
                    operations=json.dumps(operations, indent=2),
                ),
            }
        ],
    )

    revised_operations = result.get("revised_operations", operations)
    critique_notes = result.get("critique_notes", [])

    if critique_notes:
        logger.info(f"Agent 2 made {len(critique_notes)} correction(s)")
        for note in critique_notes:
            rule = note.get("rule", "?")
            column = note.get("column") or note.get("target_column", "?")
            correction = note.get("correction", "")
            logger.info(f"  [{rule}] {column}: {correction}")
    else:
        logger.info("Agent 2: no corrections needed")

    return {
        "revised_operations": revised_operations,
        "critique_notes": critique_notes,
    }
