"""LangGraph StateGraph + step-by-step runner for the Streamlit UI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
from langgraph.graph import StateGraph, END

from src.agents.state import PipelineState
from src.agents.orchestrator import (
    load_source_node,
    analyze_schema_node,
    check_registry_node,
)
from src.agents.critic import critique_schema_node
from src.agents.prompts import SEQUENCE_PLANNING_PROMPT
from src.models.llm import call_llm_json, get_orchestrator_llm
from src.registry.block_registry import BlockRegistry
from src.pipeline.runner import PipelineRunner

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"


# ── Pipeline execution nodes ─────────────────────────────────────────


def plan_sequence_node(state: PipelineState) -> dict:
    """
    Agent 3: LLM call to determine optimal block execution order.

    Receives the block pool (determined by domain/enrichment settings) plus
    domain, source schema, and gap/registry context. Returns an ordered sequence.
    Agent 3 can only reorder — it cannot add or remove blocks from the pool.
    """
    if state.get("block_sequence"):
        return {}
    domain = state.get("domain", "nutrition")
    source_schema = state.get("source_schema", {})
    gaps = state.get("gaps", [])
    registry_misses = state.get("registry_misses", [])
    block_registry_hits = state.get("block_registry_hits", {})
    unified_schema = state.get("unified_schema")
    enable_enrichment = state.get("enable_enrichment", True)

    block_reg = BlockRegistry.instance()

    pool = block_reg.get_default_sequence(
        domain=domain,
        unified_schema=unified_schema,
        enable_enrichment=enable_enrichment,
    )
    blocks_metadata = block_reg.get_blocks_with_metadata(pool)

    generated_block_prefixes = (
        "COLUMN_RENAME_",
        "COLUMN_DROP_",
        "FORMAT_TRANSFORM_",
        "DYNAMIC_MAPPING_",
        "DERIVE_",
    )

    gap_summary = {
        "gaps_detected": len(gaps),
        "block_registry_hits": block_registry_hits,
        "misses_requiring_generated_blocks": [
            g["target_column"] for g in registry_misses
        ],
        "generated_block_prefixes": generated_block_prefixes,
    }

    compact_schema = {
        col: {"type": info.get("dtype", "unknown")}
        for col, info in source_schema.items()
    }

    model = get_orchestrator_llm()
    result = call_llm_json(
        model=model,
        messages=[
            {
                "role": "user",
                "content": SEQUENCE_PLANNING_PROMPT.format(
                    domain=domain,
                    source_schema=json.dumps(compact_schema, indent=2),
                    gap_summary=json.dumps(gap_summary, indent=2),
                    blocks_metadata=json.dumps(blocks_metadata, indent=2),
                ),
            }
        ],
    )

    sequence = result.get("block_sequence", pool)
    reasoning = result.get("reasoning", "")

    # Expand stages in pool so the missing-check uses individual block names,
    # matching what Agent 3 received and returned (get_blocks_with_metadata expands stages).
    expanded_pool = []
    for item in pool:
        if block_reg.is_stage(item):
            expanded_pool.extend(block_reg.expand_stage(item))
        else:
            expanded_pool.append(item)

    missing = [b for b in expanded_pool if b not in sequence]
    if missing:
        logger.warning(
            f"Agent 3 omitted blocks {missing} — appending at end before dq_score_post"
        )
        if "dq_score_post" in sequence:
            idx = sequence.index("dq_score_post")
            for b in missing:
                sequence.insert(idx, b)
        else:
            sequence.extend(missing)

    logger.info(f"Agent 3 planned sequence ({len(sequence)} blocks): {sequence}")
    if reasoning:
        logger.info(f"Agent 3 reasoning: {reasoning}")

    return {
        "block_sequence": sequence,
        "sequence_reasoning": reasoning,
    }


def run_pipeline_node(state: PipelineState) -> dict:
    """Execute the block sequence on the working DataFrame."""
    if state.get("working_df") is not None:
        return {}
    block_registry = BlockRegistry.instance()
    runner = PipelineRunner(block_registry)

    domain = state.get("domain", "nutrition")
    block_sequence = state.get("block_sequence") or block_registry.get_default_sequence(
        domain=domain,
        unified_schema=state.get("unified_schema"),
        enable_enrichment=state.get("enable_enrichment", True),
    )

    config = {
        "dq_weights": (state.get("unified_schema") or {}).get("dq_weights"),
        "domain": domain,
        "unified_schema": state.get("unified_schema"),
    }

    df = state.get("source_df")
    if df is None:
        raise ValueError("Missing 'source_df' in state — load_source_node did not complete successfully.")
    df = df.copy()
    column_mapping = state.get("column_mapping", {})

    result_df, audit_log = runner.run(
        df=df,
        block_sequence=block_sequence,
        column_mapping=column_mapping,
        config=config,
    )

    dq_pre = (
        float(result_df["dq_score_pre"].mean())
        if "dq_score_pre" in result_df.columns
        else 0.0
    )
    dq_post = (
        float(result_df["dq_score_post"].mean())
        if "dq_score_post" in result_df.columns
        else 0.0
    )

    enrichment_stats = {}
    try:
        enrich_block = block_registry.get("llm_enrich")
        enrichment_stats = getattr(enrich_block, "last_enrichment_stats", {})
    except Exception:
        pass

    unified_schema = state.get("unified_schema", {})
    required_cols = [
        col
        for col, spec in unified_schema.get("columns", {}).items()
        if spec.get("required") and not spec.get("computed")
    ]

    existing_required = [c for c in required_cols if c in result_df.columns]
    missing_cols = [c for c in required_cols if c not in result_df.columns]

    if missing_cols:
        logger.warning(
            f"Schema mismatch: {len(missing_cols)} required columns missing from output: {missing_cols}"
        )
        quarantined_mask = (
            result_df[existing_required].isna().any(axis=1)
            if existing_required
            else pd.Series(False, index=result_df.index)
        )
    else:
        quarantined_mask = (
            result_df[required_cols].isna().any(axis=1)
            if required_cols
            else pd.Series(False, index=result_df.index)
        )
    quarantined_df = result_df[quarantined_mask].copy()
    clean_df = result_df[~quarantined_mask].copy()

    quarantine_reasons = []
    for idx in quarantined_df.index:
        missing = [
            c for c in required_cols
            if c in quarantined_df.columns and pd.isna(quarantined_df.at[idx, c])
        ]
        quarantine_reasons.append(
            {
                "row_idx": int(idx),
                "missing_fields": missing,
                "reason": f"Null in required field(s): {', '.join(missing)}",
            }
        )

    if len(quarantined_df) > 0:
        logger.info(
            f"Quarantine: {len(quarantined_df)} rows failed post-enrichment validation"
        )

    return {
        "working_df": clean_df,
        "quarantined_df": quarantined_df,
        "quarantine_reasons": quarantine_reasons,
        "block_sequence": block_sequence,
        "audit_log": audit_log,
        "enrichment_stats": enrichment_stats,
        "dq_score_pre": round(dq_pre, 2),
        "dq_score_post": round(dq_post, 2),
    }


def save_output_node(state: PipelineState) -> dict:
    """Save the final DataFrame to output/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_path = state.get("source_path", "unknown")
    source_name = Path(source_path).stem
    output_path = OUTPUT_DIR / f"{source_name}_unified.csv"

    df = state.get("working_df")
    if df is None:
        raise ValueError("Missing 'working_df' in state — run_pipeline_node did not complete successfully.")
    df.to_csv(output_path, index=False)
    logger.info(f"Output saved to {output_path} ({len(df)} rows)")

    return {"output_path": str(output_path)}



# ── Step-by-step runner (for Streamlit UI) ───────────────────────────

NODE_MAP = {
    "load_source": load_source_node,
    "analyze_schema": analyze_schema_node,
    "critique_schema": critique_schema_node,
    "check_registry": check_registry_node,
    "plan_sequence": plan_sequence_node,
    "run_pipeline": run_pipeline_node,
    "save_output": save_output_node,
}


def run_step(step_name: str, state: dict) -> dict:
    """
    Run a single pipeline step by name. Used by the Streamlit wizard
    to execute nodes sequentially with HITL gates in between.
    """
    if step_name not in NODE_MAP:
        raise KeyError(f"Unknown step: {step_name}. Available: {list(NODE_MAP.keys())}")

    node_fn = NODE_MAP[step_name]
    updates = node_fn(state)
    state.update(updates)
    return state


# ── Full graph builder (for CLI / non-interactive use) ───────────────


def build_graph() -> StateGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("load_source", load_source_node)
    graph.add_node("analyze_schema", analyze_schema_node)
    graph.add_node("critique_schema", critique_schema_node)
    graph.add_node("check_registry", check_registry_node)
    graph.add_node("plan_sequence", plan_sequence_node)
    graph.add_node("run_pipeline", run_pipeline_node)
    graph.add_node("save_output", save_output_node)

    graph.add_edge("load_source", "analyze_schema")
    graph.add_edge("analyze_schema", "critique_schema")
    graph.add_edge("critique_schema", "check_registry")
    graph.add_edge("check_registry", "plan_sequence")
    graph.add_edge("plan_sequence", "run_pipeline")
    graph.add_edge("run_pipeline", "save_output")
    graph.add_edge("save_output", END)

    graph.set_entry_point("load_source")
    return graph.compile()
