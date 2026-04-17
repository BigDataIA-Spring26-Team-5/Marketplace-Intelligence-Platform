"""
Streamlit UI for the Schema-Driven Self-Extending ETL Pipeline.

Step-by-step wizard with HITL approval gates.
Run: streamlit run app.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Configure logging (terminal output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
logger = logging.getLogger("app")

from src.agents.graph import run_step
from src.ui.styles import GLOBAL_CSS
from src.ui.components import (
    render_step_bar,
    render_source_profile,
    render_schema_delta,
    render_missing_columns,
    render_yaml_review,
    render_registry_results,
    render_dq_cards,
    render_summary_cards,
    render_block_waterfall,
    render_enrichment_breakdown,
    render_quarantine_table,
    render_pipeline_remembered,
    render_run_history,
)

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

STEP_LABELS = [
    "Select Source",
    "Schema Analysis",
    "Schema Mapping",
    "Pipeline Execution",
    "Results",
]

# ── Page config ──────────────────────────────────────────────────────

st.set_page_config(
    page_title="ETL Pipeline — UC1 Data Enrichment",
    page_icon="&#9881;",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


# ── Session state init ───────────────────────────────────────────────


def init_state():
    defaults = {
        "step": 0,
        "max_completed_step": -1,  # highest step completed (-1 = none)
        "pipeline_state": {},
        "source_file": None,
        "domain": "nutrition",
        "enable_enrichment": True,
        "runs": [],  # persists across dataset resets — never cleared by "Run Another Dataset"
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _advance_step(new_step: int) -> None:
    """Advance to a new step and update max_completed_step if needed."""
    current = st.session_state["step"]
    # Mark the current step as completed if advancing forward
    if new_step > current:
        st.session_state["max_completed_step"] = max(
            st.session_state["max_completed_step"], current
        )
    st.session_state["step"] = new_step


def _save_run_summary(state: dict) -> None:
    """Append a summary of the current run to st.session_state['runs'], once per run."""
    if state.get("_run_saved"):
        return
    working_df = state.get("working_df")
    st.session_state["runs"].append(
        {
            "run_num": len(st.session_state["runs"]) + 1,
            "source": Path(state.get("source_path", "?")).name,
            "domain": state.get("domain", "?"),
            "rows": len(working_df) if working_df is not None else 0,
            "dq_pre": state.get("dq_score_pre", 0),
            "dq_post": state.get("dq_score_post", 0),
            "dq_delta": state.get("dq_score_post", 0) - state.get("dq_score_pre", 0),
            "gaps": len(state.get("gaps", [])),
            "registry_hits": len(state.get("block_registry_hits", {})),
            "functions_generated": len(state.get("operations", [])),
            "schema_existed": state.get("unified_schema_existed", False),
        }
    )
    state["_run_saved"] = True
    st.session_state["pipeline_state"] = state


init_state()


# ── Header ───────────────────────────────────────────────────────────

st.markdown(
    '<h2 style="color:#24292f; margin-bottom:0;">Schema-Driven ETL Pipeline</h2>'
    '<p style="color:#57606a; margin-top:4px;">UC1 — Data Enrichment with Two-Agent Architecture</p>',
    unsafe_allow_html=True,
)

st.markdown(
    render_step_bar(
        st.session_state["step"],
        STEP_LABELS,
        st.session_state["max_completed_step"],
    ),
    unsafe_allow_html=True,
)

# ── Sidebar navigation (visible after first completion) ──────────────

if st.session_state["max_completed_step"] >= 0:
    with st.sidebar:
        st.markdown("### Navigate Steps")
        for i, label in enumerate(STEP_LABELS):
            if i <= st.session_state["max_completed_step"]:
                is_current = i == st.session_state["step"]
                btn_type = "primary" if is_current else "secondary"
                if st.button(
                    f"{'→ ' if is_current else ''}{label}",
                    key=f"nav_{i}",
                    type=btn_type,
                    disabled=is_current,
                    use_container_width=True,
                ):
                    st.session_state["step"] = i
                    st.rerun()


# ── Step 0: Select Source ────────────────────────────────────────────


def step_select_source():
    st.markdown(
        '<div class="section-header">Select Data Source</div>', unsafe_allow_html=True
    )

    # Find CSV files in data/
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    csv_names = [f.name for f in csv_files]

    col1, col2 = st.columns([2, 1])
    with col1:
        selected = st.selectbox("Data source", csv_names, index=0)
    with col2:
        domain = st.selectbox("Domain", ["nutrition", "safety", "pricing"], index=0)
        enable_enrichment = st.checkbox(
            "Enable enrichment",
            value=True,
            help="Uncheck to skip allergen extraction and LLM enrichment blocks.",
        )

    if selected:
        source_path = str(DATA_DIR / selected)
        preview_df = pd.read_csv(source_path, nrows=5)
        st.markdown(
            '<div class="section-header">Preview (first 5 rows)</div>',
            unsafe_allow_html=True,
        )
        st.dataframe(preview_df, width="stretch", height=220)

    if st.session_state["runs"]:
        st.markdown(
            '<div class="section-header">Run History</div>', unsafe_allow_html=True
        )
        st.markdown(
            render_run_history(st.session_state["runs"]), unsafe_allow_html=True
        )

    if st.button("Analyze Schema", type="primary", width="stretch"):
        st.session_state["source_file"] = str(DATA_DIR / selected)
        st.session_state["domain"] = domain
        st.session_state["pipeline_state"] = {
            "source_path": str(DATA_DIR / selected),
            "domain": domain,
            "enable_enrichment": enable_enrichment,
        }
        _advance_step(1)
        st.rerun()


# ── Step 1: Schema Analysis + HITL Mapping Approval ─────────────────


def step_schema_analysis():
    state = st.session_state["pipeline_state"]

    with st.spinner("Loading source data..."):
        state = run_step("load_source", state)
        source_df = state.get("source_df")
        source_schema = state.get("source_schema", {})
        if source_df is None or not source_schema:
            st.error("Failed to load source data — source_df or source_schema missing.")
            return
        logger.info(f"Loaded {len(source_df)} rows, {len(source_schema)} columns")

    with st.spinner("Analyzing schema (LLM call)..."):
        state = run_step("analyze_schema", state)

    st.session_state["pipeline_state"] = state

    # Display source profile
    st.markdown(
        '<div class="section-header">Source Schema Profile</div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_source_profile(state["source_schema"]), unsafe_allow_html=True)

    # Schema delta
    schema_existed = state.get("unified_schema_existed", False)
    if schema_existed:
        st.markdown(
            '<div class="section-header">Schema Delta — Source vs. Unified</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="section-header">Derived Unified Schema (First Run)</div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        render_schema_delta(
            state["source_schema"],
            state.get("column_mapping", {}),
            state.get("gaps", []),
            state.get("unified_schema"),
            missing_columns=state.get("missing_columns"),
            derivable_gaps=state.get("derivable_gaps"),
            enrichment_columns=state.get("enrichment_columns_to_generate"),
            enrich_alias_ops=state.get("enrich_alias_ops"),
        ),
        unsafe_allow_html=True,
    )

    # Summary
    n_mapped = len(state.get("column_mapping", {}))
    n_derivable = len(state.get("derivable_gaps", []))
    n_missing = len(state.get("missing_columns", []))
    n_aliases = len(state.get("enrich_alias_ops", []))
    n_total_gaps = n_derivable + n_missing

    _alias_card = (
        f'<div class="metric-card">'
        f'<div class="metric-label">Enrichment Aliases</div>'
        f'<div class="metric-value val-good">{n_aliases}</div>'
        f'<div class="metric-sub">Filled post-enrichment</div>'
        f"</div>"
        if n_aliases > 0 else ""
    )
    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card">'
        f'<div class="metric-label">Columns Mapped</div>'
        f'<div class="metric-value val-good">{n_mapped}</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">Derivable Gaps</div>'
        f'<div class="metric-value {"val-warn" if n_derivable > 0 else "val-good"}">{n_derivable}</div>'
        f'<div class="metric-sub">Will be transformed</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">Missing Columns</div>'
        f'<div class="metric-value {"val-bad" if n_missing > 0 else "val-good"}">{n_missing}</div>'
        f'<div class="metric-sub">No source data</div>'
        f"</div>"
        f"{_alias_card}"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Mapping warnings
    for w in state.get("mapping_warnings", []):
        st.warning(w)

    # Missing columns HITL decision
    missing_cols = state.get("missing_columns", [])
    aliased_targets = {a["target"] for a in state.get("enrich_alias_ops", [])}
    truly_missing_cols = [mc for mc in missing_cols if mc["target_column"] not in aliased_targets]
    if missing_cols:
        st.markdown(
            '<div class="section-header">Missing Columns — No Source Data</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            render_missing_columns(truly_missing_cols), unsafe_allow_html=True
        )
        st.markdown(
            '<p style="color:#bc4c00; font-size:0.85em; margin:8px 0 4px 0;">'
            'Preliminary list (Agent 1.5 in Step 2 may detect semantic aliases to enrichment columns). '
            'Rows with null values in truly unresolvable required columns will be quarantined.</p>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p style="color:#57606a; font-size:0.85em; margin:0 0 16px 0;">'
            'You can exclude individual columns from the required schema for this run '
            'to prevent them from triggering quarantine.</p>',
            unsafe_allow_html=True,
        )

        decisions = {}
        for mc in missing_cols:
            col_name = mc["target_column"]
            col_type = mc.get("target_type", "string")
            if col_name in aliased_targets:
                alias_src = next(
                    (a["source"] for a in state.get("enrich_alias_ops", []) if a["target"] == col_name),
                    "enrichment",
                )
                st.info(
                    f"`{col_name}` ({col_type}) — will be auto-filled from enrichment column `{alias_src}` after pipeline runs. No action needed.",
                    icon="ℹ️",
                )
            else:
                exclude = st.checkbox(
                    f"Exclude `{col_name}` ({col_type}) from required schema",
                    key=f"missing_exclude_{col_name}",
                )
                decisions[col_name] = {"action": "exclude"} if exclude else {"action": "accept_null"}

        state["missing_column_decisions"] = decisions
        st.session_state["pipeline_state"] = state

    # HITL Gate 1
    # Determine if there are truly unresolvable missing cols (not covered by enrichment alias)
    truly_missing = truly_missing_cols  # Already computed above
    st.markdown("---")
    if truly_missing:
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Force Continue (Quarantine Expected)", type="primary", width="stretch"):
                _advance_step(2)
                st.rerun()
        with col2:
            if st.button("Abort Ingestion", width="stretch"):
                st.warning("Ingestion aborted. Missing columns cannot be filled from this source.")
                st.session_state["step"] = 0
        with col3:
            if st.button("Back to Source Selection", width="stretch"):
                st.session_state["step"] = 0
                st.rerun()
    else:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Approve Mapping & Continue", type="primary", width="stretch"):
                _advance_step(2)
                st.rerun()
        with col2:
            if st.button("Back to Source Selection", width="stretch"):
                st.session_state["step"] = 0
                st.rerun()


# ── Step 2: Registry Check + Schema Mapping Review ───────────────────


def step_code_generation():
    """Step 2: Schema Mapping — critique, registry check, YAML generation, sequence planning."""
    state = st.session_state["pipeline_state"]

    # Agent 1.5: critique Agent 1's operations
    with st.spinner("Agent 1.5: Critiquing schema analysis..."):
        state = run_step("critique_schema", state)

    with st.spinner("Checking block registry and building schema mapping..."):
        state = run_step("check_registry", state)

    st.session_state["pipeline_state"] = state

    hits = state.get("block_registry_hits", {})
    unresolvable = state.get("unresolvable_gaps", [])
    mapping_warnings = state.get("mapping_warnings", [])

    st.markdown(
        '<div class="section-header">Block Registry Lookup</div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_registry_results(hits, []), unsafe_allow_html=True)

    # Agent 1.5 Critique section
    critique_notes = state.get("critique_notes", [])
    with st.expander("Agent 1.5 Critique", expanded=bool(critique_notes)):
        if critique_notes:
            for note in critique_notes:
                rule = note.get("rule", "Unknown rule")
                column = note.get("column", "?")
                original = note.get("original", "—")
                correction = note.get("correction", "—")
                st.markdown(
                    f"**{rule}** — `{column}`\n\n"
                    f"- **Original:** {original}\n"
                    f"- **Correction:** {correction}",
                )
                st.markdown("---")
        else:
            st.info("No corrections needed.")

    # Show YAML mapping review if a YAML was generated
    yaml_path = state.get("mapping_yaml_path")
    if yaml_path:
        st.markdown(
            '<div class="section-header">Declarative Column Operations (YAML)</div>',
            unsafe_allow_html=True,
        )
        try:
            from src.blocks.mapping_io import read_mapping_yaml
            yaml_ops = read_mapping_yaml(yaml_path)
            st.markdown(render_yaml_review(yaml_ops), unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Could not read YAML mapping: {e}")

    # Show any unresolvable gaps as informational warnings
    for ur in unresolvable:
        st.warning(
            f"Column `{ur.get('target_column', '?')}` is unresolvable: "
            f"{ur.get('reason', 'no source data')} — will be set to null."
        )

    for warning in mapping_warnings:
        st.warning(warning)

    # Show registry hits and enrichment info
    enrichment_cols = state.get("enrichment_columns_to_generate", [])
    block_hits_map = state.get("block_registry_hits", {})

    if hits or block_hits_map:
        st.markdown(render_pipeline_remembered(hits), unsafe_allow_html=True)
        # Exclude enrichment columns — they're shown in the enrichment section below
        enrichment_col_set = set(state.get("enrichment_columns_to_generate", []))
        gap_hits = {col: blk for col, blk in block_hits_map.items() if col not in enrichment_col_set}
        if gap_hits:
            st.info(
                "Pipeline blocks will handle: "
                + ", ".join(f"`{col}` via `{blk}`" for col, blk in gap_hits.items())
            )

    if enrichment_cols:
        st.info(
            f"{len(enrichment_cols)} enrichment column(s) will be generated by pipeline "
            f"blocks: {', '.join(f'`{c}`' for c in enrichment_cols)}"
        )
    elif not hits and not block_hits_map and not yaml_path:
        st.info("No schema gaps detected. Proceeding to pipeline execution.")

    # Agent 3: plan the execution sequence
    with st.spinner("Agent 3: Planning execution sequence..."):
        state = run_step("plan_sequence", state)
    st.session_state["pipeline_state"] = state

    sequence = state.get("block_sequence", [])
    if sequence:
        st.markdown(
            '<div class="section-header">Planned Execution Sequence (Agent 3)</div>',
            unsafe_allow_html=True,
        )
        reasoning = state.get("sequence_reasoning", "")
        if reasoning:
            st.caption(f"Reasoning: {reasoning}")
        st.markdown(" → ".join(f"`{b}`" for b in sequence))

    if st.button("Run Pipeline", type="primary", width="stretch"):
        _advance_step(3)
        st.rerun()


# ── Step 3: Pipeline Execution ───────────────────────────────────────


def step_pipeline_execution():
    state = st.session_state["pipeline_state"]

    st.markdown(
        '<div class="section-header">Pipeline Execution</div>', unsafe_allow_html=True
    )

    sequence = state.get("block_sequence", [])
    if sequence:
        st.markdown("**Execution sequence:** " + " → ".join(f"`{b}`" for b in sequence))
        reasoning = state.get("sequence_reasoning", "")
        if reasoning:
            st.caption(f"Agent 3 reasoning: {reasoning}")

    with st.spinner("Running transformation pipeline..."):
        state = run_step("run_pipeline", state)

    with st.spinner("Saving output..."):
        state = run_step("save_output", state)

    st.session_state["pipeline_state"] = state
    # Mark step 3 as complete and advance to results
    st.session_state["max_completed_step"] = 4  # All steps now navigable
    st.session_state["step"] = 4
    st.rerun()


# ── Step 4: Results & Analytics ──────────────────────────────────────


def step_results():
    state = st.session_state["pipeline_state"]

    # Save run summary exactly once per completed run
    _save_run_summary(state)
    state = st.session_state["pipeline_state"]  # re-read after potential update

    working_df = state.get("working_df")
    if working_df is None:
        st.error("No pipeline results found.")
        return

    # DQ Cards
    st.markdown(
        '<div class="section-header">Data Quality Scores</div>', unsafe_allow_html=True
    )
    st.markdown(
        render_dq_cards(state.get("dq_score_pre", 0), state.get("dq_score_post", 0)),
        unsafe_allow_html=True,
    )

    # Summary cards
    st.markdown(
        '<div class="section-header">Pipeline Summary</div>', unsafe_allow_html=True
    )
    yaml_op_count = 0
    yaml_path = state.get("mapping_yaml_path")
    if yaml_path:
        try:
            from src.blocks.mapping_io import read_mapping_yaml
            yaml_op_count = len(read_mapping_yaml(yaml_path))
        except Exception:
            pass
    st.markdown(
        render_summary_cards(
            rows=len(working_df),
            clusters=working_df["duplicate_group_id"].nunique()
            if "duplicate_group_id" in working_df.columns
            else len(working_df),
            registry_hits=len(state.get("block_registry_hits", {})),
            functions_generated=yaml_op_count,
        ),
        unsafe_allow_html=True,
    )

    # Block waterfall
    st.markdown(
        '<div class="section-header">Block Execution Trace</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        render_block_waterfall(state.get("audit_log", [])),
        unsafe_allow_html=True,
    )

    # Enrichment breakdown
    st.markdown(
        '<div class="section-header">Enrichment Tier Breakdown</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        render_enrichment_breakdown(state.get("enrichment_stats", {})),
        unsafe_allow_html=True,
    )

    # Quarantine — HITL Gate 3
    st.markdown(
        '<div class="section-header">Post-Enrichment Quarantine</div>',
        unsafe_allow_html=True,
    )
    quarantine_reasons = state.get("quarantine_reasons", [])
    quarantined_df = state.get("quarantined_df")
    st.markdown(
        render_quarantine_table(quarantine_reasons, quarantined_df),
        unsafe_allow_html=True,
    )

    if quarantine_reasons:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Accept Quarantine (exclude from output)", width="stretch"):
                st.success(
                    f"{len(quarantine_reasons)} rows excluded from final output."
                )
        with col2:
            if st.button("Override: Include All Rows", width="stretch"):
                # Merge quarantined rows back
                if quarantined_df is not None and len(quarantined_df) > 0:
                    state["working_df"] = pd.concat(
                        [working_df, quarantined_df], ignore_index=True
                    )
                    state["quarantine_reasons"] = []
                    state["quarantined_df"] = pd.DataFrame()
                    st.session_state["pipeline_state"] = state
                    st.success("All rows included in output.")
                    st.rerun()

    # Output preview
    st.markdown(
        '<div class="section-header">Output Preview</div>', unsafe_allow_html=True
    )
    st.dataframe(working_df.head(100), width="stretch", height=400)

    # Download
    csv_data = working_df.to_csv(index=False)
    source_name = Path(state.get("source_path", "output")).stem
    st.download_button(
        label="Download Unified CSV",
        data=csv_data,
        file_name=f"{source_name}_unified.csv",
        mime="text/csv",
        width="stretch",
    )

    st.markdown("---")

    # Cross-run comparison — shown when more than one run is complete
    if len(st.session_state["runs"]) > 1:
        st.markdown(
            '<div class="section-header">Cross-Run Comparison</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            render_run_history(st.session_state["runs"]), unsafe_allow_html=True
        )

    if st.button("Run Another Dataset", type="primary", width="stretch"):
        st.session_state["step"] = 0
        st.session_state["max_completed_step"] = -1  # Reset navigation
        st.session_state["pipeline_state"] = {}
        # Intentionally preserve st.session_state["runs"] across resets
        st.rerun()


# ── Router ───────────────────────────────────────────────────────────

STEP_HANDLERS = {
    0: step_select_source,
    1: step_schema_analysis,
    2: step_code_generation,
    3: step_pipeline_execution,
    4: step_results,
}

STEP_HANDLERS[st.session_state["step"]]()
