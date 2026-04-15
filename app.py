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
    render_registry_results,
    render_code_review,
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
    "Code Generation",
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
        "pipeline_state": {},
        "source_file": None,
        "domain": "nutrition",
        "enable_enrichment": True,
        "runs": [],  # persists across dataset resets — never cleared by "Run Another Dataset"
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _save_run_summary(state: dict) -> None:
    """Append a summary of the current run to st.session_state['runs'], once per run."""
    if state.get("_run_saved"):
        return
    generated = state.get("generated_blocks", [])
    n_gen = len([f for f in generated if f.get("validation_passed")])
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
            "functions_generated": n_gen,
            "schema_existed": state.get("unified_schema_existed", False),
        }
    )
    state["_run_saved"] = True
    st.session_state["pipeline_state"] = state


init_state()


# ── Header ───────────────────────────────────────────────────────────

st.markdown(
    '<h2 style="color:#e6edf3; margin-bottom:0;">Schema-Driven ETL Pipeline</h2>'
    '<p style="color:#8b949e; margin-top:4px;">UC1 — Data Enrichment with Two-Agent Architecture</p>',
    unsafe_allow_html=True,
)

st.markdown(
    render_step_bar(st.session_state["step"], STEP_LABELS),
    unsafe_allow_html=True,
)


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
        st.session_state["step"] = 1
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
        ),
        unsafe_allow_html=True,
    )

    # Summary
    n_mapped = len(state.get("column_mapping", {}))
    n_gaps = len(state.get("gaps", []))
    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card">'
        f'<div class="metric-label">Columns Mapped</div>'
        f'<div class="metric-value val-good">{n_mapped}</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">Gaps Detected</div>'
        f'<div class="metric-value {"val-warn" if n_gaps > 0 else "val-good"}">{n_gaps}</div>'
        f"</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Enrichment info and mapping warnings
    enrichment_cols = state.get("enrichment_columns_to_generate", [])
    if enrichment_cols:
        st.info(
            f"{len(enrichment_cols)} enrichment column(s) absent from source will be "
            f"generated by pipeline blocks: {', '.join(enrichment_cols)}"
        )
    for w in state.get("mapping_warnings", []):
        st.warning(w)

    # HITL Gate 1: Approve mapping
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Approve Mapping & Continue", type="primary", width="stretch"):
            st.session_state["step"] = 2
            st.rerun()
    with col2:
        if st.button("Back to Source Selection", width="stretch"):
            st.session_state["step"] = 0
            st.rerun()


# ── Step 2: Registry Check + Code Generation + HITL Code Review ─────


def step_code_generation():
    state = st.session_state["pipeline_state"]

    with st.spinner("Checking function registry..."):
        state = run_step("check_registry", state)

    st.session_state["pipeline_state"] = state

    hits = state.get("block_registry_hits", {})
    misses = state.get("registry_misses", [])

    st.markdown(
        '<div class="section-header">Block Registry Lookup</div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_registry_results(hits, misses), unsafe_allow_html=True)

    if misses:
        st.markdown(
            '<div class="section-header">Code Generation (Agent 2)</div>',
            unsafe_allow_html=True,
        )

        with st.spinner(f"Generating {len(misses)} transform function(s) via LLM..."):
            state = run_step("generate_code", state)
            # Run validation pass-through
            state = run_step("validate_code", state)

        st.session_state["pipeline_state"] = state

        # HITL Gate 2: Review each generated function
        generated = state.get("generated_blocks", [])
        for func in generated:
            st.markdown(render_code_review(func), unsafe_allow_html=True)

        st.markdown("---")

        # Check if any failed
        failed = [f for f in generated if not f.get("validation_passed")]
        if failed:
            st.warning(
                f"{len(failed)} function(s) failed validation. You can approve the passing ones or regenerate."
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button(
                "Approve & Register Functions", type="primary", width="stretch"
            ):
                with st.spinner("Registering functions..."):
                    state = run_step("register_blocks", state)
                with st.spinner("Agent 3: Planning execution sequence..."):
                    state = run_step("plan_sequence", state)
                st.session_state["pipeline_state"] = state
                st.session_state["step"] = 3
                st.rerun()
        with col2:
            if st.button("Regenerate Failed", width="stretch"):
                with st.spinner("Regenerating..."):
                    state = run_step("generate_code", state)
                    state = run_step("validate_code", state)
                st.session_state["pipeline_state"] = state
                st.rerun()
        with col3:
            if st.button("Skip Code Gen & Run Pipeline", width="stretch"):
                state["generated_blocks"] = []
                with st.spinner("Agent 3: Planning execution sequence..."):
                    state = run_step("plan_sequence", state)
                st.session_state["pipeline_state"] = state
                st.session_state["step"] = 3
                st.rerun()
    else:
        # No misses — all gaps covered by registry (or no gaps)
        pipeline_state = st.session_state["pipeline_state"]
        enrichment_cols = pipeline_state.get("enrichment_columns_to_generate", [])
        block_hits_map = pipeline_state.get("block_registry_hits", {})
        mapping_warnings = pipeline_state.get("mapping_warnings", [])

        for warning in mapping_warnings:
            st.warning(warning)

        if hits or block_hits_map:
            st.markdown(render_pipeline_remembered(hits), unsafe_allow_html=True)
            if block_hits_map:
                st.info(
                    "Pipeline blocks will handle: "
                    + ", ".join(
                        f"`{col}` via `{blk}`" for col, blk in block_hits_map.items()
                    )
                )

        if enrichment_cols:
            st.info(
                f"{len(enrichment_cols)} enrichment column(s) will be generated by pipeline "
                f"blocks: {', '.join(f'`{c}`' for c in enrichment_cols)}"
            )
        elif not hits and not block_hits_map:
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
            st.session_state["step"] = 3
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
    generated = state.get("generated_blocks", [])
    n_generated = len([f for f in generated if f.get("validation_passed")])
    st.markdown(
        render_summary_cards(
            rows=len(working_df),
            clusters=working_df["duplicate_group_id"].nunique()
            if "duplicate_group_id" in working_df.columns
            else len(working_df),
            registry_hits=len(state.get("block_registry_hits", {})),
            functions_generated=n_generated,
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
