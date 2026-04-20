"""Streamlit 5-step HITL wizard for the Schema-Driven ETL Pipeline."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from src.ui.components import (
    render_agent_header,
    render_block_metrics_table,
    render_block_waterfall,
    render_critique_notes,
    render_dq_cards,
    render_enrichment_breakdown,
    render_hitl_gate,
    render_log_panel,
    render_missing_columns,
    render_operations_review,

    render_quarantine_table,
    render_registry_results,
    render_sampling_stats,
    render_schema_delta,
    render_source_profile,
    render_step_bar,
    render_summary_cards,
)
from src.ui.styles import GLOBAL_CSS

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

STEPS = ["Source", "Schema Analysis", "Code Gen", "Execution", "Results"]
DOMAINS = ["nutrition", "safety", "pricing"]


# ── Log capture ──────────────────────────────────────────────────────────


class StreamlitLogHandler(logging.Handler):
    """Appends structured log records to st.session_state.log_entries."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if "log_entries" not in st.session_state:
                return
            st.session_state.log_entries.append({
                "time": datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
                "level": record.levelname,
                "logger": record.name,
                "event": record.getMessage(),
                "step": st.session_state.get("step", 0),
            })
        except Exception:
            pass


def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for noisy in ("litellm", "LiteLLM", "httpx", "httpcore", "urllib3",
                  "sentence_transformers", "faiss", "transformers"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Check by class name, not identity — isinstance fails across hot-reloads
    # because module reload creates a new class object, breaking isinstance.
    if not any(type(h).__name__ == "StreamlitLogHandler" for h in root.handlers):
        root.addHandler(StreamlitLogHandler())


# ── Session state ─────────────────────────────────────────────────────────


def _init_state() -> None:
    defaults: dict = {
        "step": 0,
        "max_completed": -1,
        "pipeline_state": {},
        "log_entries": [],
        "hitl_decisions": {},
        "error": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _advance(new_step: int) -> None:
    st.session_state.max_completed = max(st.session_state.max_completed, new_step - 1)
    st.session_state.step = new_step
    st.rerun()


def _run_step(step_name: str) -> None:
    from src.agents.graph import run_step
    st.session_state.pipeline_state = run_step(
        step_name, st.session_state.pipeline_state
    )


# ── Step 0: Source Selection ──────────────────────────────────────────────


def _step_0_source_selection() -> None:
    st.markdown(
        render_agent_header(
            1, "Source Loader",
            "Select a CSV file from data/ and choose a domain to begin.",
        ),
        unsafe_allow_html=True,
    )

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        st.error(f"No CSV files found in `{DATA_DIR}`. Add data files and restart.")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        chosen = st.selectbox(
            "CSV File",
            [f.name for f in csv_files],
            key="csv_select",
        )
    with col2:
        domain = st.selectbox("Domain", DOMAINS, key="domain_select")

    source_path = str(DATA_DIR / chosen)

    st.markdown(
        render_hitl_gate(0, "Source Confirmation", ["Analyze Schema"]),
        unsafe_allow_html=True,
    )

    if st.button("▶  Analyze Schema", type="primary", use_container_width=True):
        ps: dict = {
            "source_path": source_path,
            "domain": domain,
            "missing_column_decisions": {},
        }
        st.session_state.pipeline_state = ps
        with st.spinner("Loading source + profiling schema…"):
            try:
                _run_step("load_source")
                _run_step("analyze_schema")
                st.session_state.error = None
                _advance(1)
            except Exception as exc:
                st.session_state.error = str(exc)
                st.error(f"Load failed: {exc}")


# ── Step 1: Schema Analysis ───────────────────────────────────────────────


def _step_1_schema_analysis() -> None:
    ps = st.session_state.pipeline_state
    source_schema = ps.get("source_schema", {})
    column_mapping = ps.get("column_mapping", {})
    gaps = ps.get("gaps", [])
    from src.schema.analyzer import get_unified_schema as _get_schema
    try:
        unified_schema = _get_schema()
    except FileNotFoundError:
        unified_schema = None
    missing_columns = ps.get("missing_columns", [])
    derivable_gaps = ps.get("derivable_gaps", [])
    enrich_alias_ops = ps.get("enrich_alias_ops", [])
    enrichment_cols = ps.get("enrichment_columns_to_generate", [])
    sampling_strategy = ps.get("sampling_strategy")

    st.markdown(
        render_agent_header(
            1, "Schema Analyzer",
            "Profiling source columns and mapping to the unified schema.",
        ),
        unsafe_allow_html=True,
    )

    if sampling_strategy:
        st.markdown(render_sampling_stats(sampling_strategy), unsafe_allow_html=True)

    with st.expander("Source Column Profile", expanded=True):
        st.markdown(render_source_profile(source_schema), unsafe_allow_html=True)

    st.markdown('<div class="section-header">Schema Delta</div>', unsafe_allow_html=True)
    st.markdown(
        render_schema_delta(
            source_profile=source_schema,
            column_mapping=column_mapping,
            gaps=gaps,
            unified_schema=unified_schema,
            missing_columns=missing_columns,
            derivable_gaps=derivable_gaps,
            enrichment_columns=enrichment_cols,
            enrich_alias_ops=enrich_alias_ops,
        ),
        unsafe_allow_html=True,
    )

    if missing_columns:
        st.markdown('<div class="section-header">Unavailable Columns</div>', unsafe_allow_html=True)
        st.markdown(render_missing_columns(missing_columns), unsafe_allow_html=True)

    _render_log_expander()

    st.markdown(
        render_hitl_gate(1, "Schema Review — approve to generate transforms", ["Generate Code"]),
        unsafe_allow_html=True,
    )

    if st.button("▶  Generate Code & Run Critic", type="primary", use_container_width=True):
        with st.spinner("Agent 2 (Critic) reviewing schema analysis…"):
            try:
                _run_step("critique_schema")
                st.session_state.error = None
                _advance(2)
            except Exception as exc:
                st.session_state.error = str(exc)
                st.error(f"Critic failed: {exc}")


# ── Step 2: Code Generation + HITL ───────────────────────────────────────


def _step_2_code_generation() -> None:
    ps = st.session_state.pipeline_state
    revised_ops = ps.get("revised_operations") or ps.get("operations", [])
    critique_notes = ps.get("critique_notes", [])
    unresolvable = ps.get("unresolvable_gaps", [])

    st.markdown(
        render_agent_header(
            2, "Critic",
            "Validating Agent 1 output. Review transforms and make decisions on unavailable columns.",
        ),
        unsafe_allow_html=True,
    )

    # Agent 2 corrections
    st.markdown('<div class="section-header">Agent 2 Corrections</div>', unsafe_allow_html=True)
    st.markdown(render_critique_notes(critique_notes), unsafe_allow_html=True)

    # Operations table
    st.markdown('<div class="section-header">Schema Transform Operations</div>', unsafe_allow_html=True)
    st.markdown(render_operations_review(revised_ops), unsafe_allow_html=True)

    # HITL decisions for set_null ops + unresolvable
    need_decision: list[dict] = [
        op for op in revised_ops if op.get("action") == "set_null"
    ]
    seen_cols = {op.get("target_column") for op in need_decision}
    for gap in unresolvable:
        col = gap.get("target_column")
        if col and col not in seen_cols:
            need_decision.append({
                "target_column": col,
                "target_type": "string",
                "action": "set_null",
                "reason": gap.get("reason", "No source data"),
            })
            seen_cols.add(col)

    decisions: dict = {}

    if need_decision:
        st.markdown(
            '<div class="section-header">HITL Decisions — Unavailable Columns</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            render_hitl_gate(
                2,
                "Choose what to do with each unavailable column",
                ["Accept Null", "Set Default", "Exclude"],
            ),
            unsafe_allow_html=True,
        )

        for op in need_decision:
            tgt = op.get("target_column", "?")
            reason = op.get("reason", "No source data available")
            st.markdown(f"**`{tgt}`** &nbsp;—&nbsp; _{reason}_")
            c_radio, c_val = st.columns([3, 2])
            with c_radio:
                choice = st.radio(
                    f"Decision for `{tgt}`",
                    ["Accept Null", "Set Default Value", "Exclude Column"],
                    key=f"hitl_{tgt}",
                    horizontal=True,
                    label_visibility="collapsed",
                )
            default_val: str = ""
            if choice == "Set Default Value":
                with c_val:
                    default_val = st.text_input(
                        f"Default value for `{tgt}`",
                        key=f"hitl_val_{tgt}",
                    )

            if choice == "Accept Null":
                decisions[tgt] = {"action": "accept_null"}
            elif choice == "Set Default Value":
                decisions[tgt] = {"action": "set_default", "value": default_val}
            else:
                decisions[tgt] = {"action": "exclude"}
    else:
        decisions = {}

    st.session_state.hitl_decisions = decisions

    _render_log_expander()

    st.markdown(
        render_hitl_gate(
            2,
            "Registry Check & Sequence Planning — approve to run pipeline",
            ["Run Pipeline"],
        ),
        unsafe_allow_html=True,
    )

    if st.button("▶  Check Registry & Plan Sequence", type="primary", use_container_width=True):
        with st.spinner("Checking block registry…"):
            try:
                st.session_state.pipeline_state["missing_column_decisions"] = (
                    st.session_state.hitl_decisions
                )
                _run_step("check_registry")
            except Exception as exc:
                st.session_state.error = str(exc)
                st.error(f"Registry check failed: {exc}")
                return

        with st.spinner("Agent 3 planning block sequence…"):
            try:
                _run_step("plan_sequence")
                st.session_state.error = None
                _advance(3)
            except Exception as exc:
                st.session_state.error = str(exc)
                st.error(f"Sequence planning failed: {exc}")


# ── Step 3: Pipeline Execution ────────────────────────────────────────────


def _step_3_pipeline_execution() -> None:
    ps = st.session_state.pipeline_state
    block_sequence = ps.get("block_sequence", [])

    st.markdown(
        render_agent_header(
            3, "Pipeline Runner",
            "Executing the planned block sequence on the source DataFrame.",
        ),
        unsafe_allow_html=True,
    )

    if block_sequence:
        st.markdown(f"**Planned sequence ({len(block_sequence)} blocks):**")
        st.code(" → ".join(block_sequence), language="text")

    if ps.get("working_df") is not None:
        _advance(4)
        return

    with st.status("Running pipeline…", expanded=True) as status:
        st.write("Executing block sequence…")
        try:
            _run_step("run_pipeline")
            st.write("Saving output…")
            _run_step("save_output")
            output_path = st.session_state.pipeline_state.get("output_path", "")
            status.update(
                label=f"Pipeline complete — output: `{output_path}`",
                state="complete",
                expanded=False,
            )
            st.session_state.error = None
        except Exception as exc:
            status.update(label=f"Pipeline failed: {exc}", state="error")
            st.session_state.error = str(exc)
            st.error(f"Pipeline error: {exc}")

            with st.expander("Error Logs", expanded=True):
                err_entries = [
                    e for e in st.session_state.log_entries
                    if e.get("level") in ("ERROR", "CRITICAL")
                ]
                st.markdown(
                    render_log_panel(err_entries, tall=True),
                    unsafe_allow_html=True,
                )
            return

    _render_log_expander()

    if st.button("▶  View Results", type="primary", use_container_width=True):
        _advance(4)


# ── Step 4: Results ───────────────────────────────────────────────────────


def _step_4_results() -> None:
    ps = st.session_state.pipeline_state
    working_df = ps.get("working_df")
    audit_log = ps.get("audit_log", [])
    enrichment_stats = ps.get("enrichment_stats", {})
    dq_pre = float(ps.get("dq_score_pre", 0.0))
    dq_post = float(ps.get("dq_score_post", 0.0))
    quarantine_reasons = ps.get("quarantine_reasons", [])
    quarantined_df = ps.get("quarantined_df")
    block_registry_hits = ps.get("block_registry_hits", {})
    output_path = ps.get("output_path", "")

    rows = len(working_df) if working_df is not None else 0
    registry_hits = len(block_registry_hits)
    dynamic_blocks = len([
        e for e in audit_log
        if any(pfx in e.get("block", "") for pfx in ("DYNAMIC_MAPPING", "DERIVE_"))
    ])

    st.markdown('<div class="section-header">Run Summary</div>', unsafe_allow_html=True)
    st.markdown(
        render_summary_cards(rows, 0, registry_hits, dynamic_blocks),
        unsafe_allow_html=True,
    )

    st.markdown('<div class="section-header">Data Quality Scores</div>', unsafe_allow_html=True)
    st.markdown(render_dq_cards(dq_pre, dq_post), unsafe_allow_html=True)

    col_left, col_right = st.columns([3, 2])

    with col_left:
        with st.expander("Block Execution Waterfall", expanded=True):
            st.markdown(render_block_waterfall(audit_log), unsafe_allow_html=True)

        with st.expander("Block Metrics — rows in/out per block", expanded=True):
            st.markdown(render_block_metrics_table(audit_log), unsafe_allow_html=True)

        if enrichment_stats:
            with st.expander("Enrichment Tier Breakdown", expanded=True):
                st.markdown(render_enrichment_breakdown(enrichment_stats), unsafe_allow_html=True)

        with st.expander("Quarantine Table", expanded=len(quarantine_reasons) > 0):
            st.markdown(
                render_quarantine_table(quarantine_reasons, quarantined_df),
                unsafe_allow_html=True,
            )

        if block_registry_hits:
            with st.expander("Registry Hits", expanded=False):
                misses = ps.get("registry_misses", [])
                st.markdown(render_registry_results(block_registry_hits, misses), unsafe_allow_html=True)

        if output_path:
            st.success(f"Output saved: `{output_path}`")

    with col_right:
        _render_log_panel_with_filters(tall=True)

    st.divider()

    if st.button("↺  Start New Run", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()


# ── Shared helpers ────────────────────────────────────────────────────────


def _render_log_expander() -> None:
    entries = st.session_state.get("log_entries", [])
    if not entries:
        return
    with st.expander(f"Pipeline Logs ({len(entries)} entries)", expanded=False):
        st.markdown(render_log_panel(entries), unsafe_allow_html=True)


def _render_log_panel_with_filters(tall: bool = False) -> None:
    """Filterable log panel with copy-for-LLM block."""
    entries = st.session_state.get("log_entries", [])

    st.markdown("**Pipeline Logs**")
    fc1, fc2 = st.columns(2)
    with fc1:
        level_filter = st.selectbox(
            "Level", ["ALL", "INFO", "WARNING", "ERROR"], key="res_level"
        )
    with fc2:
        step_filter = st.selectbox(
            "Step", ["ALL"] + [str(i) for i in range(5)], key="res_step"
        )

    st.markdown(
        render_log_panel(entries, level_filter, step_filter, tall=tall),
        unsafe_allow_html=True,
    )
    st.caption(f"{len(entries)} total entries — filtered view above")

    with st.expander("Copy Logs for LLM Debugging", expanded=False):
        st.caption("One-click copy. Paste into your LLM with your question.")
        text = _format_logs_as_text(entries, level_filter, step_filter)
        st.code(text, language="text")


def _format_logs_as_text(
    entries: list[dict],
    level_filter: str = "ALL",
    step_filter: str = "ALL",
) -> str:
    filtered = entries
    if level_filter != "ALL":
        filtered = [e for e in filtered if e.get("level") == level_filter]
    if step_filter != "ALL":
        filtered = [e for e in filtered if str(e.get("step", "")) == step_filter]
    lines = [
        f"{e.get('time','')} [{e.get('level','')}] {e.get('logger','')}: {e.get('event','')}"
        for e in filtered
    ]
    return "\n".join(lines) if lines else "(no log entries)"


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="ETL Pipeline — HITL Wizard",
        page_icon="⚙",
        layout="wide",
    )
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    _init_state()
    _setup_logging()

    st.title("Schema-Driven ETL Pipeline")
    st.caption(
        "5-step HITL wizard — pick a CSV, review schema analysis, "
        "approve transforms, run pipeline, inspect results."
    )

    st.markdown(
        render_step_bar(
            st.session_state.step, STEPS, st.session_state.max_completed
        ),
        unsafe_allow_html=True,
    )

    # Sidebar: live log feed
    with st.sidebar:
        st.markdown("### Live Logs")
        entries = st.session_state.get("log_entries", [])
        if entries:
            sb_level = st.selectbox(
                "Level filter", ["ALL", "INFO", "WARNING", "ERROR"], key="sb_level"
            )
            recent = entries[-200:]
            st.markdown(
                render_log_panel(recent, sb_level),
                unsafe_allow_html=True,
            )
            st.caption(f"{len(entries)} total entries")
        else:
            st.caption("Logs appear here as pipeline steps run.")

    # Error banner
    if st.session_state.get("error"):
        st.error(f"**Last error:** {st.session_state.error}")

    step = st.session_state.step
    if step == 0:
        _step_0_source_selection()
    elif step == 1:
        _step_1_schema_analysis()
    elif step == 2:
        _step_2_code_generation()
    elif step == 3:
        _step_3_pipeline_execution()
    elif step == 4:
        _step_4_results()


if __name__ == "__main__":
    main()
