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
        "cache_client": None,
        "cache_no_cache": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    # Initialize CacheClient once per session if not already done
    if st.session_state.get("cache_client") is None and not st.session_state.get("cache_no_cache"):
        try:
            from src.cache.client import CacheClient
            st.session_state.cache_client = CacheClient()
        except Exception:
            st.session_state.cache_client = None


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
            "cache_client": st.session_state.get("cache_client"),
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


# ── UC3 Search ────────────────────────────────────────────────────────────


def _render_search_page() -> None:
    from src.uc3_search.hybrid_search import HybridSearch
    from src.uc3_search.indexer import ProductIndexer

    st.header("Product Search (UC3)")
    st.caption("Hybrid BM25 + Semantic search with Reciprocal Rank Fusion over the unified gold catalog.")

    # Build index button
    col_info, col_btn = st.columns([4, 1])
    with col_btn:
        if st.button("Build / Refresh Index", key="uc3_build"):
            with st.spinner("Loading gold catalog from BigQuery and building indexes…"):
                try:
                    from google.cloud import bigquery
                    client = bigquery.Client(project="mip-platform-2024")
                    df = client.query(
                        "SELECT product_name, brand_name, primary_category, ingredients, "
                        "allergens, dietary_tags, is_organic, dq_score_post, data_source, "
                        "is_recalled, recall_class "
                        "FROM `mip-platform-2024.mip_gold.products` WHERE product_name IS NOT NULL"
                    ).to_dataframe()
                    indexer = ProductIndexer()
                    n = indexer.build(df)
                    st.session_state.uc3_search = HybridSearch()
                    st.success(f"Indexed {n} products. Search ready!")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Index build failed: {exc}")

    if "uc3_search" not in st.session_state:
        st.session_state.uc3_search = HybridSearch()

    hs: HybridSearch = st.session_state.uc3_search

    with col_info:
        if hs.is_ready():
            st.success("Search indexes loaded and ready.")
        else:
            st.warning("Indexes not built yet — click **Build / Refresh Index** to load the gold catalog.")
            return

    st.markdown("---")

    _EXAMPLE_QUERIES = [
        ("Semantic", "organic gluten-free oat cereal"),
        ("Keyword",  "peanut butter"),
        ("Recall",   "romaine lettuce"),
        ("Brand",    "Kraft cheddar cheese"),
        ("Safety",   "nut-free snack for kids"),
    ]
    st.caption("**Example queries** — click to load:")
    ex_cols = st.columns(len(_EXAMPLE_QUERIES))
    for col, (label, q) in zip(ex_cols, _EXAMPLE_QUERIES):
        with col:
            if st.button(f"{label}", key=f"uc3_ex_{label}"):
                st.session_state["uc3_query_val"] = q

    default_query = st.session_state.get("uc3_query_val", "")
    query = st.text_input("Search products", value=default_query,
                          placeholder="e.g., organic gluten-free cereal", key="uc3_query")

    col1, col2, col3 = st.columns(3)
    with col1:
        search_mode = st.selectbox("Search mode", ["hybrid", "bm25", "semantic"], key="uc3_mode")
    with col2:
        top_k = st.slider("Results", 1, 20, 10, key="uc3_topk")
    with col3:
        suppress = st.checkbox("Hide Class I recalled products", value=True, key="uc3_suppress")

    if query:
        with st.spinner("Searching…"):
            results = hs.search(query, top_k=top_k, mode=search_mode, suppress_recalled=suppress)

        if not results:
            st.info("No results found.")
            return

        st.caption(f"{len(results)} result(s) — mode: **{search_mode}**")
        import pandas as pd
        cols_show = ["rank", "product_name", "brand_name", "primary_category",
                     "dietary_tags", "allergens", "is_organic", "dq_score_post",
                     "data_source", "score"]
        df_results = pd.DataFrame(results)
        show_cols = [c for c in cols_show if c in df_results.columns]
        st.dataframe(df_results[show_cols], use_container_width=True)


# ── UC4 Recommendations ───────────────────────────────────────────────────


def _render_recommendations_page() -> None:
    from src.uc4_recommendations.recommender import ProductRecommender

    st.header("Product Recommendations (UC4)")
    st.caption("Association rules (also-bought) + graph traversal (cross-category) from Instacart + UC1 enriched catalog.")

    if "uc4_rec" not in st.session_state:
        if ProductRecommender.is_saved():
            try:
                st.session_state.uc4_rec = ProductRecommender.load()
            except Exception:
                st.session_state.uc4_rec = ProductRecommender()
        else:
            st.session_state.uc4_rec = ProductRecommender()

    rec: ProductRecommender = st.session_state.uc4_rec

    if not rec.is_ready():
        st.info("Recommender not built. Load Instacart transaction data from BigQuery to start.")
        col_s, col_b = st.columns([3, 1])
        with col_s:
            sample_orders = st.number_input("Sample orders to load", min_value=1000,
                                             max_value=500000, value=50000, step=10000,
                                             key="uc4_sample")
        with col_b:
            st.write("")
            if st.button("Load from BigQuery", key="uc4_load"):
                with st.spinner("Loading Instacart data from BigQuery — this may take a few minutes…"):
                    try:
                        tx_df, prod_df = ProductRecommender.load_from_bigquery(
                            sample_orders=int(sample_orders)
                        )
                        stats = rec.build(prod_df, tx_df)
                        rec.save()
                        st.session_state.uc4_rec = rec
                        st.session_state.uc4_tx_df = tx_df
                        st.success("Recommender ready! (saved to disk)")
                        st.json(stats)
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Build failed: {exc}")
        return

    stats = rec.stats()
    c1, c2, c3 = st.columns(3)
    c1.metric("Products", f"{stats['products']:,}")
    c2.metric("Association Rules", f"{stats['rules']:,}")
    c3.metric("Graph Edges", f"{stats['graph'].get('copurchase_edges', 0):,}")

    # Pull example products from rules for UI hints
    examples = rec.top_antecedents(n=8)
    example_names = [e["product_name"] for e in examples]

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["Also Bought", "You Might Like", "Before vs After (Demo)"])

    with tab1:
        st.caption("Co-purchase association rules (FP-Growth). Type a product name or pick an example.")
        if example_names:
            ex1 = st.selectbox("Quick examples", ["— type below or pick —"] + example_names, key="uc4_ex1")
        else:
            ex1 = None
        default1 = ex1 if ex1 and ex1 != "— type below or pick —" else ""
        pid = st.text_input("Product name or ID", value=default1, key="uc4_pid_also",
                            placeholder="e.g., Organic Yellow Onion")
        if pid:
            recs = rec.also_bought(pid, top_k=10)
            if recs:
                found_pid = rec.find_product(pid)
                found_name = rec._get_product_name(found_pid) if found_pid else pid
                st.write(f"**Showing rules for:** {found_name}")
                st.dataframe(pd.DataFrame(recs), use_container_width=True)
            else:
                st.info("No co-purchase rules found. Try one of the example products above.")

    with tab2:
        st.caption("Cross-category affinity via graph traversal. Finds products in different departments.")
        if example_names:
            ex2 = st.selectbox("Quick examples", ["— type below or pick —"] + example_names, key="uc4_ex2")
        else:
            ex2 = None
        default2 = ex2 if ex2 and ex2 != "— type below or pick —" else ""
        pid2 = st.text_input("Product name or ID", value=default2, key="uc4_pid_like",
                             placeholder="e.g., Limes")
        if pid2:
            recs2 = rec.you_might_like(pid2, top_k=10)
            if recs2:
                found_pid2 = rec.find_product(pid2)
                found_name2 = rec._get_product_name(found_pid2) if found_pid2 else pid2
                st.write(f"**Showing cross-category for:** {found_name2}")
                st.dataframe(pd.DataFrame(recs2), use_container_width=True)
            else:
                st.info("No cross-category recommendations found. Products may share the same department.")

    with tab3:
        st.caption(
            "Shows lift improvement from UC1 deduplication. "
            "**Raw** uses product names as IDs (text fragmentation). "
            "**Enriched** uses canonical product IDs (consolidated signal)."
        )
        if example_names:
            ex3 = st.selectbox("Quick examples (pick a product with high lift)",
                               ["— type below or pick —"] + example_names, key="uc4_ex3")
        else:
            ex3 = None
        default3 = ex3 if ex3 and ex3 != "— type below or pick —" else ""
        pid3 = st.text_input("Product name or ID", value=default3, key="uc4_pid_demo",
                             placeholder="e.g., Organic Yellow Onion")

        if pid3 and st.button("Run Comparison", key="uc4_demo_btn"):
            # Use stored tx_df if available; otherwise fetch a small sample
            if "uc4_tx_df" not in st.session_state:
                with st.spinner("Fetching transaction sample from BigQuery for demo…"):
                    try:
                        tx_df_demo, _ = ProductRecommender.load_from_bigquery(sample_orders=50000)
                        st.session_state.uc4_tx_df = tx_df_demo
                    except Exception as exc:
                        st.error(f"Failed to load transactions: {exc}")
                        st.stop()
            tx_df = st.session_state.uc4_tx_df

            with st.spinner("Mining rules on raw (names) vs enriched (IDs) — ~30 sec…"):
                try:
                    result = rec.demo_comparison(tx_df, pid3, top_k=5)

                    st.markdown(f"### Results for: **{result['product_name']}**")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.subheader("Raw (before UC1)")
                        st.caption("product_name as ID — text variants fragment signal")
                        st.metric("Max Lift", result["max_lift_raw"])
                        st.metric("Unique Product IDs", f"{result['raw_unique_ids']:,}")
                        if result["raw_recommendations"]:
                            st.dataframe(pd.DataFrame(result["raw_recommendations"]),
                                         use_container_width=True)
                        else:
                            st.info("No rules found in raw data.")
                    with col_b:
                        st.subheader("Enriched (after UC1)")
                        st.caption("canonical product_id — consolidated signal")
                        delta = result["lift_improvement"]
                        st.metric("Max Lift", result["max_lift_enriched"],
                                  delta=f"+{delta:.4f}" if delta >= 0 else f"{delta:.4f}")
                        st.metric("Unique Product IDs", f"{result['enriched_unique_ids']:,}")
                        if result["enriched_recommendations"]:
                            st.dataframe(pd.DataFrame(result["enriched_recommendations"]),
                                         use_container_width=True)
                        else:
                            st.info("No rules found in enriched data.")

                    st.metric("Signal Consolidation Ratio",
                              result["signal_consolidation_ratio"],
                              help="raw_unique_ids / enriched_unique_ids — higher = more fragmentation in raw")
                except Exception as exc:
                    st.error(f"Comparison failed: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────


_GRAFANA_BASE    = "http://35.239.47.242:3000"
_GRAFANA_DASH_UID = "etl-pipeline-observability"
_GRAFANA_DASH_URL = (
    f"{_GRAFANA_BASE}/d/{_GRAFANA_DASH_UID}/etl-pipeline-observability"
    "?orgId=1&kiosk=tv&theme=dark&refresh=30s"
)

_OBS_SAMPLE_QUESTIONS = [
    "What was the average DQ score across all runs?",
    "Which source had the most quarantined rows?",
    "Show me the last 5 pipeline runs and their status.",
    "Which run had the highest LLM enrichment cost?",
    "What enrichment tier breakdown do we see across sources?",
    "Were any anomalies detected recently?",
    "Compare DQ score before vs after pipeline for USDA runs.",
    "What is the average run duration per source?",
]


def _render_observability_page() -> None:
    from src.uc2_observability.log_store import RunLogStore
    from src.uc2_observability.rag_chatbot import ObservabilityChatbot

    if "obs_chatbot" not in st.session_state:
        store = RunLogStore()
        bot = ObservabilityChatbot(store)
        count = bot.ingest_audit_logs()
        st.session_state.obs_chatbot = bot
        st.session_state.obs_messages = []
        st.session_state.obs_last_refresh = datetime.now()
        st.session_state.obs_run_count = count

    bot: ObservabilityChatbot = st.session_state.obs_chatbot

    st.header("Pipeline Observability (UC2)")
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.caption(
            f"Loaded **{st.session_state.obs_run_count}** run log(s). "
            f"Last refresh: {st.session_state.obs_last_refresh.strftime('%H:%M:%S')}"
        )
    with col2:
        if st.button("Refresh logs", key="obs_refresh"):
            count = bot.ingest_audit_logs()
            st.session_state.obs_run_count = count
            st.session_state.obs_last_refresh = datetime.now()
            st.rerun()
    with col3:
        st.link_button("Open Grafana ↗", _GRAFANA_BASE)

    tab_dash, tab_chat = st.tabs(["Metrics Dashboard", "RAG Chatbot"])

    with tab_dash:
        st.caption(
            "Live Grafana dashboard — DQ scores, enrichment tiers, quarantine rates, run durations. "
            f"[Open full screen ↗]({_GRAFANA_DASH_URL})"
        )
        st.components.v1.iframe(_GRAFANA_DASH_URL, height=720, scrolling=True)

    with tab_chat:
        st.caption("Ask natural-language questions about pipeline run history.")

        # Sample question chips
        st.markdown("**Sample questions:**")
        cols = st.columns(2)
        for i, q in enumerate(_OBS_SAMPLE_QUESTIONS):
            if cols[i % 2].button(q, key=f"obs_sq_{i}", use_container_width=True):
                st.session_state.obs_pending_question = q

        st.markdown("---")

        # Render chat history
        for msg in st.session_state.obs_messages:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
                if msg.get("cited_run_ids"):
                    with st.expander(f"Cited run IDs ({len(msg['cited_run_ids'])})"):
                        for rid in msg["cited_run_ids"]:
                            st.code(rid)

        # Handle pending question from sample button click
        pending = st.session_state.pop("obs_pending_question", None)

        question = st.chat_input("Ask about pipeline runs…", key="obs_input") or pending
        if question:
            st.session_state.obs_messages.append({"role": "user", "content": question})
            with st.spinner("Thinking…"):
                response = bot.query(question)
            st.session_state.obs_messages.append({
                "role": "assistant",
                "content": response.answer,
                "cited_run_ids": response.cited_run_ids,
            })
            st.rerun()

        if st.session_state.obs_messages:
            if st.button("Clear chat", key="obs_clear"):
                st.session_state.obs_messages = []
                st.rerun()


def _render_test_coverage_page() -> None:
    """Test Coverage dashboard — parses /tmp/cov.json if present, else runs pytest."""
    import json
    import subprocess
    from pathlib import Path

    st.title("Test Coverage")
    st.caption("Unit + Integration + Property-based (Hypothesis) testing results.")

    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("Run tests + recompute", key="run_cov_btn"):
            with st.spinner("Running pytest…"):
                cfg = Path("/tmp/cov.ini")
                cfg.write_text(
                    "[run]\nsource = src\nomit =\n"
                    "    src/ui/*\n"
                    "    src/uc2_observability/streamlit_app.py\n"
                    "    src/uc2_observability/dashboard.py\n"
                    "    src/uc2_observability/anomaly_detection.py\n"
                    "    src/blocks/templates/*\n"
                )
                result = subprocess.run(
                    ["python3", "-m", "pytest",
                     "--cov-config=/tmp/cov.ini", "--cov=src",
                     "--cov-report=json:/tmp/cov.json",
                     "-q", "--ignore=tests/unit/unit_tests.py"],
                    capture_output=True, text=True, timeout=300,
                )
                st.session_state["cov_stdout_tail"] = result.stdout[-2000:]

    cov_path = Path("/tmp/cov.json")
    if not cov_path.exists():
        st.info("No coverage report yet. Click **Run tests + recompute** to generate.")
        return

    with open(cov_path) as f:
        data = json.load(f)

    totals = data["totals"]
    pct = totals["percent_covered"]
    total_stmts = totals["num_statements"]
    covered = totals["covered_lines"]
    missing = totals["missing_lines"]

    # Headline metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Coverage", f"{pct:.2f}%",
              delta=f"{pct - 80:.1f} vs 80% target" if pct >= 80 else None)
    c2.metric("Statements", f"{total_stmts:,}")
    c3.metric("Covered",    f"{covered:,}")
    c4.metric("Missing",    f"{missing:,}")

    # Testing strategies
    st.markdown("### Testing Strategies")
    strat_rows = [
        {"Strategy": "Unit testing",          "Status": "✓ Implemented",
         "Location": "tests/unit/",            "Files": 41, "Tests": "~850"},
        {"Strategy": "Integration testing",   "Status": "✓ Implemented",
         "Location": "tests/integration/ + tests/uc2_observability/",
         "Files": 7, "Tests": "~60"},
        {"Strategy": "Property-based (Hypothesis)", "Status": "✓ Implemented",
         "Location": "tests/property/",        "Files": 1, "Tests": "12"},
    ]
    st.table(pd.DataFrame(strat_rows))

    # Per-module table
    st.markdown("### Per-Module Coverage")
    files = data["files"]
    rows = []
    for path, info in files.items():
        s = info["summary"]
        rows.append({
            "Module": path,
            "Statements": s["num_statements"],
            "Covered": s["covered_lines"],
            "Missing": s["missing_lines"],
            "Coverage %": round(s["percent_covered"], 1),
        })
    df = pd.DataFrame(rows).sort_values("Coverage %", ascending=True)

    # Filter
    min_cov = st.slider("Show modules with coverage <=", 0, 100, 100, key="cov_slider")
    filtered = df[df["Coverage %"] <= min_cov]
    st.dataframe(filtered, use_container_width=True, height=400)

    # Highlight worst
    st.markdown("### Remaining Gaps (lowest 10)")
    st.table(df.head(10).reset_index(drop=True))

    # Last pytest output
    tail = st.session_state.get("cov_stdout_tail")
    if tail:
        with st.expander("Last pytest output (tail)"):
            st.code(tail, language="text")


def main() -> None:
    st.set_page_config(
        page_title="ETL Pipeline — HITL Wizard",
        page_icon="⚙",
        layout="wide",
    )
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    _init_state()
    _setup_logging()

    # Consume _mode_override written by "Run Pipeline" button in domain_kits.py (FR-8)
    _mode_override = st.session_state.pop("_mode_override", None)
    _domain_override = st.session_state.pop("_domain_override", None)
    if _mode_override in ("Pipeline", "Observability", "Domain Packs"):
        st.session_state["app_mode"] = _mode_override
    if _domain_override:
        st.session_state["domain_select"] = _domain_override

    # Sidebar: mode selector + cache controls + live log feed
    with st.sidebar:
        mode = st.radio("Mode", ["Pipeline", "Search", "Recommendations", "Observability", "MLflow", "EDA", "Test Coverage"], key="app_mode")
        _modes = ["Pipeline", "Observability", "Domain Packs"]
        _current_mode_idx = _modes.index(st.session_state.get("app_mode", "Pipeline"))
        mode = st.radio("Mode", _modes, index=_current_mode_idx, key="app_mode")
        st.markdown("---")
        st.markdown("### Cache Controls")
        no_cache = st.checkbox(
            "Bypass cache (--no-cache)",
            value=st.session_state.get("cache_no_cache", False),
            key="cache_no_cache_toggle",
        )
        if no_cache != st.session_state.get("cache_no_cache", False):
            st.session_state.cache_no_cache = no_cache
            if no_cache:
                st.session_state.cache_client = None
            else:
                from src.cache.client import CacheClient
                st.session_state.cache_client = CacheClient()
        if st.button("Flush cache", key="flush_cache_btn"):
            cc = st.session_state.get("cache_client")
            if cc is not None:
                deleted = cc.flush_all_prefixes()
                st.success(f"Flushed {deleted} cache keys")
            else:
                st.warning("Cache not connected or bypass mode active")
        st.markdown("---")
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

    if mode == "Pipeline":
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
    elif mode == "Search":
        _render_search_page()
    elif mode == "Recommendations":
        _render_recommendations_page()
    elif mode == "Observability":
        _render_observability_page()
    elif mode == "MLflow":
        from src.uc2_observability.mlflow_streamlit import render_mlflow_page
        render_mlflow_page()
    elif mode == "Domain Packs":
        from src.ui.domain_kits import render_domain_kits_page
        render_domain_kits_page()


if __name__ == "__main__":
    main()
