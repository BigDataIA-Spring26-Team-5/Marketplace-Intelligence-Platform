"""Pipeline Wizard — HITL step-through of the LangGraph ETL pipeline."""
from __future__ import annotations
import time
import streamlit as st

STEPS = [
    "load_source",
    "analyze_schema",
    "check_registry",
    "plan_sequence",
    "run_pipeline",
    "save_output",
]

STEP_LABELS = {
    "load_source":     "Load Source",
    "analyze_schema":  "Analyze Schema",
    "check_registry":  "Check Registry",
    "plan_sequence":   "Plan Sequence",
    "run_pipeline":    "Run Pipeline",
    "save_output":     "Save Output",
}

DOMAINS = ["nutrition", "safety", "pricing", "retail", "finance", "manufacturing"]


def _stepper_html(current_step_idx: int) -> str:
    html = '<div class="stepper">'
    for i, s in enumerate(STEPS):
        if i < current_step_idx:
            circle_cls = "done"
            label_cls  = "done"
            circle_inner = "✓"
        elif i == current_step_idx:
            circle_cls = "active"
            label_cls  = "active"
            circle_inner = str(i + 1)
        else:
            circle_cls = ""
            label_cls  = ""
            circle_inner = str(i + 1)

        html += f"""
        <div class="step">
          <div class="step-node">
            <div class="step-circle {circle_cls}">{circle_inner}</div>
            <div class="step-label {label_cls}">{STEP_LABELS[s]}</div>
          </div>"""
        if i < len(STEPS) - 1:
            line_cls = "done" if i < current_step_idx else ""
            html += f'<div class="step-line {line_cls}"></div>'
        html += "</div>"
    html += "</div>"
    return html


def _block_chips_html(blocks: list[str], current: str | None) -> str:
    html = '<div class="block-chips">'
    for b in blocks:
        if b == current:
            cls = "running"
        elif current and blocks.index(b) < blocks.index(current):
            cls = "done"
        else:
            cls = ""
        html += f'<div class="block-chip {cls}">{b}</div>'
    html += "</div>"
    return html


_PRIM_COLOR = {
    "RENAME": "info", "CAST": "warning", "FORMAT": "warning",
    "DERIVE": "purple", "SPLIT": "purple", "UNIFY": "purple",
    "ADD": "orange", "DELETE": "error", "ENRICH_ALIAS": "running",
}


def _render_hitl_panels(results: dict, current_step: int):
    """Render HITL review cards for all completed steps."""

    # ── load_source: data preview + schema ───────────────────────────────────
    ls = results.get("load_source", {})
    if ls:
        preview = ls.get("preview_rows", [])
        cols_info = ls.get("cols_info", [])
        n_cols = ls.get("n_cols", 0)
        n_rows = ls.get("n_rows_sample", 0)

        with st.expander(f"📥 Source Preview — {n_rows} rows × {n_cols} cols", expanded=(current_step == 2)):
            if preview and cols_info:
                col_names = [c["column"] for c in cols_info]
                header = "".join(f'<th>{c}</th>' for c in col_names[:8])
                body = ""
                for row in preview[:6]:
                    cells = "".join(
                        f'<td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                        f'{str(row.get(c,""))[:30]}</td>'
                        for c in col_names[:8]
                    )
                    body += f"<tr>{cells}</tr>"
                st.markdown(
                    f'<div style="overflow-x:auto"><table class="data-table" style="font-size:12px">'
                    f'<thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></div>',
                    unsafe_allow_html=True,
                )
            # Column type summary
            type_rows = "".join(
                f'<tr><td class="mono">{c["column"]}</td>'
                f'<td>{c["dtype"]}</td>'
                f'<td style="color:{"var(--red)" if c["null_pct"]>20 else "var(--text-muted)"}">{c["null_pct"]}%</td>'
                f'<td class="tc-dim" style="font-size:12px">{", ".join(str(v) for v in c["sample"][:2])}</td></tr>'
                for c in cols_info[:12]
            )
            st.markdown(
                f'<table class="data-table" style="margin-top:10px;font-size:13px">'
                f'<thead><tr><th>Column</th><th>Type</th><th>Null%</th><th>Samples</th></tr></thead>'
                f'<tbody>{type_rows}</tbody></table>',
                unsafe_allow_html=True,
            )

    # ── analyze_schema: column mapping + operations ───────────────────────────
    az = results.get("analyze_schema", {})
    if az:
        mapping_rows = az.get("mapping_rows", [])
        op_rows = az.get("op_rows", [])
        n_gaps = az.get("n_gaps", 0)
        cache_hit = az.get("cache_hit", False)
        hit_label = " 🗲 cache hit" if cache_hit else ""

        with st.expander(f"🧠 Schema Delta — {len(mapping_rows)} mapped, {n_gaps} gaps{hit_label}", expanded=(current_step == 3)):
            if mapping_rows:
                st.markdown("**Column Mapping** (source → unified schema):", unsafe_allow_html=False)
                rows_html = ""
                for r in mapping_rows:
                    prim = r["primitive"]
                    badge_cls = _PRIM_COLOR.get(prim, "info")
                    rows_html += (
                        f'<tr><td class="mono" style="font-size:13px">{r["source"]}</td>'
                        f'<td><span class="badge {badge_cls}" style="font-size:11px">{prim}</span></td>'
                        f'<td class="mono tc-accent" style="font-size:13px">{r["unified"]}</td></tr>'
                    )
                st.markdown(
                    f'<table class="data-table" style="font-size:13px">'
                    f'<thead><tr><th>Source Column</th><th>Op</th><th>Unified Column</th></tr></thead>'
                    f'<tbody>{rows_html}</tbody></table>',
                    unsafe_allow_html=True,
                )

            if op_rows:
                st.markdown("**All Operations:**", unsafe_allow_html=False)
                op_html = ""
                for op in op_rows:
                    prim = op["primitive"]
                    badge_cls = _PRIM_COLOR.get(prim, "info")
                    reason = f'<div class="tc-dim" style="font-size:11px;margin-top:2px">{op["reason"]}</div>' if op["reason"] else ""
                    op_html += (
                        f'<div style="display:flex;gap:8px;align-items:flex-start;margin-bottom:6px;padding:6px 8px;'
                        f'background:var(--surface);border-radius:4px;border:1px solid var(--border)">'
                        f'<span class="badge {badge_cls}" style="font-size:11px;flex-shrink:0">{prim}</span>'
                        f'<div><span class="mono" style="font-size:13px">{op["source"]}</span>'
                        f'<span class="tc-dim"> → </span>'
                        f'<span class="mono tc-accent" style="font-size:13px">{op["target"]}</span>'
                        f'{reason}</div></div>'
                    )
                st.markdown(op_html, unsafe_allow_html=True)

    # ── check_registry: generated YAML ───────────────────────────────────────
    cr = results.get("check_registry", {})
    if cr:
        yaml_text = cr.get("yaml_text", "")
        yaml_path = cr.get("yaml_path", "")
        n_hits = cr.get("n_hits", 0)
        label = yaml_path.split("/")[-1] if yaml_path else "mapping"

        with st.expander(f"⚙️ Generated YAML — {label} ({n_hits} registry hits)", expanded=(current_step == 4)):
            if n_hits:
                hits = cr.get("block_hits", {})
                hits_html = "".join(
                    f'<span class="badge success" style="font-size:11px;margin:2px">{k}</span>'
                    for k in list(hits.keys())[:10]
                )
                st.markdown(f'<div style="margin-bottom:8px">Registry hits: {hits_html}</div>', unsafe_allow_html=True)
            if yaml_text:
                st.code(yaml_text, language="yaml")
            else:
                st.markdown('<div class="tc-dim">No YAML generated (all columns mapped via registry)</div>', unsafe_allow_html=True)

    # ── plan_sequence: block order + reasoning ────────────────────────────────
    ps_res = results.get("plan_sequence", {})
    if ps_res:
        seq = ps_res.get("block_sequence", [])
        reasoning = ps_res.get("reasoning", "")
        skipped = ps_res.get("skipped", {})

        with st.expander(f"📋 Block Sequence — {len(seq)} blocks", expanded=(current_step == 5)):
            running_block = results.get("run_pipeline", {}).get("current_block")
            if seq:
                st.markdown(_block_chips_html(seq, running_block), unsafe_allow_html=True)
            if reasoning:
                st.markdown(
                    f'<div class="terminal" style="margin-top:10px;font-size:13px;padding:10px">'
                    f'<div class="tc-dim" style="font-size:11px;margin-bottom:4px">AGENT 3 REASONING</div>'
                    f'{reasoning}</div>',
                    unsafe_allow_html=True,
                )
            if skipped:
                skip_html = "".join(
                    f'<div style="font-size:12px"><span class="mono tc-amber">{k}</span>'
                    f'<span class="tc-dim"> — {v}</span></div>'
                    for k, v in skipped.items()
                )
                st.markdown(f'<div style="margin-top:8px">{skip_html}</div>', unsafe_allow_html=True)

    # ── run_pipeline: row counts + DQ ────────────────────────────────────────
    rp = results.get("run_pipeline", {})
    if rp and rp.get("rows_in") is not None:
        rows_in  = rp["rows_in"]
        rows_out = rp.get("rows_out", 0)
        dq_pre   = rp.get("dq_pre")
        dq_post  = rp.get("dq_post")
        dq_delta = rp.get("dq_delta")
        delta_str = ""
        if dq_delta is not None:
            sign = "+" if dq_delta >= 0 else ""
            delta_str = f' <span style="color:{"var(--green)" if dq_delta>=0 else "var(--red)"}">{sign}{dq_delta:.2f}</span>'

        with st.expander("🏃 Pipeline Execution", expanded=(current_step == 6)):
            st.markdown(
                f'<div class="stat-card" style="margin-bottom:8px">'
                f'<div class="stat-label">Rows</div>'
                f'<div style="font-size:20px;font-weight:700">{rows_in:,} → {rows_out:,}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if dq_pre is not None:
                st.markdown(
                    f'<div class="stat-card">'
                    f'<div class="stat-label">DQ Score</div>'
                    f'<div style="font-size:20px;font-weight:700">{dq_pre:.1f} → '
                    f'<span style="color:var(--green)">{dq_post:.1f}</span>{delta_str}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── save_output ───────────────────────────────────────────────────────────
    so = results.get("save_output", {})
    if so:
        with st.expander("💾 Output", expanded=(current_step == 7)):
            uri = so.get("silver_uri") or so.get("output_path", "")
            score = so.get("dq_score_post")
            if uri:
                st.markdown(f'<div class="mono tc-accent" style="font-size:13px;word-break:break-all">{uri}</div>', unsafe_allow_html=True)
            if score is not None:
                st.markdown(f'<div style="margin-top:8px">Final DQ: <strong style="color:var(--green)">{score:.2f}</strong></div>', unsafe_allow_html=True)


def render_pipeline():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Pipeline Wizard</div>
        <div class="page-subtitle">Human-in-the-loop ETL execution with live block tracing</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    ps  = st.session_state.pipeline_state
    step = st.session_state.get("step", 0)

    # ── Step 0: Configure source ──────────────────────────────────────────────
    if step == 0 and not ps.get("source_path"):
        st.markdown(_stepper_html(0), unsafe_allow_html=True)

        # ── Demo Bronze push ──────────────────────────────────────────────────
        st.markdown("""
        <div class="alert orange" style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
          <span style="font-size:18px;">📤</span>
          <div>
            <strong>Demo Mode</strong> — push a 50-row USDA slice to GCS Bronze,
            then run the full pipeline against it in one click.
          </div>
        </div>""", unsafe_allow_html=True)

        d1, d2 = st.columns([1, 3])
        with d1:
            push_clicked = st.button("📤  Push Demo Data → Bronze", use_container_width=True)
        with d2:
            if st.session_state.get("demo_uri"):
                st.markdown(
                    f'<div class="alert green" style="margin:0;padding:8px 12px;">'
                    f'✓ Pushed → <span class="mono">{st.session_state.demo_uri}</span></div>',
                    unsafe_allow_html=True,
                )
            elif st.session_state.get("demo_err"):
                st.markdown(
                    f'<div class="alert red" style="margin:0;padding:8px 12px;">'
                    f'✗ {st.session_state.demo_err}</div>',
                    unsafe_allow_html=True,
                )

        if push_clicked:
            from src.ui.utils.demo_push import push_demo_bronze
            with st.spinner("Pushing 50 rows to GCS Bronze…"):
                ok, uri, msg = push_demo_bronze()
            if ok:
                st.session_state.demo_uri = uri
                st.session_state.demo_err = ""
            else:
                st.session_state.demo_uri = ""
                st.session_state.demo_err = msg
            st.rerun()

        st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
        st.markdown('<div class="card"><div class="card-title">Source Configuration</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        # Pre-fill source from demo push if available
        _default_src = st.session_state.get("demo_uri", "")
        with col1:
            source_path = st.text_input(
                "Source path / GCS URI",
                value=_default_src,
                placeholder="data/usda_fooddata_sample.csv",
            )
        with col2:
            domain = st.selectbox("Domain", DOMAINS)

        col3, col4 = st.columns(2)
        with col3:
            pipeline_mode = st.selectbox("Pipeline mode", ["full", "silver", "gold"])
        with col4:
            with_critic = st.toggle("Enable Agent 2 (Critic)", value=False)

        resume = st.checkbox("Resume from checkpoint")
        force_fresh = st.checkbox("Force fresh (bypass cache)")
        st.markdown("</div>", unsafe_allow_html=True)

        if st.button("▶  Start Pipeline", type="primary", use_container_width=True):
            if not source_path:
                st.error("Source path required.")
            else:
                st.session_state.pipeline_state = {
                    "source_path": source_path,
                    "domain": domain,
                    "pipeline_mode": pipeline_mode,
                    "with_critic": with_critic,
                    "resume": resume,
                    "force_fresh": force_fresh,
                    "step_results": {},
                }
                st.session_state.step = 1
                st.session_state.log_entries = []
                st.rerun()
        return

    # ── Pipeline running: steps 1–6 ──────────────────────────────────────────
    current_step_idx = min(step - 1, len(STEPS) - 1)
    st.markdown(_stepper_html(current_step_idx), unsafe_allow_html=True)

    source = ps.get("source_path", "")
    domain = ps.get("domain", "nutrition")

    info_c, ctrl_c = st.columns([3, 1])
    with info_c:
        st.markdown(f"""
        <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px;">
          <span class="badge info">{ps.get("pipeline_mode","full")}</span>
          <span class="badge purple">{domain}</span>
          <span class="mono tc-dim">{source}</span>
          {"<span class='badge warning'>critic on</span>" if ps.get("with_critic") else ""}
        </div>""", unsafe_allow_html=True)
    with ctrl_c:
        if st.button("✕  Reset", use_container_width=True):
            st.session_state.step = 0
            st.session_state.pipeline_state = {}
            st.session_state.log_entries = []
            st.rerun()

    # Left: live log terminal, Right: block chips + agent outputs
    left, right = st.columns([3, 2])

    with left:
        log_entries = st.session_state.get("log_entries", [])
        log_html = ""
        for entry in log_entries[-60:]:
            cls  = entry.get("cls", "")
            text = entry.get("text", "")
            log_html += f'<div class="{cls}">{text}</div>'

        if step <= len(STEPS):
            current_step_name = STEPS[current_step_idx]
            # Show waiting indicator — not "Running" (misleading before button click)
            log_html += (
                f'<div class="t-dim" style="margin-top:8px;border-top:1px solid #dee2e6;'
                f'padding-top:8px;">⏸ Awaiting approval — click '
                f'<strong>▶ Run {STEP_LABELS[current_step_name]}</strong> below to execute</div>'
            )

        st.markdown(f"""
        <div class="card">
          <div class="card-title">Live Log</div>
          <div class="terminal" style="height:340px;overflow-y:auto;">{log_html}</div>
        </div>""", unsafe_allow_html=True)

    with right:
        results = ps.get("step_results", {})
        _render_hitl_panels(results, step)

    # ── Execute current step ──────────────────────────────────────────────────
    if step <= len(STEPS):
        step_name = STEPS[step - 1]
        st.markdown("<hr style='border:none;border-top:1px solid #dee2e6;margin:8px 0 12px'>", unsafe_allow_html=True)
        col_run, col_skip = st.columns([2, 1])
        with col_run:
            run_clicked = st.button(
                f"▶  Run {STEP_LABELS[step_name]}",
                type="primary",
                use_container_width=True,
                key=f"run_btn_{step}",
            )
        with col_skip:
            skip_clicked = st.button(
                "⏭  Skip",
                use_container_width=True,
                key=f"skip_btn_{step}",
            )

        if run_clicked or skip_clicked:
            if run_clicked:
                with st.spinner(f"Running {STEP_LABELS[step_name]}…"):
                    try:
                        from src.agents.graph import run_step
                        state = dict(ps)
                        state.setdefault("step_results", {})
                        result_state = run_step(step_name, state)
                        ps["step_results"][step_name] = _extract_step_summary(step_name, result_state)
                        ps.update({k: v for k, v in result_state.items() if k != "step_results"})
                        st.session_state.log_entries.append({
                            "cls": "t-green",
                            "text": f"✓ {step_name} completed",
                        })
                    except Exception as e:
                        st.session_state.log_entries.append({
                            "cls": "t-red",
                            "text": f"✗ {step_name} failed: {e}",
                        })
            else:
                st.session_state.log_entries.append({
                    "cls": "t-dim",
                    "text": f"⟶ {step_name} skipped",
                })

            st.session_state.pipeline_state = ps
            st.session_state.step = step + 1
            st.rerun()
    else:
        st.markdown("""
        <div class="alert green">✓ Pipeline complete — all steps executed successfully.</div>
        """, unsafe_allow_html=True)
        if st.button("↺  Run Another", use_container_width=True):
            st.session_state.step = 0
            st.session_state.pipeline_state = {}
            st.session_state.log_entries = []
            st.rerun()


def _extract_step_summary(step_name: str, state: dict) -> dict:
    if step_name == "load_source":
        df = state.get("source_df")
        schema = state.get("source_schema", {})
        cols_info = []
        for col, info in schema.items():
            if col == "__meta__":
                continue
            cols_info.append({
                "column": col,
                "dtype": info.get("dtype", ""),
                "null_pct": round(info.get("null_rate", 0) * 100, 1),
                "sample": info.get("sample_values", [])[:3],
            })
        preview_rows = []
        if df is not None:
            preview_rows = df.head(8).fillna("").astype(str).to_dict(orient="records")
        return {
            "cols_info": cols_info,
            "preview_rows": preview_rows,
            "n_rows_sample": len(df) if df is not None else 0,
            "n_cols": len(cols_info),
        }

    if step_name == "analyze_schema":
        col_map = state.get("column_mapping", {})
        operations = state.get("operations", [])
        gaps = state.get("gaps", [])
        missing = state.get("missing_columns", [])
        derivable = state.get("derivable_gaps", [])
        mapping_rows = []
        for src, tgt in col_map.items():
            prim = next(
                (op.get("primitive", "RENAME") for op in operations
                 if op.get("source_column") == src or op.get("target_column") == tgt),
                "RENAME",
            )
            mapping_rows.append({"source": src, "primitive": prim, "unified": tgt})
        op_rows = [
            {
                "primitive": op.get("primitive", ""),
                "source": op.get("source_column") or ", ".join(op.get("sources", [])) or "—",
                "target": op.get("target_column", ""),
                "reason": op.get("reason", ""),
            }
            for op in operations
        ]
        return {
            "mapping_rows": mapping_rows,
            "op_rows": op_rows,
            "n_gaps": len(gaps) + len(missing),
            "n_derivable": len(derivable),
            "cache_hit": bool(state.get("cache_yaml_hit")),
        }

    if step_name == "check_registry":
        yaml_path = state.get("mapping_yaml_path")
        yaml_text = ""
        if yaml_path:
            try:
                from pathlib import Path as _P
                yaml_text = _P(yaml_path).read_text(encoding="utf-8")
            except Exception:
                yaml_text = f"(could not read {yaml_path})"
        hits = state.get("block_registry_hits", {})
        return {
            "yaml_text": yaml_text,
            "yaml_path": yaml_path or "",
            "block_hits": hits,
            "n_hits": len(hits),
        }

    if step_name == "plan_sequence":
        return {
            "block_sequence": state.get("block_sequence", []),
            "reasoning": state.get("sequence_reasoning", ""),
            "skipped": state.get("skipped_blocks", {}),
        }

    if step_name == "run_pipeline":
        return {
            "rows_in":  state.get("rows_in"),
            "rows_out": state.get("rows_out"),
            "dq_pre":   state.get("dq_score_pre"),
            "dq_post":  state.get("dq_score_post"),
            "dq_delta": state.get("dq_delta"),
            "current_block": None,
        }

    if step_name == "save_output":
        return {
            "dq_score_post":   state.get("dq_score_post"),
            "output_path":     state.get("output_path", ""),
            "silver_uri":      state.get("silver_output_uri", ""),
        }
    return {}
