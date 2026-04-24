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

        st.markdown('<div class="card"><div class="card-title">Source Configuration</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            source_path = st.text_input("Source path / GCS URI", placeholder="data/usda_fooddata_sample.csv")
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
            log_html += f'<div><span class="stream-dot"></span><span class="t-blue">Running {current_step_name}...</span></div>'

        st.markdown(f"""
        <div class="card">
          <div class="card-title">Live Log</div>
          <div class="terminal" style="height:340px;overflow-y:auto;">{log_html}</div>
        </div>""", unsafe_allow_html=True)

    with right:
        results = ps.get("step_results", {})
        # Block chips (visible after plan_sequence)
        block_seq = results.get("plan_sequence", {}).get("block_sequence", [])
        if block_seq:
            running_block = results.get("run_pipeline", {}).get("current_block")
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Block Sequence</div>
              {_block_chips_html(block_seq, running_block)}
            </div>""", unsafe_allow_html=True)

        # Agent outputs
        for sname, sdata in results.items():
            if not sdata:
                continue
            ops   = sdata.get("operations", [])
            gaps  = sdata.get("gaps", [])
            score = sdata.get("dq_score_post")
            rows_in  = sdata.get("rows_in")
            rows_out = sdata.get("rows_out")

            if ops or gaps or score is not None or rows_in is not None:
                inner = ""
                if rows_in is not None:
                    inner += f'<div class="resolve-row">Rows in: <strong>{rows_in:,}</strong> → out: <strong>{rows_out:,}</strong></div>'
                if score is not None:
                    inner += f'<div class="resolve-row">DQ post: <strong style="color:var(--green)">{score:.2f}</strong></div>'
                if ops:
                    inner += f'<div class="tc-dim" style="font-size:12px;margin-top:6px;">{len(ops)} operations planned</div>'
                if gaps:
                    inner += f'<div class="tc-amber" style="font-size:12px;">{len(gaps)} schema gaps detected</div>'

                st.markdown(f"""
                <div class="card">
                  <div class="card-title">{STEP_LABELS.get(sname, sname)}</div>
                  {inner}
                </div>""", unsafe_allow_html=True)

    # ── Execute current step ──────────────────────────────────────────────────
    if step <= len(STEPS):
        step_name = STEPS[step - 1]
        col_run, col_skip = st.columns([1, 4])
        with col_run:
            run_clicked = st.button(f"▶  Run {STEP_LABELS[step_name]}", type="primary")
        with col_skip:
            skip_clicked = st.button("⏭  Skip")

        if run_clicked or skip_clicked:
            if run_clicked:
                # Actually invoke the pipeline node
                try:
                    from src.agents.graph import run_step
                    state = dict(ps)
                    state.setdefault("step_results", {})
                    result_state = run_step(step_name, state)
                    ps["step_results"][step_name] = _extract_step_summary(step_name, result_state)
                    ps.update({k: v for k, v in result_state.items() if k != "step_results"})
                    st.session_state.log_entries.append({
                        "cls": "t-green",
                        "text": f"✓ {step_name} completed"
                    })
                except Exception as e:
                    st.session_state.log_entries.append({
                        "cls": "t-red",
                        "text": f"✗ {step_name} failed: {e}"
                    })
            else:
                st.session_state.log_entries.append({
                    "cls": "t-dim",
                    "text": f"⟶ {step_name} skipped"
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
    if step_name == "analyze_schema":
        return {
            "operations": state.get("operations", []),
            "gaps": state.get("gaps", []),
        }
    if step_name == "plan_sequence":
        return {"block_sequence": state.get("block_sequence", [])}
    if step_name == "run_pipeline":
        return {
            "rows_in":  state.get("rows_in"),
            "rows_out": state.get("rows_out"),
            "current_block": None,
        }
    if step_name == "save_output":
        return {
            "dq_score_post": state.get("dq_score_post"),
        }
    return {}
