"""MLflow Tracker — experiments, runs table, metrics charts."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import mlflow_experiments, mlflow_runs

MLFLOW_URL = "http://localhost:5000"


def render_mlflow():
    st.markdown(f"""
    <div class="page-header">
      <div>
        <div class="mlflow-logo">
          <div class="mlflow-logo-icon">ML</div>
          MLflow Tracker
        </div>
        <div class="page-subtitle" style="margin-top:6px;">Experiment tracking, run metrics, and model registry</div>
      </div>
      <a href="{MLFLOW_URL}" target="_blank"
         style="font-size:13px;color:var(--accent);text-decoration:none;font-weight:600;">
        Open MLflow UI ↗
      </a>
    </div>
    """, unsafe_allow_html=True)

    # ── Experiment picker ─────────────────────────────────────────────────────
    experiments = mlflow_experiments()
    if not experiments:
        st.markdown('<div class="alert orange">MLflow unreachable — check that the server is running on :5000</div>', unsafe_allow_html=True)
        return

    exp_names = [e["name"] for e in experiments]
    exp_ids   = {e["name"]: e["id"] for e in experiments}

    sel_exp = st.selectbox("Experiment", exp_names, key="mlflow_exp")
    exp_id  = exp_ids[sel_exp]

    runs = mlflow_runs(exp_id)

    # ── KPI row ───────────────────────────────────────────────────────────────
    if runs:
        total_runs   = len(runs)
        total_rows   = sum(r.get("rows_in", 0) for r in runs)
        total_cost   = sum(r.get("cost_usd", 0.0) for r in runs)
        avg_dq_post  = sum(r.get("dq_score_post", 0.0) for r in runs) / total_runs
        total_llm    = sum(r.get("llm_calls", 0) for r in runs)
    else:
        total_runs = total_rows = total_llm = 0
        total_cost = avg_dq_post = 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Total Runs</div>
          <div class="stat-value sv-lg">{total_runs}</div>
          <div class="stat-delta up">in experiment</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Rows Processed</div>
          <div class="stat-value sv-lg">{total_rows:,}</div>
          <div class="stat-delta up">cumulative</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        dq_color = "var(--green)" if avg_dq_post >= 70 else ("var(--amber)" if avg_dq_post >= 50 else "var(--red)")
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Avg DQ Post</div>
          <div class="stat-value sv-lg" style="color:{dq_color}">{avg_dq_post:.1f}</div>
          <div class="stat-delta">quality score 0–100</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">LLM Calls</div>
          <div class="stat-value sv-lg">{total_llm:,}</div>
          <div class="stat-delta">enrichment + analysis</div>
        </div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">LLM Cost</div>
          <div class="stat-value sv-lg" style="color:var(--amber)">${total_cost:.4f}</div>
          <div class="stat-delta">USD cumulative</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    if not runs:
        st.markdown('<div class="alert orange">No runs found for this experiment.</div>', unsafe_allow_html=True)
        return

    # ── Charts row ────────────────────────────────────────────────────────────
    ch1, ch2 = st.columns(2)

    with ch1:
        # DQ pre/post bar chart (last 10 runs)
        recent = runs[:10]
        max_dq = 100.0
        bars_html = ""
        for r in reversed(recent):
            name   = (r.get("run_name") or r.get("run_id", ""))[:14]
            pre    = r.get("dq_score_pre", 0.0)
            post   = r.get("dq_score_post", 0.0)
            pre_p  = pre / max_dq * 100
            post_p = post / max_dq * 100
            bars_html += f"""
            <div class="bar-row" style="margin-bottom:6px;">
              <div class="bar-label">{name}</div>
              <div style="flex:1;display:flex;flex-direction:column;gap:3px;">
                <div class="bar-track"><div class="bar-fill" style="width:{pre_p:.1f}%;background:var(--text-dim);"></div></div>
                <div class="bar-track"><div class="bar-fill bar-green" style="width:{post_p:.1f}%"></div></div>
              </div>
              <div class="bar-val" style="text-align:left;"><span style="color:var(--text-dim)">{pre:.1f}</span>→<span style="color:var(--green)">{post:.1f}</span></div>
            </div>"""
        st.markdown(f"""
        <div class="card">
          <div class="card-title">DQ Score Pre → Post (last {len(recent)} runs)</div>
          <div class="bar-chart">{bars_html}</div>
          <div style="font-size:12px;color:var(--text-dim);margin-top:10px;">
            <span style="display:inline-block;width:10px;height:10px;background:var(--text-dim);border-radius:2px;margin-right:4px;"></span>Pre
            <span style="display:inline-block;width:10px;height:10px;background:var(--green);border-radius:2px;margin-right:4px;margin-left:12px;"></span>Post
          </div>
        </div>""", unsafe_allow_html=True)

    with ch2:
        # Enrichment tier breakdown
        s1_total = sum(r.get("s1_count", 0) for r in runs)
        s2_total = sum(r.get("s2_count", 0) for r in runs)
        s3_total = sum(r.get("s3_count", 0) for r in runs)
        grand = s1_total + s2_total + s3_total or 1

        tier_bars = [
            ("S1 Deterministic", s1_total, "bar-green"),
            ("S2 KNN Corpus",    s2_total, "bar-accent"),
            ("S3 RAG-LLM",       s3_total, "bar-amber"),
        ]
        bars2_html = ""
        for label, val, cls in tier_bars:
            pct = val / grand * 100
            bars2_html += f"""
            <div class="bar-row">
              <div class="bar-label" style="width:120px;">{label}</div>
              <div class="bar-track"><div class="bar-fill {cls}" style="width:{pct:.1f}%"></div></div>
              <div class="bar-val">{val:,}</div>
            </div>"""

        # Anomaly count
        anomaly_total = sum(r.get("anomaly_count", 0) for r in runs)
        cost_html = f'<div style="margin-top:14px;font-size:13px;color:var(--text-muted);">Anomalies flagged: <strong style="color:var(--red)">{anomaly_total}</strong></div>'

        st.markdown(f"""
        <div class="card">
          <div class="card-title">Enrichment Tier Breakdown</div>
          <div class="bar-chart">{bars2_html}</div>
          {cost_html}
        </div>""", unsafe_allow_html=True)

    # ── Full runs table ───────────────────────────────────────────────────────
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    status_filter = st.selectbox("Filter by status", ["All", "FINISHED", "FAILED", "RUNNING"], key="mlflow_status")
    filtered_runs = runs if status_filter == "All" else [r for r in runs if r.get("status") == status_filter]

    rows_html = ""
    for r in filtered_runs:
        status = r.get("status", "")
        status_cls = {"FINISHED": "success", "FAILED": "error", "RUNNING": "running"}.get(status, "warning")
        pre   = r.get("dq_score_pre", 0.0)
        post  = r.get("dq_score_post", 0.0)
        delta = post - pre
        d_color = "var(--green)" if delta >= 0 else "var(--red)"
        d_sign  = "+" if delta >= 0 else ""
        rows_html += f"""
        <tr>
          <td><span class="mono tc-dim">{r.get("run_id","")}</span></td>
          <td><span class="mono">{r.get("run_name","")}</span></td>
          <td><span class="badge {status_cls}">{status}</span></td>
          <td class="tc-dim">{r.get("start_time","")}</td>
          <td><span class="mono">{r.get("source","")}</span></td>
          <td class="mono">{pre:.2f}</td>
          <td class="mono" style="color:var(--green)">{post:.2f}</td>
          <td class="mono" style="color:{d_color}">{d_sign}{delta:.2f}</td>
          <td class="mono">{r.get("rows_in",0):,}</td>
          <td class="mono">{r.get("rows_out",0):,}</td>
          <td class="mono tc-amber">${r.get("cost_usd",0.0):.4f}</td>
          <td class="mono tc-dim">{r.get("llm_calls",0)}</td>
          <td class="tc-green">{r.get("s1_count",0):,}</td>
          <td class="tc-accent">{r.get("s2_count",0):,}</td>
          <td class="tc-amber">{r.get("s3_count",0):,}</td>
          <td class="{'tc-red' if r.get('anomaly_count',0)>0 else 'tc-dim'}">{r.get("anomaly_count",0)}</td>
        </tr>"""

    st.markdown(f"""
    <div class="card" style="overflow-x:auto;">
      <div class="card-title">All Runs — {len(filtered_runs)}</div>
      <table class="data-table" style="font-size:13px;">
        <thead><tr>
          <th>Run ID</th><th>Name</th><th>Status</th><th>Start</th><th>Source</th>
          <th>DQ Pre</th><th>DQ Post</th><th>Δ DQ</th>
          <th>Rows In</th><th>Rows Out</th><th>Cost</th><th>LLM Calls</th>
          <th>S1</th><th>S2</th><th>S3</th><th>Anomalies</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>""", unsafe_allow_html=True)
