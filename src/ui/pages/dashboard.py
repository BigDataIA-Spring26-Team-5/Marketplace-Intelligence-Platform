"""Dashboard page — KPIs, recent runs, quick actions, active DAGs."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import dashboard_kpis, load_run_logs
from src.ui.utils.airflow_client import list_dags


def render_dashboard():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Dashboard</div>
        <div class="page-subtitle">Real-time pipeline health and activity overview</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── KPI row ──────────────────────────────────────────────────────────────
    kpis = dashboard_kpis()
    runs_today   = kpis.get("runs_today", 0)
    success_rate = kpis.get("success_rate", 0.0)
    avg_delta    = kpis.get("avg_dq_delta", 0.0)
    qrate        = kpis.get("quarantine_rate", 0.0)

    delta_color = "var(--green)" if avg_delta >= 0 else "var(--red)"
    delta_arrow = "↑" if avg_delta >= 0 else "↓"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Runs Today</div>
          <div class="stat-value">{runs_today}</div>
          <div class="stat-delta up">pipeline executions</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        rate_color = "var(--green)" if success_rate >= 90 else ("var(--amber)" if success_rate >= 70 else "var(--red)")
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Success Rate</div>
          <div class="stat-value" style="color:{rate_color}">{success_rate}<span class="stat-unit">%</span></div>
          <div class="stat-delta up">completed without error</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Avg DQ Delta</div>
          <div class="stat-value" style="color:{delta_color}">{delta_arrow}{abs(avg_delta)}<span class="stat-unit">pts</span></div>
          <div class="stat-delta up">quality improvement per run</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        q_color = "var(--red)" if qrate > 5 else ("var(--amber)" if qrate > 1 else "var(--green)")
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Quarantine Rate</div>
          <div class="stat-value" style="color:{q_color}">{qrate}<span class="stat-unit">%</span></div>
          <div class="stat-delta">rows flagged for review</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # ── Main 2-col ────────────────────────────────────────────────────────────
    left, right = st.columns([3, 2])

    with left:
        logs = load_run_logs(limit=8)

        def _domain_badge(d):
            colors = {"nutrition": "info", "safety": "error", "pricing": "warning", "retail": "purple"}
            cls = colors.get((d or "").lower(), "info")
            return f'<span class="badge {cls}">{d or "—"}</span>'

        def _status_badge(s):
            cls = {"success": "success", "error": "error", "running": "running"}.get(s, "warning")
            return f'<span class="badge {cls}">{s}</span>'

        def _dq_arrow(pre, post, delta):
            pre_s  = f"{pre:.1f}"  if pre  is not None else "—"
            post_s = f"{post:.1f}" if post is not None else "—"
            post_cls = "after" if post is not None else "after na"
            d_s = ""
            if delta is not None:
                sign = "+" if delta >= 0 else ""
                d_s = f'<span class="delta" style="color:{"var(--green)" if delta>=0 else "var(--red)"}">{sign}{delta:.1f}</span>'
            return f'<span class="dq-arrow"><span class="before">{pre_s}</span><span class="arrow"> → </span><span class="{post_cls}">{post_s}</span> {d_s}</span>'

        rows_html = ""
        for r in logs:
            src = r.get("source_name", r.get("source", "—"))
            domain = r.get("domain", "")
            status = r.get("status", "")
            pre    = r.get("dq_score_pre")
            post   = r.get("dq_score_post")
            delta  = r.get("dq_delta")
            ts_raw = r.get("timestamp", "")
            ts     = ts_raw[11:16] if len(ts_raw) > 16 else ts_raw[:16]
            rows_html += f"""
            <tr>
              <td><span class="mono">{src}</span></td>
              <td>{_domain_badge(domain)}</td>
              <td>{_dq_arrow(pre, post, delta)}</td>
              <td>{_status_badge(status)}</td>
              <td class="tc-dim">{ts}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card">
          <div class="card-title">Recent Runs</div>
          <table class="data-table">
            <thead><tr>
              <th>Source</th><th>Domain</th><th>DQ Score</th><th>Status</th><th>Time</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

    with right:
        # Quick Actions
        st.markdown('<div class="card"><div class="card-title">Quick Actions</div>', unsafe_allow_html=True)
        qa1, qa2 = st.columns(2)
        with qa1:
            if st.button("▶  Run Pipeline", key="qa_pipeline", use_container_width=True):
                st.session_state.page = "pipeline"
                st.rerun()
            if st.button("◎  Observability", key="qa_obs", use_container_width=True):
                st.session_state.page = "observability"
                st.rerun()
        with qa2:
            if st.button("◈  MLflow", key="qa_mlflow", use_container_width=True):
                st.session_state.page = "mlflow"
                st.rerun()
            if st.button("◈  Airflow", key="qa_airflow", use_container_width=True):
                st.session_state.page = "airflow"
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Active DAGs strip
        try:
            dags = list_dags()
            active = [d for d in dags if not d.get("is_paused", True)][:6]
        except Exception:
            active = []

        if active:
            items_html = ""
            for d in active:
                dag_id = d.get("dag_id", "")
                items_html += f"""
                <div class="dag-strip-item">
                  <div class="dag-spin"></div>
                  <div>
                    <div class="dag-strip-name">{dag_id}</div>
                    <div class="dag-strip-time">active</div>
                  </div>
                </div>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Active DAGs</div>
              <div class="dag-strip">{items_html}</div>
            </div>""", unsafe_allow_html=True)
        else:
            total_dags = len(list_dags()) if dags else 12
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Active DAGs</div>
              <div style="color:var(--text-dim);font-size:13px;padding:8px 0;">
                {total_dags} DAGs scheduled · all currently paused or idle
              </div>
            </div>""", unsafe_allow_html=True)

    # ── Prometheus source bar chart ───────────────────────────────────────────
    try:
        from src.ui.utils.api_client import prom_series
        series = prom_series('sum by (source) (etl_rows_in)')
        if series:
            series_sorted = sorted(series, key=lambda x: x[1], reverse=True)[:8]
            max_val = max(v for _, v in series_sorted) or 1
            bars_html = ""
            for labels, val in series_sorted:
                src = labels.get("source", "unknown")
                pct = val / max_val * 100
                bars_html += f"""
                <div class="bar-row">
                  <div class="bar-label">{src[:12]}</div>
                  <div class="bar-track"><div class="bar-fill bar-accent" style="width:{pct:.1f}%"></div></div>
                  <div class="bar-val">{int(val):,}</div>
                </div>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Rows Processed by Source</div>
              <div class="bar-chart">{bars_html}</div>
            </div>""", unsafe_allow_html=True)
    except Exception:
        pass
