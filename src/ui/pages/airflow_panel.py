"""Airflow Panel — DAG list, trigger, run history, task logs."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.airflow_client import (
    list_dags, list_dag_runs, trigger_dag, get_task_logs, get_running_dags,
)

AIRFLOW_URL = "http://localhost:8080"


def _run_state_badge(state: str) -> str:
    cls = {"success": "success", "failed": "error", "running": "running",
           "queued": "warning"}.get((state or "").lower(), "info")
    return f'<span class="badge {cls}">{state or "—"}</span>'


def render_airflow():
    st.markdown(f"""
    <div class="page-header">
      <div>
        <div class="page-title">Airflow</div>
        <div class="page-subtitle">DAG management, triggering, and task log inspection</div>
      </div>
      <a href="{AIRFLOW_URL}" target="_blank"
         style="font-size:13px;color:var(--accent);text-decoration:none;font-weight:600;">
        Open Airflow UI ↗
      </a>
    </div>
    """, unsafe_allow_html=True)

    # ── Load DAGs ─────────────────────────────────────────────────────────────
    with st.spinner("Loading DAGs…"):
        try:
            dags = list_dags()
        except Exception as e:
            st.error(f"Failed to load DAGs: {e}")
            return

    active  = [d for d in dags if not d.get("is_paused", True)]
    paused  = [d for d in dags if d.get("is_paused", False)]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Total DAGs</div>
          <div class="stat-value sv-lg">{len(dags)}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Active</div>
          <div class="stat-value sv-lg" style="color:var(--green)">{len(active)}</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Paused</div>
          <div class="stat-value sv-lg" style="color:var(--amber)">{len(paused)}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    left, right = st.columns([3, 2])

    with left:
        # DAG table
        rows_html = ""
        for d in dags:
            dag_id   = d.get("dag_id", "")
            owner    = d.get("owner", "")
            paused_v = d.get("is_paused", True)
            status_badge = '<span class="badge warning">paused</span>' if paused_v else '<span class="badge success">active</span>'
            rows_html += f"""
            <tr>
              <td><span class="mono">{dag_id}</span></td>
              <td class="tc-dim">{owner}</td>
              <td>{status_badge}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card">
          <div class="card-title">All DAGs — {len(dags)}</div>
          <table class="data-table">
            <thead><tr><th>DAG ID</th><th>Owner</th><th>Status</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

    with right:
        # ── Trigger DAG ───────────────────────────────────────────────────────
        st.markdown('<div class="card"><div class="card-title">Trigger DAG</div>', unsafe_allow_html=True)
        dag_ids = [d.get("dag_id", "") for d in dags]
        sel_dag = st.selectbox("Select DAG", dag_ids if dag_ids else ["—"], key="trigger_dag_id")
        conf_str = st.text_area("Config JSON (optional)", value="{}", height=80, key="trigger_conf")

        if st.button("▶  Trigger", type="primary", use_container_width=True):
            try:
                import json
                conf = json.loads(conf_str or "{}")
                ok = trigger_dag(sel_dag, conf)
                if ok:
                    st.success(f"✓ DAG '{sel_dag}' triggered successfully")
                else:
                    st.error("Trigger failed — check Airflow connectivity")
            except Exception as e:
                st.error(str(e))
        st.markdown("</div>", unsafe_allow_html=True)

        # ── Running DAGs strip ────────────────────────────────────────────────
        try:
            running = get_running_dags()
        except Exception:
            running = []

        if running:
            items_html = ""
            for r in running[:5]:
                dag_id  = r.get("dag_id", "")
                run_id  = r.get("dag_run_id", r.get("run_id", ""))[:16]
                items_html += f"""
                <div class="dag-strip-item">
                  <div class="dag-spin"></div>
                  <div>
                    <div class="dag-strip-name">{dag_id}</div>
                    <div class="dag-strip-time">{run_id}</div>
                  </div>
                </div>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Currently Running</div>
              <div class="dag-strip">{items_html}</div>
            </div>""", unsafe_allow_html=True)

    # ── Run History ───────────────────────────────────────────────────────────
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="card"><div class="card-title">DAG Run History</div>', unsafe_allow_html=True)

    sel_dag_runs = st.selectbox("DAG", dag_ids if dag_ids else ["—"], key="runs_dag_id")
    if sel_dag_runs and sel_dag_runs != "—":
        with st.spinner("Loading runs…"):
            try:
                runs = list_dag_runs(sel_dag_runs, limit=10)
            except Exception as e:
                runs = []
                st.error(str(e))

        if runs:
            run_rows = ""
            for r in runs:
                run_id  = r.get("dag_run_id", r.get("run_id", ""))[:30]
                exec_dt = r.get("execution_date", r.get("logical_date", ""))[:19]
                state   = r.get("state", "")
                run_rows += f"""
                <tr>
                  <td class="mono tc-dim">{run_id}</td>
                  <td class="tc-dim">{exec_dt}</td>
                  <td>{_run_state_badge(state)}</td>
                </tr>"""
            st.markdown(f"""
            <table class="data-table">
              <thead><tr><th>Run ID</th><th>Execution Date</th><th>State</th></tr></thead>
              <tbody>{run_rows}</tbody>
            </table>""", unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:var(--text-dim);font-size:13px;padding:8px 0;">No runs found for this DAG.</div>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Task Logs ─────────────────────────────────────────────────────────────
    with st.expander("Task Logs"):
        lc1, lc2, lc3 = st.columns(3)
        with lc1:
            log_dag   = st.text_input("DAG ID", key="log_dag_id")
        with lc2:
            log_run   = st.text_input("Run ID", key="log_run_id")
        with lc3:
            log_task  = st.text_input("Task ID", key="log_task_id")

        if st.button("Fetch Logs", key="fetch_logs"):
            if log_dag and log_run and log_task:
                with st.spinner("Fetching logs…"):
                    logs = get_task_logs(log_dag, log_run, log_task)
                if logs:
                    st.markdown(f'<div class="terminal" style="height:300px;overflow-y:auto;white-space:pre-wrap;">{logs}</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="alert orange">No logs found.</div>', unsafe_allow_html=True)
            else:
                st.warning("Fill DAG ID, Run ID, and Task ID")
