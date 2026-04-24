"""Market Intelligence Platform Streamlit UI - Main App Entry Point."""

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

VALID_PAGES = {
    "dashboard",
    "domain",
    "pipeline",
    "observability",
    "search",
    "recs",
    "airflow",
    "tests",
}

STYLES = """
<style>
    /* ── Global Reset & Variables ─────────────────────── */
    .stApp { background-color: #ffffff; }
    
    /* ── Hide Streamlit elements ────────────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {visibility: hidden;}
    header {visibility: hidden;}

    /* ── App Shell Layout ───────────────────────────── */
    .app-shell {
        display: grid;
        grid-template-columns: 220px 1fr;
        grid-template-rows: 52px 1fr;
        height: 100vh;
    }

    /* ── Topbar ─────────────────────────────────────── */
    .topbar-section {
        background: #ffffff;
        border-bottom: 1px solid #dee2e6;
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px 20px;
    }
    .topbar-brand { display: flex; align-items: center; gap: 10px; }
    .logo {
        width: 28px; height: 28px;
        background: #1971c2;
        border-radius: 5px;
        display: flex; align-items: center; justify-content: center;
        font-size: 12px; font-weight: 700; color: #fff;
    }
    .brand-name { font-size: 15px; font-weight: 700; color: #212529; }
    .brand-name span { color: #1971c2; }

    .health-rail { display: flex; align-items: center; gap: 6px; }
    .health-label {
        font-size: 10px; font-weight: 700; color: #adb5bd;
        text-transform: uppercase; letter-spacing: 0.07em;
    }
    .health-pill {
        display: flex; align-items: center; gap: 5px;
        padding: 3px 10px; border-radius: 20px;
        border: 1px solid #dee2e6; background: #f8f9fa;
        font-size: 11px; font-weight: 500; color: #6c757d;
    }
    .health-dot { width: 6px; height: 6px; border-radius: 50%; }
    .dot-ok { background: #2f9e44; }
    .dot-warn { background: #e67700; }
    .dot-error { background: #c92a2a; }

    .run-badge {
        display: flex; align-items: center; gap: 6px;
        padding: 4px 12px; border-radius: 20px;
        background: #ebf9ee; border: 1px solid rgba(47,158,68,0.2);
        font-size: 11px; font-weight: 600; color: #2f9e44;
    }
    .run-badge::before {
        content: ''; width: 6px; height: 6px; border-radius: 50%;
        background: #2f9e44;
        animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.5;transform:scale(0.8)} }

    /* ── Main Content Area ───────────────────────────── */
    .main-content { padding: 28px 32px; }

    /* ── Page Header ────────────────────────────────────── */
    .page-header {
        display: flex; align-items: flex-start;
        justify-content: space-between; margin-bottom: 24px;
    }
    .page-title { font-size: 20px; font-weight: 700; letter-spacing: -0.4px; color: #212529; }
    .page-subtitle { font-size: 12px; color: #6c757d; }
    .page-controls { display: flex; align-items: center; gap: 12px; }

    /* ── Cards ───────────────────────────────────────── */
    .card {
        background: #ffffff; border: 1px solid #dee2e6;
        border-radius: 10px; padding: 18px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.07);
    }
    .card-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; }
    .card-title {
        display: flex; align-items: center; gap: 7px;
        font-size: 11px; font-weight: 700; color: #6c757d;
        text-transform: uppercase; letter-spacing: 0.06em;
    }
    .card-title::before {
        content: ''; width: 3px; height: 12px;
        background: #1971c2; border-radius: 2px;
    }

    /* ── Stat Cards ──────────────────────────────── */
    .stat-card {
        background: #f8f9fa; border: 1px solid #dee2e6;
        border-radius: 6px; padding: 14px 16px;
    }
    .stat-label {
        font-size: 10px; font-weight: 700; color: #adb5bd;
        text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px;
    }
    .stat-value { font-size: 26px; font-weight: 700; letter-spacing: -0.8px; color: #212529; }
    .stat-unit { font-size: 14px; font-weight: 500; color: #6c757d; }
    .stat-delta { font-size: 11px; font-weight: 500; margin-top: 6px; }
    .delta-up { color: #2f9e44; }
    .delta-down { color: #c92a2a; }

    /* ── Grid Helpers ───────────────────────────── */
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .grid-4 { display: grid; grid-template-columns: repeat(4,1fr); gap: 16px; }

    /* ── Badges ───────────────────────────────────── */
    .badge {
        display: inline-flex; align-items: center; gap: 4px;
        padding: 2px 7px; border-radius: 4px;
        font-size: 11px; font-weight: 600; white-space: nowrap;
    }
    .badge-success { background: #ebf9ee; color: #2f9e44; border: 1px solid rgba(47,158,68,0.15); }
    .badge-error { background: #fff5f5; color: #c92a2a; border: 1px solid rgba(201,42,42,0.15); }
    .badge-warning { background: #fff3bf; color: #e67700; border: 1px solid rgba(230,119,0,0.15); }
    .badge-info { background: #e7f0fb; color: #1971c2; border: 1px solid rgba(25,113,194,0.15); }
    .badge-running { background: #e3fafc; color: #0c8599; border: 1px solid rgba(12,133,153,0.15); }
    .badge-purple { background: #f3f0ff; color: #6741d9; border: 1px solid rgba(103,65,217,0.15); }

    /* ── Data Table ───────────────────────────── */
    .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .data-table th {
        font-size: 10px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.06em; color: #adb5bd; padding: 8px 12px;
        border-bottom: 1px solid #dee2e6; background: #f8f9fa; text-align: left;
    }
    .data-table td {
        padding: 10px 12px; border-bottom: 1px solid #dee2e6;
        color: #6c757d; vertical-align: middle;
    }
    .data-table td:first-child { color: #212529; font-weight: 500; }
    .data-table tbody tr:hover { background: #f8f9fa; }

    /* ── DQ Arrow ──────────────────────────────── */
    .dq-arrow {
        display: flex; align-items: center; gap: 4px;
        font-family: monospace; font-size: 12px;
    }
    .dq-before { color: #6c757d; }
    .dq-arrow { color: #adb5bd; }
    .dq-after { color: #2f9e44; font-weight: 600; }
    .dq-delta { color: #2f9e44; font-size: 11px; }

    /* ── Terminal ──────────────────────────────── */
    .terminal {
        background: #f8f9fa; border: 1px solid #dee2e6;
        border-left: 3px solid #ced4da; border-radius: 6px;
        padding: 14px 16px;
        font-family: monospace; font-size: 12px; line-height: 1.7;
        color: #6c757d; overflow-y: auto;
    }
    .t-green { color: #2f9e44; }
    .t-blue { color: #1971c2; }
    .t-dim { color: #adb5bd; }
    .t-text { color: #212529; }

    .stream-dot {
        display: inline-block; width: 6px; height: 6px; border-radius: 50%;
        background: #2f9e44; animation: pulse 2s ease-in-out infinite;
        vertical-align: middle; margin-right: 4px;
    }

    /* ── Stepper ───────────────────────────────────── */
    .stepper { display: flex; align-items: flex-start; margin-bottom: 28px; }
    .step { display: flex; align-items: center; flex: 1; }
    .step-node { display: flex; flex-direction: column; align-items: center; gap: 5px; flex-shrink: 0; }
    .step-circle {
        width: 30px; height: 30px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        font-size: 12px; font-weight: 700; font-family: monospace;
        border: 2px solid #dee2e6; background: #f8f9fa; color: #adb5bd;
    }
    .step-circle.done { background: #ebf9ee; border-color: #2f9e44; color: #2f9e44; }
    .step-circle.active { background: #e7f0fb; border-color: #1971c2; color: #1971c2; box-shadow: 0 0 0 3px rgba(25,113,194,0.1); }
    .step-label { font-size: 11px; font-weight: 500; color: #adb5bd; white-space: nowrap; }
    .step-label.done { color: #2f9e44; }
    .step-label.active { color: #1971c2; }
    .step-line { flex: 1; height: 1px; background: #dee2e6; margin: 0 4px; transform: translateY(-12px); }
    .step-line.done { background: #2f9e44; }

    /* ── Block Chips ──────────────────────────────── */
    .block-chips { display: flex; flex-wrap: wrap; gap: 6px; }
    .block-chip {
        display: flex; align-items: center; gap: 5px; padding: 4px 10px;
        border-radius: 4px; font-family: monospace; font-size: 11px;
        font-weight: 500; border: 1px solid #dee2e6; background: #f8f9fa; color: #6c757d;
    }
    .block-chip.done { background: #ebf9ee; border-color: rgba(47,158,68,0.25); color: #2f9e44; }
    .block-chip.running { background: #e7f0fb; border-color: rgba(25,113,194,0.25); color: #1971c2; animation: blink 1.5s ease-in-out infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.55} }

    /* ── Buttons ─────────────────────────────────── */
    .btn {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 7px 14px; border-radius: 6px;
        font-size: 13px; font-weight: 600; cursor: pointer;
        border: none;
    }
    .btn-primary { background: #1971c2; color: #fff; }
    .btn-primary:hover { background: #1864ab; }
    .btn-ghost { background: transparent; color: #6c757d; border: 1px solid #dee2e6; }
    .btn-ghost:hover { background: #f1f3f5; color: #212529; }
    .btn-sm { padding: 4px 10px; font-size: 12px; }

    /* ── Mode Toggle ──────────────────────────────── */
    .mode-toggle { display: flex; background: #f1f3f5; border: 1px solid #dee2e6; border-radius: 6px; overflow: hidden; }
    .mode-option { padding: 5px 12px; font-size: 12px; font-weight: 600; color: #6c757d; cursor: pointer; }
    .mode-option.active { background: #ffffff; color: #1971c2; box-shadow: 0 1px 3px rgba(0,0,0,0.07); }

    /* ── Toggle ─────────────────────────────────────── */
    .toggle-inline { display: flex; align-items: center; gap: 8px; font-size: 13px; font-weight: 500; color: #6c757d; }
    .toggle { width: 34px; height: 18px; background: #e9ecef; border-radius: 9px; border: 1px solid #dee2e6; }
    .toggle.on { background: #1971c2; border-color: #1971c2; }
    .toggle::after {
        content: ''; position: absolute; width: 12px; height: 12px; background: #fff; border-radius: 50%;
        top: 2px; left: 2px;
    }
    .toggle.on::after { left: 18px; }

    /* ── Alert ─────────────────────────────────────── */
    .alert { padding: 10px 12px; border-radius: 6px; font-size: 12px; font-weight: 500; }
    .alert-purple { background: #f3f0ff; border: 1px solid rgba(103,65,217,0.12); color: #6741d9; }
    .alert-green { background: #ebf9ee; border: 1px solid rgba(47,158,68,0.12); color: #2f9e44; }

    /* ── Decision Card ──────────────────────────── */
    .decision-card { display: flex; align-items: center; gap: 12px; padding: 12px 14px; border-radius: 6px; background: #f8f9fa; border: 1px solid #dee2e6; }
    .decision-field { font-family: monospace; font-size: 12px; font-weight: 600; color: #1971c2; }
    .decision-reason { font-family: monospace; font-size: 11px; color: #adb5bd; margin-top: 2px; }
    .decision-body { flex: 1; }
    .decision-actions { display: flex; gap: 5px; flex-shrink: 0; }

    /* ── Quick Actions ──────────────────────────── */
    .quick-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .quick-action {
        display: flex; align-items: center; gap: 7px; padding: 9px 14px;
        border-radius: 6px; background: #f8f9fa; border: 1px solid #dee2e6;
        font-size: 13px; font-weight: 600; color: #6c757d; cursor: pointer;
    }
    .quick-action:hover { border-color: #1971c2; color: #1971c2; background: #e7f0fb; }

    /* ── DAG Strip ──────────────────────────────── */
    .dag-strip { display: flex; gap: 10px; overflow-x: auto; }
    .dag-strip-item { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-radius: 6px; border: 1px solid rgba(12,133,153,0.2); background: #e3fafc; white-space: nowrap; }
    .dag-name { font-family: monospace; font-size: 12px; font-weight: 600; color: #0c8599; }
    .dag-time { font-family: monospace; font-size: 10px; color: #adb5bd; }
    .dag-spin {
        width: 12px; height: 12px; border-radius: 50%;
        border: 2px solid rgba(12,133,153,0.2); border-top-color: #0c8599;
        animation: spin 0.8s linear infinite; flex-shrink: 0;
    }
    @keyframes spin { to{transform:rotate(360deg)} }

    /* ── Grafana ─────────────────────────────────── */
    .grafana-toolbar {
        display: flex; align-items: center; justify-content: space-between;
        padding: 10px 16px; background: #f8f9fa; border: 1px solid #dee2e6;
        border-radius: 10px 10px 0 0; border-bottom: none;
    }
    .grafana-logo { display: flex; align-items: center; gap: 7px; padding-right: 16px; margin-right: 8px; border-right: 1px solid #dee2e6; }
    .grafana-logo-text { font-size: 13px; font-weight: 700; color: #f46800; }
    .grafana-breadcrumb { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; }
    .grafana-controls { display: flex; align-items: center; gap: 8px; }
    .grafana-timerange { display: flex; background: #f1f3f5; border: 1px solid #dee2e6; border-radius: 6px; overflow: hidden; }
    .timerange-btn { padding: 4px 10px; font-size: 11px; font-weight: 600; color: #6c757d; cursor: pointer; font-family: monospace; }
    .timerange-btn:hover { background: #e9ecef; }
    .timerange-btn.active { background: #1971c2; color: #fff; }
    .grafana-embed { background: #f1f3f5; border: 1px solid #dee2e6; border-radius: 0 0 10px 10px; min-height: 480px; }

    /* ── Chat ─────────────────────────────────────── */
    .chat-scroll { height: 300px; overflow-y: auto; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 14px; }
    .chat-msg { display: flex; gap: 10px; margin-bottom: 14px; }
    .chat-avatar { width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 700; }
    .chat-avatar.user { background: #e7f0fb; color: #1971c2; }
    .chat-avatar.ai { background: #f3f0ff; color: #6741d9; }
    .chat-bubble { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 9px 13px; font-size: 13px; color: #6c757d; max-width: 75%; }
    .chat-input { display: flex; gap: 8px; margin-top: 12px; }

    /* ── Field Inputs ─────────────────────────── */
    .field-label { font-size: 11px; font-weight: 600; color: #6c757d; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 5px; }
    .field-input { width: 100%; padding: 8px 11px; background: #fff; border: 1px solid #dee2e6; border-radius: 6px; color: #212529; font-size: 13px; outline: none; }
    .field-input:focus { border-color: #1971c2; box-shadow: 0 0 0 3px rgba(25,113,194,0.08); }
    .drop-zone { border: 1.5px dashed #ced4da; border-radius: 10px; padding: 28px; text-align: center; cursor: pointer; color: #adb5bd; font-size: 12px; font-weight: 500; }
    .drop-zone:hover { border-color: #1971c2; background: #e7f0fb; color: #1971c2; }
    .drop-zone-icon { font-size: 18px; margin-bottom: 8px; }

    /* ── Tabs ────────────────────────────────────── */
    .tabs { display: flex; border-bottom: 1px solid #dee2e6; margin-bottom: 20px; }
    .tab { padding: 9px 16px; font-size: 13px; font-weight: 600; color: #6c757d; cursor: pointer; border-bottom: 2px solid transparent; }
    .tab:hover { color: #212529; }
    .tab.active { color: #1971c2; border-bottom-color: #1971c2; }

    /* ── Utility ───────────────────────────────── */
    .mb { margin-bottom: 16px; }
    .mt-6 { margin-top: 6px; }
    .mb-12 { margin-bottom: 12px; }
    .mb-16 { margin-bottom: 16px; }

    /* ── YAML Editor ────────────────────��─��──── */
    .yaml-editor { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 12px 14px; font-family: monospace; font-size: 11.5px; line-height: 1.8; }
    .yaml-key { color: #1971c2; }
    .yaml-val { color: #2f9e44; }
    .yaml-num { color: #e67700; }
    .yaml-bool { color: #c92a2a; }
    .yaml-editor.new { border-color: rgba(47,158,68,0.25); }
    .section-label { font-size: 10px; font-weight: 700; color: #adb5bd; text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 8px; }
    .section-label.generated { color: #2f9e44; }

    /* ── Scrollbar ──────────────────────────────── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-thumb { background: #dee2e6; border-radius: 3px; }
</style>
"""


def init_session_state():
    """Initialize session state variables."""
    if "current_page" not in st.session_state:
        st.session_state.current_page = "dashboard"


def get_current_page() -> str:
    """Resolve the current page from query params across Streamlit versions."""
    raw_page = st.query_params.get("page", st.session_state.current_page)

    if isinstance(raw_page, list):
        page = raw_page[0] if raw_page else st.session_state.current_page
    else:
        page = raw_page

    page = str(page or st.session_state.current_page).strip().lower()
    if page not in VALID_PAGES:
        page = "dashboard"

    st.session_state.current_page = page
    return page


def load_ui_html() -> str:
    """Load the canonical HTML UI file from the repo root."""
    ui_path = Path(__file__).resolve().parents[2] / "dataforge_components.html"
    return ui_path.read_text(encoding="utf-8")


def render_html_shell() -> None:
    """Render the exact UI defined in the canonical HTML file."""
    try:
        html = load_ui_html()
    except FileNotFoundError:
        st.error("Missing `dataforge_components.html`; cannot render the canonical UI.")
        return

    components.html(html, height=1200, scrolling=False)


def render_dashboard():
    """Render the Dashboard page."""
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Dashboard</div>
            <div class="page-subtitle">System at a glance · Last refreshed 14s ago</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Stats cards
    st.markdown('<div class="grid-4 mb">', unsafe_allow_html=True)
    cols = st.columns(4)
    stats = [
        ("Runs Today", "47", "+12 vs yesterday", "delta-up"),
        ("Success Rate", "96.3%", "+1.8 pp", "delta-up"),
        ("Avg DQ Delta", "+8.4pp", "improving", "delta-up"),
        ("Quarantine Rate", "2.1%", "-0.4 pp", "delta-down"),
    ]
    for i, (label, value, delta, delta_cls) in enumerate(stats):
        with cols[i]:
            st.markdown("""
            <div class="stat-card">
                <div class="stat-label">{}</div>
                <div class="stat-value">{}</div>
                <div class="stat-delta {}">{}</div>
            </div>
            """.format(label, value, delta_cls, delta), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Main content: Recent Runs + Quick Actions
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown("""
        <div class="card">
            <div class="card-title">Recent Runs</div>
        </div>
        """, unsafe_allow_html=True)
        runs = [
            ("usda_april.csv", "nutrition", "74.1", "83.8", "+9.7", "success"),
            ("fda_recalls_q2.csv", "safety", "61.2", "78.5", "+17.3", "success"),
            ("kroger_pricing.gcs", "pricing", "82.0", "—", "—", "running"),
            ("whole_foods_sku.csv", "nutrition", "55.3", "69.0", "+13.7", "error"),
            ("open_food_facts.csv", "nutrition", "70.8", "81.2", "+10.4", "success"),
        ]
        st.markdown("""
        <table class="data-table">
            <thead><tr><th>Source</th><th>Domain</th><th>DQ Score</th><th>Status</th></tr></thead>
            <tbody>
        """, unsafe_allow_html=True)
        for source, domain, dq_pre, dq_post, delta, status in runs:
            badge = '<span class="badge badge-{}">{}</span>'.format(status, status)
            dq_arrow = '''
            <div class="dq-arrow">
                <span class="dq-before">{}</span>
                <span class="dq-arrow"> → </span>
                <span class="dq-after">{}</span>
                <span class="dq-delta"> ({})</span>
            </div>
            '''.format(dq_pre, dq_post, delta)
            domain_badge = '<span class="badge badge-info">{}</span>'.format(domain)
            st.markdown("""
            <tr>
                <td>{}</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}</td>
            </tr>
            """.format(source, domain_badge, dq_arrow, badge), unsafe_allow_html=True)
        st.markdown("</tbody></table>", unsafe_allow_html=True)

    with col_right:
        st.markdown("""
        <div class="card">
            <div class="card-title">Quick Actions</div>
            <div class="quick-actions">
                <div class="quick-action" onclick="window.location.href='?page=pipeline'">Start Pipeline Run</div>
                <div class="quick-action" onclick="window.location.href='?page=airflow'">Trigger DAG</div>
                <div class="quick-action" onclick="window.location.href='?page=tests'">Run Tests</div>
            </div>
        </div>
        <div class="card">
            <div class="card-title">Active DAGs</div>
            <div class="dag-strip">
                <div class="dag-strip-item">
                    <div class="dag-spin"></div>
                    <div>
                        <div class="dag-name">bronze_to_silver_dag</div>
                        <div class="dag-time">running · 4m 22s</div>
                    </div>
                </div>
                <div class="dag-strip-item">
                    <div class="dag-spin"></div>
                    <div>
                        <div class="dag-name">uc2_anomaly_detector</div>
                        <div class="dag-time">running · 1m 08s</div>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_pipeline():
    """Render the Pipeline Wizard page."""
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Pipeline Wizard</div>
            <div class="page-subtitle">Bronze → Silver → Gold · Human-in-the-loop pipeline</div>
        </div>
        <div class="page-controls">
            <div class="mode-toggle">
                <div class="mode-option active">Full</div>
                <div class="mode-option">Silver only</div>
                <div class="mode-option">Gold only</div>
            </div>
            <div class="toggle-inline">
                <div class="toggle on"></div>
                <span>Critic</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Stepper
    steps = [("Source", "done"), ("Schema", "done"), ("HITL", "active"), ("Execute", ""), ("Results", "")]
    st.markdown('<div class="stepper">', unsafe_allow_html=True)
    for i, (step, status) in enumerate(steps):
        check = "✓" if status == "done" else str(i + 1)
        st.markdown("""
        <div class="step">
            <div class="step-node">
                <div class="step-circle {}">{}</div>
                <div class="step-label {}">{}</div>
            </div>
            {}
        </div>
        """.format(status, check, status, step,
                  '<div class="step-line {}"></div>'.format(status) if i < len(steps) - 1 else ''), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Main content
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("""
        <div class="card">
            <div class="card-title">Step 3 — HITL Decisions</div>
            <div class="alert alert-purple mb">Critic: "allergen_info field is sparse (38% null). Consider excluding or imputing from product description."</div>
            <div class="section-label">Missing Columns</div>
            <div style="display: flex; flex-direction: column; gap: 8px;">
        """, unsafe_allow_html=True)

        decisions = [
            ("allergen_info", "38% null · LLM-derivable from description"),
            ("serving_size_g", "92% null · No reliable source"),
            ("is_organic", "Missing · Derivable from label_text"),
        ]
        for field, reason in decisions:
            st.markdown("""
            <div class="decision-card">
                <div class="decision-body">
                    <div class="decision-field">{}</div>
                    <div class="decision-reason">{}</div>
                </div>
                <div class="decision-actions">
                    <button class="btn btn-ghost btn-sm">Null</button>
                    <button class="btn btn-primary btn-sm">Default</button>
                    <button class="btn btn-ghost btn-sm">Exclude</button>
                </div>
            </div>
            """.format(field, reason), unsafe_allow_html=True)

        st.markdown("</div></div>", unsafe_allow_html=True)

        st.markdown("""
        <div class="card">
            <div class="card-title">Block Sequence Preview</div>
            <div class="block-chips">
                <div class="block-chip done">Normalize</div>
                <div class="block-chip done">Deduplicate</div>
                <div class="block-chip done">SchemaAlign</div>
                <div class="block-chip running">Enrich_S1</div>
                <div class="block-chip">Enrich_S2</div>
                <div class="block-chip">Enrich_S3</div>
                <div class="block-chip">DQ_Score</div>
                <div class="block-chip">Quarantine</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_right:
        st.markdown("""
        <div class="card">
            <div class="card-header">
                <div class="card-title">Live Log</div>
                <span style="font-size: 11px; font-weight: 600; color: #2f9e44;"><span class="stream-dot"></span>streaming</span>
            </div>
            <div class="terminal" style="height: 380px; overflow-y: auto;">
        """, unsafe_allow_html=True)

        log_lines = [
            ("[14:22:01]", "OK", "normalize_block", "completed", "12,840 rows"),
            ("[14:22:04]", "OK", "deduplicate_block", "removed", "234 dupes → 12,606"),
            ("[14:22:08]", "OK", "schema_align", "47 renames, 3 casts", ""),
            ("[14:22:12]", "RUN", "enrich_s1_block", "starting...", ""),
            ("[14:22:22]", "OK", "enrich_s1_block", "12,606 rows enriched", ""),
            ("[14:22:23]", "RUN", "enrich_s2_block", "starting (LLM)...", ""),
        ]
        for time, level, event, detail, extra in log_lines:
            color = "t-green" if level == "OK" else ("t-blue" if level == "RUN" else "t-dim")
            st.markdown("""
            <span class="t-dim">{}</span> <span class="{}">{}</span> <span class="t-text">{}</span> {} <span class="t-green">{}</span><br>
            """.format(time, color, level, event, detail, extra), unsafe_allow_html=True)

        st.markdown("</div></div></div>", unsafe_allow_html=True)


def render_domain_packs():
    """Render the Domain Packs page."""
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Domain Packs</div>
            <div class="page-subtitle">Generate, validate, and manage domain enrichment configurations</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="tabs">
        <div class="tab active">Generate Pack</div>
        <div class="tab">Block Scaffold</div>
        <div class="tab">Preview / Validate</div>
        <div class="tab">Manage</div>
    </div>
    """, unsafe_allow_html=True)

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("""
        <div class="card">
            <div class="card-title">Pack Configuration</div>
            <div class="mb-12">
                <div class="field-label">Domain Name</div>
                <input class="field-input" type="text" value="cosmetics" placeholder="e.g. cosmetics, supplements">
            </div>
            <div class="mb-12">
                <div class="field-label">Description</div>
                <textarea class="field-input" rows="3" style="resize:none">Beauty and personal care products. Focus on ingredient safety, skin type suitability, and SPF values.</textarea>
            </div>
            <div class="mb-16">
                <div class="field-label">Sample CSV (for field detection)</div>
                <div class="drop-zone">
                    <div class="drop-zone-icon">[ csv ]</div>
                    <div>Drop CSV or click to browse</div>
                    <div class="mt-6" style="color: #adb5bd; font-size: 11px;">cosmetics_sample.csv · 1,200 rows detected</div>
                </div>
            </div>
            <button class="btn btn-primary" style="width: 100%;">Generate Pack</button>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="card">
            <div class="card-title">Agent Progress</div>
            <div style="display: flex; flex-direction: column; gap: 10px;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span class="badge badge-success">done</span>
                    <span style="font-family: monospace; font-size: 12px; color: #6c757d;">Analyze CSV</span>
                    <span style="font-family: monospace; font-size: 11px; margin-left: auto; color: #2f9e44;">28 fields</span>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span class="badge badge-success">done</span>
                    <span style="font-family: monospace; font-size: 12px; color: #6c757d;">Generate Rules</span>
                    <span style="font-family: monospace; font-size: 11px; margin-left: auto; color: #2f9e44;">14 rules</span>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span class="badge badge-running">run</span>
                    <span style="font-family: monospace; font-size: 12px; color: #1971c2;">Validate</span>
                    <span style="font-family: monospace; font-size: 11px; margin-left: auto; color: #adb5bd;">running...</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_right:
        st.markdown("""
        <div class="card">
            <div class="card-title">Generated YAML — Side-by-Side Review</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div>
                    <div class="section-label">Existing</div>
                    <div class="yaml-editor">
<span class="yaml-key">domain:</span> <span class="yaml-val">nutrition</span><br>
<span class="yaml-key">version:</span> <span class="yaml-num">1.2</span><br>
<span class="yaml-key">fields:</span><br>
&nbsp;&nbsp;- name: <span class="yaml-val">product_name</span><br>
&nbsp;&nbsp;&nbsp;&nbsp;type: <span class="yaml-val">string</span>
                    </div>
                </div>
                <div>
                    <div class="section-label generated">Generated</div>
                    <div class="yaml-editor new">
<span class="yaml-key">domain:</span> <span class="yaml-val">cosmetics</span><br>
<span class="yaml-key">version:</span> <span class="yaml-num">1.0</span><br>
<span class="yaml-key">fields:</span><br>
&nbsp;&nbsp;- name: <span class="yaml-val">brand_name</span><br>
&nbsp;&nbsp;&nbsp;&nbsp;type: <span class="yaml-val">string</span>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_observability():
    """Render the Observability page."""
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Observability</div>
            <div class="page-subtitle">Monitor runs, analytics, and chat</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="tabs">
        <div class="tab active">Run History</div>
        <div class="tab">Grafana</div>
        <div class="tab">Chatbot</div>
    </div>
    """, unsafe_allow_html=True)

    # Run history
    st.markdown("""
    <div class="card mb">
        <div class="card-title">Run History</div>
    </div>
    """, unsafe_allow_html=True)

    runs = [
        (1, "usda_april.csv", "nutrition", 12840, 74.1, 83.8, 9.7, 12, 3, "EXISTS"),
        (2, "fda_recalls_q2.csv", "safety", 8420, 61.2, 78.5, 17.3, 8, 5, "DERIVED"),
        (3, "kroger_pricing.gcs", "pricing", 15620, 82.0, 91.2, 9.2, 15, 2, "EXISTS"),
        (4, "whole_foods_sku.csv", "nutrition", 5340, 55.3, 69.0, 13.7, 6, 4, "DERIVED"),
    ]
    st.markdown("""
    <table class="data-table">
        <thead><tr><th>#</th><th>Source</th><th>Domain</th><th>Rows</th><th>DQ (Pre→Post)</th><th>Delta</th><th>Reg. Hits</th><th>Generated</th><th>Schema</th></tr></thead>
        <tbody>
    """, unsafe_allow_html=True)
    for run_num, source, domain, rows, dq_pre, dq_post, delta, reg_hits, generated, schema in runs:
        delta_cls = "delta-up" if delta > 0 else "delta-down"
        delta_sign = "+" if delta > 0 else ""
        schema_badge = '<span class="badge badge-success">{}</span>'.format(schema)
        st.markdown("""
        <tr>
            <td>Run {}</td>
            <td>{}</td>
            <td><span class="badge badge-info">{}</span></td>
            <td>{:,}</td>
            <td>{:.1f}% → {:.1f}%</td>
            <td class="{}">{}{:.1f}%</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        """.format(run_num, source, domain, rows, dq_pre, dq_post, delta_cls, delta_sign, delta, reg_hits, generated, schema_badge), unsafe_allow_html=True)
    st.markdown("</tbody></table>", unsafe_allow_html=True)

    # Grafana
    st.markdown("""
    <div class="card mb">
        <div class="card-title">Grafana Dashboard</div>
        <div class="grafana-toolbar">
            <div class="grafana-logo">
                <span class="grafana-logo-text">Grafana</span>
            </div>
            <div class="grafana-breadcrumb">
                <span>Home</span> / <span>Dashboards</span> / <span>UC1 Pipeline</span>
            </div>
            <div class="grafana-controls">
                <div class="grafana-timerange">
                    <div class="timerange-btn">15m</div>
                    <div class="timerange-btn active">30m</div>
                    <div class="timerange-btn">1h</div>
                    <div class="timerange-btn">6h</div>
                </div>
            </div>
        </div>
        <div class="grafana-embed" style="height: 400px; display: flex; align-items: center; justify-content: center; color: #adb5bd;">
            <div style="text-align: center;">
                <div style="font-size: 14px; font-weight: 600; margin-bottom: 8px;">Grafana Dashboard</div>
                <div style="font-size: 12px;">Configure GRAFANA_URL in settings to view metrics</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Chatbot
    st.markdown("""
    <div class="card">
        <div class="card-title">Pipeline Assistant</div>
        <div class="chat-scroll">
            <div class="chat-msg">
                <div class="chat-avatar ai">AI</div>
                <div class="chat-bubble">Hi! I'm your pipeline assistant. Ask me about runs, DQ scores, or any enrichment results.</div>
            </div>
            <div class="chat-msg">
                <div class="chat-avatar user">You</div>
                <div class="chat-bubble">Show me the recent runs with the highest DQ improvement</div>
            </div>
            <div class="chat-msg">
                <div class="chat-avatar ai">AI</div>
                <div class="chat-bubble">Based on the run history, the top DQ improvements were:<br><br>
                1. <b>fda_recalls_q2.csv</b>: +17.3% (61.2% → 78.5%)<br>
                2. <b>whole_foods_sku.csv</b>: +13.7% (55.3% → 69.0%)</div>
            </div>
        </div>
        <div class="chat-input">
            <input class="field-input" placeholder="Ask about pipeline runs, DQ scores, or enrichment...">
            <button class="btn btn-primary">Send</button>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_topbar():
    """Render the topbar."""
    st.markdown("""
    <div class="topbar-section">
        <div class="topbar-brand">
            <div class="logo">DF</div>
            <div class="brand-name">Data<span>Forge</span></div>
        </div>
        <div class="health-rail">
            <span class="health-label">Infra</span>
            <div class="health-pill"><span class="health-dot dot-ok"></span>Redis</div>
            <div class="health-pill"><span class="health-dot dot-ok"></span>Postgres</div>
            <div class="health-pill"><span class="health-dot dot-warn"></span>Kafka</div>
            <div class="health-pill"><span class="health-dot dot-ok"></span>ChromaDB</div>
            <div class="health-pill"><span class="health-dot dot-ok"></span>GCS</div>
        </div>
        <div class="run-badge">2 active runs</div>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar(page: str):
    """Render the sidebar navigation."""
    with st.sidebar:
        st.markdown(STYLES, unsafe_allow_html=True)

        pages = [
            ("dashboard", "db", "Dashboard"),
            ("domain", "dp", "Domain Packs"),
            ("pipeline", "pl", "Pipeline", "ETL"),
            ("observability", "ob", "Observability"),
            ("search", "sr", "Search"),
            ("recs", "rc", "Recommendations"),
            ("airflow", "af", "Airflow", "12"),
            ("tests", "ts", "Tests"),
        ]

        for p in pages:
            page_id = p[0]
            icon = p[1]
            label = p[2]
            badge = p[3] if len(p) > 3 else None

            badge_html = ""
            if badge:
                badge_html = '<span class="badge" style="margin-left: auto; font-size: 10px; padding: 1px 6px; background: #e9ecef;">{}</span>'.format(badge)

            st.markdown("""
            <a href="?page={page_id}" style="text-decoration: none;">
                <div class="quick-action" style="margin: 4px 8px;">
                    <span style="font-family: monospace; font-size: 10px; font-weight: 700; width: 20px; opacity: 0.6;">{icon}</span>
                    <span>{label}</span>
                    {badge_html}
                </div>
            </a>
            """.format(page_id=page_id, icon=icon, label=label, badge_html=badge_html), unsafe_allow_html=True)


def main():
    """Main app entry point."""
    st.set_page_config(
        page_title="Market Intelligence Platform",
        page_icon="DF",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    init_session_state()
    get_current_page()
    render_html_shell()


if __name__ == "__main__":
    main()
