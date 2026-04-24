"""DataForge — Marketplace Intelligence Platform — Main Streamlit Entry Point."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st

st.set_page_config(
    page_title="Marketplace Intelligence Platform",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
GLOBAL_CSS = """
/* ── reset & root ── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --bg:#ffffff; --surface:#f8f9fa; --surface2:#f1f3f5; --surface3:#e9ecef;
  --border:#dee2e6; --border-hi:#ced4da;
  --text:#212529; --text-muted:#6c757d; --text-dim:#adb5bd;
  --accent:#1971c2; --accent-dim:#e7f0fb;
  --green:#2f9e44; --green-dim:#ebf9ee;
  --amber:#e67700; --amber-dim:#fff3bf;
  --red:#c92a2a; --red-dim:#fff5f5;
  --purple:#6741d9; --purple-dim:#f3f0ff;
  --cyan:#0c8599; --cyan-dim:#e3fafc;
  --orange:#d9480f; --orange-dim:#fff4e6;
  --mono:'SF Mono','Fira Code',Consolas,monospace;
  --radius:6px; --radius-lg:10px;
  --shadow:0 1px 3px rgba(0,0,0,0.07),0 1px 2px rgba(0,0,0,0.04);
  --shadow-md:0 4px 12px rgba(0,0,0,0.08);
  --gap:16px; --card-pad:18px 20px;
}

/* ── Streamlit overrides ── */
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { display: none; }
.main .block-container {
  padding-top: 0.5rem !important;
  padding-left: 1.5rem !important;
  padding-right: 1.5rem !important;
  max-width: 100% !important;
}
/* ── Global font size ── */
body, p, div, span, li, td, th, input, select, textarea, label,
[class*="css"] {
  font-size: 16px !important;
  font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif !important;
}
h1 { font-size: 28px !important; font-weight: 700 !important; }
h2 { font-size: 24px !important; font-weight: 700 !important; }
h3 { font-size: 20px !important; font-weight: 600 !important; }
/* ── Sidebar width + styling ── */
[data-testid="stSidebar"] {
  background: var(--surface) !important;
  border-right: 1px solid var(--border);
  min-width: 300px !important;
  max-width: 300px !important;
  width: 300px !important;
}
[data-testid="stSidebar"] > div:first-child { padding: 0 !important; }
[data-testid="stSidebar"] .stButton > button {
  background: transparent !important;
  border: none !important;
  border-left: 3px solid transparent !important;
  color: var(--text) !important;
  font-size: 18px !important;
  font-weight: 500 !important;
  padding: 10px 16px !important;
  width: 100% !important;
  text-align: left !important;
  border-radius: 0 !important;
  display: flex !important;
  align-items: center !important;
  gap: 10px !important;
  transition: all 0.1s !important;
  box-shadow: none !important;
  justify-content: flex-start !important;
}
[data-testid="stSidebar"] .stButton > button p {
  font-size: 18px !important;
  text-align: left !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
  color: var(--accent) !important;
  background: var(--accent-dim) !important;
  border-left-color: var(--accent) !important;
}
/* ── Default Streamlit button overrides ── */
.stButton > button {
  background: var(--surface) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius) !important;
  font-size: 16px !important;
  font-weight: 600 !important;
  box-shadow: none !important;
  transition: all 0.12s !important;
}
.stButton > button:hover {
  border-color: var(--accent) !important;
  color: var(--accent) !important;
  background: var(--accent-dim) !important;
}
button[kind="primary"], .stButton > button[kind="primary"] {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
}
button[kind="primary"]:hover { background: #1562a8 !important; }
.sidebar-brand {
  display:flex; align-items:center; gap:10px;
  padding:18px 16px 14px; border-bottom:1px solid var(--border); margin-bottom:4px;
}
.sidebar-logo {
  width:38px; height:38px; background:var(--accent); border-radius:6px;
  display:flex; align-items:center; justify-content:center;
  font-size:13px; font-weight:800; color:#fff; letter-spacing:-0.5px; flex-shrink:0;
}
.sidebar-name { font-size:18px; font-weight:700; color:var(--text); line-height:1.3; }
.sidebar-name span { color:var(--accent); }
.nav-section-label {
  font-size:13px; font-weight:700; color:var(--text-dim);
  letter-spacing:.1em; text-transform:uppercase;
  padding:14px 16px 4px; display:block;
}
.nav-badge {
  margin-left:auto; font-size:12px; font-weight:700;
  padding:2px 7px; border-radius:10px;
  background:var(--surface3); color:var(--text-muted);
  flex-shrink:0;
}
.nav-badge.exp { background:var(--orange-dim); color:var(--orange); }

/* ── topbar ── */
.df-topbar {
  display:flex; align-items:center; justify-content:space-between;
  background:var(--bg); border-bottom:1px solid var(--border);
  padding:0 20px; height:44px; margin-top:-8px; margin-bottom:12px;
  position:sticky; top:0; z-index:100;
}
.health-rail { display:flex; align-items:center; gap:5px; flex-wrap:wrap; }
.health-label { font-size:13px; font-weight:700; color:var(--text-dim); text-transform:uppercase; letter-spacing:.07em; margin-right:4px; }
.health-pill {
  display:inline-flex; align-items:center; gap:4px;
  padding:2px 8px; border-radius:20px;
  border:1px solid var(--border); background:var(--surface);
  font-size:14px; font-weight:500; color:var(--text-muted);
}
.health-dot { width:7px; height:7px; border-radius:50%; flex-shrink:0; }
.health-dot.ok    { background:var(--green); }
.health-dot.warn  { background:var(--amber); }
.health-dot.error { background:var(--red); }
.run-badge {
  display:inline-flex; align-items:center; gap:6px;
  padding:3px 11px; border-radius:20px;
  background:var(--accent-dim); border:1px solid rgba(25,113,194,.2);
  font-size:14px; font-weight:600; color:var(--accent);
}
@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.5;transform:scale(.8)} }

/* ── page header ── */
.page-header { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:22px; }
.page-title { font-size:26px !important; font-weight:700; letter-spacing:-.4px; color:var(--text); }
.page-subtitle { font-size:16px !important; color:var(--text-muted); margin-top:4px; }

/* ── cards ── */
.card {
  background:var(--bg); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:var(--card-pad);
  box-shadow:var(--shadow); margin-bottom:var(--gap);
}
.card-title {
  display:flex; align-items:center; gap:7px;
  font-size:14px; font-weight:700; color:var(--text-muted);
  text-transform:uppercase; letter-spacing:.06em; margin-bottom:14px;
}
.card-title::before {
  content:''; width:3px; height:14px;
  background:var(--accent); border-radius:2px; flex-shrink:0;
}

/* ── stat cards ── */
.stat-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius); padding:14px 16px; min-height:110px;
}
.stat-label { font-size:13px; font-weight:700; color:var(--text-dim); text-transform:uppercase; letter-spacing:.06em; margin-bottom:8px; }
.stat-value { font-size:34px; font-weight:700; letter-spacing:-.8px; color:var(--text); line-height:1; }
.stat-value.sv-xl { font-size:34px; }
.stat-value.sv-lg { font-size:30px; }
.stat-value.sv-md { font-size:26px; }
.stat-value.sv-sm { font-size:22px; }
.stat-value.sv-xs { font-size:16px; font-family:var(--mono); letter-spacing:0; margin-top:4px; }
.stat-unit { font-size:18px; font-weight:500; color:var(--text-muted); letter-spacing:0; }
.stat-delta { font-size:13px; font-weight:500; margin-top:6px; }
.stat-delta.up   { color:var(--green); }
.stat-delta.down { color:var(--red); }

/* ── badges ── */
.badge {
  display:inline-flex; align-items:center; gap:4px;
  padding:2px 8px; border-radius:4px;
  font-size:13px; font-weight:600; white-space:nowrap;
}
.badge.success { background:var(--green-dim); color:var(--green); border:1px solid rgba(47,158,68,.15); }
.badge.error   { background:var(--red-dim);   color:var(--red);   border:1px solid rgba(201,42,42,.15); }
.badge.warning { background:var(--amber-dim); color:var(--amber); border:1px solid rgba(230,119,0,.15); }
.badge.info    { background:var(--accent-dim);color:var(--accent);border:1px solid rgba(25,113,194,.15); }
.badge.running { background:var(--cyan-dim);  color:var(--cyan);  border:1px solid rgba(12,133,153,.15); }
.badge.purple  { background:var(--purple-dim);color:var(--purple);border:1px solid rgba(103,65,217,.15); }
.badge.orange  { background:var(--orange-dim);color:var(--orange);border:1px solid rgba(217,72,15,.15); }

/* ── tables ── */
.data-table { width:100%; border-collapse:collapse; font-size:16px; }
.data-table th {
  font-size:13px; font-weight:700; text-transform:uppercase; letter-spacing:.06em;
  color:var(--text-dim); padding:10px 14px;
  border-bottom:1px solid var(--border); background:var(--surface); text-align:left;
}
.data-table td { padding:10px 14px; border-bottom:1px solid var(--border); color:var(--text-muted); vertical-align:middle; font-size:16px; }
.data-table td:first-child { color:var(--text); font-weight:500; }
.data-table tr:last-child td { border-bottom:none; }
.data-table tbody tr:hover { background:var(--surface); }

/* ── dq arrow ── */
.dq-arrow { display:flex; align-items:center; gap:4px; font-family:var(--mono); font-size:15px; }
.dq-arrow .before { color:var(--text-muted); }
.dq-arrow .arrow  { color:var(--text-dim); }
.dq-arrow .after  { color:var(--green); font-weight:600; }
.dq-arrow .delta  { color:var(--green); font-size:13px; }
.dq-arrow .after.na { color:var(--text-dim); }

/* ── terminal ── */
.terminal {
  background:var(--surface); border:1px solid var(--border);
  border-left:3px solid var(--border-hi); border-radius:var(--radius);
  padding:14px 16px; font-family:var(--mono); font-size:15px; line-height:1.7;
  color:var(--text-muted); overflow-y:auto;
}
.terminal .t-green { color:var(--green); }
.terminal .t-amber { color:var(--amber); }
.terminal .t-blue  { color:var(--accent); }
.terminal .t-red   { color:var(--red); }
.terminal .t-dim   { color:var(--text-dim); }
.terminal .t-text  { color:var(--text); }
.stream-dot {
  display:inline-block; width:8px; height:8px; border-radius:50%;
  background:var(--green); animation:pulse 2s ease-in-out infinite;
  vertical-align:middle; margin-right:4px;
}

/* ── stepper ── */
.stepper { display:flex; align-items:flex-start; margin-bottom:28px; }
.step { display:flex; align-items:center; flex:1; }
.step-node { display:flex; flex-direction:column; align-items:center; gap:5px; flex-shrink:0; }
.step-circle {
  width:36px; height:36px; border-radius:50%;
  display:flex; align-items:center; justify-content:center;
  font-size:14px; font-weight:700; font-family:var(--mono);
  border:2px solid var(--border); background:var(--surface); color:var(--text-dim);
}
.step-circle.done   { background:var(--green-dim);  border-color:var(--green);  color:var(--green); }
.step-circle.active { background:var(--accent-dim); border-color:var(--accent); color:var(--accent); box-shadow:0 0 0 3px rgba(25,113,194,.1); }
.step-label { font-size:13px; font-weight:500; color:var(--text-dim); white-space:nowrap; }
.step-label.done   { color:var(--green); }
.step-label.active { color:var(--accent); }
.step-line { flex:1; height:1px; background:var(--border); margin:0 4px; transform:translateY(-15px); }
.step-line.done { background:var(--green); }

/* ── block chips ── */
.block-chips { display:flex; flex-wrap:wrap; gap:6px; }
.block-chip {
  display:flex; align-items:center; gap:5px;
  padding:4px 10px; border-radius:4px;
  font-family:var(--mono); font-size:13px; font-weight:500;
  border:1px solid var(--border); background:var(--surface); color:var(--text-muted);
}
.block-chip.done    { background:var(--green-dim);  border-color:rgba(47,158,68,.25);  color:var(--green); }
.block-chip.running { background:var(--accent-dim); border-color:rgba(25,113,194,.25); color:var(--accent); animation:blink 1.5s ease-in-out infinite; }
.block-chip.error   { background:var(--red-dim);    border-color:rgba(201,42,42,.25);  color:var(--red); }
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:.55} }

/* ── buttons ── */
.btn {
  display:inline-flex; align-items:center; gap:6px;
  padding:8px 16px; border-radius:var(--radius);
  font-size:16px; font-weight:600; font-family:inherit;
  cursor:pointer; transition:all .12s; border:none; outline:none;
}
.btn-primary { background:var(--accent); color:#fff; }
.btn-ghost   { background:transparent; color:var(--text-muted); border:1px solid var(--border); }
.btn-orange  { background:var(--orange-dim); color:var(--orange); border:1px solid rgba(217,72,15,.2); }
.btn-danger  { background:var(--red-dim); color:var(--red); border:1px solid rgba(201,42,42,.2); }
.btn-sm { padding:5px 11px; font-size:13px; }

/* ── bar charts ── */
.bar-chart { display:flex; flex-direction:column; gap:10px; }
.bar-row { display:flex; align-items:center; gap:10px; }
.bar-label { width:100px; flex-shrink:0; text-align:right; font-family:var(--mono); font-size:13px; color:var(--text-muted); }
.bar-track { flex:2; height:12px; background:var(--surface2); border-radius:4px; overflow:hidden; }
.bar-fill  { height:100%; border-radius:4px; }
.bar-fill.bar-accent { background:var(--accent); }
.bar-fill.bar-green  { background:var(--green); }
.bar-fill.bar-amber  { background:var(--amber); }
.bar-fill.bar-orange { background:var(--orange); }
.bar-fill.bar-red    { background:var(--red); }
.bar-val   { width:60px; font-family:var(--mono); font-size:13px; color:var(--text-muted); }

/* ── tier bars ── */
.tier-bar { display:flex; overflow:hidden; gap:2px; border-radius:4px; height:10px; }
.tier-s1 { background:var(--green); }
.tier-s2 { background:var(--accent); }
.tier-s3 { background:var(--amber); }
.tier-s4 { background:var(--red); }
.tier-legend { display:flex; gap:14px; margin-top:12px; flex-wrap:wrap; }
.tier-legend-item { display:flex; align-items:center; gap:6px; font-size:13px; color:var(--text-muted); }
.tier-dot { width:9px; height:9px; border-radius:50%; flex-shrink:0; }
.tier-dot.s1 { background:var(--green); }
.tier-dot.s2 { background:var(--accent); }
.tier-dot.s3 { background:var(--amber); }
.tier-dot.s4 { background:var(--red); }

/* ── sliders ── */
.slider-row { display:flex; align-items:center; gap:12px; margin-bottom:12px; }
.slider-name { font-family:var(--mono); font-size:13px; font-weight:500; color:var(--text-muted); width:200px; flex-shrink:0; }

/* ── yaml editor ── */
.yaml-editor {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius); padding:13px 15px;
  font-family:var(--mono); font-size:13px; line-height:1.8; min-height:100px;
}
.yaml-key  { color:var(--accent); }
.yaml-val  { color:var(--green); }
.yaml-num  { color:var(--amber); }
.yaml-bool { color:var(--red); }
.yaml-editor.new { border-color:rgba(47,158,68,.25); }
.section-label { font-size:12px; font-weight:700; color:var(--text-dim); text-transform:uppercase; letter-spacing:.07em; margin-bottom:8px; }
.section-label.generated { color:var(--green); }

/* ── chat ── */
.chat-bubble {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius); padding:12px 16px;
  font-size:16px; line-height:1.6; color:var(--text-muted);
}
.run-chip {
  display:inline-flex; align-items:center; padding:1px 7px; border-radius:4px;
  background:var(--accent-dim); color:var(--accent);
  font-family:var(--mono); font-size:12px; font-weight:600; margin:0 2px;
}

/* ── decision cards ── */
.decision-card {
  display:flex; align-items:center; gap:12px;
  padding:13px 15px; border-radius:var(--radius);
  background:var(--surface); border:1px solid var(--border); margin-bottom:8px;
}
.decision-field  { font-family:var(--mono); font-size:15px; font-weight:600; color:var(--accent); }
.decision-reason { font-family:var(--mono); font-size:13px; color:var(--text-dim); margin-top:2px; }

/* ── alerts ── */
.alert { padding:11px 13px; border-radius:var(--radius); font-size:15px; font-weight:500; margin-bottom:14px; }
.alert.purple { background:var(--purple-dim); border:1px solid rgba(103,65,217,.12); color:var(--purple); }
.alert.red    { background:var(--red-dim);    border:1px solid rgba(201,42,42,.12);  color:var(--red); }
.alert.green  { background:var(--green-dim);  border:1px solid rgba(47,158,68,.12);  color:var(--green); }
.alert.orange { background:var(--orange-dim); border:1px solid rgba(217,72,15,.12);  color:var(--orange); }

/* ── dag chain ── */
.dag-chain { display:flex; align-items:center; overflow-x:auto; padding:14px 0; gap:0; }
.dag-node { display:flex; flex-direction:column; align-items:center; gap:5px; flex-shrink:0; }
.dag-box {
  padding:9px 18px; border-radius:var(--radius);
  border:1px solid var(--border); background:var(--surface);
  font-family:var(--mono); font-size:13px; font-weight:500; color:var(--text-muted); white-space:nowrap;
}
.dag-box.ok      { border-color:rgba(47,158,68,.25); background:var(--green-dim); color:var(--green); }
.dag-box.running { border-color:rgba(12,133,153,.25); background:var(--cyan-dim); color:var(--cyan); animation:blink 1.5s infinite; }
.dag-box.paused  { border-color:rgba(230,119,0,.25); background:var(--amber-dim); color:var(--amber); }
.dag-schedule { font-family:var(--mono); font-size:12px; color:var(--text-dim); }
.dag-schedule.running { color:var(--cyan); }
.dag-arrow-line { width:28px; height:1px; background:var(--border); flex-shrink:0; position:relative; top:-11px; margin:0 2px; }

/* ── dag strip ── */
.dag-strip { display:flex; gap:10px; overflow-x:auto; flex-wrap:wrap; }
.dag-strip-item {
  display:flex; align-items:center; gap:10px; padding:10px 15px;
  border-radius:var(--radius); border:1px solid rgba(12,133,153,.2);
  background:var(--cyan-dim); white-space:nowrap;
}
.dag-strip-name { font-family:var(--mono); font-size:15px; font-weight:600; color:var(--cyan); }
.dag-strip-time { font-family:var(--mono); font-size:12px; color:var(--text-dim); }
.dag-spin {
  width:14px; height:14px; border-radius:50%;
  border:2px solid rgba(12,133,153,.2); border-top-color:var(--cyan);
  animation:spin .8s linear infinite; flex-shrink:0;
}
@keyframes spin { to { transform:rotate(360deg); } }

/* ── product grid ── */
.product-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:13px; }
.product-card {
  background:var(--bg); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:15px;
  cursor:pointer; box-shadow:var(--shadow);
}
.product-card:hover { border-color:var(--accent); box-shadow:var(--shadow-md); }
.product-card.recalled { border-color:rgba(201,42,42,.2); }
.product-name  { font-size:17px; font-weight:700; color:var(--text); margin-bottom:3px; }
.product-brand { font-size:13px; font-weight:600; color:var(--text-dim); text-transform:uppercase; letter-spacing:.04em; margin-bottom:10px; }
.product-tags  { display:flex; flex-wrap:wrap; gap:4px; }

/* ── guardrails ── */
.guardrail-badge {
  display:flex; align-items:center; gap:8px;
  padding:10px 14px; border-radius:var(--radius);
  background:var(--green-dim); border:1px solid rgba(47,158,68,.15);
  font-family:var(--mono); font-size:13px; font-weight:500; color:var(--green);
  margin-bottom:6px;
}

/* ── misc ── */
.mono { font-family:var(--mono); font-size:15px; }
.tc-green  { color:var(--green); font-weight:600; }
.tc-red    { color:var(--red); }
.tc-amber  { color:var(--amber); }
.tc-accent { color:var(--accent); font-weight:600; }
.tc-dim    { color:var(--text-dim); font-size:13px; }
.tc-orange { color:var(--orange); font-weight:600; }
.c-green  { color:var(--green); }
.c-red    { color:var(--red); }
.c-amber  { color:var(--amber); }
.c-accent { color:var(--accent); }
.c-muted  { color:var(--text-muted); }
.c-dim    { color:var(--text-dim); }
.mlflow-logo { display:flex; align-items:center; gap:8px; font-size:17px; font-weight:700; color:var(--orange); }
.mlflow-logo-icon {
  width:26px; height:26px; border-radius:4px; background:var(--orange); color:#fff;
  display:flex; align-items:center; justify-content:center;
  font-size:13px; font-weight:800; font-family:var(--mono);
}
.resolve-row { display:flex; align-items:center; gap:8px; font-size:15px; color:var(--text-muted); }
.failed-list { font-family:var(--mono); font-size:13px; line-height:1.9; }
.filter-row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:16px; }
.agent-row { display:flex; align-items:center; gap:10px; margin-bottom:10px; }
.agent-label { font-family:var(--mono); font-size:15px; color:var(--text-muted); }
.agent-result { font-family:var(--mono); font-size:13px; margin-left:auto; }
.mcp-badge {
  display:inline-flex; align-items:center; gap:5px;
  padding:3px 10px; border-radius:20px; font-size:13px; font-weight:500;
  border:1px solid var(--border); background:var(--surface); color:var(--text-muted);
  margin-right:6px; margin-bottom:6px;
}
.drop-zone {
  border:1.5px dashed var(--border-hi); border-radius:var(--radius-lg);
  padding:28px; text-align:center; cursor:pointer;
  color:var(--text-dim); font-size:15px; font-weight:500;
}
.divider { border:none; border-top:1px solid var(--border); margin:14px 0; }

/* Streamlit tab overrides */
.stTabs [data-baseweb="tab"] { font-size:17px !important; font-weight:600 !important; }
.stTabs [data-baseweb="tab-list"] { border-bottom:1px solid var(--border); }
.stSlider { margin-bottom:4px; }
/* Streamlit widget font sizes */
.stSelectbox label, .stTextInput label, .stTextArea label,
.stCheckbox label, .stRadio label, .stFileUploader label { font-size:16px !important; }
.stSelectbox > div > div, .stTextInput > div > div > input { font-size:16px !important; }
.stMarkdown p, .stMarkdown li { font-size:16px !important; line-height:1.65; }
/* stat-card numbers */
.stat-value { font-size:34px !important; }
.stat-value.sv-lg { font-size:30px !important; }
.stat-value.sv-md { font-size:26px !important; }
.stat-label { font-size:13px !important; }
.card-title { font-size:14px !important; }
/* Remove Streamlit red focus ring */
.stTextInput > div > div > input:focus, .stTextArea textarea:focus {
  border-color: var(--accent) !important; box-shadow: 0 0 0 2px rgba(25,113,194,.12) !important;
}
/* Streamlit expander */
details summary { font-size:17px !important; font-weight:600 !important; }
"""

st.markdown(f"<style>{GLOBAL_CSS}</style>", unsafe_allow_html=True)


# ── Session state init ────────────────────────────────────────────────────────
if "page" not in st.session_state:
    st.session_state.page = "dashboard"
if "pipeline_state" not in st.session_state:
    st.session_state.pipeline_state = {}
if "step" not in st.session_state:
    st.session_state.step = 0
if "log_entries" not in st.session_state:
    st.session_state.log_entries = []


# ── Topbar ────────────────────────────────────────────────────────────────────
from src.ui.utils.service_health import check_all_services
from src.ui.utils.airflow_client import list_dags as _list_dags_topbar

try:
    health = check_all_services()
except Exception:
    health = {"Redis": "warn", "Postgres": "warn", "Kafka": "warn",
              "ChromaDB": "warn", "MLflow": "warn", "Grafana": "warn"}

pills_html = ""
for svc, status in health.items():
    pills_html += f'<div class="health-pill"><span class="health-dot {status}"></span>{svc}</div>'

try:
    _all_dags = _list_dags_topbar()
    total_dags = len(_all_dags)
except Exception:
    total_dags = 12

dag_badge = f'<div class="run-badge">🔄 {total_dags} DAGs scheduled</div>'

st.markdown(f"""
<div class="df-topbar">
  <div style="display:flex;align-items:center;gap:10px;">
    <div style="width:30px;height:30px;background:var(--accent);border-radius:6px;
                display:flex;align-items:center;justify-content:center;
                font-size:10px;font-weight:800;color:#fff;letter-spacing:-0.5px;">MIP</div>
    <div style="font-size:22px;font-weight:800;color:var(--text);letter-spacing:-.3px;">Marketplace <span style="color:var(--accent);">Intelligence</span> Platform</div>
  </div>
  <div class="health-rail">
    <span class="health-label">Infra</span>
    {pills_html}
    <div style="width:1px;height:18px;background:var(--border);margin:0 4px;"></div>
    {dag_badge}
  </div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar navigation ────────────────────────────────────────────────────────
def _nav_btn(label: str, page_key: str, icon: str, badge: str = "", badge_class: str = ""):
    is_active = st.session_state.page == page_key
    badge_html = f'<span class="nav-badge {badge_class}">{badge}</span>' if badge else ""
    # Inject active class via CSS hack using a unique key
    if is_active:
        st.markdown(f"""
        <style>
        [data-testid="stSidebar"] [data-testid="stButton"][key="nav_{page_key}"] > button {{
          color:var(--accent) !important; background:var(--accent-dim) !important;
          border-left-color:var(--accent) !important;
        }}
        </style>""", unsafe_allow_html=True)
    clicked = st.sidebar.button(
        f"{icon}  {label}",
        key=f"nav_{page_key}",
        use_container_width=True,
    )
    if clicked:
        st.session_state.page = page_key
        st.rerun()


with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
      <div class="sidebar-logo" style="font-size:12px;letter-spacing:-0.5px;">MIP</div>
      <div>
        <div class="sidebar-name" style="font-size:18px;">Marketplace <span>Intel</span></div>
        <div style="font-size:13px;color:var(--text-dim);margin-top:1px;">Platform v2.0</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _nav_btn("Dashboard",      "dashboard",  "🏠")

    st.markdown('<span class="nav-section-label">Pipeline</span>', unsafe_allow_html=True)
    _nav_btn("Domain Packs",   "domain",     "📦")
    _nav_btn("Pipeline",       "pipeline",   "▶", "ETL")
    _nav_btn("Enrichment Lab", "enrichment", "🔬")

    st.markdown('<span class="nav-section-label">Analytics</span>', unsafe_allow_html=True)
    _nav_btn("Observability",    "observability",  "📊")
    _nav_btn("MLflow",           "mlflow",         "📈", "EXP", "exp")
    _nav_btn("Search",           "search",         "🔍")
    _nav_btn("Recommendations",  "recs",           "💡")

    st.markdown('<span class="nav-section-label">Ops</span>', unsafe_allow_html=True)
    _nav_btn("Airflow",  "airflow", "🔄", "12")
    _nav_btn("Tests",    "tests",   "✅")


# ── Route to page ─────────────────────────────────────────────────────────────
page = st.session_state.page

if page == "dashboard":
    from src.ui.pages.dashboard import render_dashboard
    render_dashboard()
elif page == "pipeline":
    from src.ui.pages.pipeline_wizard import render_pipeline
    render_pipeline()
elif page == "domain":
    from src.ui.pages.domain_packs import render_domain_packs
    render_domain_packs()
elif page == "enrichment":
    from src.ui.pages.enrichment_lab import render_enrichment_lab
    render_enrichment_lab()
elif page == "observability":
    from src.ui.pages.observability import render_observability
    render_observability()
elif page == "mlflow":
    from src.ui.pages.mlflow_tracker import render_mlflow
    render_mlflow()
elif page == "search":
    from src.ui.pages.search import render_search
    render_search()
elif page == "recs":
    from src.ui.pages.recommendations import render_recommendations
    render_recommendations()
elif page == "airflow":
    from src.ui.pages.airflow_panel import render_airflow
    render_airflow()
elif page == "tests":
    from src.ui.pages.tests_runner import render_tests
    render_tests()
