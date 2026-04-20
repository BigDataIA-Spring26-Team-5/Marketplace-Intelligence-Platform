"""CSS styles for the ETL pipeline Streamlit app."""

GLOBAL_CSS = """
<style>
    /* ── Base overrides ─────────────────────────────────── */
    .stApp { background-color: #ffffff; }

    /* ── Step indicator bar ──────────────────────────────── */
    .step-bar {
        display: flex; gap: 0; margin: 1.5rem 0 2rem 0;
        border-radius: 8px; overflow: hidden;
        border: 1px solid #d0d7de;
    }
    .step-item {
        flex: 1; padding: 12px 16px; text-align: center;
        background: #f6f8fa; color: #57606a;
        font-size: 0.82rem; font-weight: 500;
        border-right: 1px solid #d0d7de;
        transition: all 0.2s ease;
        cursor: default;
    }
    .step-item:last-child { border-right: none; }
    .step-item.clickable {
        cursor: pointer;
    }
    .step-item.clickable:hover {
        background: #eaeef2;
    }
    .step-item.active {
        background: #ddf4ff; color: #0969da;
        box-shadow: inset 0 -2px 0 #0969da;
    }
    .step-item.done {
        background: #dafbe1; color: #1a7f37;
    }
    .step-item .step-num {
        display: inline-block; width: 22px; height: 22px;
        line-height: 22px; border-radius: 50%;
        background: #eaeef2; color: #57606a;
        font-size: 0.72rem; margin-right: 6px;
    }
    .step-item.active .step-num { background: #b6e3ff; color: #0969da; }
    .step-item.done .step-num { background: #aceebb; color: #1a7f37; }

    /* ── Section headers ────────────────────────────────── */
    .section-header {
        font-size: 1.1rem; font-weight: 600;
        color: #24292f; margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #d0d7de;
    }

    /* ── Badges ─────────────────────────────────────────── */
    .badge {
        display: inline-block; padding: 2px 8px;
        border-radius: 12px; font-size: 0.72rem;
        font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .badge-map { background: #ddf4ff; color: #0969da; }
    .badge-add { background: #fff8c5; color: #bf8700; }
    .badge-drop { background: #ffebe9; color: #cf222e; }
    .badge-new { background: #dafbe1; color: #1a7f37; }
    .badge-hit { background: #dafbe1; color: #1a7f37; }
    .badge-miss { background: #ffebe9; color: #cf222e; }
    .badge-pass { background: #dafbe1; color: #1a7f37; }
    .badge-fail { background: #ffebe9; color: #cf222e; }
    .badge-missing { background: #ffebe9; color: #cf222e; }
    .badge-derivable { background: #ddf4ff; color: #0550ae; }
    .badge-type_cast { background: #ddf4ff; color: #0550ae; }
    .badge-derive { background: #fbefff; color: #8250df; }
    .badge-format_transform { background: #ddf4ff; color: #0550ae; }
    .badge-enrichment { background: #dafbe1; color: #1a7f37; }
    .badge-alias { background: #fbefff; color: #6e40c9; }

    /* ── Schema delta table ─────────────────────────────── */
    .schema-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.84rem; margin: 0.5rem 0;
    }
    .schema-table th {
        background: #f6f8fa; color: #57606a;
        padding: 10px 14px; text-align: left;
        font-weight: 600; font-size: 0.76rem;
        text-transform: uppercase; letter-spacing: 0.04em;
        border-bottom: 2px solid #d0d7de;
    }
    .schema-table td {
        padding: 9px 14px; color: #24292f;
        border-bottom: 1px solid #eaeef2;
    }
    .schema-table tr:hover td { background: #f6f8fa; }
    .schema-table .col-source { color: #bc4c00; font-family: monospace; }
    .schema-table .col-unified { color: #0969da; font-family: monospace; }
    .schema-table .col-type { color: #57606a; font-family: monospace; font-size: 0.78rem; }
    .schema-table .col-null { font-family: monospace; }
    .schema-table .null-low { color: #1a7f37; }
    .schema-table .null-mid { color: #bf8700; }
    .schema-table .null-high { color: #cf222e; }

    /* ── Metric cards ───────────────────────────────────── */
    .metric-row { display: flex; gap: 16px; margin: 1rem 0; }
    .metric-card {
        flex: 1; background: #f6f8fa;
        border: 1px solid #d0d7de; border-radius: 10px;
        padding: 20px 24px; text-align: center;
    }
    .metric-card .metric-label {
        font-size: 0.76rem; color: #57606a;
        text-transform: uppercase; letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-card .metric-value {
        font-size: 2rem; font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-card .metric-sub {
        font-size: 0.78rem; color: #6e7781;
        margin-top: 4px;
    }
    .val-good { color: #1a7f37; }
    .val-warn { color: #bf8700; }
    .val-bad { color: #cf222e; }
    .val-neutral { color: #0969da; }

    /* ── Code review block ──────────────────────────────── */
    .code-review {
        background: #ffffff; border: 1px solid #d0d7de;
        border-radius: 8px; margin: 0.75rem 0;
        overflow: hidden;
    }
    .code-review-header {
        background: #f6f8fa; padding: 10px 16px;
        display: flex; justify-content: space-between;
        align-items: center; border-bottom: 1px solid #d0d7de;
    }
    .code-review-header .fn-name {
        font-family: monospace; color: #8250df;
        font-size: 0.88rem; font-weight: 600;
    }
    .code-review pre {
        margin: 0; padding: 16px;
        background: #f6f8fa; color: #24292f;
        font-size: 0.82rem; line-height: 1.5;
        overflow-x: auto; font-family: 'JetBrains Mono', monospace;
    }
    .code-review .validation-bar {
        background: #f6f8fa; padding: 10px 16px;
        border-top: 1px solid #d0d7de;
        font-size: 0.8rem; color: #57606a;
    }

    /* ── Sample I/O table ───────────────────────────────── */
    .io-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .io-table th {
        background: #f6f8fa; color: #57606a;
        padding: 8px 12px; text-align: left;
        font-size: 0.74rem; text-transform: uppercase;
        border-bottom: 1px solid #d0d7de;
    }
    .io-table td {
        padding: 7px 12px; color: #24292f;
        border-bottom: 1px solid #eaeef2;
        font-family: monospace;
    }
    .io-table .val-in { color: #bc4c00; }
    .io-table .val-out { color: #1a7f37; }

    /* ── Block waterfall ────────────────────────────────── */
    .waterfall { margin: 1rem 0; }
    .waterfall-row {
        display: flex; align-items: center;
        margin: 4px 0; font-size: 0.8rem;
    }
    .waterfall-label {
        width: 180px; color: #57606a;
        font-family: monospace; font-size: 0.78rem;
        text-align: right; padding-right: 12px;
        flex-shrink: 0;
    }
    .waterfall-bar-wrap { flex: 1; display: flex; align-items: center; gap: 8px; }
    .waterfall-bar {
        height: 22px; border-radius: 3px;
        background: #54aeff; min-width: 4px;
        transition: width 0.4s ease;
    }
    .waterfall-bar.loss { background: #ffcecb; }
    .waterfall-count {
        color: #6e7781; font-family: monospace;
        font-size: 0.76rem; white-space: nowrap;
    }

    /* ── Quarantine table ───────────────────────────────── */
    .quarantine-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .quarantine-table th {
        background: #ffebe9; color: #cf222e;
        padding: 10px 14px; text-align: left;
        font-size: 0.76rem; text-transform: uppercase;
        border-bottom: 2px solid #ffcecb;
    }
    .quarantine-table td {
        padding: 8px 14px; color: #24292f;
        border-bottom: 1px solid #eaeef2;
    }
    .quarantine-table .reason { color: #bc4c00; font-size: 0.78rem; }

    /* ── Enrichment breakdown ───────────────────────────── */
    .enrich-breakdown { margin: 1rem 0; }
    .enrich-row {
        display: flex; align-items: center;
        margin: 6px 0; font-size: 0.82rem;
    }
    .enrich-tier {
        width: 140px; color: #57606a;
        font-size: 0.78rem; flex-shrink: 0;
    }
    .enrich-bar-wrap { flex: 1; display: flex; align-items: center; gap: 8px; }
    .enrich-bar {
        height: 20px; border-radius: 3px; min-width: 2px;
        transition: width 0.4s ease;
    }
    .enrich-bar.tier-1 { background: #4ac26b; }
    .enrich-bar.tier-2 { background: #54aeff; }
    .enrich-bar.tier-3 { background: #d4a72c; }
    .enrich-bar.tier-4 { background: #ff8182; }
    .enrich-count {
        color: #6e7781; font-family: monospace; font-size: 0.76rem;
    }

    /* ── Source profile table ───────────────────────────── */
    .profile-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .profile-table th {
        background: #f6f8fa; color: #57606a;
        padding: 8px 12px; text-align: left;
        font-size: 0.74rem; text-transform: uppercase;
        border-bottom: 2px solid #d0d7de;
    }
    .profile-table td {
        padding: 7px 12px; color: #24292f;
        border-bottom: 1px solid #eaeef2;
    }
    .profile-table .col-name { font-family: monospace; color: #bc4c00; }
    .profile-table .sample { color: #6e7781; font-size: 0.76rem; max-width: 300px; overflow: hidden; text-overflow: ellipsis; }

    /* ── Pipeline Remembered banner ─────────────────────── */
    .remembered-banner {
        background: #dafbe1;
        border: 1px solid #4ac26b;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 0.5rem 0 1rem;
    }
    .remembered-title {
        color: #1a7f37;
        font-weight: 600;
        font-size: 1rem;
        margin-bottom: 8px;
    }
    .remembered-list {
        color: #24292f;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        margin: 0 0 10px 1.2rem;
        padding: 0;
    }
    .remembered-sub {
        color: #57606a;
        font-size: 0.82rem;
    }

    /* ── Agent header ─────────────────────────────────────── */
    .agent-header {
        background: linear-gradient(135deg, #f6f8fa, #eaeef2);
        border-left: 4px solid #0969da;
        padding: 14px 18px;
        margin: 16px 0;
        border-radius: 0 8px 8px 0;
    }
    .agent-header-1 { border-left-color: #0969da; }
    .agent-header-2 { border-left-color: #8250df; }
    .agent-header-3 { border-left-color: #1a7f37; }

    /* ── Sampling stats ───────────────────────────────────── */
    .sampling-stats {
        background: #f6f8fa;
        border: 1px solid #d0d7de;
        border-radius: 8px;
        padding: 14px;
        margin: 12px 0;
    }

    /* ── HITL Gate ───────────────────────────────────────── */
    .hitl-gate {
        border: 2px dashed #f0883e;
        background: linear-gradient(180deg, #fff8c5, #fff1c2);
        padding: 18px;
        border-radius: 10px;
        margin: 16px 0;
    }

    /* ── Log panel ──────────────────────────────────────── */
    .log-panel {
        background: #1e1e1e;
        border-radius: 8px;
        padding: 12px;
        font-family: 'JetBrains Mono', 'Consolas', monospace;
        font-size: 0.72rem;
        max-height: 450px;
        overflow-y: auto;
    }
    .log-panel-tall { max-height: 600px; }
    .log-entry { margin: 2px 0; line-height: 1.5; }
    .log-time { color: #6a9955; }
    .log-logger { color: #9cdcfe; font-size: 0.68rem; }
    .log-info { color: #569cd6; }
    .log-warn { color: #dcdcaa; }
    .log-error { color: #f14c4c; }
    .log-debug { color: #808080; }
    .log-text { color: #d4d4d4; }

    /* ── Beautiful buttons ────────────────────────────────── */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }

    /* ── Enhanced step bar ───────────────────────────────── */
    .step-bar {
        background: linear-gradient(180deg, #fafbfc, #f3f4f6);
        padding: 8px;
        border-radius: 12px;
    }

    /* ── Card enhancements ─────────────────────────────────── */
    .metric-card {
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.1);
    }

    /* ── Animated success ─────────────────────────────────── */
    @keyframes slideIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .animate-in {
        animation: slideIn 0.3s ease-out;
    }
</style>
"""
