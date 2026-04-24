"""UI styles for DataForge Streamlit app - matching dataforge_components.html."""

STYLES = """
<style>
    /* ── Global Reset & Variables ─────────────────────── */
    .stApp {
        background-color: #ffffff;
    }

    /* ── App Shell Layout ───────────────────────────── */
    .app-shell {
        display: grid;
        grid-template-columns: 220px 1fr;
        grid-template-rows: 52px 1fr;
        height: 100vh;
    }

    /* ── Topbar ─────────────────────────────────────── */
    .topbar {
        grid-column: 1 / -1;
        background: #ffffff;
        border-bottom: 1px solid #dee2e6;
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0 20px;
        z-index: 100;
    }
    .topbar-brand {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .topbar-brand .logo {
        width: 28px;
        height: 28px;
        background: #1971c2;
        border-radius: 5px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        font-weight: 700;
        color: #fff;
        letter-spacing: -0.5px;
    }
    .topbar-brand .name {
        font-size: 15px;
        font-weight: 700;
        color: #212529;
    }
    .topbar-brand .name span {
        color: #1971c2;
    }

    .health-rail {
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .health-label {
        font-size: 10px;
        font-weight: 700;
        color: #adb5bd;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        margin-right: 4px;
    }
    .health-pill {
        display: flex;
        align-items: center;
        gap: 5px;
        padding: 3px 10px;
        border-radius: 20px;
        border: 1px solid #dee2e6;
        background: #f8f9fa;
        font-size: 11px;
        font-weight: 500;
        color: #6c757d;
    }
    .health-dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        flex-shrink: 0;
    }
    .health-dot.ok { background: #2f9e44; }
    .health-dot.warn { background: #e67700; }
    .health-dot.error { background: #c92a2a; }

    .topbar-right {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .run-badge {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 20px;
        background: #ebf9ee;
        border: 1px solid rgba(47,158,68,0.2);
        font-size: 11px;
        font-weight: 600;
        color: #2f9e44;
    }
    .run-badge::before {
        content: '';
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: #2f9e44;
        animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse {
        0%,100%{opacity:1;transform:scale(1)}
        50%{opacity:0.5;transform:scale(0.8)}
    }

    /* ── Sidebar ──────────────────────────────────────────── */
    .sidebar {
        background: #f8f9fa;
        border-right: 1px solid #dee2e6;
        display: flex;
        flex-direction: column;
        padding: 10px 0;
        overflow-y: auto;
    }
    .nav-section {
        padding: 2px 0;
    }
    .nav-label {
        font-size: 10px;
        font-weight: 700;
        color: #adb5bd;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        padding: 10px 14px 4px;
    }
    .nav-item {
        display: flex;
        align-items: center;
        gap: 9px;
        padding: 7px 14px;
        cursor: pointer;
        border-left: 2px solid transparent;
        font-size: 13px;
        font-weight: 500;
        color: #6c757d;
        transition: background 0.1s, color 0.1s;
    }
    .nav-item:hover {
        color: #212529;
        background: #f1f3f5;
    }
    .nav-item.active {
        color: #1971c2;
        background: #e7f0fb;
        border-left-color: #1971c2;
    }
    .nav-icon {
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 10px;
        font-weight: 700;
        width: 20px;
        text-align: center;
        flex-shrink: 0;
        color: inherit;
        opacity: 0.6;
    }
    .nav-badge {
        margin-left: auto;
        font-size: 10px;
        font-weight: 700;
        padding: 1px 6px;
        border-radius: 10px;
        background: #e9ecef;
        color: #6c757d;
    }

    /* ── Main Content Area ─────────────────────────────── */
    .main {
        overflow-y: auto;
        background: #ffffff;
        padding: 28px 32px;
    }

    /* ── Page Header ────────────────────────────────────── */
    .page-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        margin-bottom: 24px;
    }
    .page-header-left {
        display: flex;
        flex-direction: column;
        gap: 4px;
    }
    .page-title {
        font-size: 20px;
        font-weight: 700;
        letter-spacing: -0.4px;
        color: #212529;
    }
    .page-subtitle {
        font-size: 12px;
        color: #6c757d;
    }
    .page-controls {
        display: flex;
        align-items: center;
        gap: 12px;
    }

    /* ── Cards ───────────────────────────────────────── */
    .card {
        background: #ffffff;
        border: 1px solid #dee2e6;
        border-radius: 10px;
        padding: 18px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.07), 0 1px 2px rgba(0,0,0,0.04);
    }
    .card-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 14px;
    }
    .card-title {
        display: flex;
        align-items: center;
        gap: 7px;
        font-size: 11px;
        font-weight: 700;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .card-title::before {
        content: '';
        width: 3px;
        height: 12px;
        background: #1971c2;
        border-radius: 2px;
        flex-shrink: 0;
    }

    /* ── Stat Cards ──────────────────────────────── */
    .stat-card {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        padding: 14px 16px;
    }
    .stat-label {
        font-size: 10px;
        font-weight: 700;
        color: #adb5bd;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 8px;
    }
    .stat-value {
        font-size: 26px;
        font-weight: 700;
        letter-spacing: -0.8px;
        color: #212529;
        line-height: 1;
    }
    .stat-unit {
        font-size: 14px;
        font-weight: 500;
        color: #6c757d;
        letter-spacing: 0;
    }
    .stat-delta {
        font-size: 11px;
        font-weight: 500;
        margin-top: 6px;
    }
    .stat-delta.up { color: #2f9e44; }
    .stat-delta.down { color: #c92a2a; }

    /* ── Grid Helpers ───────────────────────────── */
    .grid-2 {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
    }
    .grid-3 {
        display: grid;
        grid-template-columns: 1fr 1fr 1fr;
        gap: 16px;
    }
    .grid-4 {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
    }
    .stack {
        display: flex;
        flex-direction: column;
        gap: 16px;
    }
    .mb { margin-bottom: 16px; }

    /* ── Badges ───────────────────────────────────── */
    .badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 2px 7px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
        white-space: nowrap;
    }
    .badge.success {
        background: #ebf9ee;
        color: #2f9e44;
        border: 1px solid rgba(47,158,68,0.15);
    }
    .badge.error {
        background: #fff5f5;
        color: #c92a2a;
        border: 1px solid rgba(201,42,42,0.15);
    }
    .badge.warning {
        background: #fff3bf;
        color: #e67700;
        border: 1px solid rgba(230,119,0,0.15);
    }
    .badge.info {
        background: #e7f0fb;
        color: #1971c2;
        border: 1px solid rgba(25,113,194,0.15);
    }
    .badge.running {
        background: #e3fafc;
        color: #0c8599;
        border: 1px solid rgba(12,133,153,0.15);
    }
    .badge.purple {
        background: #f3f0ff;
        color: #6741d9;
        border: 1px solid rgba(103,65,217,0.15);
    }

    /* ── Data Table ───────────────────────────── */
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    .data-table th {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #adb5bd;
        padding: 8px 12px;
        border-bottom: 1px solid #dee2e6;
        background: #f8f9fa;
        text-align: left;
    }
    .data-table td {
        padding: 10px 12px;
        border-bottom: 1px solid #dee2e6;
        color: #6c757d;
        vertical-align: middle;
    }
    .data-table td:first-child {
        color: #212529;
        font-weight: 500;
    }
    .data-table tr:last-child td {
        border-bottom: none;
    }
    .data-table tbody tr {
        cursor: pointer;
        transition: background 0.08s;
    }
    .data-table tbody tr:hover {
        background: #f8f9fa;
    }

    /* ── DQ Arrow ──────────────────────────────── */
    .dq-arrow {
        display: flex;
        align-items: center;
        gap: 4px;
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 12px;
    }
    .dq-arrow .before {
        color: #6c757d;
    }
    .dq-arrow .arrow {
        color: #adb5bd;
    }
    .dq-arrow .after {
        color: #2f9e44;
        font-weight: 600;
    }
    .dq-arrow .delta {
        color: #2f9e44;
        font-size: 11px;
    }

    /* ── Terminal ──────────────────────────────── */
    .terminal {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-left: 3px solid #ced4da;
        border-radius: 6px;
        padding: 14px 16px;
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 12px;
        line-height: 1.7;
        color: #6c757d;
        overflow-y: auto;
    }
    .terminal .t-green { color: #2f9e44; }
    .terminal .t-amber { color: #e67700; }
    .terminal .t-blue { color: #1971c2; }
    .terminal .t-red { color: #c92a2a; }
    .terminal .t-dim { color: #adb5bd; }
    .terminal .t-text { color: #212529; }

    .stream-dot {
        display: inline-block;
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: #2f9e44;
        animation: pulse 2s ease-in-out infinite;
        vertical-align: middle;
        margin-right: 4px;
    }

    /* ── Stepper ───────────────────────────────────── */
    .stepper {
        display: flex;
        align-items: flex-start;
        margin-bottom: 28px;
    }
    .step {
        display: flex;
        align-items: center;
        flex: 1;
    }
    .step-node {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 5px;
        flex-shrink: 0;
    }
    .step-circle {
        width: 30px;
        height: 30px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        font-weight: 700;
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        border: 2px solid #dee2e6;
        background: #f8f9fa;
        color: #adb5bd;
        transition: all 0.2s;
    }
    .step-circle.done {
        background: #ebf9ee;
        border-color: #2f9e44;
        color: #2f9e44;
    }
    .step-circle.active {
        background: #e7f0fb;
        border-color: #1971c2;
        color: #1971c2;
        box-shadow: 0 0 0 3px rgba(25,113,194,0.1);
    }
    .step-label {
        font-size: 11px;
        font-weight: 500;
        color: #adb5bd;
        white-space: nowrap;
    }
    .step-label.done { color: #2f9e44; }
    .step-label.active { color: #1971c2; }
    .step-line {
        flex: 1;
        height: 1px;
        background: #dee2e6;
        margin: 0 4px;
        transform: translateY(-12px);
    }
    .step-line.done { background: #2f9e44; }

    /* ── Block Chips ──────────────────────────────── */
    .block-chips {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
    }
    .block-chip {
        display: flex;
        align-items: center;
        gap: 5px;
        padding: 4px 10px;
        border-radius: 4px;
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 11px;
        font-weight: 500;
        border: 1px solid #dee2e6;
        background: #f8f9fa;
        color: #6c757d;
    }
    .block-chip.done {
        background: #ebf9ee;
        border-color: rgba(47,158,68,0.25);
        color: #2f9e44;
    }
    .block-chip.running {
        background: #e7f0fb;
        border-color: rgba(25,113,194,0.25);
        color: #1971c2;
        animation: blink 1.5s ease-in-out infinite;
    }
    .block-chip.error {
        background: #fff5f5;
        border-color: rgba(201,42,42,0.25);
        color: #c92a2a;
    }
    @keyframes blink {
        0%,100%{opacity:1}
        50%{opacity:0.55}
    }

    /* ── Buttons ─────────────────────────────────── */
    .btn {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 7px 14px;
        border-radius: 6px;
        font-size: 13px;
        font-weight: 600;
        font-family: inherit;
        cursor: pointer;
        transition: all 0.12s;
        border: none;
        outline: none;
    }
    .btn:disabled {
        opacity: 0.4;
        cursor: not-allowed;
    }
    .btn-primary {
        background: #1971c2;
        color: #fff;
    }
    .btn-primary:hover:not(:disabled) {
        background: #1864ab;
    }
    .btn-ghost {
        background: transparent;
        color: #6c757d;
        border: 1px solid #dee2e6;
    }
    .btn-ghost:hover:not(:disabled) {
        background: #f1f3f5;
        color: #212529;
    }
    .btn-danger {
        background: #fff5f5;
        color: #c92a2a;
        border: 1px solid rgba(201,42,42,0.2);
    }
    .btn-sm {
        padding: 4px 10px;
        font-size: 12px;
    }

    /* ── Mode Toggle ──────────────────────────────── */
    .mode-toggle {
        display: flex;
        background: #f1f3f5;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        overflow: hidden;
    }
    .mode-option {
        padding: 5px 12px;
        font-size: 12px;
        font-weight: 600;
        color: #6c757d;
        cursor: pointer;
        transition: all 0.12s;
    }
    .mode-option.active {
        background: #ffffff;
        color: #1971c2;
        box-shadow: 0 1px 3px rgba(0,0,0,0.07);
    }

    /* ── Toggle ─────────────────────────────────────── */
    .toggle-inline {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 13px;
        font-weight: 500;
        color: #6c757d;
    }
    .toggle {
        width: 34px;
        height: 18px;
        background: #e9ecef;
        border-radius: 9px;
        cursor: pointer;
        position: relative;
        transition: background 0.2s;
        border: 1px solid #dee2e6;
        flex-shrink: 0;
    }
    .toggle.on {
        background: #1971c2;
        border-color: #1971c2;
    }
    .toggle::after {
        content: '';
        position: absolute;
        width: 12px;
        height: 12px;
        background: #ffffff;
        border-radius: 50%;
        top: 2px;
        left: 2px;
        transition: left 0.2s;
        box-shadow: 0 1px 2px rgba(0,0,0,0.15);
    }
    .toggle.on::after {
        left: 18px;
    }

    /* ── Alert ─────────────────────────────────────── */
    .alert {
        padding: 10px 12px;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 500;
    }
    .alert.purple {
        background: #f3f0ff;
        border: 1px solid rgba(103,65,217,0.12);
        color: #6741d9;
    }
    .alert.green {
        background: #ebf9ee;
        border: 1px solid rgba(47,158,68,0.12);
        color: #2f9e44;
    }

    /* ── Decision Card ──────────────────────────── */
    .decision-card {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 12px 14px;
        border-radius: 6px;
        background: #f8f9fa;
        border: 1px solid #dee2e6;
    }
    .decision-field {
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 12px;
        font-weight: 600;
        color: #1971c2;
    }
    .decision-reason {
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 11px;
        color: #adb5bd;
        margin-top: 2px;
    }
    .decision-body {
        flex: 1;
    }
    .decision-actions {
        display: flex;
        gap: 5px;
        flex-shrink: 0;
    }

    /* ── Quick Actions ──────────────────────────── */
    .quick-actions {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
    }
    .quick-action {
        display: flex;
        align-items: center;
        gap: 7px;
        padding: 9px 14px;
        border-radius: 6px;
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        font-size: 13px;
        font-weight: 600;
        color: #6c757d;
        cursor: pointer;
        transition: all 0.12s;
    }
    .quick-action:hover {
        border-color: #1971c2;
        color: #1971c2;
        background: #e7f0fb;
    }

    /* ── DAG Strip ──────────────────────────────── */
    .dag-strip {
        display: flex;
        gap: 10px;
        overflow-x: auto;
    }
    .dag-strip-item {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 8px 12px;
        border-radius: 6px;
        border: 1px solid rgba(12,133,153,0.2);
        background: #e3fafc;
        white-space: nowrap;
    }
    .dag-strip-name {
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 12px;
        font-weight: 600;
        color: #0c8599;
    }
    .dag-strip-time {
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
        font-size: 10px;
        color: #adb5bd;
    }
    .dag-spin {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid rgba(12,133,153,0.2);
        border-top-color: #0c8599;
        animation: spin 0.8s linear infinite;
        flex-shrink: 0;
    }
    @keyframes spin {
        to{transform:rotate(360deg)}
    }

    /* ── Grafana ─────────────────────────────────── */
    .grafana-toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 16px;
        margin-bottom: 4px;
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 10px 10px 0 0;
        border-bottom: none;
    }
    .grafana-logo {
        display: flex;
        align-items: center;
        gap: 7px;
        padding-right: 16px;
        margin-right: 8px;
        border-right: 1px solid #dee2e6;
    }
    .grafana-logo-text {
        font-size: 13px;
        font-weight: 700;
        color: #f46800;
        letter-spacing: -0.2px;
    }
    .grafana-breadcrumb {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 12px;
        font-weight: 500;
        color: #212529;
    }
    .grafana-controls {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .grafana-timerange {
        display: flex;
        background: #f1f3f5;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        overflow: hidden;
    }
    .grafana-timerange-btn {
        padding: 4px 10px;
        font-size: 11px;
        font-weight: 600;
        color: #6c757d;
        cursor: pointer;
        transition: all 0.12s;
        font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
    }
    .grafana-timerange-btn:hover {
        background: #e9ecef;
        color: #212529;
    }
    .grafana-timerange-btn.active {
        background: #1971c2;
        color: #ffffff;
    }
    .grafana-embed-wrap {
        position: relative;
        background: #f1f3f5;
        border: 1px solid #dee2e6;
        border-radius: 0 0 10px 10px;
        overflow: hidden;
        min-height: 480px;
        display: flex;
        flex-direction: column;
    }
    .grafana-iframe {
        width: 100%;
        flex: 1;
        min-height: 480px;
        border: none;
        display: block;
        background: #ffffff;
    }

    /* ── Chat ─────────────────────────────────────── */
    .chat-scroll {
        height: 300px;
        overflow-y: auto;
    }
    .chat-msg {
        display: flex;
        gap: 10px;
        margin-bottom: 14px;
    }
    .chat-avatar {
        width: 28px;
        height: 28px;
        border-radius: 50%;
        flex-shrink: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 700;
    }
    .chat-avatar.user {
        background: #e7f0fb;
        color: #1971c2;
        border: 1px solid rgba(25,113,194,0.15);
    }
    .chat-avatar.ai {
        background: #f3f0ff;
        color: #6741d9;
        border: 1px solid rgba(103,65,217,0.15);
    }
    .chat-bubble {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        padding: 9px 13px;
        font-size: 13px;
        line-height: 1.6;
        max-width: 75%;
        color: #6c757d;
    }

    /* ── Field Inputs ─────────────────────────── */
    .field-label {
        font-size: 11px;
        font-weight: 600;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 5px;
    }
    .field-input {
        width: 100%;
        padding: 8px 11px;
        background: #ffffff;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        color: #212529;
        font-family: inherit;
        font-size: 13px;
        outline: none;
        transition: border-color 0.12s, box-shadow 0.12s;
    }
    .field-input:focus {
        border-color: #1971c2;
        box-shadow: 0 0 0 3px rgba(25,113,194,0.08);
    }
    .field-input::placeholder {
        color: #adb5bd;
    }

    .drop-zone {
        border: 1.5px dashed #ced4da;
        border-radius: 10px;
        padding: 28px;
        text-align: center;
        cursor: pointer;
        transition: all 0.12s;
        color: #adb5bd;
        font-size: 12px;
        font-weight: 500;
    }
    .drop-zone:hover {
        border-color: #1971c2;
        background: #e7f0fb;
        color: #1971c2;
    }
    .drop-zone-icon {
        font-size: 18px;
        margin-bottom: 8px;
        color: #adb5bd;
    }

    /* ── Tabs ──���─���─────────────────────────────────── */
    .tabs {
        display: flex;
        border-bottom: 1px solid #dee2e6;
        margin-bottom: 20px;
    }
    .tab {
        padding: 9px 16px;
        font-size: 13px;
        font-weight: 600;
        color: #6c757d;
        cursor: pointer;
        border-bottom: 2px solid transparent;
        transition: color 0.12s, border-color 0.12s;
    }
    .tab:hover {
        color: #212529;
    }
    .tab.active {
        color: #1971c2;
        border-bottom-color: #1971c2;
    }

    /* ── Utility ───────────────────────────────── */
    .row {
        display: flex;
        align-items: center;
        gap: 16px;
    }
    .row-sm {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .flex-1 {
        flex: 1;
    }
    .ml-auto {
        margin-left: auto;
    }
    .mt-6 { margin-top: 6px; }
    .mt-10 { margin-top: 10px; }
    .mb-6 { margin-bottom: 6px; }
    .mb-10 { margin-bottom: 10px; }
    .mb-12 { margin-bottom: 12px; }
    .mb-16 { margin-bottom: 16px; }

    /* ── Scrollbar ──────────────────────────────── */
    ::-webkit-scrollbar {
        width: 5px;
        height: 5px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: #dee2e6;
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #ced4da;
    }

    /* ── Hide Streamlit elements ────────────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {visibility: hidden;}
    header {visibility: hidden;}
</style>
"""