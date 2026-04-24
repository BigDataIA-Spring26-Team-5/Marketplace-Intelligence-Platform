"""Tests Runner — run pytest suites and stream output."""
from __future__ import annotations
import subprocess
import threading
import queue
import time
import streamlit as st

TEST_SUITES = {
    "All Tests":          ["pytest", "tests/"],
    "Unit Tests":         ["pytest", "-m", "not integration", "tests/"],
    "Integration Tests":  ["pytest", "-m", "integration", "tests/"],
    "Log Writer":         ["pytest", "tests/uc2_observability/test_log_writer.py", "-v"],
    "Cache Pipeline":     ["pytest", "tests/integration/test_cache_pipeline.py", "-v"],
    "Cache Client":       ["pytest", "tests/unit/test_cache_client.py", "-v"],
}


def _run_pytest(cmd: list[str]) -> tuple[str, int]:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=180,
            cwd=str(__file__).replace("/src/ui/pages/tests_runner.py", ""),
        )
        output = result.stdout + result.stderr
        return output, result.returncode
    except subprocess.TimeoutExpired:
        return "Test run timed out after 180 seconds.", 1
    except Exception as e:
        return f"Failed to run tests: {e}", 1


def _colorize_output(line: str) -> str:
    line_e = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if " PASSED" in line or line.startswith("passed"):
        return f'<div class="t-green">{line_e}</div>'
    if " FAILED" in line or " ERROR" in line or line.startswith("FAILED"):
        return f'<div class="t-red">{line_e}</div>'
    if " WARNED" in line or "warning" in line.lower():
        return f'<div class="t-amber">{line_e}</div>'
    if line.startswith("=") or line.startswith("_"):
        return f'<div class="t-blue">{line_e}</div>'
    if line.startswith("PASSED") or "passed" in line:
        return f'<div class="t-green">{line_e}</div>'
    if line.startswith("collected"):
        return f'<div class="t-blue">{line_e}</div>'
    return f'<div class="t-dim">{line_e}</div>'


def render_tests():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Test Runner</div>
        <div class="page-subtitle">Run pytest suites and inspect results in real time</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Suite picker ──────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        suite_name = st.selectbox("Test suite", list(TEST_SUITES.keys()))
    with col2:
        verbose    = st.checkbox("Verbose (-v)", value=False)
    with col3:
        no_integ   = st.checkbox("Skip integration", value=False)

    cmd = list(TEST_SUITES[suite_name])
    if verbose and "-v" not in cmd:
        cmd.append("-v")
    if no_integ and "-m" not in cmd:
        cmd.extend(["-m", "not integration"])

    # Custom path
    with st.expander("Custom pytest command"):
        custom_cmd = st.text_input("Command", value=" ".join(cmd))
        if custom_cmd:
            cmd = custom_cmd.split()

    run_col, _ = st.columns([1, 4])
    with run_col:
        run_clicked = st.button("▶  Run Tests", type="primary", use_container_width=True)

    # Show previous result if available
    if "test_output" in st.session_state and not run_clicked:
        prev_output   = st.session_state.get("test_output", "")
        prev_rc       = st.session_state.get("test_rc", 0)
        prev_suite    = st.session_state.get("test_suite", "")
        prev_duration = st.session_state.get("test_duration", 0.0)

        status_html = (
            '<span class="badge success">PASSED</span>'
            if prev_rc == 0
            else '<span class="badge error">FAILED</span>'
        )
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
          {status_html}
          <span class="mono tc-dim">{prev_suite}</span>
          <span class="tc-dim">·</span>
          <span class="tc-dim" style="font-size:13px;">{prev_duration:.1f}s</span>
        </div>""", unsafe_allow_html=True)

        lines = prev_output.splitlines()
        terminal_html = "\n".join(_colorize_output(l) for l in lines)
        st.markdown(f"""
        <div class="terminal" style="height:420px;overflow-y:auto;">{terminal_html}</div>
        """, unsafe_allow_html=True)

        # Parse summary line
        summary = next((l for l in reversed(lines) if "passed" in l or "failed" in l or "error" in l), "")
        if summary:
            st.markdown(f'<div style="margin-top:8px;font-family:var(--mono);font-size:13px;color:var(--text-muted);">{summary}</div>', unsafe_allow_html=True)

    if run_clicked:
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
          <span class="stream-dot"></span>
          <span style="font-size:13px;color:var(--text-muted);">Running: <span class="mono">{" ".join(cmd)}</span></span>
        </div>""", unsafe_allow_html=True)

        output_placeholder = st.empty()
        output_placeholder.markdown("""
        <div class="terminal" style="height:420px;overflow-y:auto;">
          <div class="t-blue">Collecting tests…</div>
        </div>""", unsafe_allow_html=True)

        t0 = time.time()
        output, rc = _run_pytest(cmd)
        duration = time.time() - t0

        st.session_state.test_output   = output
        st.session_state.test_rc       = rc
        st.session_state.test_suite    = suite_name
        st.session_state.test_duration = duration

        lines = output.splitlines()
        terminal_html = "\n".join(_colorize_output(l) for l in lines)
        output_placeholder.markdown(f"""
        <div class="terminal" style="height:420px;overflow-y:auto;">{terminal_html}</div>
        """, unsafe_allow_html=True)

        status_cls  = "success" if rc == 0 else "error"
        status_text = "All tests passed" if rc == 0 else f"Tests failed (exit code {rc})"
        st.markdown(f'<div class="alert {"green" if rc==0 else "red"}" style="margin-top:12px;">{status_text} — {duration:.1f}s</div>', unsafe_allow_html=True)

        st.rerun()

    # ── Static coverage metrics (from docs/TEST_COVERAGE_REPORT.md) ──────────
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    metrics = [
        ("Coverage (core)", "81.72%", "var(--green)"),
        ("Tests Passing",   "920",    "var(--green)"),
        ("Tests Failing",   "2",      "var(--red)"),
        ("Tests Skipped",   "1",      "var(--amber)"),
        ("Test Files",      "43",     "var(--accent)"),
    ]
    for col, (label, val, color) in zip([c1, c2, c3, c4, c5], metrics):
        with col:
            st.markdown(f"""
            <div class="stat-card">
              <div class="stat-label">{label}</div>
              <div class="stat-value sv-md" style="color:{color}">{val}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Module coverage breakdown
    coverage_data = [
        ("src/uc3_search/",           100, "success"),
        ("src/schema/analyzer.py",    99,  "success"),
        ("src/enrichment/deterministic.py", 98, "success"),
        ("src/uc4_recommendations/association_rules.py", 97, "success"),
        ("src/uc2_observability/mcp_server.py", 97, "success"),
        ("src/uc4_recommendations/recommender.py", 92, "success"),
        ("src/enrichment/embedding.py", 96, "success"),
        ("src/pipeline/runner.py",    87,  "warning"),
        ("src/models/llm.py",         84,  "warning"),
        ("src/agents/graph.py",       72,  "warning"),
        ("src/cache/client.py",       69,  "warning"),
        ("src/blocks/dq_score.py",    33,  "error"),
        ("src/blocks/extract_quantity_column.py", 32, "error"),
    ]

    lc, rc = st.columns(2)
    with lc:
        bars_html = ""
        for module, pct, status in coverage_data[:7]:
            color = {"success": "var(--green)", "warning": "var(--amber)", "error": "var(--red)"}[status]
            bars_html += f"""
            <div class="bar-row">
              <div class="bar-label" style="width:220px;text-align:left;font-size:12px;">{module.split("/")[-1]}</div>
              <div class="bar-track"><div class="bar-fill" style="width:{pct}%;background:{color};"></div></div>
              <div class="bar-val">{pct}%</div>
            </div>"""
        st.markdown(f"""
        <div class="card">
          <div class="card-title">Module Coverage (top)</div>
          <div class="bar-chart">{bars_html}</div>
        </div>""", unsafe_allow_html=True)

    with rc:
        bars_html2 = ""
        for module, pct, status in coverage_data[7:]:
            color = {"success": "var(--green)", "warning": "var(--amber)", "error": "var(--red)"}[status]
            bars_html2 += f"""
            <div class="bar-row">
              <div class="bar-label" style="width:220px;text-align:left;font-size:12px;">{module.split("/")[-1]}</div>
              <div class="bar-track"><div class="bar-fill" style="width:{pct}%;background:{color};"></div></div>
              <div class="bar-val">{pct}%</div>
            </div>"""
        st.markdown(f"""
        <div class="card">
          <div class="card-title">Module Coverage (gaps)</div>
          <div class="bar-chart">{bars_html2}</div>
        </div>""", unsafe_allow_html=True)

    # Test strategy breakdown
    strategy_rows = """
    <tr><td>Unit Testing</td><td class="badge success">Implemented</td><td class="mono">tests/unit/</td><td>41 files, ~850 tests</td></tr>
    <tr><td>Integration Testing</td><td class="badge success">Implemented</td><td class="mono">tests/integration/</td><td>7 files, ~60 tests</td></tr>
    <tr><td>Property-Based (Hypothesis)</td><td class="badge success">Implemented</td><td class="mono">tests/property/</td><td>1 file, 12 tests</td></tr>
    <tr><td>UC2 Observability</td><td class="badge success">Implemented</td><td class="mono">tests/uc2_observability/</td><td>5 files</td></tr>
    """
    st.markdown(f"""
    <div class="card">
      <div class="card-title">Test Strategies — Generated: 2026-04-24</div>
      <table class="data-table">
        <thead><tr><th>Strategy</th><th>Status</th><th>Location</th><th>Count</th></tr></thead>
        <tbody>{strategy_rows}</tbody>
      </table>
    </div>""", unsafe_allow_html=True)

    # Suite list (always visible)
    if "test_output" not in st.session_state:
        st.markdown("""
        <div class="card">
          <div class="card-title">Available Test Suites</div>
          <table class="data-table">
            <thead><tr><th>Suite</th><th>Path</th><th>Description</th></tr></thead>
            <tbody>
              <tr><td>All Tests</td><td class="mono">tests/</td><td>Full test suite</td></tr>
              <tr><td>Unit Tests</td><td class="mono">tests/ -m "not integration"</td><td>Fast, no external deps</td></tr>
              <tr><td>Integration Tests</td><td class="mono">tests/ -m integration</td><td>Requires Redis, Postgres, GCS</td></tr>
              <tr><td>Log Writer</td><td class="mono">tests/uc2_observability/test_log_writer.py</td><td>UC2 run log write/read</td></tr>
              <tr><td>Cache Pipeline</td><td class="mono">tests/integration/test_cache_pipeline.py</td><td>Redis + SQLite cache</td></tr>
              <tr><td>Cache Client</td><td class="mono">tests/unit/test_cache_client.py</td><td>Cache client unit tests</td></tr>
            </tbody>
          </table>
        </div>""", unsafe_allow_html=True)
