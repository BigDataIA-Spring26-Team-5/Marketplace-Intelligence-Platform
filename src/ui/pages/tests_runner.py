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

    # ── Test coverage hint ────────────────────────────────────────────────────
    if "test_output" not in st.session_state:
        st.markdown("""
        <div class="card">
          <div class="card-title">Available Test Suites</div>
          <table class="data-table">
            <thead><tr><th>Suite</th><th>Path</th><th>Description</th></tr></thead>
            <tbody>
              <tr><td>All Tests</td><td class="mono">tests/</td><td>Full test suite</td></tr>
              <tr><td>Unit Tests</td><td class="mono">tests/ -m "not integration"</td><td>Fast tests, no external deps</td></tr>
              <tr><td>Integration Tests</td><td class="mono">tests/ -m integration</td><td>Requires Redis, Postgres, GCS</td></tr>
              <tr><td>Log Writer</td><td class="mono">tests/uc2_observability/test_log_writer.py</td><td>UC2 run log write/read</td></tr>
              <tr><td>Cache Pipeline</td><td class="mono">tests/integration/test_cache_pipeline.py</td><td>Redis + SQLite cache integration</td></tr>
              <tr><td>Cache Client</td><td class="mono">tests/unit/test_cache_client.py</td><td>Cache client unit tests</td></tr>
            </tbody>
          </table>
        </div>""", unsafe_allow_html=True)
