"""Observability page — Run history table, Grafana iframe, Chatbot with mem0."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import load_run_logs

GRAFANA_URL  = "http://localhost:3000"
GRAFANA_DASH = f"{GRAFANA_URL}/d/etl-pipeline-observability/etl-pipeline-observability?orgId=1&refresh=10s&theme=light&kiosk"


def _status_badge(s: str) -> str:
    cls = {"success": "success", "error": "error", "running": "running"}.get(s, "warning")
    return f'<span class="badge {cls}">{s}</span>'


def _domain_badge(d: str) -> str:
    colors = {"nutrition": "info", "safety": "error", "pricing": "warning", "retail": "purple"}
    cls = colors.get((d or "").lower(), "info")
    return f'<span class="badge {cls}">{d or "—"}</span>'


def _dq_arrow(pre, post, delta) -> str:
    pre_s  = f"{pre:.1f}"  if pre  is not None else "—"
    post_s = f"{post:.1f}" if post is not None else "—"
    post_cls = "after" if post is not None else "after na"
    d_html = ""
    if delta is not None:
        sign = "+" if delta >= 0 else ""
        color = "var(--green)" if delta >= 0 else "var(--red)"
        d_html = f'<span class="delta" style="color:{color}">({sign}{delta:.1f})</span>'
    return f'<span class="dq-arrow"><span class="before">{pre_s}</span><span class="arrow"> → </span><span class="{post_cls}">{post_s}</span> {d_html}</span>'


def render_observability():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Observability</div>
        <div class="page-subtitle">Pipeline run history, Grafana dashboards, and AI-powered chatbot</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["Run History", "Grafana", "Chatbot"])

    # ── Tab 0: Run History ────────────────────────────────────────────────────
    with tabs[0]:
        logs = load_run_logs()

        # Filters
        fc1, fc2, fc3, _ = st.columns([2, 2, 2, 3])
        with fc1:
            sources = sorted({r.get("source_name", r.get("source", "")) for r in logs if r.get("source_name") or r.get("source")})
            sel_src = st.selectbox("Source", ["All"] + sources, key="obs_src")
        with fc2:
            domains = sorted({r.get("domain", "") for r in logs if r.get("domain")})
            sel_dom = st.selectbox("Domain", ["All"] + domains, key="obs_dom")
        with fc3:
            sel_status = st.selectbox("Status", ["All", "success", "error", "running"], key="obs_status")

        filtered = logs
        if sel_src != "All":
            filtered = [r for r in filtered if r.get("source_name", r.get("source", "")) == sel_src]
        if sel_dom != "All":
            filtered = [r for r in filtered if r.get("domain", "") == sel_dom]
        if sel_status != "All":
            filtered = [r for r in filtered if r.get("status", "") == sel_status]

        total_ok  = sum(1 for r in filtered if r.get("status") == "success")
        total_err = sum(1 for r in filtered if r.get("status") == "error")

        st.markdown(f"""
        <div style="display:flex;gap:10px;align-items:center;margin-bottom:14px;">
          <span class="badge info">{len(filtered)} runs</span>
          <span class="badge success">{total_ok} success</span>
          <span class="badge error">{total_err} error</span>
        </div>""", unsafe_allow_html=True)

        rows_html = ""
        for r in filtered:
            src     = r.get("source_name", r.get("source", "—"))
            domain  = r.get("domain", "")
            status  = r.get("status", "")
            pre     = r.get("dq_score_pre")
            post    = r.get("dq_score_post")
            delta   = r.get("dq_delta")
            ts      = r.get("timestamp", "")[:19].replace("T", " ")
            dur     = r.get("duration_seconds")
            rows_in = r.get("rows_in", 0) or 0
            quaran  = r.get("rows_quarantined", 0) or 0
            run_id  = r.get("run_id", "")[:8]
            dur_s   = f"{dur:.1f}s" if dur is not None else "—"

            enrich  = r.get("enrichment_stats", {})
            s1 = enrich.get("deterministic", enrich.get("s1", 0)) or 0
            s2 = enrich.get("embedding",     enrich.get("s2", 0)) or 0
            s3 = enrich.get("llm",           enrich.get("s3", 0)) or 0

            rows_html += f"""
            <tr>
              <td><span class="mono tc-dim">{run_id}</span></td>
              <td><span class="mono">{src}</span></td>
              <td>{_domain_badge(domain)}</td>
              <td>{_status_badge(status)}</td>
              <td>{_dq_arrow(pre, post, delta)}</td>
              <td class="mono">{rows_in:,}</td>
              <td class="{'tc-red' if quaran > 0 else 'tc-dim'}">{quaran:,}</td>
              <td class="tc-green">{s1:,}</td>
              <td class="tc-accent">{s2:,}</td>
              <td class="tc-amber">{s3:,}</td>
              <td class="tc-dim">{dur_s}</td>
              <td class="tc-dim" style="font-size:12px;">{ts}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card" style="overflow-x:auto;">
          <div class="card-title">Pipeline Runs — {len(filtered)} records</div>
          <table class="data-table">
            <thead><tr>
              <th>Run ID</th><th>Source</th><th>Domain</th><th>Status</th>
              <th>DQ Score</th><th>Rows In</th><th>Quarantined</th>
              <th>S1</th><th>S2</th><th>S3</th><th>Duration</th><th>Timestamp</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

    # ── Tab 1: Grafana ────────────────────────────────────────────────────────
    with tabs[1]:
        st.markdown(f"""
        <div class="card">
          <div class="card-title">Grafana — ETL Pipeline Observability</div>
          <iframe
            src="{GRAFANA_DASH}"
            width="100%"
            height="720"
            frameborder="0"
            style="border-radius:6px;border:1px solid var(--border);"
          ></iframe>
          <div style="margin-top:8px;font-size:12px;color:var(--text-dim);">
            Direct link: <a href="{GRAFANA_URL}/d/etl-pipeline-observability/" target="_blank"
            style="color:var(--accent);">Open in Grafana ↗</a>
          </div>
        </div>""", unsafe_allow_html=True)

    # ── Tab 2: Chatbot ────────────────────────────────────────────────────────
    with tabs[2]:
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []

        # MCP server status pill
        try:
            import requests
            r = requests.get("http://localhost:8001/health", timeout=2)
            mcp_ok = r.status_code < 400
        except Exception:
            mcp_ok = False

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
          <div class="health-pill">
            <span class="health-dot {'ok' if mcp_ok else 'warn'}"></span>MCP Server
          </div>
          <span style="font-size:13px;color:var(--text-dim);">
            Ask questions about pipeline runs, DQ scores, anomalies, and cost.
          </span>
        </div>""", unsafe_allow_html=True)

        # Render existing chat history
        for msg in st.session_state.chat_history:
            role = msg["role"]
            content = msg["content"]
            cited = msg.get("cited_runs", [])
            if role == "user":
                st.markdown(f"""
                <div style="display:flex;justify-content:flex-end;margin-bottom:10px;">
                  <div style="background:var(--accent-dim);border:1px solid rgba(25,113,194,.15);
                              border-radius:var(--radius);padding:10px 14px;
                              max-width:75%;font-size:14px;color:var(--text);">{content}</div>
                </div>""", unsafe_allow_html=True)
            else:
                cited_html = ""
                if cited:
                    run_chips = " ".join(f'<span class="run-chip">{r}</span>' for r in cited[:6])
                    cited_html = f'<div style="margin-top:8px;font-size:12px;color:var(--text-dim);">Cited runs: {run_chips}</div>'
                st.markdown(f"""
                <div style="display:flex;justify-content:flex-start;margin-bottom:10px;">
                  <div class="chat-bubble" style="max-width:80%;">{content}{cited_html}</div>
                </div>""", unsafe_allow_html=True)

        # Suggested prompts
        if not st.session_state.chat_history:
            sp1, sp2, sp3 = st.columns(3)
            suggestions = [
                "What was the avg DQ delta this week?",
                "Which source has the most quarantined rows?",
                "Show me runs with DQ score below 50",
            ]
            for col, prompt in zip([sp1, sp2, sp3], suggestions):
                with col:
                    if st.button(prompt, key=f"sugg_{prompt[:20]}", use_container_width=True):
                        st.session_state._pending_chat = prompt
                        st.rerun()

        # Input
        user_input = st.chat_input("Ask about pipeline runs, quality, cost…")
        pending = st.session_state.pop("_pending_chat", None)
        query = user_input or pending

        if query:
            st.session_state.chat_history.append({"role": "user", "content": query})
            with st.spinner("Analyzing run history…"):
                answer, cited = _chatbot_query(query)
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": answer,
                "cited_runs": cited,
            })
            st.rerun()

        if st.session_state.chat_history:
            if st.button("Clear chat", key="clear_chat"):
                st.session_state.chat_history = []
                st.rerun()


def _chatbot_query(query: str) -> tuple[str, list[str]]:
    """Try ObservabilityChatbot, fallback to simple log analysis."""
    try:
        from src.uc2_observability.rag_chatbot import ObservabilityChatbot
        bot = ObservabilityChatbot()
        resp = bot.chat(query)
        return resp.answer, resp.cited_run_ids
    except Exception:
        pass

    # Simple fallback
    logs = load_run_logs()
    q = query.lower()

    if "avg" in q and "dq" in q:
        deltas = [r["dq_delta"] for r in logs if r.get("dq_delta") is not None]
        if deltas:
            avg = sum(deltas) / len(deltas)
            return f"Average DQ delta across {len(deltas)} runs: {avg:+.2f} points.", []
        return "No DQ delta data available.", []

    if "quarantin" in q:
        by_src: dict[str, int] = {}
        for r in logs:
            src = r.get("source_name", r.get("source", "unknown"))
            by_src[src] = by_src.get(src, 0) + (r.get("rows_quarantined") or 0)
        if by_src:
            top = max(by_src, key=lambda k: by_src[k])
            return f"Source with most quarantined rows: **{top}** ({by_src[top]:,} rows).", []
        return "No quarantine data found.", []

    if "error" in q or "fail" in q:
        errors = [r for r in logs if r.get("status") == "error"]
        if errors:
            srcs = ", ".join({r.get("source_name", "?") for r in errors[:5]})
            return f"Found {len(errors)} failed runs. Sources: {srcs}.", [r.get("run_id", "")[:8] for r in errors[:5]]
        return "No failed runs found.", []

    if "success" in q or "rate" in q:
        total = len(logs)
        ok = sum(1 for r in logs if r.get("status") == "success")
        rate = ok / total * 100 if total else 0
        return f"Overall success rate: {rate:.1f}% ({ok}/{total} runs).", []

    total = len(logs)
    ok = sum(1 for r in logs if r.get("status") == "success")
    return (
        f"I have {total} pipeline runs in memory. {ok} succeeded. "
        "Ask about DQ scores, quarantine rates, enrichment tiers, or specific sources.",
        []
    )
