"""Observability page — Run history table, Grafana iframe, Chatbot with mem0."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import load_run_logs

import os
GRAFANA_URL  = os.getenv("GRAFANA_BASE_URL", "http://35.239.47.242:3000")
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
        # Test if Grafana allows embedding
        grafana_embed_ok = False
        try:
            import requests as _req
            r = _req.get(f"{GRAFANA_URL}/api/health", timeout=3)
            if r.status_code < 500:
                # Check allow_embedding setting
                grafana_embed_ok = True
        except Exception:
            pass

        if grafana_embed_ok:
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Grafana — ETL Pipeline Observability</div>
              <div style="margin-bottom:10px;">
                <a href="{GRAFANA_DASH}" target="_blank"
                   style="font-size:13px;color:var(--accent);font-weight:600;text-decoration:none;">
                   Open in Grafana ↗
                </a>
                <span style="font-size:12px;color:var(--text-dim);margin-left:12px;">
                  (if iframe blocked, use direct link above)
                </span>
              </div>
              <iframe
                src="{GRAFANA_DASH}"
                width="100%"
                height="740"
                frameborder="0"
                style="border-radius:6px;border:1px solid var(--border);display:block;"
                allowfullscreen
              ></iframe>
            </div>""", unsafe_allow_html=True)
        else:
            # Grafana unreachable — show instructions
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Grafana — ETL Pipeline Observability</div>
              <div class="alert orange" style="margin-bottom:16px;">
                Grafana not reachable at <code>{GRAFANA_URL}</code> — ensure the service is running.
              </div>
              <div style="font-size:14px;color:var(--text-muted);line-height:1.8;">
                <strong>Start Grafana:</strong>
                <div class="terminal" style="margin-top:8px;">
                  <div>docker-compose -p mip up -d grafana</div>
                </div>
                <div style="margin-top:12px;">
                  <strong>Enable embedding</strong> — add to <code>grafana.ini</code>:
                </div>
                <div class="terminal" style="margin-top:6px;">
                  <div>[security]</div>
                  <div>allow_embedding = true</div>
                  <div>cookie_samesite = disabled</div>
                </div>
                <div style="margin-top:12px;">
                  <a href="{GRAFANA_DASH}" target="_blank"
                     style="color:var(--accent);font-weight:600;text-decoration:none;">
                     Open dashboard directly ↗
                  </a>
                </div>
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
            suggestions = [
                ("📊", "What's the overall pipeline success rate?"),
                ("🔴", "Which source has the most quarantined rows?"),
                ("📈", "Which source improved DQ the most?"),
                ("💰", "What was the total LLM cost across all runs?"),
                ("⚠️",  "Show me all failed runs and their errors"),
                ("🧬", "How many rows were enriched via S3 LLM?"),
            ]
            r1, r2, r3 = st.columns(3)
            r4, r5, r6 = st.columns(3)
            for col, (icon, prompt) in zip([r1, r2, r3, r4, r5, r6], suggestions):
                with col:
                    if st.button(f"{icon} {prompt}", key=f"sugg_{prompt[:24]}", use_container_width=True):
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
    """Try ObservabilityChatbot, fallback to rich log analytics."""
    try:
        from src.uc2_observability.rag_chatbot import ObservabilityChatbot
        bot = ObservabilityChatbot()
        resp = bot.chat(query)
        if resp.answer and len(resp.answer) > 20:
            return resp.answer, resp.cited_run_ids
    except Exception:
        pass

    logs = load_run_logs()
    if not logs:
        return "No pipeline run data available yet. Run a pipeline first.", []

    q = query.lower()
    total = len(logs)
    ok    = sum(1 for r in logs if r.get("status") == "success")
    err   = sum(1 for r in logs if r.get("status") == "error")

    # ── success rate ──
    if any(w in q for w in ["success rate", "pass", "overall", "how many success"]):
        rate = ok / total * 100 if total else 0
        cited = [r.get("run_id", "")[:8] for r in logs if r.get("status") == "success"][:5]
        return (
            f"**Overall success rate: {rate:.1f}%** ({ok} succeeded, {err} failed out of {total} total runs).\n\n"
            f"Sources: {', '.join(sorted({r.get('source_name','?') for r in logs if r.get('status')=='success'}))}"
        ), cited

    # ── DQ delta / improvement ──
    if any(w in q for w in ["dq", "quality", "delta", "improv", "score"]):
        deltas = [(r.get("source_name", "?"), r.get("dq_delta") or 0, r.get("dq_score_pre") or 0, r.get("dq_score_post") or 0)
                  for r in logs if r.get("dq_delta") is not None]
        if deltas:
            avg_d = sum(d for _, d, _, _ in deltas) / len(deltas)
            best = max(deltas, key=lambda x: x[1])
            worst = min(deltas, key=lambda x: x[1])
            return (
                f"**Average DQ improvement: {avg_d:+.2f} pts** across {len(deltas)} runs.\n\n"
                f"🟢 Best improvement: **{best[0]}** ({best[2]:.1f} → {best[3]:.1f}, Δ={best[1]:+.2f})\n\n"
                f"🔴 Worst: **{worst[0]}** ({worst[2]:.1f} → {worst[3]:.1f}, Δ={worst[1]:+.2f})\n\n"
                f"High DQ delta means the pipeline cleaned and enriched the data significantly."
            ), []

    # ── quarantine ──
    if any(w in q for w in ["quarantin", "flagged", "rejected"]):
        by_src: dict[str, int] = {}
        for r in logs:
            src = r.get("source_name", r.get("source", "unknown"))
            by_src[src] = by_src.get(src, 0) + (r.get("rows_quarantined") or 0)
        total_q = sum(by_src.values())
        total_r = sum((r.get("rows_in") or 0) for r in logs)
        qrate = total_q / total_r * 100 if total_r else 0
        if by_src:
            top = max(by_src, key=lambda k: by_src[k])
            rows_sorted = sorted(by_src.items(), key=lambda x: x[1], reverse=True)
            breakdown = "\n".join(f"  • {s}: {v:,}" for s, v in rows_sorted[:5])
            return (
                f"**Total quarantined: {total_q:,} rows** ({qrate:.2f}% of all input).\n\n"
                f"Source with most quarantined rows: **{top}** ({by_src[top]:,})\n\n"
                f"Breakdown:\n{breakdown}\n\n"
                f"Quarantine reasons include: null key columns, schema violations, duplicate records."
            ), []

    # ── enrichment / LLM / tiers ──
    if any(w in q for w in ["enrich", "llm", "s3", "s1", "s2", "tier", "deterministic"]):
        s1 = sum((r.get("enrichment_stats") or {}).get("deterministic", 0) for r in logs)
        s2 = sum((r.get("enrichment_stats") or {}).get("embedding", 0) for r in logs)
        s3 = sum((r.get("enrichment_stats") or {}).get("llm", 0) for r in logs)
        grand = s1 + s2 + s3 or 1
        return (
            f"**Total enrichment across all runs: {grand:,} resolutions**\n\n"
            f"🟢 S1 Deterministic (regex/keyword): **{s1:,}** ({s1/grand*100:.1f}%)\n\n"
            f"🔵 S2 KNN Corpus (FAISS similarity): **{s2:,}** ({s2/grand*100:.1f}%)\n\n"
            f"🟡 S3 RAG-LLM (Claude/LLM-assisted): **{s3:,}** ({s3/grand*100:.1f}%)\n\n"
            f"S3 only fires when S1 and S2 can't resolve — keeps LLM cost minimal."
        ), []

    # ── cost ──
    if any(w in q for w in ["cost", "usd", "spend", "money", "expensive"]):
        from src.ui.utils.api_client import prom_scalar
        try:
            total_cost = prom_scalar('sum(etl_llm_cost_usd_total)') or 0.0
            return (
                f"**Total LLM cost: ${total_cost:.4f} USD** across all pipeline runs.\n\n"
                f"Cost comes from S3 RAG-LLM enrichment (Claude Haiku). "
                f"S1 and S2 are free. The architecture is designed to minimize S3 calls."
            ), []
        except Exception:
            return "Cost data not available — Prometheus may be offline.", []

    # ── errors / failures ──
    if any(w in q for w in ["error", "fail", "broken", "crash"]):
        errors = [r for r in logs if r.get("status") == "error"]
        if errors:
            srcs = sorted({r.get("source_name", "?") for r in errors})
            err_details = "\n".join(
                f"  • {r.get('source_name','?')} — {str(r.get('error','unknown'))[:60]}"
                for r in errors[:5]
            )
            cited = [r.get("run_id", "")[:8] for r in errors[:5]]
            return (
                f"**{len(errors)} failed run(s)** out of {total} total.\n\n"
                f"Sources affected: {', '.join(srcs)}\n\n"
                f"Details:\n{err_details}"
            ), cited
        return f"No failed runs found. All {total} recorded runs completed successfully.", []

    # ── source breakdown ──
    if any(w in q for w in ["source", "which source", "breakdown"]):
        by_src: dict[str, dict] = {}
        for r in logs:
            src = r.get("source_name", "unknown")
            if src not in by_src:
                by_src[src] = {"runs": 0, "rows": 0, "ok": 0}
            by_src[src]["runs"] += 1
            by_src[src]["rows"] += (r.get("rows_in") or 0)
            if r.get("status") == "success":
                by_src[src]["ok"] += 1
        lines = "\n".join(
            f"  • **{s}**: {v['runs']} runs, {v['rows']:,} rows, {v['ok']/v['runs']*100:.0f}% success"
            for s, v in sorted(by_src.items(), key=lambda x: x[1]["rows"], reverse=True)
        )
        return f"**Pipeline runs by source:**\n\n{lines}", []

    # ── default summary ──
    total_rows = sum((r.get("rows_in") or 0) for r in logs)
    total_out  = sum((r.get("rows_out") or 0) for r in logs)
    return (
        f"**MIP Pipeline Summary** — {total} runs recorded\n\n"
        f"✅ Success: {ok} | ❌ Failed: {err}\n\n"
        f"📦 Total rows processed: {total_rows:,} in → {total_out:,} out\n\n"
        f"Try asking: *success rate*, *DQ improvement*, *quarantine breakdown*, "
        f"*enrichment tiers*, *LLM cost*, *failed runs*, or *source breakdown*."
    ), []
