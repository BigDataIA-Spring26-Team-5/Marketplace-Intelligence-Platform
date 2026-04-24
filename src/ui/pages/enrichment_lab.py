"""Enrichment Lab — tier breakdown, ChromaDB corpus stats, enrichment rules."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import prom_series, chroma_collections


def _prom_tier_totals() -> dict[str, int]:
    totals: dict[str, int] = {"s1": 0, "s2": 0, "s3": 0}
    for metric, key in [
        ("etl_enrichment_s1_resolved", "s1"),
        ("etl_enrichment_s2_resolved", "s2"),
        ("etl_enrichment_s3_resolved", "s3"),
    ]:
        try:
            series = prom_series(f'sum({metric})')
            if series:
                totals[key] = int(series[0][1])
        except Exception:
            pass
    return totals


def _prom_tier_by_source() -> dict[str, dict]:
    result: dict[str, dict] = {}
    for metric, key in [
        ("etl_enrichment_s1_resolved", "s1"),
        ("etl_enrichment_s2_resolved", "s2"),
        ("etl_enrichment_s3_resolved", "s3"),
    ]:
        try:
            series = prom_series(f'sum by (source) ({metric})')
            for labels, val in series:
                src = labels.get("source", "unknown")
                result.setdefault(src, {"s1": 0, "s2": 0, "s3": 0})
                result[src][key] = int(val)
        except Exception:
            pass
    return result


def render_enrichment_lab():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Enrichment Lab</div>
        <div class="page-subtitle">Three-tier enrichment pipeline — S1 Deterministic → S2 KNN → S3 RAG-LLM</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Tier overview KPIs ────────────────────────────────────────────────────
    totals = _prom_tier_totals()
    s1 = totals["s1"]
    s2 = totals["s2"]
    s3 = totals["s3"]
    grand = s1 + s2 + s3 or 1

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">S1 Deterministic</div>
          <div class="stat-value sv-lg" style="color:var(--green)">{s1:,}</div>
          <div class="stat-delta up">regex / keyword resolved</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">S2 KNN Corpus</div>
          <div class="stat-value sv-lg" style="color:var(--accent)">{s2:,}</div>
          <div class="stat-delta up">FAISS similarity resolved</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">S3 RAG-LLM</div>
          <div class="stat-value sv-lg" style="color:var(--amber)">{s3:,}</div>
          <div class="stat-delta">LLM-assisted resolved</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        coverage = round((s1 + s2 + s3) / grand * 100, 1) if grand > 1 else 0.0
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Total Resolved</div>
          <div class="stat-value sv-lg">{s1+s2+s3:,}</div>
          <div class="stat-delta up">across all sources</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    left, right = st.columns([3, 2])

    _SKIP = {"part_0000", "*", "usda"}

    with left:
        # Per-source tier breakdown table
        by_source = _prom_tier_by_source()
        by_source = {k: v for k, v in by_source.items() if k not in _SKIP}
        if by_source:
            rows_html = ""
            for src, tiers in sorted(by_source.items(), key=lambda x: sum(x[1].values()), reverse=True):
                total = sum(tiers.values()) or 1
                s1v, s2v, s3v = tiers["s1"], tiers["s2"], tiers["s3"]
                s1p = s1v / total * 100
                s2p = s2v / total * 100
                s3p = s3v / total * 100
                bar = f"""
                <div class="tier-bar" style="width:160px;">
                  <div class="tier-s1" style="flex:{s1p:.0f}"></div>
                  <div class="tier-s2" style="flex:{s2p:.0f}"></div>
                  <div class="tier-s3" style="flex:{s3p:.0f}"></div>
                </div>"""
                rows_html += f"""
                <tr>
                  <td><span class="mono">{src}</span></td>
                  <td class="tc-green">{s1v:,}</td>
                  <td class="tc-accent">{s2v:,}</td>
                  <td class="tc-amber">{s3v:,}</td>
                  <td>{bar}</td>
                </tr>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Enrichment by Source</div>
              <table class="data-table">
                <thead><tr><th>Source</th><th>S1</th><th>S2</th><th>S3</th><th>Mix</th></tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
              <div class="tier-legend">
                <div class="tier-legend-item"><span class="tier-dot s1"></span>S1 Deterministic</div>
                <div class="tier-legend-item"><span class="tier-dot s2"></span>S2 KNN</div>
                <div class="tier-legend-item"><span class="tier-dot s3"></span>S3 RAG-LLM</div>
              </div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="card">
              <div class="card-title">Enrichment by Source</div>
              <div style="color:var(--text-dim);font-size:13px;padding:8px 0;">
                No enrichment metrics in Prometheus yet — run a pipeline first.
              </div>
            </div>""", unsafe_allow_html=True)

    with right:
        # ChromaDB collections
        collections = chroma_collections()
        if collections:
            st.markdown('<div class="card"><div class="card-title">ChromaDB Collections</div>', unsafe_allow_html=True)
            for c in collections:
                name = str(c.get("name", "")).replace("<", "&lt;").replace(">", "&gt;")
                st.markdown(f"""
                <div style="display:flex;align-items:center;justify-content:space-between;
                            padding:9px 0;border-bottom:1px solid var(--border);">
                  <span class="mono" style="font-size:13px;color:var(--text);">{name}</span>
                  <span class="badge info">collection</span>
                </div>""", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # Safety guardrails
        st.markdown("""
        <div class="card">
          <div class="card-title">Safety Guardrails</div>
          <div class="guardrail-badge">✓ allergens — S1 extraction only (never inferred)</div>
          <div class="guardrail-badge">✓ dietary_tags — S1 extraction only</div>
          <div class="guardrail-badge">✓ is_organic — S1 extraction only</div>
          <div style="font-size:12px;color:var(--text-dim);margin-top:8px;">
            S2/S3 only resolves <code>primary_category</code>. Safety fields are never
            inferred by KNN or LLM — false positives are worse than nulls.
          </div>
        </div>""", unsafe_allow_html=True)

    # ── LLM cost breakdown ────────────────────────────────────────────────────
    try:
        cost_series = prom_series('sum by (source) (etl_llm_cost_usd_total)')
        if cost_series:
            # Filter zeros and skip sources
            cost_series = [(l, v) for l, v in cost_series
                           if v > 0 and l.get("source", "") not in _SKIP]
            cost_sorted = sorted(cost_series, key=lambda x: x[1], reverse=True)
            if cost_sorted:
                max_cost = max(v for _, v in cost_sorted) or 1
                bars_html = ""
                for labels, val in cost_sorted:
                    src = labels.get("source", "unknown")
                    pct = val / max_cost * 100
                    bars_html += f"""
                    <div class="bar-row">
                      <div class="bar-label">{src[:14]}</div>
                      <div class="bar-track"><div class="bar-fill bar-amber" style="width:{pct:.1f}%"></div></div>
                      <div class="bar-val">${val:.4f}</div>
                    </div>"""
                st.markdown(f"""
                <div class="card">
                  <div class="card-title">LLM Cost by Source (USD)</div>
                  <div class="bar-chart">{bars_html}</div>
                </div>""", unsafe_allow_html=True)
    except Exception:
        pass

    # ── FAISS corpus info ─────────────────────────────────────────────────────
    try:
        from pathlib import Path
        from datetime import datetime
        import json as _json

        today = datetime.now().strftime("%Y-%m-%d")
        corpus_meta = Path("corpus/corpus_summary.json")
        size = "—"
        last_updated = today

        if corpus_meta.exists():
            meta = _json.loads(corpus_meta.read_text())
            size = meta.get("total_vectors", meta.get("size", "—"))
            raw_ts = meta.get("last_updated", "")
            last_updated = str(raw_ts)[:10] if raw_ts else today

        # Check if faiss index exists even if summary missing
        faiss_bin = Path("corpus/faiss_index.bin")
        if not corpus_meta.exists() and not faiss_bin.exists():
            raise FileNotFoundError

        st.markdown(f"""
        <div class="card">
          <div class="card-title">FAISS Corpus</div>
          <div style="display:flex;gap:20px;">
            <div class="stat-card" style="flex:1;padding:12px;">
              <div class="stat-label">Corpus Vectors</div>
              <div class="stat-value sv-md">{size if size == "—" else f"{int(size):,}"}</div>
            </div>
            <div class="stat-card" style="flex:1;padding:12px;">
              <div class="stat-label">Last Updated</div>
              <div class="stat-value sv-xs">{last_updated}</div>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)
    except Exception:
        pass
