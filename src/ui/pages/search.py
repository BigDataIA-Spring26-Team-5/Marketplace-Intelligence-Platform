"""Search page — BM25 + Semantic hybrid search over unified product catalog."""
from __future__ import annotations
import streamlit as st

CATEGORIES = ["All", "Snacks", "Beverages", "Dairy", "Produce", "Frozen", "Bakery", "Meat", "Seafood"]


def _tag_badge(tag: str) -> str:
    tag_map = {
        "organic": "success", "gluten-free": "info", "vegan": "purple",
        "dairy-free": "warning", "recalled": "error", "kosher": "orange",
    }
    cls = tag_map.get(tag.lower(), "info")
    return f'<span class="badge {cls}" style="font-size:11px;">{tag}</span>'


def render_search():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Product Search</div>
        <div class="page-subtitle">BM25 + Semantic hybrid search with Reciprocal Rank Fusion</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Search bar + filters ──────────────────────────────────────────────────
    sc1, sc2, sc3, sc4 = st.columns([4, 1, 1, 1])
    with sc1:
        query = st.text_input("Search products", placeholder="organic gluten-free cereal…", label_visibility="collapsed")
    with sc2:
        mode  = st.selectbox("Mode", ["hybrid", "semantic", "bm25"], label_visibility="collapsed")
    with sc3:
        top_k = st.selectbox("Top K", [10, 20, 50], label_visibility="collapsed")
    with sc4:
        suppress_recalled = st.checkbox("Hide recalled", value=False)

    category_filter = st.selectbox("Category", CATEGORIES, label_visibility="visible")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    EXAMPLE_QUERIES = [
        "organic gluten-free cereal",
        "Greek yogurt high protein",
        "almond milk unsweetened",
        "frozen pizza pepperoni",
        "vitamin C supplement",
        "grass fed beef",
    ]

    if not query:
        eg_buttons = st.columns(3)
        for i, eq in enumerate(EXAMPLE_QUERIES):
            with eg_buttons[i % 3]:
                if st.button(f'🔍 {eq}', key=f"eg_{i}", use_container_width=True):
                    st.session_state._search_query = eq
                    st.rerun()

        # Pick up pre-set query from button click
        if "_search_query" in st.session_state:
            query = st.session_state.pop("_search_query")
        else:
            st.markdown("""
            <div class="card" style="text-align:center;padding:32px;">
              <div style="font-size:28px;margin-bottom:12px;">🔍</div>
              <div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:6px;">Search the Product Catalog</div>
              <div style="font-size:14px;color:var(--text-muted);">
                BM25 + ChromaDB semantic search with Reciprocal Rank Fusion over the unified product catalog.
                <br>Covers product names, brands, ingredients, categories, allergens, and dietary tags.
              </div>
            </div>""", unsafe_allow_html=True)
            return

    # ── Run search ────────────────────────────────────────────────────────────
    with st.spinner("Searching…"):
        results, error = _run_search(query, top_k=top_k, mode=mode, suppress_recalled=suppress_recalled)

    if error:
        st.markdown(f'<div class="alert orange">{error}</div>', unsafe_allow_html=True)
        return

    if category_filter != "All":
        results = [r for r in results if category_filter.lower() in (r.get("primary_category") or "").lower()]

    st.markdown(f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px;">
      <span class="badge info">{len(results)} results</span>
      <span class="badge purple">{mode} mode</span>
      <span style="font-size:13px;color:var(--text-dim);">for "{query}"</span>
    </div>""", unsafe_allow_html=True)

    if not results:
        st.markdown('<div class="alert orange">No results found. Try a different query or mode.</div>', unsafe_allow_html=True)
        return

    # ── Product grid (3 columns) ──────────────────────────────────────────────
    cols = st.columns(3)
    for i, r in enumerate(results):
        def _safe(v, default=""):
            """Return empty string for nan/None/empty."""
            if v is None: return default
            s = str(v).strip()
            return default if s in ("nan", "None", "none", "") else s

        name      = _safe(r.get("product_name") or r.get("description") or r.get("text"), "Unknown Product")
        brand     = _safe(r.get("brand_name") or r.get("brand"))
        category  = _safe(r.get("primary_category") or r.get("category"))
        allergens = _safe(r.get("allergens"))
        is_organic = str(r.get("is_organic", "")).lower() in ("true", "1", "yes")
        dietary   = _safe(r.get("dietary_tags") or r.get("tags"))
        score     = r.get("score") or r.get("rrf_score") or r.get("distance") or 0.0
        try:
            score = float(score)
        except Exception:
            score = 0.0
        recalled  = "recall" in _safe(r.get("status")).lower()

        # Build tag list
        tags = []
        if is_organic:
            tags.append("organic")
        if recalled:
            tags.append("recalled")
        if dietary:
            for t in dietary.split(","):
                t = t.strip()
                if t and t not in tags:
                    tags.append(t)

        tag_html = " ".join(_tag_badge(t) for t in tags[:5])
        if not tag_html and category:
            tag_html = f'<span class="badge info" style="font-size:11px;">{category}</span>'

        card_cls = "product-card recalled" if recalled else "product-card"

        # Score display (round to 3 dp, hide if 0)
        score_html = ""
        if score and abs(score) > 0.0001:
            score_html = f'<div style="font-size:11px;color:var(--text-dim);margin-top:8px;font-family:var(--mono);">score: {score:.3f}</div>'

        cat_badge = f'<span class="badge purple" style="font-size:11px;margin-bottom:8px;display:inline-block;">{category}</span>' if category else ""

        allergen_html = ""
        if allergens:
            allergen_html = f'<div style="font-size:12px;color:var(--amber);margin-top:6px;">⚠ {allergens[:60]}</div>'

        with cols[i % 3]:
            recalled_border = "border-color:rgba(201,42,42,.3);" if recalled else ""
            st.markdown(f"""
            <div style="background:var(--bg);border:1px solid var(--border);border-radius:10px;
                        padding:14px 16px;margin-bottom:10px;{recalled_border}">
              <div style="font-size:15px;font-weight:700;color:var(--text);line-height:1.35;
                          margin-bottom:4px;word-break:break-word;">{name[:70]}</div>
              <div style="font-size:12px;font-weight:600;color:var(--text-dim);text-transform:uppercase;
                          letter-spacing:.04em;margin-bottom:9px;">{brand[:40] if brand else "&nbsp;"}</div>
              {f'<div style="margin-bottom:8px;">{cat_badge}</div>' if cat_badge else ""}
              <div style="display:flex;flex-wrap:wrap;gap:4px;">{tag_html}</div>
              {allergen_html}
              {score_html}
            </div>""", unsafe_allow_html=True)


def _run_search(query: str, top_k: int = 10, mode: str = "hybrid", suppress_recalled: bool = False) -> tuple[list[dict], str]:
    try:
        from src.uc3_search.hybrid_search import HybridSearch
        hs = HybridSearch()
        results = hs.search(query, top_k=top_k, mode=mode, suppress_recalled=suppress_recalled)
        return results, ""
    except Exception as e:
        err_str = str(e)
        # Fall back to ChromaDB direct search
        try:
            import chromadb
            client = chromadb.HttpClient(host="localhost", port=8000)
            coll = client.get_or_create_collection("uc3_products")
            res = coll.query(query_texts=[query], n_results=min(top_k, 20))
            docs = res.get("documents", [[]])[0]
            metas = res.get("metadatas", [[]])[0]
            dists = res.get("distances", [[]])[0]
            out = []
            for doc, meta, dist in zip(docs, metas, dists):
                row = dict(meta or {})
                row.setdefault("product_name", doc[:80])
                row["score"] = round(1 - dist, 4)
                out.append(row)
            return out, ""
        except Exception as e2:
            return [], f"Search unavailable: {err_str}. ChromaDB fallback also failed: {e2}"
