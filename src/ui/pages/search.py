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

    if not query:
        st.markdown("""
        <div class="card" style="text-align:center;padding:40px;">
          <div style="font-size:32px;margin-bottom:12px;">⊕</div>
          <div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:6px;">Search the Product Catalog</div>
          <div style="font-size:13px;color:var(--text-muted);">
            Searches across product names, brands, ingredients, and categories using BM25 + ChromaDB semantic search.
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
        name     = r.get("product_name", r.get("description", "Unknown"))
        brand    = r.get("brand_name", r.get("brand", ""))
        category = r.get("primary_category", "")
        allergens = r.get("allergens", "")
        is_organic = r.get("is_organic", False)
        dietary = r.get("dietary_tags", "") or ""
        score    = r.get("score", r.get("rrf_score", 0.0))
        recalled = "recall" in str(r.get("status", "")).lower()

        tags = []
        if is_organic:
            tags.append("organic")
        if recalled:
            tags.append("recalled")
        if dietary:
            for t in str(dietary).split(","):
                t = t.strip()
                if t:
                    tags.append(t)

        tag_html = " ".join(_tag_badge(t) for t in tags[:4])
        card_cls = "product-card recalled" if recalled else "product-card"
        score_html = f'<div style="font-size:11px;color:var(--text-dim);margin-top:8px;font-family:var(--mono);">score: {score:.3f}</div>' if score else ""

        cat_html = f'<span class="badge info" style="font-size:11px;">{category}</span>' if category else ""

        allergen_html = ""
        if allergens and str(allergens) not in ("nan", "None", ""):
            allergen_html = f'<div style="font-size:11px;color:var(--amber);margin-top:6px;">⚠ {allergens}</div>'

        with cols[i % 3]:
            st.markdown(f"""
            <div class="{card_cls}">
              <div class="product-name">{name}</div>
              <div class="product-brand">{brand}</div>
              {f'<div style="margin-bottom:8px;">{cat_html}</div>' if cat_html else ""}
              <div class="product-tags">{tag_html}</div>
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
