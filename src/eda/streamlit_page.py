"""Streamlit EDA page — loads bronze/silver/gold, renders tabs.

Data sources for this page:
- `src/eda/report.py` loaders for bronze/silver/gold + run_logs
- `output/eda/<slug>/summary.json` precomputed stats (from eda_full_report CLI)
- Prometheus at localhost:9090 for telemetry gauges
- run_logs JSON for wallclock per block + token spend

Design goals:
- Never block on missing data — every tab degrades to "not available" text
- Cache loaders for 5 minutes so tab switches are snappy
- Defer heavyweight imports (matplotlib / plotly) until tab opens
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.eda.report import (
    compute_stats,
    load_bronze,
    load_gold,
    load_run_logs,
    load_silver,
)

logger = logging.getLogger(__name__)

EDA_ARTIFACT_ROOT = Path("output/eda")

# Anchors locked in docs/data_inventory.md §7
LOCKED_ANCHORS: list[tuple[str, str, str]] = [
    ("usda", "2026/04/21", "nutrition"),
    ("off", "2026/04/22", "nutrition"),
    ("openfda", "2026/04/20", "safety"),
    ("esci", "2026/04/20", "retail"),
]


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner="Loading bronze…")
def _cached_bronze(source: str, date: str, limit: int) -> pd.DataFrame:
    return load_bronze(source, date, limit=limit)


@st.cache_data(ttl=300, show_spinner="Loading silver…")
def _cached_silver(source: str, date: str) -> pd.DataFrame:
    return load_silver(source, date)


@st.cache_data(ttl=300, show_spinner="Loading gold…")
def _cached_gold(source: str | None, use_bq: bool) -> pd.DataFrame:
    return load_gold(source=source, use_bq=use_bq, limit=100_000)


@st.cache_data(ttl=300, show_spinner="Loading run logs…")
def _cached_run_logs() -> pd.DataFrame:
    return load_run_logs(include_gcs=True)


def _slug(source: str, date: str) -> str:
    return f"{source}_{date.replace('/', '')}"


def _precomputed_summary(slug: str) -> dict | None:
    path = EDA_ARTIFACT_ROOT / slug / "summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.warning(f"summary.json load failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Prometheus helper
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def _prom_query(query: str, prom_url: str = "http://localhost:9090") -> list[dict]:
    try:
        import requests  # lazy import

        r = requests.get(f"{prom_url}/api/v1/query", params={"query": query}, timeout=3)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            return []
        return data["data"]["result"]
    except Exception as exc:
        logger.warning(f"Prometheus {query} failed: {exc}")
        return []


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------
def render_eda_page() -> None:
    st.title("📊 EDA — Bronze / Silver / Gold")
    st.caption(
        "Exploratory data analysis across pipeline layers. "
        "Pick a `(source, date)` anchor, inspect shape, schema, nulls, DQ, enrichment, "
        "dedup, categories, and telemetry."
    )

    # Anchor picker ----------------------------------------------------------
    with st.sidebar:
        st.markdown("### EDA controls")
        anchor_labels = [f"{s} · {d} · {dom}" for s, d, dom in LOCKED_ANCHORS]
        anchor_labels.append("Custom…")
        pick = st.selectbox("Anchor", anchor_labels, index=1, key="eda_anchor")

        if pick == "Custom…":
            source = st.text_input("Source", value="off", key="eda_custom_src").strip()
            date = st.text_input("Date (YYYY/MM/DD)", value="2026/04/22", key="eda_custom_date").strip()
            domain = st.text_input("Domain", value="nutrition", key="eda_custom_dom").strip()
        else:
            source, date, domain = LOCKED_ANCHORS[anchor_labels.index(pick)]

        use_bq = st.checkbox("Fetch Gold from BigQuery (slow)", value=False, key="eda_use_bq")
        bronze_limit = st.slider("Bronze sample size", 200, 10_000, 2000, step=500, key="eda_bronze_limit")

        if st.button("↻ Refresh caches", key="eda_refresh"):
            _cached_bronze.clear()
            _cached_silver.clear()
            _cached_gold.clear()
            _cached_run_logs.clear()
            _prom_query.clear()
            st.rerun()

    slug = _slug(source, date)

    # Load data --------------------------------------------------------------
    bronze = _cached_bronze(source, date, bronze_limit)
    silver = _cached_silver(source, date)
    gold = _cached_gold(None if not use_bq else None, use_bq)
    run_logs = _cached_run_logs()

    # Prefer precomputed summary.json for heavy stats when available
    precomp = _precomputed_summary(slug)
    if precomp:
        st.info(f"Using precomputed artifact `{slug}/summary.json`. Uncheck via ↻ Refresh to recompute live.")
        stats_dict = precomp
    else:
        stats_dict = compute_stats(bronze, silver, gold, source=source, date=date).as_dict()

    # Top banner -------------------------------------------------------------
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bronze rows", f"{stats_dict.get('bronze_shape', [0, 0])[0]:,}")
    c2.metric("Silver rows", f"{stats_dict.get('silver_shape', [0, 0])[0]:,}")
    c3.metric("Gold rows", f"{stats_dict.get('gold_shape', [0, 0])[0]:,}")
    dq_mean = stats_dict.get("dq_pre_stats", {}).get("mean")
    c4.metric("DQ pre mean", f"{dq_mean:.1f}" if isinstance(dq_mean, (int, float)) else "—")

    # Tabs ------------------------------------------------------------------
    tabs = st.tabs([
        "Shape", "Schema diff", "Nulls", "DQ scores",
        "Enrichment", "Dedup", "Categories", "Telemetry", "UC3 / UC4",
    ])

    with tabs[0]:
        _tab_shape(stats_dict, bronze, silver, gold)
    with tabs[1]:
        _tab_schema(stats_dict, bronze, silver)
    with tabs[2]:
        _tab_nulls(stats_dict, bronze, silver, gold)
    with tabs[3]:
        _tab_dq(stats_dict, silver, gold)
    with tabs[4]:
        _tab_enrichment(stats_dict, gold)
    with tabs[5]:
        _tab_dedup(stats_dict, gold)
    with tabs[6]:
        _tab_categories(stats_dict)
    with tabs[7]:
        _tab_telemetry(run_logs, source)
    with tabs[8]:
        _tab_uc3_uc4()


# ---------------------------------------------------------------------------
# Tab bodies
# ---------------------------------------------------------------------------
def _tab_shape(stats: dict, bronze: pd.DataFrame, silver: pd.DataFrame, gold: pd.DataFrame) -> None:
    st.subheader("Layer shape")
    df = pd.DataFrame([
        {"layer": "bronze", "rows": stats["bronze_shape"][0], "cols": stats["bronze_shape"][1]},
        {"layer": "silver", "rows": stats["silver_shape"][0], "cols": stats["silver_shape"][1]},
        {"layer": "gold", "rows": stats["gold_shape"][0], "cols": stats["gold_shape"][1]},
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)

    delta = stats["silver_shape"][0] - stats["bronze_shape"][0]
    st.caption(
        f"Silver row delta vs bronze sample: **{delta:+,}** "
        "(positive = row-level expansion from SPLIT ops; negative = quarantine / filter)."
    )


def _tab_schema(stats: dict, bronze: pd.DataFrame, silver: pd.DataFrame) -> None:
    st.subheader("Bronze → Silver schema diff")
    c1, c2, c3 = st.columns(3)
    c1.markdown("**Dropped** (bronze-only)")
    c1.write(stats.get("bronze_only", []) or "—")
    c2.markdown("**Added** (silver-only)")
    c2.write(stats.get("silver_only", []) or "—")
    c3.markdown("**Shared**")
    c3.write(stats.get("shared", []) or "—")


def _tab_nulls(stats: dict, bronze: pd.DataFrame, silver: pd.DataFrame, gold: pd.DataFrame) -> None:
    st.subheader("Null rates per column")
    layer = st.radio("Layer", ["bronze", "silver", "gold"], horizontal=True, key="eda_null_layer")
    rates = stats.get(f"{layer}_nulls", {})
    if not rates:
        st.info(f"No {layer} data for this anchor.")
        return
    df = pd.DataFrame({"column": list(rates.keys()), "null_pct": [v * 100 for v in rates.values()]})
    df = df.sort_values("null_pct", ascending=False)
    st.bar_chart(df.set_index("column")["null_pct"].head(25))
    st.dataframe(df, use_container_width=True, hide_index=True)


def _tab_dq(stats: dict, silver: pd.DataFrame, gold: pd.DataFrame) -> None:
    st.subheader("Data quality scores")
    pre = stats.get("dq_pre_stats", {})
    post = stats.get("dq_post_stats", {})
    delta = stats.get("dq_delta_stats", {})

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Pre**")
        st.write(pre or "—")
    with c2:
        st.markdown("**Post**")
        st.write(post or "—")
    with c3:
        st.markdown("**Delta**")
        st.write(delta or "—")

    # Histogram from live silver/gold
    src = gold if "dq_score_pre" in gold.columns and not gold.empty else silver
    if "dq_score_pre" in src.columns and src["dq_score_pre"].notna().any():
        st.caption("dq_score_pre histogram")
        st.bar_chart(
            src["dq_score_pre"].dropna().astype(float).round(0).value_counts().sort_index()
        )
    if "dq_delta" in src.columns and src["dq_delta"].notna().any():
        st.caption("dq_delta distribution")
        st.bar_chart(src["dq_delta"].dropna().astype(float).round(0).value_counts().sort_index())
    else:
        st.info(
            "No `dq_delta` — this anchor ran in silver mode (enrichment skipped) "
            "or post-score column is unpopulated."
        )


def _tab_enrichment(stats: dict, gold: pd.DataFrame) -> None:
    st.subheader("Enrichment fill rates (gold)")
    fill = stats.get("enrichment_fill_rate", {})
    if not fill:
        st.info("No enrichment fields present in gold.")
        return
    df = pd.DataFrame({
        "field": list(fill.keys()),
        "pct_non_null": [v * 100 for v in fill.values()],
    })
    st.bar_chart(df.set_index("field")["pct_non_null"])
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(
        "Reminder: `allergens`, `dietary_tags`, `is_organic` are **S1-only** — "
        "extracted from the product's own text, never inferred by S2/S3."
    )


def _tab_dedup(stats: dict, gold: pd.DataFrame) -> None:
    st.subheader("Dedup summary")
    total = stats.get("dedup_rows", 0)
    groups = stats.get("dedup_groups", 0)
    ratio = stats.get("dedup_ratio", 0.0)
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows (gold)", f"{total:,}")
    c2.metric("Unique groups", f"{groups:,}")
    c3.metric("Dedup ratio", f"{ratio:.1%}")
    if total == 0:
        st.info("`duplicate_group_id` absent in gold for this anchor — silver-only runs leave it null.")


def _tab_categories(stats: dict) -> None:
    st.subheader("Top categories (gold)")
    cats = stats.get("top_categories", [])
    if not cats:
        st.info("No categories computed.")
        return
    df = pd.DataFrame(cats, columns=["category", "rows"])
    st.bar_chart(df.set_index("category")["rows"])
    st.dataframe(df, use_container_width=True, hide_index=True)


def _tab_telemetry(run_logs: pd.DataFrame, source: str) -> None:
    st.subheader("Pipeline telemetry")

    if run_logs.empty:
        st.info("No run logs discovered (local + GCS run-logs empty).")
    else:
        st.markdown(f"**{len(run_logs)} run(s) indexed** (local + `gs://mip-silver-2024/run-logs/`)")
        cols = [c for c in ["run_id", "timestamp", "source_name", "domain", "status",
                            "duration_seconds", "rows_in", "rows_out", "dq_score_pre"]
                if c in run_logs.columns]
        st.dataframe(run_logs[cols].sort_values("timestamp", ascending=False, na_position="last"),
                     use_container_width=True, hide_index=True)

        if "duration_seconds" in run_logs.columns:
            st.caption("Duration distribution")
            st.bar_chart(run_logs["duration_seconds"].dropna())

    # Prometheus live gauges
    st.markdown("---")
    st.markdown("**Live Prometheus gauges** (`localhost:9090`)")
    metrics = [
        "etl_rows_processed_total",
        "etl_dq_score_mean",
        "etl_pipeline_duration_seconds",
        "etl_cache_hit_ratio",
        "etl_anomaly_flag",
    ]
    rows = []
    for m in metrics:
        results = _prom_query(m)
        if not results:
            rows.append({"metric": m, "value": "—", "labels": ""})
            continue
        for r in results[:3]:  # cap fanout
            val = r.get("value", [None, None])[1]
            rows.append({
                "metric": m,
                "value": val,
                "labels": ", ".join(f"{k}={v}" for k, v in r.get("metric", {}).items() if k != "__name__"),
            })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption("Metrics pulled live. '—' = metric absent or Prometheus unreachable.")


def _tab_uc3_uc4() -> None:
    st.subheader("UC3 / UC4 — artifacts on disk")
    # UC3 index
    uc3_path = Path("output/uc3/index")
    if uc3_path.exists():
        files = sorted(uc3_path.glob("*"))
        total = sum(f.stat().st_size for f in files if f.is_file())
        st.markdown(f"**UC3 index** — `{uc3_path}` · {len(files)} files · {total / 1e6:.1f} MB")
    else:
        st.info("UC3 index directory not found at `output/uc3/index/`.")

    # UC4 artifacts
    uc4_path = Path("output/uc4")
    if uc4_path.exists():
        files = sorted(uc4_path.rglob("*.parquet")) + sorted(uc4_path.rglob("*.json"))
        st.markdown(f"**UC4 artifacts** — `{uc4_path}` · {len(files)} files")
        for f in files[:20]:
            st.caption(f"- `{f.relative_to(uc4_path)}` · {f.stat().st_size / 1024:.1f} KB")
    else:
        st.info("UC4 artifacts directory not found at `output/uc4/`.")

    st.caption(
        "Reference counts (from project log): UC3 indexed 99,666 products; "
        "UC4 built at 50k orders with 49,688 products, 105 rules, 105 edges. "
        "Local disk state may differ from host where UC4 was built."
    )
