"""
MLflow Experiment Tracker — Streamlit page

Shows all historical pipeline runs logged to MLflow:
  - Experiment selector (by source)
  - DQ score trends over time
  - Cost & LLM call breakdown
  - Enrichment tier distribution (S1/S2/S3/unresolved)
  - Anomaly flag history
  - Full run comparison table

MLflow is the persistent experiment store — Prometheus is ephemeral (scrape window),
MLflow retains every run forever with full params + metrics for cross-run comparison.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

MLFLOW_TRACKING_URI = "http://localhost:5000"


@st.cache_data(ttl=30)
def _load_experiments() -> list[dict]:
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.tracking.MlflowClient()
        exps = client.search_experiments()
        return [{"id": e.experiment_id, "name": e.name} for e in exps
                if e.name != "Default"]
    except Exception as exc:
        return []


@st.cache_data(ttl=30)
def _load_runs(experiment_id: str) -> pd.DataFrame:
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.tracking.MlflowClient()
        runs = client.search_runs(
            experiment_ids=[experiment_id],
            order_by=["start_time DESC"],
            max_results=200,
        )
        rows = []
        for r in runs:
            row: dict[str, Any] = {
                "run_name":   r.data.tags.get("mlflow.runName", r.info.run_id[:8]),
                "run_id":     r.info.run_id,
                "start_time": pd.to_datetime(r.info.start_time, unit="ms"),
                "status":     r.info.status,
            }
            row.update(r.data.params)
            row.update(r.data.metrics)
            rows.append(row)
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def render_mlflow_page() -> None:
    st.title("MLflow Experiment Tracker")

    st.markdown(
        """
        **Why MLflow?** Prometheus metrics expire after the retention window.
        MLflow persists every pipeline run forever — params, DQ scores, LLM costs,
        enrichment tier counts — so you can compare runs across days/weeks, reproduce
        any result, and spot quality regressions before they reach production.
        """
    )

    # ── connection check ───────────────────────────────────────────────────────
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.tracking.MlflowClient().search_experiments()
    except Exception:
        st.error(
            f"Cannot reach MLflow at `{MLFLOW_TRACKING_URI}`. "
            "Start it with: `docker-compose -p mip up -d mlflow`"
        )
        return

    # ── experiment selector ────────────────────────────────────────────────────
    experiments = _load_experiments()
    if not experiments:
        st.warning("No experiments found. Run the backfill first:")
        st.code(
            "python3 -c \"\nimport sys; sys.path.insert(0,'.')\n"
            "from src.uc2_observability.mlflow_bridge import backfill_from_prometheus\n"
            "backfill_from_prometheus()\n\""
        )
        return

    exp_names  = [e["name"] for e in experiments]
    exp_ids    = {e["name"]: e["id"] for e in experiments}
    chosen_exp = st.selectbox("Experiment (source)", exp_names)
    exp_id     = exp_ids[chosen_exp]

    df = _load_runs(exp_id)
    if df.empty:
        st.info("No runs logged for this experiment yet.")
        return

    # Drop invalid runs silently
    for col in ("dq_score_post", "rows_in", "rows_out"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    mask = pd.Series(True, index=df.index)
    if "dq_score_post" in df.columns:
        mask &= df["dq_score_post"].fillna(0) != 0.0
    if "rows_in" in df.columns:
        mask &= df["rows_in"].fillna(0) > 0
    if {"rows_in", "rows_out"}.issubset(df.columns):
        mask &= df["rows_out"].fillna(0) <= df["rows_in"].fillna(0)
    df = df[mask].reset_index(drop=True)

    if df.empty:
        st.info("No valid runs for this experiment yet.")
        return

    st.caption(f"{len(df)} runs — refreshes every 30s")

    # ── top KPI cards ──────────────────────────────────────────────────────────
    metric_cols = ["dq_score_pre", "dq_score_post", "dq_delta",
                   "rows_in", "rows_out", "cost_usd", "anomaly_count"]
    available   = [c for c in metric_cols if c in df.columns]

    if available:
        cols = st.columns(min(4, len(available)))
        kpis = [
            ("dq_score_post", "Avg DQ Post",   "{:.2f}"),
            ("dq_delta",      "Avg DQ Delta",  "{:+.2f}"),
            ("cost_usd",      "Total Cost $",  "${:.4f}"),
            ("anomaly_count", "Total Anomalies","{:.0f}"),
        ]
        for i, (col_name, label, fmt) in enumerate(kpis):
            if col_name in df.columns:
                val = df[col_name].sum() if col_name in ("cost_usd", "anomaly_count") \
                      else df[col_name].mean()
                cols[i % 4].metric(label, fmt.format(val))

    st.divider()

    # ── DQ score trend ─────────────────────────────────────────────────────────
    dq_cols = [c for c in ("dq_score_pre", "dq_score_post") if c in df.columns]
    if dq_cols and "start_time" in df.columns:
        st.subheader("DQ Score Over Time")
        chart_df = df[["start_time"] + dq_cols].dropna().sort_values("start_time")
        chart_df = chart_df.set_index("start_time")
        st.line_chart(chart_df, height=250)

    # ── enrichment tier breakdown ──────────────────────────────────────────────
    tier_cols = [c for c in ("s1_count", "s2_count", "s3_count", "s4_unresolved") if c in df.columns]
    if tier_cols:
        st.subheader("Enrichment Tier Distribution")
        tier_totals = df[tier_cols].sum().rename({
            "s1_count":      "S1 Deterministic",
            "s2_count":      "S2 KNN",
            "s3_count":      "S3 RAG-LLM",
            "s4_unresolved": "Unresolved",
        })
        st.bar_chart(tier_totals, height=220)

    # ── cost & LLM calls ──────────────────────────────────────────────────────
    cost_cols = [c for c in ("cost_usd", "llm_calls") if c in df.columns]
    if cost_cols and "start_time" in df.columns:
        st.subheader("LLM Cost & Calls Over Time")
        cost_df = df[["start_time"] + cost_cols].dropna().sort_values("start_time").set_index("start_time")
        st.area_chart(cost_df, height=220)

    # ── anomaly flag history ──────────────────────────────────────────────────
    if "anomaly_count" in df.columns and "start_time" in df.columns:
        anomaly_runs = df[df["anomaly_count"] > 0][["start_time", "run_name", "anomaly_count", "source"]] \
            if "source" in df.columns \
            else df[df["anomaly_count"] > 0][["start_time", "run_name", "anomaly_count"]]
        if not anomaly_runs.empty:
            st.subheader(f"Anomaly Flags ({len(anomaly_runs)} runs affected)")
            st.dataframe(anomaly_runs.sort_values("start_time", ascending=False), use_container_width=True)

    # ── full run table ─────────────────────────────────────────────────────────
    st.subheader("All Runs")
    display_cols = ["run_name", "start_time", "status", "source",
                    "dq_score_pre", "dq_score_post", "dq_delta",
                    "rows_in", "rows_out", "cost_usd", "llm_calls",
                    "s1_count", "s2_count", "s3_count", "anomaly_count"]
    show_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[show_cols].sort_values("start_time", ascending=False) if "start_time" in show_cols else df[show_cols],
        use_container_width=True,
        height=400,
    )

    # ── backfill trigger ──────────────────────────────────────────────────────
    with st.expander("Backfill historical runs from Prometheus"):
        st.caption("Reads all run_ids from Prometheus and logs them to MLflow. Safe to re-run — skips already-logged runs.")
        if st.button("Run Backfill Now"):
            with st.spinner("Backfilling..."):
                try:
                    from src.uc2_observability.mlflow_bridge import backfill_from_prometheus
                    import io, contextlib
                    buf = io.StringIO()
                    with contextlib.redirect_stdout(buf):
                        backfill_from_prometheus(dry_run=False)
                    st.success("Backfill complete")
                    st.text(buf.getvalue())
                    st.cache_data.clear()
                except Exception as exc:
                    st.error(f"Backfill failed: {exc}")

    st.link_button("Open MLflow UI", MLFLOW_TRACKING_URI)
