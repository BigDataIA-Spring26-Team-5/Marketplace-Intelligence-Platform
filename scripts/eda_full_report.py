#!/usr/bin/env python3
"""EDA full report driver.

Iterates the canonical (source, date, domain) anchors defined in
docs/data_inventory.md, loads bronze/silver/gold, and dumps:

    output/eda/<source>_<date_slug>/tables.csv
    output/eda/<source>_<date_slug>/plots/*.png
    output/eda/<source>_<date_slug>/summary.json

Plus a rollup `output/eda/SUMMARY.md` linking every per-anchor directory.

Usage:
    python scripts/eda_full_report.py                # run all anchors
    python scripts/eda_full_report.py --anchors off:2026/04/22 usda:2026/04/21
    python scripts/eda_full_report.py --no-bq        # skip BigQuery Gold fetch
    python scripts/eda_full_report.py --bronze-limit 2000
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.eda.report import (  # noqa: E402
    EDAStats,
    compute_stats,
    load_bronze,
    load_gold,
    load_run_logs,
    load_silver,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("eda_cli")

OUTPUT_ROOT = Path("output/eda")

# (source, date, domain) — locked in docs/data_inventory.md §7
DEFAULT_ANCHORS: list[tuple[str, str, str]] = [
    ("usda", "2026/04/21", "nutrition"),
    ("off", "2026/04/22", "nutrition"),
    ("openfda", "2026/04/20", "safety"),
    ("esci", "2026/04/20", "retail"),
]


# ---------------------------------------------------------------------------
# Per-anchor run
# ---------------------------------------------------------------------------
def run_anchor(
    source: str,
    date: str,
    domain: str,
    bronze_limit: int,
    use_bq: bool,
    output_root: Path,
) -> dict:
    slug = f"{source}_{date.replace('/', '')}"
    out_dir = output_root / slug
    plots_dir = out_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{slug}] loading bronze…")
    bronze = load_bronze(source, date, limit=bronze_limit)

    logger.info(f"[{slug}] loading silver…")
    silver = load_silver(source, date)

    logger.info(f"[{slug}] loading gold…")
    gold = load_gold(source=None, use_bq=use_bq, limit=100_000)

    logger.info(f"[{slug}] computing stats…")
    stats = compute_stats(bronze, silver, gold, source=source, date=date)

    _dump_tables(out_dir, bronze, silver, gold)
    _dump_plots(plots_dir, silver, gold, stats)

    summary = {
        "source": source,
        "date": date,
        "domain": domain,
        **stats.as_dict(),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    logger.info(f"[{slug}] done → {out_dir}")
    return summary


# ---------------------------------------------------------------------------
# Table dumps
# ---------------------------------------------------------------------------
def _dump_tables(out_dir: Path, bronze: pd.DataFrame, silver: pd.DataFrame, gold: pd.DataFrame) -> None:
    rows = []
    for layer, df in [("bronze", bronze), ("silver", silver), ("gold", gold)]:
        rows.append({
            "layer": layer,
            "rows": len(df),
            "cols": df.shape[1] if not df.empty else 0,
            "null_total_pct": (df.isna().mean().mean() * 100) if not df.empty else 0.0,
        })
    pd.DataFrame(rows).to_csv(out_dir / "tables.csv", index=False)

    # Per-column null rates for each layer
    for layer, df in [("bronze", bronze), ("silver", silver), ("gold", gold)]:
        if df.empty:
            continue
        nulls = (df.isna().mean() * 100).round(2).reset_index()
        nulls.columns = ["column", "null_pct"]
        nulls.sort_values("null_pct", ascending=False).to_csv(
            out_dir / f"nulls_{layer}.csv", index=False
        )


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------
def _dump_plots(plots_dir: Path, silver: pd.DataFrame, gold: pd.DataFrame, stats: EDAStats) -> None:
    # DQ histogram (pre)
    src = gold if "dq_score_pre" in gold.columns and not gold.empty else silver
    if "dq_score_pre" in src.columns and src["dq_score_pre"].notna().any():
        fig, ax = plt.subplots(figsize=(6, 4))
        src["dq_score_pre"].dropna().astype(float).hist(bins=30, ax=ax, color="#4a90d9")
        ax.set_xlabel("dq_score_pre")
        ax.set_ylabel("rows")
        ax.set_title("DQ Pre-Score Distribution")
        fig.tight_layout()
        fig.savefig(plots_dir / "dq_pre_hist.png", dpi=100)
        plt.close(fig)

    # DQ delta boxplot
    if "dq_delta" in src.columns and src["dq_delta"].notna().any():
        fig, ax = plt.subplots(figsize=(4, 4))
        ax.boxplot(src["dq_delta"].dropna().astype(float), vert=True)
        ax.set_ylabel("dq_delta")
        ax.set_title("Enrichment Lift (post − pre)")
        fig.tight_layout()
        fig.savefig(plots_dir / "dq_delta_box.png", dpi=100)
        plt.close(fig)

    # Top categories bar
    if stats.top_categories:
        labels = [c[:30] for c, _ in stats.top_categories[:15]]
        values = [v for _, v in stats.top_categories[:15]]
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(labels[::-1], values[::-1], color="#88b04b")
        ax.set_xlabel("rows")
        ax.set_title("Top Categories")
        fig.tight_layout()
        fig.savefig(plots_dir / "top_categories.png", dpi=100)
        plt.close(fig)

    # Enrichment fill rates
    if stats.enrichment_fill_rate:
        keys = list(stats.enrichment_fill_rate.keys())
        vals = [stats.enrichment_fill_rate[k] * 100 for k in keys]
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(keys, vals, color="#d96a4a")
        ax.set_ylabel("% non-null")
        ax.set_ylim(0, 100)
        ax.set_title("Enrichment Fill Rate (Gold)")
        fig.tight_layout()
        fig.savefig(plots_dir / "enrichment_fill.png", dpi=100)
        plt.close(fig)

    # Null heatmap-style bar: silver top-15 null cols
    if not silver.empty:
        null_series = (silver.isna().mean() * 100).sort_values(ascending=False).head(15)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(null_series.index[::-1], null_series.values[::-1], color="#7a7a7a")
        ax.set_xlabel("% null")
        ax.set_title("Silver top-15 null columns")
        fig.tight_layout()
        fig.savefig(plots_dir / "silver_nulls.png", dpi=100)
        plt.close(fig)


# ---------------------------------------------------------------------------
# Rollup
# ---------------------------------------------------------------------------
def write_rollup(summaries: list[dict], run_logs: pd.DataFrame, out_path: Path) -> None:
    lines = [
        "# EDA Summary",
        "",
        f"Generated from `scripts/eda_full_report.py` across {len(summaries)} anchor(s).",
        "",
        "## Per-anchor rollup",
        "",
        "| Source | Date | Domain | Bronze | Silver | Gold | DQ pre (mean) | Dedup ratio |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for s in summaries:
        dq_mean = s.get("dq_pre_stats", {}).get("mean")
        dq_str = f"{dq_mean:.1f}" if isinstance(dq_mean, (int, float)) else "—"
        dedup = s.get("dedup_ratio", 0.0)
        lines.append(
            f"| {s['source']} | {s['date']} | {s['domain']} | "
            f"{s['bronze_shape'][0]:,} | {s['silver_shape'][0]:,} | {s['gold_shape'][0]:,} | "
            f"{dq_str} | {dedup:.3f} |"
        )

    lines += ["", "## Artifacts", ""]
    for s in summaries:
        slug = f"{s['source']}_{s['date'].replace('/', '')}"
        lines.append(f"- [`{slug}/`]({slug}/) — summary.json, tables.csv, plots/")

    if not run_logs.empty:
        lines += [
            "",
            "## Run-log stats",
            "",
            f"- Total runs indexed: **{len(run_logs)}**",
        ]
        if "status" in run_logs.columns:
            status_counts = run_logs["status"].value_counts().to_dict()
            lines.append(f"- Status mix: {status_counts}")
        if "duration_seconds" in run_logs.columns:
            lines.append(
                f"- Duration p50/p90/max: "
                f"{run_logs['duration_seconds'].quantile(0.5):.1f}s / "
                f"{run_logs['duration_seconds'].quantile(0.9):.1f}s / "
                f"{run_logs['duration_seconds'].max():.1f}s"
            )
        if "source_name" in run_logs.columns:
            lines.append(f"- Sources seen: {sorted(run_logs['source_name'].dropna().unique().tolist())}")

    lines += [
        "",
        "## Notes",
        "",
        "- Bronze loaders sample to keep reports fast; see `--bronze-limit`.",
        "- Gold falls back to `output/gold/*.parquet` when `--no-bq` or BQ fails.",
        "- Per-anchor `summary.json` is the machine-readable source of truth.",
    ]
    out_path.write_text("\n".join(lines) + "\n")
    logger.info(f"rollup → {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_anchors(anchor_strs: list[str]) -> list[tuple[str, str, str]]:
    out = []
    for a in anchor_strs:
        # accept "source:date" or "source:date:domain"
        parts = a.split(":")
        if len(parts) == 2:
            out.append((parts[0], parts[1], "nutrition"))
        elif len(parts) == 3:
            out.append((parts[0], parts[1], parts[2]))
        else:
            raise SystemExit(f"bad --anchors entry {a!r}; use source:date[:domain]")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Run EDA across anchor (source, date) pairs.")
    parser.add_argument("--anchors", nargs="*", default=None,
                        help="override anchors; format source:date[:domain]")
    parser.add_argument("--bronze-limit", type=int, default=5000)
    parser.add_argument("--no-bq", action="store_true", help="skip BigQuery gold fetch")
    parser.add_argument("--output-root", default="output/eda")
    args = parser.parse_args()

    anchors = _parse_anchors(args.anchors) if args.anchors else DEFAULT_ANCHORS
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    summaries: list[dict] = []
    for source, date, domain in anchors:
        try:
            summaries.append(
                run_anchor(source, date, domain, args.bronze_limit, not args.no_bq, output_root)
            )
        except Exception as exc:
            logger.error(f"[{source} {date}] failed: {exc}", exc_info=True)

    run_logs = load_run_logs(include_gcs=True)
    write_rollup(summaries, run_logs, output_root / "SUMMARY.md")

    logger.info(f"done. {len(summaries)}/{len(anchors)} anchors produced artifacts.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
