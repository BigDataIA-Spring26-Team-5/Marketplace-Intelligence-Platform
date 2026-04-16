"""
Entry point for the schema-driven ETL pipeline demo.

Run 1: USDA FoodData → establishes unified schema, runs full pipeline
Run 2: FDA Recalls → detects schema gaps, generates transforms, matches unified schema
Run 3 (optional): FDA Recalls again → registry hits, no Agent 2 call ("pipeline remembered")
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo")

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"


def run_pipeline(source_path: str, domain: str, run_label: str) -> dict:
    """Execute the full LangGraph pipeline for one data source."""
    from src.agents.graph import build_graph

    logger.info(f"\n{'=' * 60}")
    logger.info(f"  {run_label}")
    logger.info(f"  Source: {source_path}")
    logger.info(f"  Domain: {domain}")
    logger.info(f"{'=' * 60}\n")

    graph = build_graph()

    result = graph.invoke(
        {
            "source_path": source_path,
            "domain": domain,
            "missing_column_decisions": {},  # CLI default: accept nulls for all
        }
    )

    # Print summary
    working_df = result.get("working_df")
    rows = len(working_df) if working_df is not None else 0
    dq_pre = result.get("dq_score_pre", 0)
    dq_post = result.get("dq_score_post", 0)
    schema_existed = result.get("unified_schema_existed", False)
    gaps = result.get("gaps", [])
    registry_hits = result.get("block_registry_hits", {})
    unresolvable = result.get("unresolvable_gaps", [])
    critique_notes = result.get("critique_notes", [])
    audit_log = result.get("audit_log", [])

    logger.info(f"\n--- {run_label} Results ---")
    logger.info(f"  Rows output:          {rows}")
    logger.info(f"  DQ Score (pre):       {dq_pre}%")
    logger.info(f"  DQ Score (post):      {dq_post}%")
    logger.info(f"  DQ Delta:             {round(dq_post - dq_pre, 2)}%")
    logger.info(f"  Schema existed:       {schema_existed}")
    logger.info(f"  Gaps detected:        {len(gaps)}")
    logger.info(f"  Registry hits:        {len(registry_hits)}")
    logger.info(f"  Unresolvable gaps:    {len(unresolvable)}")
    logger.info(f"  Agent 1.5 corrections: {len(critique_notes)}")

    if critique_notes:
        logger.info(f"\n  Agent 1.5 critique notes:")
        for note in critique_notes:
            rule = note.get("rule", "?")
            col = note.get("column", "?")
            logger.info(f"    [{rule}] {col}: {note.get('correction', '')[:80]}")

    if audit_log:
        logger.info(f"\n  Block execution trace:")
        for entry in audit_log:
            block = entry.get("block", "?")
            r_in = entry.get("rows_in", "?")
            r_out = entry.get("rows_out", "?")
            logger.info(f"    {block}: {r_in} → {r_out} rows")

    return result


def main():
    """Run the full demo sequence."""
    logger.info("Schema-Driven Self-Extending ETL Pipeline — Demo")
    logger.info(f"Project root: {PROJECT_ROOT}")

    usda_path = str(DATA_DIR / "usda_fooddata_sample.csv")
    fda_path = str(DATA_DIR / "fda_recalls_sample.csv")

    # Check data files exist
    for path in [usda_path, fda_path]:
        if not Path(path).exists():
            logger.error(f"Data file not found: {path}")
            sys.exit(1)

    # ── Run 1: USDA FoodData (establishes unified schema) ──
    result_1 = run_pipeline(usda_path, "nutrition", "Run 1: USDA FoodData")

    # ── Run 2: FDA Recalls (gap detection + code generation) ──
    result_2 = run_pipeline(fda_path, "safety", "Run 2: FDA Recalls")

    # ── Run 3: FDA Recalls again (registry hits — "pipeline remembered") ──
    result_3 = run_pipeline(fda_path, "safety", "Run 3: FDA Recalls (replay)")

    # Final summary
    logger.info(f"\n{'=' * 60}")
    logger.info("  DEMO COMPLETE")
    logger.info(f"{'=' * 60}")
    logger.info(
        f"  Run 1 (USDA):  {result_1.get('dq_score_post', 0)}% DQ — schema established"
    )
    logger.info(
        f"  Run 2 (FDA):   {result_2.get('dq_score_post', 0)}% DQ — "
        f"{len(result_2.get('block_registry_hits', {}))} registry hits, "
        f"{len(result_2.get('unresolvable_gaps', []))} unresolvable"
    )
    logger.info(
        f"  Run 3 (FDA):   {result_3.get('dq_score_post', 0)}% DQ — "
        f"{len(result_3.get('block_registry_hits', {}))} registry hits, pipeline remembered"
    )


if __name__ == "__main__":
    main()
