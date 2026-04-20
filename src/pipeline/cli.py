"""
CLI for the ETL pipeline with checkpoint/resume support.

Usage:
    python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
    python -m src.pipeline.cli --source data/fda_recalls_sample.csv --resume
    python -m src.pipeline.cli --source data/usda_sample_raw.csv --force-fresh
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.agents.graph import build_graph
from src.pipeline.checkpoint import CheckpointManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    source_path: str,
    domain: str,
    resume: bool = False,
    force_fresh: bool = False,
    chunk_size: int = 10000,
) -> dict:
    """Execute the pipeline with checkpoint support."""
    source_file = Path(source_path)
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    checkpoint_mgr = CheckpointManager()

    if force_fresh:
        logger.info("--force-fresh: clearing all checkpoint data")
        checkpoint_mgr.force_fresh()

    run_id: Optional[str] = None

    if resume:
        checkpoint = checkpoint_mgr.get_resume_state()
        if checkpoint:
            is_valid, msg = checkpoint_mgr.validate_checkpoint(source_file)
            if is_valid:
                run_id = checkpoint["run_id"]
                logger.info(f"Resuming from checkpoint: run_id={run_id}")
                logger.info(f"Completed chunks: {len([c for c in checkpoint.get('chunks', []) if c['status'] == 'completed'])}")
            else:
                logger.warning(f"Checkpoint invalid: {msg}. Starting fresh.")
                checkpoint_mgr.force_fresh()
        else:
            logger.info("No checkpoint found, starting fresh")

    if not run_id:
        run_id = checkpoint_mgr.create(
            source_file=source_file,
            block_sequence=[],
            config={},
        )
        logger.info(f"Created new checkpoint: run_id={run_id}")

    graph = build_graph()

    result = graph.invoke({
        "source_path": source_path,
        "domain": domain,
        "missing_column_decisions": {},
        "chunk_size": chunk_size,
    })

    try:
        from src.enrichment.corpus import INDEX_PATH, META_PATH
        checkpoint_mgr.save_checkpoint(
            run_id=run_id,
            chunk_index=0,
            chunk_data={
                "record_count": len(result.get("working_df", [])),
                "dq_score_pre": result.get("dq_score_pre", 0),
                "dq_score_post": result.get("dq_score_post", 0),
            },
            plan_yaml=yaml.dump({"block_sequence": result.get("block_sequence", [])}),
            corpus_index_path=INDEX_PATH if INDEX_PATH.exists() else None,
            corpus_metadata_path=META_PATH if META_PATH.exists() else None,
        )
        logger.info("Checkpoint saved")
    except Exception as e:
        logger.warning(f"Failed to save checkpoint: {e}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline with Checkpoint Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to source CSV file",
    )
    parser.add_argument(
        "--domain",
        default="nutrition",
        choices=["nutrition", "safety", "pricing"],
        help="Domain for the pipeline (default: nutrition)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint if available",
    )
    parser.add_argument(
        "--force-fresh",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Rows per chunk for large file processing (default: 10000)",
    )

    args = parser.parse_args()

    result = run_pipeline(
        source_path=args.source,
        domain=args.domain,
        resume=args.resume,
        force_fresh=args.force_fresh,
        chunk_size=args.chunk_size,
    )

    rows = len(result.get("working_df", []))
    dq_pre = result.get("dq_score_pre", 0)
    dq_post = result.get("dq_score_post", 0)

    logger.info(f"Pipeline complete: {rows} rows, DQ: {dq_pre}% -> {dq_post}%")


if __name__ == "__main__":
    main()