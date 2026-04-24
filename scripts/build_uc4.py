"""
Build UC4 recommender from Instacart BQ data and save to disk.

Usage:
  python scripts/build_uc4.py
  python scripts/build_uc4.py --sample-orders 100000
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PG_DSN = "host=localhost port=5432 dbname=uc2 user=mip password=mip_pass"


def _log_to_postgres(sample_orders: int, stats: dict, save_path: str) -> None:
    try:
        import psycopg2
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS uc4_build_runs (
                    id          SERIAL PRIMARY KEY,
                    run_at      TIMESTAMPTZ NOT NULL,
                    sample_orders INT,
                    products_indexed INT,
                    rules_mined INT,
                    graph_edges INT,
                    stats_json  JSONB,
                    save_path   TEXT
                )
            """)
            cur.execute("""
                INSERT INTO uc4_build_runs
                    (run_at, sample_orders, products_indexed, rules_mined, graph_edges, stats_json, save_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(timezone.utc),
                sample_orders,
                stats.get("products_indexed"),
                stats.get("rules_mined"),
                stats.get("graph_edges"),
                json.dumps(stats),
                save_path,
            ))
        logger.info("Build stats logged to postgres uc4_build_runs")
    except Exception as exc:
        logger.warning("Postgres log failed (non-fatal): %s", exc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-orders", type=int, default=50_000,
                        help="Random orders to sample from Instacart BQ (default: 50000)")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output" / "uc4",
                        help="Directory to save recommender artifacts (default: <repo>/output/uc4)")
    args = parser.parse_args()

    from src.uc4_recommendations.recommender import ProductRecommender

    logger.info("Loading Instacart data from BigQuery (sample=%d)...", args.sample_orders)
    tx_df, prod_df = ProductRecommender.load_from_bigquery(sample_orders=args.sample_orders)
    logger.info("Loaded %d transactions, %d products", len(tx_df), len(prod_df))

    rec = ProductRecommender()
    stats = rec.build(prod_df, tx_df)
    logger.info("Build complete: %s", stats)

    save_path = rec.save(directory=args.output_dir)
    logger.info("Saved to %s", save_path)
    _log_to_postgres(args.sample_orders, stats, str(save_path))
    print(f"\nUC4 recommender built and saved to {save_path}")
    print(f"Stats: {stats}")


if __name__ == "__main__":
    main()
