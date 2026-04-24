"""
Build UC3 hybrid search indexes from BQ mip_gold.products.

Usage:
  python scripts/build_uc3_index.py
  python scripts/build_uc3_index.py --limit 50000
"""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT = "mip-platform-2024"
BQ_TABLE    = f"{GCP_PROJECT}.mip_gold.products"


def build(limit: int | None = None) -> int:
    from google.cloud import bigquery
    from src.uc3_search.indexer import ProductIndexer

    client = bigquery.Client(project=GCP_PROJECT)

    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT
            product_name, brand_name, primary_category, ingredients,
            allergens, dietary_tags, is_organic, dq_score_post,
            data_source, is_recalled, recall_class
        FROM `{BQ_TABLE}`
        WHERE product_name IS NOT NULL
        {limit_clause}
    """
    logger.info("Loading gold products from BQ%s...", f" (limit {limit})" if limit else "")
    df = client.query(query).to_dataframe()
    logger.info("Loaded %d rows", len(df))

    indexer = ProductIndexer()
    n = indexer.build(df)
    logger.info("UC3 index built: %d products indexed", n)
    stats = indexer.stats()
    logger.info("Stats: %s", stats)
    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build UC3 search indexes from BQ gold data")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to index (default: all)")
    args = parser.parse_args()
    n = build(limit=args.limit)
    print(f"\nIndexed {n} products into UC3 search (ChromaDB + BM25)")
