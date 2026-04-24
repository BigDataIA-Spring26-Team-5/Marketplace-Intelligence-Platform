"""
Seed a dedicated OFF ChromaDB corpus collection from OFF Silver Parquet.

Reads OFF silver data, extracts rows with known primary_category
(from S1 deterministic rules applied inline), embeds product names,
and upserts into ChromaDB collection "off_corpus".

Usage:
  python scripts/seed_off_corpus.py
  python scripts/seed_off_corpus.py --limit 20000
  python scripts/seed_off_corpus.py --date 2026/04/21
"""

import argparse
import hashlib
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OFF_CORPUS_COLLECTION = "off_corpus"
SILVER_PATH_TEMPLATE = "gs://mip-silver-2024/off/{date}/part_*.parquet"
DEFAULT_DATE = "2026/04/21"

# S1-style category keywords for OFF products
_CATEGORY_KEYWORDS = {
    "Snacks": ["chip", "crisp", "popcorn", "pretzel", "cracker", "snack", "puff", "rice cake"],
    "Beverages": ["juice", "drink", "water", "soda", "coffee", "tea", "beverage", "milk", "smoothie"],
    "Dairy": ["cheese", "yogurt", "butter", "cream", "dairy", "fromage", "lait"],
    "Bakery": ["bread", "biscuit", "cookie", "cake", "muffin", "pastry", "wafer", "toast"],
    "Condiments": ["sauce", "ketchup", "mustard", "vinegar", "dressing", "mayo", "condiment", "relish"],
    "Cereals": ["cereal", "granola", "oat", "muesli", "porridge", "flake"],
    "Confectionery": ["chocolate", "candy", "sweet", "gum", "lollipop", "confection", "sugar"],
    "Frozen Foods": ["frozen", "ice cream", "sorbet"],
    "Canned/Preserved": ["canned", "preserved", "pickled", "jar", "tin", "conserve"],
    "Oils & Fats": ["oil", "fat", "margarine", "lard", "shortening"],
    "Pasta & Grains": ["pasta", "noodle", "rice", "grain", "quinoa", "couscous", "flour"],
    "Meat & Fish": ["meat", "beef", "chicken", "pork", "fish", "salmon", "tuna", "sausage", "ham"],
    "Fruits & Vegetables": ["fruit", "vegetable", "tomato", "potato", "carrot", "spinach", "apple", "banana"],
    "Baby Food": ["baby", "infant", "toddler", "formula"],
    "Health & Nutrition": ["protein", "vitamin", "supplement", "nutrition", "energy bar", "protein bar"],
    "Spices & Herbs": ["spice", "herb", "pepper", "salt", "seasoning", "flavoring"],
    "Soups & Broths": ["soup", "broth", "stock", "bouillon", "bisque"],
    "Spreads": ["jam", "jelly", "spread", "peanut butter", "hazelnut", "nutella", "honey"],
}


def _infer_category(product_name: str, categories_col: str = "") -> str | None:
    """Simple keyword-based category inference for OFF products."""
    text = f"{product_name} {categories_col}".lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return None


def _make_vector_id(text: str, category: str) -> str:
    return hashlib.sha256(f"{text.strip()}{category}".encode()).hexdigest()[:16]


def seed_off_corpus(date: str = DEFAULT_DATE, limit: int | None = None) -> int:
    from sentence_transformers import SentenceTransformer
    import chromadb

    # Load OFF silver
    bucket_name = "mip-silver-2024"
    prefix = f"off/{date}/"
    logger.info(f"Reading OFF silver from gs://{bucket_name}/{prefix}part_*.parquet")
    try:
        from google.cloud import storage
        import io
        import pyarrow.parquet as pq
        import pyarrow as pa

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]
        if not blobs:
            logger.error(f"No parquet files found at gs://{bucket_name}/{prefix}")
            return 0
        logger.info(f"Found {len(blobs)} parquet file(s)")
        frames = []
        for blob in blobs:
            buf = io.BytesIO(blob.download_as_bytes())
            frames.append(pq.read_table(buf).to_pandas())
        df = pd.concat(frames, ignore_index=True)
    except Exception as e:
        logger.error(f"Failed to read OFF silver: {e}")
        return 0

    logger.info(f"Loaded {len(df)} rows from OFF silver")

    if limit:
        df = df.sample(n=min(limit, len(df)), random_state=42)
        logger.info(f"Sampled {len(df)} rows")

    # Infer primary_category from product_name + categories columns
    cat_col = next((c for c in ["categories", "category", "main_category", "pnns_groups_1"] if c in df.columns), None)
    def _safe_str(val):
        if val is None or (hasattr(val, '__class__') and val.__class__.__name__ == 'NAType'):
            return ""
        try:
            s = str(val)
            return "" if s in ("nan", "None", "<NA>") else s
        except Exception:
            return ""

    df["_inferred_category"] = df.apply(
        lambda row: _infer_category(
            _safe_str(row.get("product_name", "")),
            _safe_str(row.get(cat_col, "")) if cat_col else "",
        ),
        axis=1,
    )

    labeled = df[df["_inferred_category"].notna()].copy()
    logger.info(f"Rows with inferred category: {len(labeled)} / {len(df)} ({100*len(labeled)/len(df):.1f}%)")

    if len(labeled) < 100:
        logger.error("Too few labeled rows — check column names")
        logger.info(f"Available columns: {list(df.columns)}")
        return 0

    # Connect to ChromaDB
    try:
        import os
        client = chromadb.HttpClient(
            host=os.environ.get("CHROMA_HOST", "localhost"),
            port=int(os.environ.get("CHROMA_PORT", "8000")),
        )
        collection = client.get_or_create_collection(
            OFF_CORPUS_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )
        existing = collection.count()
        logger.info(f"Connected to ChromaDB collection '{OFF_CORPUS_COLLECTION}' ({existing} existing vectors)")
    except Exception as e:
        logger.error(f"ChromaDB connection failed: {e}")
        return 0

    # Build text representations
    def _build_text(row):
        parts = []
        for col in ["product_name", "brand_name", "ingredients", "categories", "category"]:
            val = row.get(col)
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip()[:200])
        return " ".join(parts)

    texts = labeled.apply(_build_text, axis=1).tolist()
    categories = labeled["_inferred_category"].tolist()
    product_names = labeled.get("product_name", pd.Series([""] * len(labeled))).tolist()

    # Embed
    logger.info("Loading sentence transformer model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info(f"Embedding {len(texts)} texts...")
    embeddings = model.encode(texts, batch_size=64, show_progress_bar=True).astype(np.float32)

    # Deduplicate by ID globally before upserting
    now_ts = time.time()
    seen_ids: set = set()
    dedup_ids, dedup_embeddings, dedup_metadatas = [], [], []
    total_before_dedup = len(texts)
    for i, (t, c, pn) in enumerate(zip(texts, categories, product_names)):
        vid = _make_vector_id(t, c)
        if vid not in seen_ids:
            seen_ids.add(vid)
            dedup_ids.append(vid)
            dedup_embeddings.append(embeddings[i])
            dedup_metadatas.append({"category": c, "product_name": str(pn), "last_seen": now_ts})
    logger.info(f"After dedup: {len(dedup_ids)} unique vectors (removed {total_before_dedup - len(dedup_ids)} dupes)")

    ids = dedup_ids
    embeddings = np.array(dedup_embeddings)
    metadatas = dedup_metadatas

    chunk_size = 500
    upserted = 0
    for i in range(0, len(ids), chunk_size):
        collection.upsert(
            embeddings=embeddings[i:i + chunk_size].tolist(),
            metadatas=metadatas[i:i + chunk_size],
            ids=ids[i:i + chunk_size],
        )
        upserted += len(ids[i:i + chunk_size])
        logger.info(f"Upserted {upserted}/{len(ids)} vectors...")

    final_count = collection.count()
    logger.info(f"Done. OFF corpus '{OFF_CORPUS_COLLECTION}': {existing} → {final_count} vectors")
    return upserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed OFF-dedicated ChromaDB corpus")
    parser.add_argument("--date", default=DEFAULT_DATE, help="OFF silver date YYYY/MM/DD")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process (default: all)")
    args = parser.parse_args()

    n = seed_off_corpus(date=args.date, limit=args.limit)
    print(f"\nSeeded {n} vectors into ChromaDB collection '{OFF_CORPUS_COLLECTION}'")
