# Data Model: Gold Layer Pipeline

**Created**: 2026-04-21

---

## Entities

### 1. SilverRecord

**Description**: Input record from Silver layer Parquet files.

**Fields** (from Silver Schema Contract):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| product_name | string | Yes | Product display name |
| brand_owner | string | No | Manufacturing company |
| brand_name | string | No | Consumer-facing brand |
| ingredients | string | No | Ingredients list |
| serving_size | float64 | No | Numeric serving size |
| serving_size_unit | string | No | Unit (g, ml, oz) |
| published_date | timestamp[ns] | No | Publication date |
| allergens | string | No | Allergen declarations |
| sizes | string | No | Package size info |
| data_source | string | Yes | Source identifier |
| _bronze_file | string | Yes | Original Bronze path |
| _source | string | Yes | Pipeline source ID |
| _pipeline_run_id | string | Yes | Run ID |
| dq_score_pre | float64 | Yes | Pre-enrichment DQ score |

---

### 2. DedupCluster

**Description**: Intermediate entity during deduplication stage.

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| duplicate_group_id | int | Cluster identifier |
| member_rows | list[SilverRecord] | Records in cluster |
| canonical_idx | int | Index of winning record |
| cluster_size | int | Number of records |

**State transitions**:
- Created by FuzzyDeduplicateBlock
- Consumed by ColumnWiseMergeBlock
- Collapsed to GoldenRecord by GoldenRecordSelectBlock

---

### 3. GoldenRecord

**Description**: Output entity — single deduplicated, enriched record.

**Fields**: Same as SilverRecord, plus:

| Field | Type | Description |
|-------|------|-------------|
| dq_score_post | float64 | Post-enrichment DQ score |
| _enrichment_log | string (JSON) | Provenance of enriched fields |

**Example _enrichment_log**:
```json
{
  "brand_name": {"tier": "S1", "method": "copy_from_brand_owner"},
  "serving_size_unit": {"tier": "S2", "method": "knn_neighbor", "similarity": 0.92}
}
```

---

### 4. RunLog

**Description**: Pipeline execution metadata.

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| run_id | string (UUID) | Unique run identifier |
| run_date | string (YYYY-MM-DD) | Target run date |
| timestamp | string (ISO8601) | Execution timestamp |
| silver_sources | dict | Source → {rows, path} |
| total_input_rows | int | Sum of all source rows |
| dedup.clusters_found | int | Duplicate clusters |
| dedup.golden_records | int | Records after dedup |
| dedup.dedup_ratio | float | Compression ratio |
| dedup.threshold | int | Fuzzy threshold used |
| enrichment.s1_fills | dict | Column → fill count |
| enrichment.s2_fills | dict | Column → fill count |
| enrichment.s3_fills | dict | Column → fill count |
| enrichment.s3_llm_calls | int | LLM calls made |
| enrichment.s3_cache_hits | int | Cache hits |
| dq_scores.mean_pre | float | Avg pre-enrichment |
| dq_scores.mean_post | float | Avg post-enrichment |
| dq_scores.mean_delta | float | Improvement |
| output_path | string | Gold Parquet path |
| output_rows | int | Final row count |
| duration_seconds | float | Total runtime |

---

## Relationships

```
SilverRecord (N) ──[grouped by]──▶ DedupCluster (1)
DedupCluster (1) ──[produces]──▶ GoldenRecord (1)
GoldenRecord (N) ──[logged in]──▶ RunLog (1)
```

---

## Validation Rules

1. **SilverRecord schema**: Must match contract exactly (types, columns)
2. **GoldenRecord completeness**: `product_name` and `data_source` always non-null
3. **Enrichment provenance**: Any field modified by S1/S2/S3 must have entry in `_enrichment_log`
4. **DQ score bounds**: 0.0 ≤ dq_score ≤ 1.0
5. **RunLog integrity**: `output_rows == dedup.golden_records`
