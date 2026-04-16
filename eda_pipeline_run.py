"""
EDA: USDA sample raw → unified pipeline run analysis
Run: poetry run python eda_pipeline_run.py
"""

import json
import textwrap
import pandas as pd
import numpy as np

# ── Load ─────────────────────────────────────────────────────────────────────
df_in  = pd.read_csv("data/usda_sample_raw.csv")
df_out = pd.read_csv("output/usda_sample_raw_unified.csv")
schema = json.load(open("config/unified_schema.json"))
schema_cols = set(schema["columns"].keys())

# ── 0. Column alignment ───────────────────────────────────────────────────────
extra   = sorted(set(df_out.columns) - schema_cols)
missing = sorted(schema_cols - set(df_out.columns))

print("=" * 70)
print("0. COLUMN ALIGNMENT")
print("=" * 70)
print(f"  Unified schema cols : {len(schema_cols)}")
print(f"  Output cols         : {len(df_out.columns)}")
print(f"  Extra cols (output) : {extra}")
print(f"  Missing cols        : {missing if missing else 'none — all schema cols present'}")

extra_origins = {
    "dataType":           "source pass-through (not in schema, not dropped)",
    "gtinUpc":            "source pass-through (not in schema, not dropped)",
    "foodNutrients":      "source pass-through (not in schema, not dropped)",
    "duplicate_group_id": "added by fuzzy_deduplicate block (dedup stage artifact)",
    "canonical":          "added by golden_record_select block (dedup stage artifact)",
    "sizes":              "added by extract_quantity_column block",
    "enriched_by_llm":    "added by llm_enrich block (diagnostic flag)",
}
print("\n  Extra column origins:")
for col, origin in extra_origins.items():
    print(f"    {col:<22} ← {origin}")

# ── 1. Shape & row flow ───────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("1. ROW FLOW")
print("=" * 70)
dedup_removed = 500 - 381
quarantined = 381 - len(df_out)
print(f"  Input rows            : {len(df_in)}")
print(f"  After dedup (pre-quar): 381  (removed {dedup_removed} duplicate rows, {dedup_removed/500*100:.1f}%)")
print(f"  Quarantined           : {quarantined}  (missing brand_owner — required field)")
print(f"  Final output rows     : {len(df_out)}")

# ── 2. Agent actions ──────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("2. AGENT ACTIONS (this run)")
print("=" * 70)

print("""
  AGENT 1 — Orchestrator
  ──────────────────────
  load_source_node:
    - Loaded 500 rows × 7 cols from data/usda_sample_raw.csv
    - Profiled each column (dtype, null_rate, unique_count, sample_values)

  analyze_schema_node (LLM call #1):
    - column_mapping  (3): description→product_name, publicationDate→published_date,
                           brandOwner→brand_owner
    - derivable_gaps  (1): fdcId→data_source (TYPE_CAST int64→string)
    - missing_columns (5): brand_name, ingredients, category,
                           serving_size, serving_size_unit
    - enrichment cols absent from source (handled by blocks):
      allergens, primary_category, dietary_tags, is_organic

  check_registry_node:
    - HITL excluded all 5 missing cols → set_null in YAML (not quarantine triggers)
    - TYPE_CAST gap data_source → YAML type_cast
    - Wrote DYNAMIC_MAPPING_usda_sample_raw.yaml (6 operations)
    - Registered DynamicMappingBlock
    - registry_misses = 0 DERIVE gaps → Agent 2 SKIPPED

  AGENT 2 — Code Generator
  ────────────────────────
    SKIPPED — no DERIVE gaps. All gaps handled by YAML mapping.
    (All transformations this run were declarative TYPE_CAST / set_null)

  AGENT 3 — Sequence Planner
  ──────────────────────────
    LLM call #3: planned 13-block sequence:
      dq_score_pre → __generated__ → strip_whitespace → lowercase_brand
      → remove_noise_words → strip_punctuation → extract_quantity_column
      → fuzzy_deduplicate → column_wise_merge → golden_record_select
      → extract_allergens → llm_enrich → dq_score_post

    __generated__ expanded to: DYNAMIC_MAPPING_nutrition
    (the DynamicMappingBlock written by Agent 1)
""")

# ── 3. YAML mapping summary ───────────────────────────────────────────────────
print("=" * 70)
print("3. YAML MAPPING (all transformations this run)")
print("=" * 70)
yaml_ops = [
    ("brand_name",       "set_null",   "string", "—",      "HITL: excluded"),
    ("ingredients",      "set_null",   "string", "—",      "HITL: excluded"),
    ("category",         "set_null",   "string", "—",      "HITL: excluded"),
    ("serving_size",     "set_null",   "float",  "—",      "HITL: excluded"),
    ("serving_size_unit","set_null",   "string", "—",      "HITL: excluded"),
    ("data_source",      "type_cast",  "string", "fdcId",  "int64→string"),
]
print(f"  {'target':<20} {'action':<12} {'type':<8} {'source':<10} {'note'}")
print(f"  {'-'*20} {'-'*12} {'-'*8} {'-'*10} {'-'*20}")
for row in yaml_ops:
    print(f"  {row[0]:<20} {row[1]:<12} {row[2]:<8} {row[3]:<10} {row[4]}")

print("\n  All 6 transformations = YAML declarative. Zero Python blocks generated.")

# ── 4. Null analysis on schema cols ──────────────────────────────────────────
print("\n" + "=" * 70)
print("4. NULL RATES — schema columns in output")
print("=" * 70)
schema_in_output = [c for c in sorted(schema_cols) if c in df_out.columns]
null_df = pd.DataFrame({
    "column":    schema_in_output,
    "null_count":[df_out[c].isna().sum() for c in schema_in_output],
    "null_pct":  [df_out[c].isna().mean()*100 for c in schema_in_output],
    "required":  [schema["columns"][c].get("required", False) for c in schema_in_output],
    "enrichment":[schema["columns"][c].get("enrichment", False) for c in schema_in_output],
})
null_df = null_df.sort_values("null_pct", ascending=False)
for _, row in null_df.iterrows():
    tag = " [enrichment]" if row.enrichment else (" [required]" if row.required else "")
    bar = "█" * int(row.null_pct / 5)
    print(f"  {row['column']:<22} {row.null_pct:5.1f}%  {bar}{tag}")

# ── 5. DQ score analysis ─────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("5. DQ SCORE ANALYSIS")
print("=" * 70)
# DQ scores stored as percentages (e.g. 72.86 = 72.86%)
print(f"  dq_score_pre  — mean: {df_out.dq_score_pre.mean():.1f}%  "
      f"min: {df_out.dq_score_pre.min():.1f}%  max: {df_out.dq_score_pre.max():.1f}%")
print(f"  dq_score_post — mean: {df_out.dq_score_post.mean():.1f}%  "
      f"min: {df_out.dq_score_post.min():.1f}%  max: {df_out.dq_score_post.max():.1f}%")
print(f"  dq_delta      — mean: {df_out.dq_delta.mean():.4f}  "
      f"min: {df_out.dq_delta.min():.4f}  max: {df_out.dq_delta.max():.4f}")
print(f"\n  Score distribution (pre):")
bins = [0, 40, 50, 60, 70, 80, 101]
labels = ["<40%","40-50%","50-60%","60-70%","70-80%","≥80%"]
for label, count in zip(labels, pd.cut(df_out.dq_score_pre, bins=bins).value_counts(sort=False)):
    bar = "█" * int(count / 5)
    print(f"    {label:<10} {count:>4}  {bar}")

# ── 6. Enrichment fill rates ──────────────────────────────────────────────────
print("\n" + "=" * 70)
print("6. ENRICHMENT FILL RATES")
print("=" * 70)
enrich_cols = ["allergens", "primary_category", "dietary_tags", "is_organic"]
for col in enrich_cols:
    if col in df_out.columns:
        filled = df_out[col].notna().sum()
        pct = filled / len(df_out) * 100
        bar = "█" * int(pct / 5)
        print(f"  {col:<22} {filled:>4}/{len(df_out)} filled ({pct:.1f}%)  {bar}")

print(f"\n  enriched_by_llm breakdown:")
print(df_out["enriched_by_llm"].value_counts().to_string())

# ── 7. Dedup stats ────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("7. DEDUPLICATION STATS")
print("=" * 70)
print(f"  Input rows          : 500")
print(f"  Unique clusters     : {df_out.duplicate_group_id.nunique()}")
print(f"  Rows removed        : {500 - 381} (23.8% duplicate rate)")
print(f"  Canonical=True rows : {df_out.canonical.sum()}")
cluster_sizes = df_out.groupby("duplicate_group_id").size()
print(f"  Cluster size dist   : mean={cluster_sizes.mean():.2f}  "
      f"max={cluster_sizes.max()}  singletons={( cluster_sizes==1).sum()}")

# ── 8. Quarantine analysis ────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("8. QUARANTINE")
print("=" * 70)
print("  3 rows quarantined post-enrichment validation:")
print("  - Row 16:  'DRIED CHERRIES'              — brand_owner missing")
print("  - Row 196: 'MILK COOKIES...'              — brand_owner missing")
print("  - Row 335: '1 ANGEL HAIR PASTA'           — brand_owner missing")
print("  brand_owner is required=True in unified_schema.json — correct behavior")

# ── 9. Column mismatch summary ────────────────────────────────────────────────
print("\n" + "=" * 70)
print("9. COLUMN MISMATCH ROOT CAUSES")
print("=" * 70)
print("""
  Issue: output has 23 cols, schema defines 16 cols.

  Root cause A — source pass-through (3 cols):
    dataType, gtinUpc, foodNutrients
    → PipelineRunner applies column_mapping renames but does NOT drop
      unmapped source columns. These survive untouched.

  Root cause B — block side-effects (4 cols):
    duplicate_group_id  ← fuzzy_deduplicate writes this
    canonical           ← golden_record_select writes this
    sizes               ← extract_quantity_column writes this
    enriched_by_llm     ← llm_enrich writes this (diagnostic flag)
    → Blocks add columns for their own bookkeeping/audit, never cleaned up.

  Fix options:
    A) Add a final "schema_enforce" block that selects only schema_cols
    B) PipelineRunner: drop unmapped source cols after rename
    C) Per-block: mark output cols as "internal" and strip at runner level
""")

print("=" * 70)
print("EDA COMPLETE")
print("=" * 70)
