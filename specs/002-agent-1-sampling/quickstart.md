# Quickstart: Agent 1 Representative Sampling

## Overview

This enhancement improves Agent 1's schema gap detection by implementing representative row sampling before analysis. This prevents false gap classifications caused by sparse or missing values in initial rows.

## What Changed

### Before (Original Behavior)
- Agent 1 analyzed a limited number of rows (first N rows only)
- Sparse columns with values in later rows were incorrectly classified as "missing"
- No confidence scoring for gap classifications

### After (New Behavior)
- Agent 1 samples a representative subset of rows based on dataset size
- Confidence scores attached to each gap classification
- HITL Gate 1 highlights low-confidence mappings for review

## How to Use

### Running the Pipeline

1. **Via Streamlit UI**:
   ```bash
   streamlit run app.py
   ```
   - Select a data source (e.g., USDA sample CSV)
   - Proceed through wizard steps
   - In Step 1 (Schema Analysis), notice enhanced schema profile with confidence indicators

2. **Via CLI**:
   ```bash
   python demo.py
   ```
   - Runs pipeline with logging showing sampling decisions

### Understanding Confidence Scores

In the Streamlit UI, gap classifications now show confidence:

| Icon | Confidence | Meaning | Action |
|------|-------------|---------|--------|
| ✅ | High (≥90%) | Clear mapping, auto-approved | None needed |
| ⚠️ | Medium (50-90%) | Some uncertainty | Review in Gate 1 |
| ❌ | Low (<50%) | Uncertain mapping | Full scan may be needed |

### Configuration

No new configuration required. The sampling algorithm automatically adapts:
- **Datasets < 500 rows**: Full scan (100% sampling)
- **Datasets 500-10K rows**: 500-700 rows sampled
- **Datasets > 10K rows**: Capped at 700 rows

## Verification

### Test with Sparse Data

To verify the enhancement works:

1. Create a test CSV where a column has values only in rows 100-200:
   ```python
   import pandas as pd
   df = pd.DataFrame({
       'id': range(300),
       'name': ['Product ' + str(i) for i in range(300)],
       'category': [None] * 100 + ['A', 'B', 'C'] * 67 + [None] * 66
   })
   df.to_csv('test_sparse.csv', index=False)
   ```

2. Run the pipeline — `category` should now be detected as mappable, not missing.

### Check Audit Logs

Sampling decisions are logged in the pipeline audit:

```python
# In audit log:
{
    "block": "load_source",
    "sample_size": 500,
    "sample_method": "random_with_fallback",
    "fallback_triggered": false
}
```

## Troubleshooting

### High False Positives Still Occurring

If you still see false gap detections:
1. Check if the dataset has >80% null rate — fallback should trigger
2. Verify sample_size in audit log is appropriate for dataset size
3. Consider increasing minimum sample size in code

### Token Usage Higher Than Expected

If LLM token usage exceeds 2× baseline:
1. Check that sample size is capped at 700 for large datasets
2. Verify no unnecessary columns are being profiled
3. Review log for sampling method used

## Integration Notes

- Sampling happens in `load_source_node` before `analyze_schema_node`
- No changes to the 7-node pipeline flow
- YAML mapping generation unchanged
- DQ scoring unaffected