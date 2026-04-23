# Quickstart: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Feature**: 015-domain-scoped-schemas
**Date**: 2026-04-22

---

## Prerequisites

```bash
cd ~/work/NEU/SPRING_26/Big\ Data/ETL
poetry install
cp .env.example .env  # add ANTHROPIC_API_KEY, DEEPSEEK_API_KEY, GROQ_API_KEY
```

Ensure `data/usda_fooddata_sample.csv` and `data/fda_recalls_sample.csv` exist (gitignored).

---

## How to Verify This Feature

### 1. Domain schema files load correctly

```bash
# Python sanity check — should print column names without error
poetry run python -c "
from src.schema.analyzer import get_domain_schema
for domain in ['nutrition', 'safety', 'pricing']:
    s = get_domain_schema(domain)
    print(f'{domain}: {len(s.columns)} columns, required={list(s.required_columns)[:3]}...')
"
```

Expected output: 3 lines, `nutrition` and `safety` have 16 columns, `pricing` has 12.

### 2. Run nutrition pipeline — verify Silver and Gold output

```bash
poetry run python demo.py --domain nutrition
```

After run completes:
- `output/silver/nutrition/usda_fooddata_sample.parquet` exists
- `output/gold/nutrition.parquet` exists
- Check column sets match:

```bash
poetry run python -c "
import pandas as pd
silver = pd.read_parquet('output/silver/nutrition/usda_fooddata_sample.parquet')
gold = pd.read_parquet('output/gold/nutrition.parquet')
print('Silver cols:', list(silver.columns))
print('Gold cols:', list(gold.columns))
print('Cols match:', list(silver.columns) == list(gold.columns))
"
```

### 3. Run safety pipeline — verify separate Gold output

```bash
poetry run python demo.py --domain safety
```

Then confirm `output/gold/safety.parquet` exists separately from `nutrition.parquet`:

```bash
ls output/gold/
# nutrition.parquet  safety.parquet
```

### 4. Verify idempotency — re-run same source, same row count

```bash
poetry run python demo.py --domain nutrition
poetry run python demo.py --domain nutrition

poetry run python -c "
import pandas as pd
g = pd.read_parquet('output/gold/nutrition.parquet')
print('Row count after 2nd run:', len(g))
# Should equal row count after 1st run — not doubled
"
```

### 5. Verify unified_schema.json is no longer loaded

```bash
grep -rn "unified_schema.json" src/ --include="*.py"
# Should return zero matches in active code paths
```

### 6. Run tests

```bash
poetry run pytest
```

All existing tests must pass without modification.

---

## Output Paths Reference

| Artifact | Path |
|----------|------|
| Nutrition Silver | `output/silver/nutrition/<source_name>.parquet` |
| Safety Silver | `output/silver/safety/<source_name>.parquet` |
| Pricing Silver | `output/silver/pricing/<source_name>.parquet` |
| Nutrition Gold | `output/gold/nutrition.parquet` |
| Safety Gold | `output/gold/safety.parquet` |
| Pricing Gold | `output/gold/pricing.parquet` |
| Domain schemas | `config/schemas/nutrition_schema.json` etc. |

---

## Common Errors

**`FileNotFoundError: config/schemas/pricing_schema.json not found`**
→ Domain schema file missing. Create `config/schemas/pricing_schema.json` (see data-model.md).

**`KeyError: domain`** in `run_pipeline_node`
→ `PipelineState["domain"]` not set. Pass `--domain <domain>` CLI arg or set in Streamlit UI.

**Gold output missing after run**
→ Check `output/silver/<domain>/` — if empty, Silver write failed upstream. Check run log for errors.
