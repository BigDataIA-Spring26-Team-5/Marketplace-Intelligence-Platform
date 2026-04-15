# Enrichment Layer Bug Summary

The enrichment pipeline reports "Unresolved: 415 rows" and shows zero meaningful
resolution in the UI despite the fact that `primary_category` and `is_organic`
are actually filled for every single row in the output. There are 6 interrelated
bugs, not one.

---

## Bug 1: `ingredients` Column Contains Nutrient Data, Not Ingredients

**Severity: Critical (corrupts downstream enrichment)**

The USDA source has a `foodNutrients` column containing a JSON array of nutrient
measurements (Protein, Fat, Carbs, Vitamin A, etc.). The generated
`TYPE_CONVERSION_ingredients` block parses this JSON into a flat string:

```
"Protein: 14.8G, Total lipid (fat): 12.1G, Carbohydrate, by difference: 10.2G, Vitamin A, RAE: 27.5UG, ..."
```

This is **nutritional data**, not an ingredients list. The USDA FoodData Central
`Branded` dataset does not include an ingredients text field in this export.

**Downstream damage:**

- **S1 deterministic category classification** scans `product_name + ingredients + category`.
  The word "Vitamin" appears in the nutrient string for 366/412 rows and "Protein"
  in 409/412 rows. The `Supplements` regex (`\b(supplement|vitamin|mineral|protein powder)\b`)
  matches "Vitamin" in the nutrient string, causing **148 rows (36%) to be
  misclassified as "Supplements"** when they are actually Dairy, Meat, Snacks, etc.
  This is the single largest category in the output.

- **`extract_allergens`** scans the fake ingredients for allergen keywords.
  It finds "milk" in 9 rows — but these matches come from nutrient names
  (e.g., "Calcium, Ca" doesn't match, but product names containing "cream"/"cheese"
  do). The block should be scanning actual ingredient lists for allergen detection;
  nutrient data is the wrong signal entirely.

**Location:** `src/blocks/generated/nutrition/TYPE_CONVERSION_ingredients_usda_sample_rawBlock.py`
is the generated block that creates this mapping. The root cause is in
`src/agents/code_generator.py` — the LLM was asked to map `foodNutrients` to
`ingredients` and it did its best, but these are semantically different fields.

**Fix:** The schema analysis (Agent 1) should recognize that `foodNutrients`
is a nested nutrient array and NOT map it to `ingredients`. The `ingredients`
column should be left null (data absent from source) rather than filled with
wrong data. A null ingredients column is honest; a misrepresented one silently
poisons every downstream block.

---

## Bug 2: `extract_allergens` Returns `None` for "No Allergens Found"

**Severity: High (poisons needs_enrichment mask for 97.8% of rows)**

`src/blocks/extract_allergens.py:51`:
```python
return ", ".join(sorted(found)) if found else None
```

When no allergen keywords match, the function returns `None`. This makes
"no allergens detected" indistinguishable from "not yet checked" — both are
null. Since `allergens` is one of the 4 enrichment columns, the
`needs_enrichment` row-level mask (`df[enrich_cols].isna().any(axis=1)`)
stays `True` for 403/412 rows — even after all other enrichment columns
are filled.

No strategy (S1/S2/S3) will ever fill `allergens` — it's handled exclusively
by `extract_allergens` which already ran. These 403 rows are permanently
"unresolved" by definition.

**Fix:** Return empty string `""` when no allergens are found. This means
"scanned, none detected" vs null meaning "not scanned."

---

## Bug 3: `deterministic.py` Leaves `dietary_tags` Null When No Keywords Match

**Severity: High (same mask-poisoning as Bug 2, affects 374/412 rows)**

`src/enrichment/deterministic.py:101-102`:
```python
if tags:
    df.at[idx, "dietary_tags"] = ", ".join(tags)
# else: dietary_tags stays null
```

Most USDA product names don't contain explicit dietary claims ("gluten-free",
"vegan", "keto"). For 374/412 rows, no keywords match and `dietary_tags`
stays null. Like Bug 2, no downstream strategy fills this field — it is
S1-only by design. These rows are permanently "unresolved."

**Fix:** Set `dietary_tags` to `""` when no keywords match. The field was
checked; nothing was found.

---

## Bug 4: `needs_enrichment` Mask Is Row-Level Across All Columns

**Severity: High (makes all resolution metrics meaningless)**

`src/blocks/llm_enrich.py:43`:
```python
needs_enrichment = df[enrich_cols].isna().any(axis=1)
```

This masks a row as "needs enrichment" if **any** of `[primary_category,
dietary_tags, is_organic, allergens]` is null. But each strategy only fills
specific columns:

| Strategy | Fills |
|----------|-------|
| S1 (deterministic) | primary_category, dietary_tags, is_organic |
| S2 (KNN) | primary_category only |
| S3 (RAG-LLM) | primary_category only |
| extract_allergens (separate block) | allergens only |

Because of Bugs 2 and 3, `allergens` is null for 403 rows and `dietary_tags`
is null for 374 rows after all strategies complete. The mask reports 415
"unresolved" rows even though:

- `primary_category`: **0 nulls** (fully resolved)
- `is_organic`: **0 nulls** (fully resolved)
- `dietary_tags`: 374 nulls (but S1 already checked — no tags to find)
- `allergens`: 403 nulls (but extract_allergens already scanned — none found)

The "Unresolved: 415" stat is **100% wrong**. Every row was actually processed.

Additionally, `embedding.py:49` and `llm_tier.py:85` both filter with
`needs_enrichment & df["primary_category"].isna()`. The `needs_enrichment`
dependency is unnecessary — S2 and S3 only care about `primary_category`
being null. The mask dependency means these strategies won't even attempt
rows where `needs_enrichment` is False (which won't happen due to Bugs 2/3,
but the coupling is still conceptually wrong).

---

## Bug 5: Enrichment Stats Keys Don't Match UI Component

**Severity: Medium (UI shows empty/zero bars)**

The `LLMEnrichBlock` stores stats with these keys:
```python
stats = {"s1_extraction": 0, "s2_knn": 0, "s3_rag_llm": 0, "unresolved": 0}
```

The UI component `render_enrichment_breakdown` (`src/ui/components.py:263-268`)
reads these keys:
```python
tiers = [
    ("Deterministic", stats.get("deterministic", 0), "tier-1"),
    ("Embedding", stats.get("embedding", 0), "tier-2"),
    ("Propagation", stats.get("propagation", 0), "tier-3"),
    ("LLM", stats.get("llm", 0), "tier-4"),
]
```

The keys are completely different: `s1_extraction` vs `deterministic`,
`s2_knn` vs `embedding`, `s3_rag_llm` vs `llm`. Every tier shows 0 in the
UI. Only `unresolved` matches (and displays the inflated 415 count from Bug 4).

---

## Bug 6: S1 "Resolved" Count Is Always ~0

**Severity: Medium (misleading metric)**

`src/enrichment/deterministic.py:113`:
```python
resolved = before_count - int(needs_enrichment.sum())
```

This counts rows where ALL enrichment columns became non-null. Due to Bugs 2
and 3, `allergens` and `dietary_tags` stay null for most rows even after S1
fills `primary_category` and `is_organic`. The delta is 0 or near-0, making
it look like S1 did nothing — when in reality S1 resolved `primary_category`
for ~298 rows (59.6% of input), `is_organic` for all 415 rows, and
`dietary_tags` for 38 rows.

---

## Impact Summary

| What the logs/UI report | What actually happened |
|---|---|
| S1 resolved 0 rows | S1 filled primary_category for ~298 rows, is_organic for 415, dietary_tags for 38 |
| S2 resolved 20 rows | Correct — S2 filled primary_category for 20 additional rows via KNN |
| S3 resolved 0 rows | Correct — primary_category already fully covered by S1+S2, nothing left for S3 |
| Unresolved: 415 rows | 0 rows are actually unresolved; all enrichment columns that CAN be filled ARE filled |
| UI enrichment bars: all 0% | Stats keys don't match UI keys; actual work is invisible |
| 148 rows categorized as "Supplements" | Misclassified — "Vitamin" in nutrient data triggered supplement regex |
| 9 rows have allergens detected | Partially correct, but scanning nutrient data instead of actual ingredient lists |
