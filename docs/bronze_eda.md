# Bronze Layer EDA — All Sources

**Generated:** 2026-04-22  
**Project:** mip-platform-2024  
**Dataset:** `bronze_raw`  
**Sources:** Open Food Facts (OFF), USDA FoodData Central (Branded + Foundation), OpenFDA Food Enforcement

---

## 1. Open Food Facts (OFF)

**Table:** `bronze_raw.off`  
**Rows:** 1,000,000

### 1.1 Schema (Bronze)

| Bronze Column | Type | Maps To (Unified) | Notes |
|---|---|---|---|
| `product_name` | RECORD REPEATED | `product_name` | Array of `{lang, text}` objects — extract `key: 'text'` |
| `brands` | STRING | `brand_name` | Comma-separated brand names |
| `ingredients_text` | RECORD REPEATED | `ingredients` | Array of `{lang, text}` objects — extract `key: 'text'` |
| `categories` | STRING | `category` | |
| `allergens` | STRING | `allergens` | Top-level string (100% null — stored in `nutriments`) |
| `serving_size` | STRING | `serving_size` + `serving_size_unit` | e.g. "28 g" — regex split required |
| `last_modified_t` | INTEGER | `published_date` | Unix timestamp → datetime |
| `code` | FLOAT | ❌ DROPPED | Barcode — not in unified schema |
| `energy_100g` | STRING | ❌ DROPPED | 100% null — must be extracted from `nutriments.energy-kcal_100g` |
| `proteins_100g` | STRING | ❌ DROPPED | 100% null — must be from `nutriments.proteins_100g` |
| `fat_100g` | STRING | ❌ DROPPED | 100% null — must be from `nutriments.fat_100g` |
| `carbohydrates_100g` | STRING | ❌ DROPPED | 100% null — must be from `nutriments.carbohydrates_100g` |
| `salt_100g` | STRING | ❌ DROPPED | 100% null — must be from `nutriments.salt_100g` |
| `nova_group` | INTEGER | ❌ DROPPED | Processing level 1–4 |
| `nutriscore_grade` | STRING | ❌ DROPPED | A–E grade |
| `traces` | STRING | ❌ DROPPED | 100% null in bronze |
| `labels` | STRING | ❌ DROPPED | |
| `countries` | STRING | ❌ DROPPED | 100% null in bronze |
| `data_quality_tags` | STRING | ❌ DROPPED | |
| `pnns_groups_1/2` | STRING | ❌ DROPPED | |
| — | — | `brand_owner` | Set to null (not in OFF) |
| — | — | `data_source` | Set to `"OpenFoodFacts"` |

### 1.2 Null Rate Analysis

| Column | Null Count | Null % | Impact on Unified Schema |
|---|---|---|---|
| `product_name` (empty array) | 0 | **0%** | ✅ All rows have a name |
| `code` | 0 | **0%** | N/A — dropped |
| `last_modified_t` | 0 | **0%** | ✅ `published_date` fully populated |
| `nutriscore_grade` | 6 | **~0%** | N/A — dropped |
| `brands` | 183,018 | **18.3%** | `brand_name` 18.3% null in silver |
| `labels` | 382,795 | **38.3%** | N/A — dropped |
| `nova_group` | 499,806 | **50.0%** | N/A — dropped |
| `ingredients_text` (empty) | 459,647 | **46.0%** | `ingredients` 46% null in silver |
| `serving_size` | 676,470 | **67.6%** | `serving_size` / `serving_size_unit` 67.6% null |
| `categories` | 227,707 | **22.8%** | `category` 22.8% null in silver |
| `allergens` | 1,000,000 | **100%** | `allergens` null from bronze — enrichment only |
| `energy_100g` | 1,000,000 | **100%** | Nested in `nutriments{}` — fix applied to DAG |
| `proteins_100g` | 1,000,000 | **100%** | Nested in `nutriments{}` — fix applied to DAG |
| `fat_100g` | 1,000,000 | **100%** | Nested in `nutriments{}` — fix applied to DAG |
| `carbohydrates_100g` | 1,000,000 | **100%** | Nested in `nutriments{}` — fix applied to DAG |
| `traces` | 1,000,000 | **100%** | N/A — dropped |
| `countries` | 1,000,000 | **100%** | N/A — dropped |

### 1.3 Distribution Stats

| Metric | Value |
|---|---|
| Total rows | 1,000,000 |
| Rows with valid product name | 999,999 (1 blank-text row) |
| Multi-language product names | 995,864 (99.6%) |
| Median language count per name | 2 |
| Distinct brands | ~145,240 |
| Distinct categories | ~144,454 |
| Date range (unix ts) | 2013-01-07 → 2026-04-18 |
| NOVA 1 (unprocessed) | 60,566 (6.1%) |
| NOVA 2 (processed ingredient) | 24,367 (2.4%) |
| NOVA 3 (processed) | 105,300 (10.5%) |
| NOVA 4 (ultra-processed) | 309,961 (31.0%) |
| NOVA unknown | 500,194 (50.0%) |

### 1.4 Bronze → Silver Transformation

| Silver Column | Source | Action | Expected Null % |
|---|---|---|---|
| `product_name` | `product_name[*].text` | `extract_json_field key=text` | ~0% |
| `brand_name` | `brands` | `strip_whitespace` | ~18% |
| `brand_owner` | — | `set_null` | 100% |
| `ingredients` | `ingredients_text[*].text` | `extract_json_field key=text` | ~46% |
| `category` | `categories` | LLM mapping | ~23% |
| `serving_size` | `serving_size` | `regex_extract` float | ~68% |
| `serving_size_unit` | `serving_size` | `regex_extract` unit | ~68% |
| `published_date` | `last_modified_t` | `parse_date unix_timestamp` | ~0% |
| `data_source` | — | `set_default "OpenFoodFacts"` | 0% |
| `allergens` | — | null (gold enrichment) | 100% |
| `primary_category` | — | null (gold enrichment) | 100% |
| `dietary_tags` | — | null (gold enrichment) | 100% |
| `is_organic` | — | null (gold enrichment) | 100% |
| `dq_score_pre` | all columns | computed | 0% |

---

## 2. USDA FoodData Central — Branded

**Table:** `bronze_raw.usda_branded`  
**Rows:** 432,706  
**Data Type:** Branded Foods only

### 2.1 Schema (Bronze)

| Bronze Column | Type | Maps To (Unified) | Notes |
|---|---|---|---|
| `description` | STRING | `product_name` | Full product description |
| `brandName` | STRING | `brand_name` | |
| `brandOwner` | STRING | `brand_owner` | |
| `ingredients` | STRING | `ingredients` | Comma-separated list |
| `brandedFoodCategory` | STRING | `category` | |
| `servingSize` | FLOAT | `serving_size` | |
| `servingSizeUnit` | STRING | `serving_size_unit` | Needs value mapping (GRM→g) |
| `modifiedDate` | DATE | `published_date` | |
| `labelNutrients` | RECORD NULLABLE | ❌ DROPPED | Nested: calories, protein, fat, carbs, sodium, etc. |
| `foodNutrients` | RECORD REPEATED | ❌ DROPPED | Detailed nutrient records |
| `gtinUpc` | INTEGER | ❌ DROPPED | Barcode |
| `foodUpdateLog` | RECORD REPEATED | ❌ DROPPED | |
| `tradeChannels` | STRING REPEATED | ❌ DROPPED | |
| `fdcId` | INTEGER | ❌ DROPPED | Internal USDA ID |
| `dataType` | STRING | ❌ DROPPED | Always "Branded" |
| All others | — | ❌ DROPPED | `subbrandName`, `gpcClassCode`, `preparationStateCode`, etc. |
| — | — | `allergens` | Null (enrichment only) |
| — | — | `data_source` | Set to `"USDA"` |

### 2.2 Null Rate Analysis

| Column | Null Count | Null % | Impact on Unified Schema |
|---|---|---|---|
| `description` | 0 | **0%** | ✅ `product_name` fully populated |
| `brandOwner` | 0 | **0%** | ✅ `brand_owner` fully populated |
| `ingredients` | 0 | **0%** | ✅ `ingredients` fully populated |
| `servingSize` | 0 | **0%** | ✅ `serving_size` fully populated |
| `servingSizeUnit` | 0 | **0%** | ✅ `serving_size_unit` fully populated |
| `modifiedDate` | 0 | **0%** | ✅ `published_date` fully populated |
| `publicationDate` | 0 | **0%** | N/A |
| `gtinUpc` | 0 | **0%** | N/A — dropped |
| `brandName` | 11,583 | **2.7%** | `brand_name` 2.7% null in silver |
| `brandedFoodCategory` | 22 | **~0%** | `category` ~0% null |
| `packageWeight` | 28,374 | **6.6%** | N/A — dropped |
| `labelNutrients` | 0 | **0%** | N/A — dropped (all have nutrient data) |

### 2.3 Distribution Stats

| Metric | Value |
|---|---|
| Total rows | 432,706 |
| Distinct brand names | ~34,200 |
| Distinct brand owners | ~22,011 |
| Distinct categories | 339 |
| Date range | 2014-04-01 → 2025-12-01 |
| Rows with calories | 427,812 (98.9%) |
| Rows with protein | 428,654 (99.1%) |
| Rows with fat | 428,626 (99.1%) |
| Rows with sodium | 428,028 (99.0%) |

### 2.4 Bronze → Silver Transformation

| Silver Column | Source | Action | Expected Null % |
|---|---|---|---|
| `product_name` | `description` | `strip_whitespace` | ~0% |
| `brand_name` | `brandName` | `strip_whitespace` | ~2.7% |
| `brand_owner` | `brandOwner` | `strip_whitespace` | ~0% |
| `ingredients` | `ingredients` | passthrough | ~0% |
| `category` | `brandedFoodCategory` | `strip_whitespace` | ~0% |
| `serving_size` | `servingSize` | `type_cast float` | ~0% |
| `serving_size_unit` | `servingSizeUnit` | `value_map GRM→g` | ~0% |
| `published_date` | `modifiedDate` | `parse_date` | ~0% |
| `data_source` | — | `set_default "USDA"` | 0% |
| `allergens` | — | null (gold enrichment) | 100% |
| `primary_category` | — | null (gold enrichment) | 100% |
| `dietary_tags` | — | null (gold enrichment) | 100% |
| `is_organic` | — | null (gold enrichment) | 100% |
| `dq_score_pre` | all columns | computed | 0% |

---

## 3. USDA FoodData Central — Foundation

**Table:** `bronze_raw.usda_foundation`  
**Rows:** 365

### 3.1 Schema (Bronze)

| Bronze Column | Type | Maps To (Unified) | Notes |
|---|---|---|---|
| `description` | STRING | `product_name` | e.g. "Butter, salted" |
| `foodCategory` | RECORD NULLABLE | `category` | Nested `{description, id}` — extract `.description` |
| `dataType` | STRING | ❌ DROPPED | Always "Foundation" |
| `fdcId` | INTEGER | ❌ DROPPED | |
| `foodNutrients` | RECORD REPEATED | ❌ DROPPED | ~30 nutrients per row |
| `publicationDate` | DATE | `published_date` | |
| `scientificName` | STRING | ❌ DROPPED | Latin species name |
| `ndbNumber` | INTEGER | ❌ DROPPED | Legacy NDB ID |
| All others | — | ❌ DROPPED | |

### 3.2 Null Rate Analysis

| Column | Null Count | Null % | Impact on Unified Schema |
|---|---|---|---|
| `description` | 0 | **0%** | ✅ `product_name` fully populated |
| `dataType` | 0 | **0%** | N/A — dropped |
| `publicationDate` | 0 | **0%** | ✅ `published_date` fully populated |
| `foodNutrients` (empty) | 0 | **0%** | N/A — all have nutrients |
| `foodCategory` | 0 | **0%** | ✅ `category` fully populated |
| `scientificName` | 342 | **93.7%** | N/A — dropped |

### 3.3 Distribution Stats

| Metric | Value |
|---|---|
| Total rows | 365 |
| Distinct products | 365 (all unique) |
| Rows with scientific name | 23 (6.3%) |
| Notable: no brand/ingredients fields | Foundation foods are reference nutrients only |

### 3.4 Bronze → Silver Transformation

| Silver Column | Source | Action | Expected Null % |
|---|---|---|---|
| `product_name` | `description` | `strip_whitespace` | ~0% |
| `brand_name` | — | `set_null` | 100% — Foundation foods have no brand |
| `brand_owner` | — | `set_null` | 100% |
| `ingredients` | — | `set_null` | 100% — raw nutrients, no ingredient list |
| `category` | `foodCategory.description` | nested extract | ~0% |
| `serving_size` | — | `set_null` | 100% |
| `serving_size_unit` | — | `set_null` | 100% |
| `published_date` | `publicationDate` | `parse_date` | ~0% |
| `data_source` | — | `set_default "USDA"` | 0% |
| `allergens` | — | null (gold enrichment) | 100% |
| `primary_category` | — | null (gold enrichment) | 100% |
| `dietary_tags` | — | null (gold enrichment) | 100% |
| `is_organic` | — | null (gold enrichment) | 100% |
| `dq_score_pre` | all columns | computed | 0% |

> **Note:** Foundation foods will have low DQ scores (~40%) due to null brand/ingredients/serving fields by design — these are reference nutritional standards, not consumer products.

---

## 4. OpenFDA Food Enforcement

**Table:** `bronze_raw.openfda`  
**Rows:** 25,100

### 4.1 Schema (Bronze)

| Bronze Column | Type | Maps To (Unified) | Notes |
|---|---|---|---|
| `product_description` | STRING | `product_name` | Full product + lot/size description |
| `recalling_firm` | STRING | `brand_owner` | Company issuing the recall |
| `product_type` | STRING | `category` | Always "Food" in this dataset |
| `reason_for_recall` | STRING | `allergens` | Safety signal — undeclared allergens, contamination |
| `recall_initiation_date` | INTEGER | `published_date` | Coalesce with center_classification_date, report_date |
| `recall_number` | STRING | ❌ DROPPED | |
| `classification` | STRING | ❌ DROPPED | Class I/II/III |
| `status` | STRING | ❌ DROPPED | Ongoing/Terminated |
| `voluntary_mandated` | STRING | ❌ DROPPED | |
| `distribution_pattern` | STRING | ❌ DROPPED | |
| `city`, `state`, `country` | STRING | ❌ DROPPED | |
| `code_info`, `more_code_info` | STRING | ❌ DROPPED | |
| `openfda` | RECORD NULLABLE | ❌ DROPPED | Always `{}` (empty) |
| — | — | `brand_name` | Set to null (not in FDA) |
| — | — | `ingredients` | Set to null (not in FDA) |
| — | — | `serving_size` | Set to null (not in FDA) |
| — | — | `data_source` | Set to `"FDA"` |

### 4.2 Null Rate Analysis

| Column | Null Count | Null % | Impact on Unified Schema |
|---|---|---|---|
| `product_description` | 0 | **0%** | ✅ `product_name` fully populated |
| `recalling_firm` | 0 | **0%** | ✅ `brand_owner` fully populated |
| `reason_for_recall` | 0 | **0%** | ✅ `allergens` populated (safety signal) |
| `classification` | 0 | **0%** | N/A — dropped |
| `product_type` | 0 | **0%** | ✅ `category` fully populated |
| `recall_initiation_date` | 0 | **0%** | ✅ `published_date` fully populated |
| `status` | 0 | **0%** | N/A — dropped |
| `voluntary_mandated` | 0 | **0%** | N/A — dropped |
| `distribution_pattern` | 0 | **0%** | N/A — dropped |
| `recall_number` | 0 | **0%** | N/A — dropped |
| `city`, `state`, `country` | 0 | **0%** | N/A — dropped |
| `code_info` | 0 | **0%** | N/A — dropped |
| `product_quantity` | 0 | **0%** | N/A — dropped |

> **Note:** FDA bronze is remarkably clean — 0% null across all columns.

### 4.3 Distribution Stats

| Metric | Value |
|---|---|
| Total rows | 25,100 |
| Distinct recalling firms | ~5,030 |
| Distinct product types | 1 (always "Food") |
| Date range | 2012-01-13 → 2026-04-02 |
| Class I (most severe — health risk) | 11,048 (44.0%) |
| Class II (may cause adverse health) | 12,600 (50.2%) |
| Class III (unlikely to cause harm) | 1,451 (5.8%) |

### 4.4 Bronze → Silver Transformation

| Silver Column | Source | Action | Expected Null % |
|---|---|---|---|
| `product_name` | `product_description` | `strip_whitespace` | ~0% |
| `brand_name` | — | `set_null` | 100% |
| `brand_owner` | `recalling_firm` | `strip_whitespace` | ~0% |
| `ingredients` | — | `set_null` | 100% |
| `category` | `product_type` | `to_lowercase` | ~0% |
| `serving_size` | — | `set_null` | 100% |
| `serving_size_unit` | — | `set_null` | 100% |
| `published_date` | `recall_initiation_date` | `coalesce + format_transform` | ~0% |
| `data_source` | — | `set_default "FDA"` | 0% |
| `allergens` | `reason_for_recall` | `strip_whitespace` | ~0% |
| `primary_category` | — | null (gold enrichment) | 100% |
| `dietary_tags` | — | null (gold enrichment) | 100% |
| `is_organic` | — | null (gold enrichment) | 100% |
| `dq_score_pre` | all columns | computed | 0% |

> **Note:** FDA silver will have low DQ scores (~40–50%) because brand_name, ingredients, serving_size are structurally absent — this is expected. Gold enrichment will not improve these as per the safety boundary rule.

---

## 5. Cross-Source Unified Schema Coverage

> **Column count:** unified_schema.json defines **16 columns**.  
> **Silver layer outputs 14** — `dq_score_post` and `dq_delta` are computed only in the gold pipeline.  
> **`SchemaEnforceBlock` guarantees all 14 silver columns are present** for every source (missing ones filled with typed null).

### Silver Layer (14 columns)

| # | Unified Column | Type | OFF Silver | USDA Branded Silver | USDA Foundation Silver | OpenFDA Silver |
|---|---|---|---|---|---|---|
| 1 | `product_name` | string | ✅ ~0% null | ✅ ~0% null | ✅ ~0% null | ✅ ~0% null |
| 2 | `brand_owner` | string | ⬜ 100% null | ✅ ~0% null | ⬜ 100% null | ✅ ~0% null |
| 3 | `brand_name` | string | 🟡 ~18% null | 🟢 ~3% null | ⬜ 100% null | ⬜ 100% null |
| 4 | `ingredients` | string | 🟡 ~46% null | ✅ ~0% null | ⬜ 100% null | ⬜ 100% null |
| 5 | `category` | string | 🟡 ~23% null | ✅ ~0% null | ✅ ~0% null | ✅ "food" |
| 6 | `serving_size` | float | 🟡 ~68% null | ✅ ~0% null | ⬜ 100% null | ⬜ 100% null |
| 7 | `serving_size_unit` | string | 🟡 ~68% null | ✅ ~0% null | ⬜ 100% null | ⬜ 100% null |
| 8 | `published_date` | string | ✅ ~0% null | ✅ ~0% null | ✅ ~0% null | ✅ ~0% null |
| 9 | `data_source` | string | ✅ "OpenFoodFacts" | ✅ "USDA" | ✅ "USDA" | ✅ "FDA" |
| 10 | `allergens` | string | ⬜ null* | ⬜ null* | ⬜ null* | ✅ from reason_for_recall |
| 11 | `primary_category` | string | ⬜ null* | ⬜ null* | ⬜ null* | ⬜ null* |
| 12 | `dietary_tags` | string | ⬜ null* | ⬜ null* | ⬜ null* | ⬜ null* |
| 13 | `is_organic` | boolean | ⬜ null* | ⬜ null* | ⬜ null* | ⬜ null* |
| 14 | `dq_score_pre` | float | ✅ computed | ✅ computed | ✅ computed | ✅ computed |

*Enrichment columns — populated by `extract_allergens` + `llm_enrich` in the **gold pipeline**, not silver.

### Gold Layer adds 2 more columns (total 16)

| # | Column | Type | Source |
|---|---|---|---|
| 15 | `dq_score_post` | float | Computed after gold enrichment |
| 16 | `dq_delta` | float | `dq_score_post - dq_score_pre` |

**Legend:** ✅ Fully populated | 🟢 >95% | 🟡 Partial | ⬜ Null (by design or enrichment-only)

---

## 6. Known Issues & Fixes Applied

| Issue | Source | Status |
|---|---|---|
| `product_name` extracted as raw JSON array `[{lang,text}]` | OFF | ✅ Fixed — `key: 'text'` in DYNAMIC_MAPPING |
| `ingredients_text` extracted as raw JSON array | OFF | ✅ Fixed — `key: 'text'` in DYNAMIC_MAPPING |
| Nutritional fields (proteins, energy, fat, etc.) 100% null | OFF | ✅ Fixed — extract from `nutriments{}` in ingestion DAG |
| USDA mapping set ingredients/brand_name/category to null | USDA | ✅ Fixed — DYNAMIC_MAPPING rewritten to use actual field names |
| `product_name` mapped from wrong field (`product_name` vs `product_description`) | OpenFDA | ✅ Fixed — `strip_whitespace from product_description` |
| Silver output included extra bronze columns not in unified schema | ALL | ✅ Fixed — `SchemaEnforceBlock` added as final silver step |
| All DYNAMIC_MAPPING YAMLs in same domain overwrote each other | ALL | ✅ Fixed — block name now uses yaml file stem |
| `resolved_source_name` used `_blob_parts[-2]` → wrong GCS silver path | OFF | ✅ Fixed — `_blob_parts[0]` |
| Nutritional fields still null for existing 1M OFF bronze rows | OFF | ⚠️ Requires re-ingestion from OFF full dump with nutriments fix |
| USDA Foundation has no brand/ingredients fields by design | Foundation | ℹ️ Expected — low DQ score is correct behaviour |
