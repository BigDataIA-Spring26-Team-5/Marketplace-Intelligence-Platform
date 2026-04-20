# Dedup Demo Steps

This document explains the 12-step flow used in [`POC/dedup_demo.py`](../dedup_demo.py).
The Streamlit demo is an interactive walkthrough of the catalog deduplication and enrichment pipeline.

## Overview

The demo starts from raw catalog data, progressively cleans and standardizes text, identifies likely duplicate products, chooses a canonical record per duplicate cluster, and then enriches the surviving records with additional structured attributes.

## Step-by-Step

### 1. Load Raw Data

Loads the selected dataset into the demo and shows the basic schema, row counts, null counts, and sample records.

Purpose:
- Establish the raw starting point before any transformations are applied.
- Confirm which columns are available for name, brand, date, ingredients, category, and ID.
- Make the demo source-agnostic by allowing either built-in datasets or an uploaded CSV.

### 2. Identify Duplicates

Finds obvious duplicate groups in the raw data, primarily by repeated product descriptions.

Purpose:
- Show that duplicate records already exist before fuzzy matching starts.
- Surface groups where the same or nearly same product appears multiple times.
- Give a baseline for how much duplication exists in the input.

### 3. Trim Whitespace

Strips leading and trailing whitespace from text fields.

Purpose:
- Remove invisible formatting differences that prevent exact or fuzzy matches.
- Ensure values like `"Hormel Foods"` and `"Hormel Foods "` are treated as the same text.
- Standardize text before later normalization steps.

### 4. Lowercase

Converts relevant text fields to lowercase.

Purpose:
- Remove case sensitivity from matching.
- Ensure values like `"Cheerios"` and `"CHEERIOS"` normalize to the same representation.
- Make downstream cleaning and comparisons more consistent.

### 5. Remove Noise Words

Removes generic legal and business suffixes from brand names and applies alias mapping to standardize known brand variants.

Purpose:
- Strip non-identifying brand tokens such as `inc`, `llc`, `company`, and similar suffixes.
- Detect additional high-frequency dataset-specific noise words automatically.
- Collapse brand variants such as `"GENERAL MILLS SALES INC."` into a canonical brand form.

### 6. Remove Punctuation

Removes punctuation and normalizes non-alphanumeric separators.

Purpose:
- Prevent punctuation from creating false differences between otherwise identical values.
- Standardize strings such as slash-separated, hyphenated, or comma-separated names.
- Improve comparability before regex cleaning and fuzzy scoring.

### 7. Regex - Strip Sizes

Uses regex rules to remove package-size expressions from product descriptions, stores the extracted size values separately, and derives rule-based allergen indicators from ingredients.

Purpose:
- Prevent package-size variants from being treated as different products.
- Convert descriptions like `"Cereal 12 oz"` and `"Cereal 18 oz"` into the same base product name.
- Preserve useful size information in structured columns instead of losing it.
- Add a first-pass allergen signal using keyword rules.

### 8. Blocking & Fuzzy Matching

Builds candidate comparison blocks and scores likely duplicate pairs using fuzzy similarity rather than comparing every row to every other row.

Purpose:
- Reduce the number of pairwise comparisons for performance.
- Compare products within likely candidate groups instead of doing a full Cartesian search.
- Use approximate string matching on cleaned name and brand information to find non-exact duplicates.

### 9. Clustering (Union-Find)

Merges matched duplicate pairs into clusters using a union-find data structure.

Purpose:
- Support transitive duplicate grouping.
- Ensure that if record A matches B and B matches C, all three are treated as one cluster.
- Produce stable groups for canonical-record selection.

### 10. Golden Record (DQ Score)

Computes a data-quality score for records in each duplicate cluster and selects the best surviving row as the golden record.

Purpose:
- Keep the highest-quality representation of a product.
- Favor records with better completeness, freshness, and richer attributes.
- Preserve useful size and serving-size details from dropped rows and aggregate them into the winning record.

### 11. LLM Enrichment (Groq)

Applies layered optimization to minimize unnecessary LLM calls, then enriches the canonical products with additional structured attributes.

Purpose:
- Avoid expensive LLM work on rows already handled by rules, caching, deduplication, or batching.
- Generate cleaner product names and structured fields such as primary category, allergens, and organic indicators.
- Restrict enrichment to the records that survive deduplication.

### 12. Final Cleaned Data

Builds the final cleaned and enriched dataset, shows before/after comparisons, and enables CSV download.

Purpose:
- Present the end result of the full deduplication pipeline.
- Make it easy to compare raw input versus cleaned output.
- Provide a downloadable artifact for reporting, validation, or downstream use.

## Output of the Demo

By the end of the walkthrough, the demo produces a final dataset that is:

- Deduplicated at the product level.
- Standardized for names and brands.
- Augmented with extracted sizes and serving-size rollups.
- Enriched with additional structured attributes such as categories and allergens.

## Source of Truth

The current step list is defined directly in [`POC/dedup_demo.py`](../dedup_demo.py), in the `STEPS` list and the corresponding `if/elif` sections that render each page of the walkthrough.
