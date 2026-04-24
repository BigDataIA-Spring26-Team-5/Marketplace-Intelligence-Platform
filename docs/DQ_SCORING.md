# Data Quality Scoring — Plain-English Guide

Every row that flows through the pipeline gets a **Data Quality score** from 0 to 100. Higher is better. This document explains what that number means, how we compute it, and why the three ingredients were chosen.

If you only read one thing: **the score answers "how useful is this row to a downstream consumer?"** — a dashboard, a search index, a dietary-filter query, a recommender. It is not a correctness score; it is a usability score.

---

## The one-line summary

A row's DQ score blends three signals:

> **How complete is it? How fresh is it? How rich is its ingredients text?**

We weight completeness at 40%, freshness at 35%, and ingredient richness at 25%. Those three numbers are each between 0 and 1, we multiply by the weights, sum them, and multiply by 100 to get the final 0–100 score.

That's it. The rest of this document is the *why* behind each piece.

---

## Why these three signals?

We looked at how downstream consumers actually use the catalog and asked: what failure modes do they complain about?

1. **"Half the fields are null."** → completeness.
2. **"This product was discontinued last year."** → freshness.
3. **"The ingredients column just says 'Cheese'."** → ingredient richness.

Every other quality concern (correct allergen tags, deduplication, schema conformance) is already handled by dedicated blocks earlier in the pipeline. The DQ score captures what is left over — the soft signals that no single block can fix, but that collectively determine whether a row is worth surfacing.

### Why not a binary pass/fail?

Because the answer depends on the consumer. A row that is useless for a recommender (no ingredients) might be perfectly fine for a recall lookup (FDA product name + lot number). A continuous score lets the consumer decide their own threshold.

### Why 40 / 35 / 25?

- Completeness gets the largest weight because a mostly-null row is useless for almost every consumer. It is the most universal signal.
- Freshness is close behind because food data decays fast — reformulations, recalls, brand changes, seasonal products. Two-year-old data starts to mislead.
- Ingredient richness is smallest because it only matters to a subset of use cases (allergen extraction, dietary filters, ingredient-aware search). Rows with sparse ingredients are still usable for category-level or brand-level work.

The weights are **tunable**. Pass `{"dq_weights": {...}}` in the block config if your workload values a different mix. The defaults are a reasonable starting point, not a sacred triple.

---

## The three components, in plain English

### 1. Completeness (weight 0.40)

**"What fraction of the fields in this row are actually filled in?"**

For each row, we count how many of the reference columns have a non-null value, then divide by the number of reference columns. A row with every field filled scores 1.0. A row with half the fields filled scores 0.5. A row with nothing but an ID scores close to 0.

Not every column counts. We exclude a small list of **bookkeeping columns** that tell you nothing about data quality:

- `dq_score_pre`, `dq_score_post`, `dq_delta` — circular (the score cannot measure itself)
- `duplicate_group_id`, `canonical` — these come from the dedup step; they are pipeline bookkeeping, not source data
- `enriched_by_llm` — an internal flag tracking which rows went through the LLM
- `sizes` — derived from the product name by a later block, not a genuine source signal

Everything else counts. Product name, brand, ingredients, category, serving size, allergens, etc.

### 2. Freshness (weight 0.35)

**"How recently was this product data published?"**

We compare the row's `published_date` to today. A row published today scores 1.0. A row published two years ago or more scores 0.0. Everything in between scales linearly — a one-year-old row scores 0.5, a six-month-old row scores 0.75.

Two years is the cutoff because that is roughly the point where food catalog data becomes stale enough to mislead: products get reformulated, recalls expire, brands rename, packaging claims change.

Edge cases:

- **Date is missing or unparseable.** We fall back to 0.5 — the neutral midpoint. We do not penalize a row for a bad date (that is a schema issue, not a quality issue).
- **Date is in the future.** Clipped to today, scores 1.0. Happens occasionally with sources that use "effective date" instead of "published date."
- **The `published_date` column does not exist at all.** Every row in the batch gets freshness = 0.5.

### 3. Ingredient richness (weight 0.25)

**"How much actual ingredients text does this row have, relative to other rows in the same batch?"**

We measure the character length of the `ingredients` string for every row in the current batch, find the longest one, and score every other row as a fraction of that maximum. The row with the longest ingredients string scores 1.0. A row with half as much text scores 0.5. A null ingredients field scores 0.

This is **batch-relative** — the denominator is the longest ingredients string in the chunk being scored, not a fixed universal constant. That is deliberate. Within a single batch we want relative ranking, so that a triage queue can surface the thinnest rows first. It does mean the same row can score slightly differently in different batches, which is fine for ranking but misleading if you are comparing raw richness across runs.

Why this column specifically? Because ingredients text is the raw material for the enrichment cascade. S1 allergen extraction reads it, S2 KNN neighbor lookup embeds it, S3 LLM enrichment uses it as context. A row with a rich ingredients string is *enrichable*; a row without one is a dead end.

---

## Pre-enrichment vs. post-enrichment — the delta that actually matters

Every row gets scored **twice**: once before enrichment (`dq_score_pre`) and once after (`dq_score_post`). The difference is `dq_delta`, and it is how we measure whether enrichment actually helped.

This sounds simple but there is a trap. Enrichment *adds new columns* to the DataFrame — `primary_category`, `allergens`, `dietary_tags`, `is_organic`. If we naively score completeness over "whatever columns exist right now," the post-score will include those new columns even for rows where enrichment filled nothing. That makes completeness look artificially lower after enrichment, and `dq_delta` goes negative for rows that enrichment did not actually hurt.

**The fix: we pin the column list at the pre stage and reuse it at the post stage.** Whatever columns we scored against at pre, those exact same columns are the denominator at post. If enrichment successfully fills in a previously-null value in one of those columns, completeness goes up and delta is positive. If enrichment fills in a brand-new column that was not in the pre set, it does not distort the delta — that column is not in the comparison.

The practical invariant: **`dq_delta` should be zero or positive.** A negative delta means either (a) enrichment overwrote a non-null value with null (a bug upstream), or (b) the pinned column list got lost between pre and post (which can happen if a DataFrame operation like `pd.concat` or `reset_index` drops metadata — we have a backup mechanism for this case, and log a warning when it fires).

---

## A note on the "safety" columns

Three enrichment columns are special: `allergens`, `is_organic`, `dietary_tags`. These are **never inferred** by the similarity or LLM stages of enrichment. They are populated only by the deterministic extraction step (S1), which reads the product's own text for explicit matches.

Why? Because a false positive here is a regulatory-grade mistake. Inferring `is_organic=true` for a product that is not certified organic is a labeling violation. Telling a user with a peanut allergy that a product is peanut-free when the model merely *guessed* based on similar products is a safety issue. The rule is: if the text does not explicitly say so, the field stays null.

But null values for these columns **still count against completeness**. That is intentional. The DQ score measures usability, and a row without allergen data is less usable regardless of why it is null. We would rather a triage queue flag "half your rows have no allergen data — consider running better source extraction" than quietly let these rows score as if they were fine.

---

## Worked example — three real rows

These are actual rows from `output/gold/nutrition.parquet`, an OpenFoodFacts run from 2026-04-22.

| # | Product | Ingredients | Date | Stored pre-score |
|---|---|---|---|---|
| 0 | Premium Shrimps | (null) | unparsed | **25.00** |
| 3 | EXTRA LEAN 6 BEEF BURGERS | "Australian Beef (70%), Carrot, …" — 175 chars | unparsed | **32.44** |
| 4 | Cheddar | "Milch, Speisesalz, …" — 90 chars | unparsed | **32.07** |

At the pre stage the DataFrame still contained all the raw OFF fields (about 20 columns) before the delete-ops stripped unused ones, and `published_date` was still a unix-timestamp string that `pd.to_datetime` could not parse — so freshness fell back to 0.5 for every row.

**Row 0 walkthrough:**

- Completeness: about 4 out of 20 columns have values (product name, brand, a few others). Score: 0.188.
- Freshness: date unparseable, fallback to 0.5.
- Richness: ingredients is null, length 0, so richness is 0.
- Score = (0.188 × 0.40 + 0.500 × 0.35 + 0.000 × 0.25) × 100 = 7.5 + 17.5 + 0 = **25.00**. ✓

Row 3 has more fields populated (completeness goes up to ~0.31) and some ingredients text (richness ~0.14), which lifts it to 32.44. Row 4 sits just below at 32.07 — same completeness, slightly shorter ingredients.

None of these rows have a `dq_score_post` because this run finished at the Silver stage — enrichment did not run, so there is no post-score to compare against. For a full-mode run with a populated delta, execute:

```bash
poetry run python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
```

---

## Summary table

| Thing | Value |
|---|---|
| Score range | 0–100 |
| Completeness weight | 0.40 |
| Freshness weight | 0.35 |
| Richness weight | 0.25 |
| Freshness decay period | 730 days (2 years) |
| Freshness fallback (missing/unparsed date) | 0.5 |
| Richness baseline | batch-maximum ingredients length |
| Delta invariant | `dq_score_post >= dq_score_pre` in practice |
| Columns excluded from completeness | `dq_score_pre/post`, `dq_delta`, `duplicate_group_id`, `canonical`, `enriched_by_llm`, `sizes` |
| Safety columns (S1-only, never inferred) | `allergens`, `is_organic`, `dietary_tags` |
| Implementation file | [`src/blocks/dq_score.py`](../src/blocks/dq_score.py) |
