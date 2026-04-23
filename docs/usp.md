 # UNIQUE SELLING POINT
 
 Making it Truly Domain-Agnostic --> Means can run it for any Domain. 

  Already Generic (no changes needed)

  - Schema gap analysis — LLM maps any source → any target schema; not food-specific
  - YAML transform engine — all 8 primitives (RENAME/CAST/FORMAT/SPLIT etc.) are domain-agnostic
  - DQ scoring framework — weights + completeness/freshness math is generic
  - Silver normalization — enforces whatever schema is loaded; doesn't care about content
  - Gold concatenation — pure parquet concat; domain-scoped by string key
  - Checkpoint/resume — SQLite-backed, fully generic
  - Quarantine logic — required-field validation is schema-driven, not food-specific
  - Block registry + YAML replay — generic infrastructure

  ---
  Must Be Done Manually Per New Domain

  1. Domain schema file (config/schemas/<domain>_schema.json)
  Someone must define: what columns matter, which are required, what types, what DQ weights. This is always manual — there's no universal answer to "what makes a good
  stock price record."

  2. Enrichment blocks — hardest part
  Current S1/S2/S3 tiers are entirely food-specific:
  - extract_allergens — food safety keywords
  - KNN corpus — seeded from USDA FoodData Central
  - LLM enrichment prompt — asks about primary_category, dietary_tags, is_organic

  For a stock dataset you'd need: different S1 rules, different corpus, different LLM prompt, different enrichment columns. Full rewrite per domain.

  3. Block registry sequences
  get_default_sequence() and get_silver_sequence() in block_registry.py — currently hardcoded around food block names. Need per-domain sequence config.

  4. Agent 1 semantic mapping examples in prompts.py
  The SCHEMA_ANALYSIS_PROMPT has hardcoded semantic hints:
  "recalling_firm" / "manufacturer" → "brand_owner"
  "ingredient_statement" → "ingredients"
  These help LLM map food columns correctly. For stock data, LLM would still work but with less accuracy — no hints for ticker, OHLCV, market_cap etc.

  ---
  Could Be Partially Automated

  ┌────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────┐
  │         Thing          │                                              How                                              │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Schema file generation │ Run FIRST_RUN_SCHEMA_PROMPT on sample data → auto-derive columns → human reviews and approves │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Semantic mapping hints │ LLM could generate domain-specific hints from the schema file itself — one-time per domain    │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Block sequence         │ Make it data-driven (YAML config per domain) instead of hardcoded in Python                   │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ S1 enrichment rules    │ Config-driven keyword lists instead of food-specific code                                     │
  └────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────┘

  ---
  Realistic Assessment

  Minimum viable effort to add one new domain (e.g. stock prices):
  1. Write config/schemas/finance_equity_schema.json — ~30 min, manual
  2. Disable enrichment blocks for that domain (already possible via skip_enrichment flag)
  3. Add domain to gold_pipeline.py --domain choices — trivial
  4. Add sequence config for new domain — ~1 hr

  No enrichment for the new domain, but Silver normalization, DQ scoring, dedup, Gold concat all work.

  Full enrichment parity (S1/S2/S3 for a new domain): weeks of work per domain — corpus building, prompt engineering, safety boundary decisions.

  Honest positioning: "Domain-extensible ETL with manual schema onboarding — enrichment requires domain-specific implementation." Not plug-and-play for arbitrary input,
  but adding a new domain is bounded and well-defined work.

  ----

    UC3 HybridSearch result fields today: product_name, brand_name, primary_category, allergens — no recall awareness. UC4 ProductRecommender takes enriched_df = nutrition
  Gold only, no safety signal.

  ---
  What it should look like:

  Silver/nutrition ──┐
                     ├──→ Gold/nutrition  (LEFT JOIN safety on product_name+brand_name)
  Silver/safety  ────┘         │
                               │  adds: is_recalled, recall_class, recall_reason
                               ↓
                        UC3 HybridSearch  →  filter Class I recalls, penalize Class II
                        UC4 Recommender   →  never surface recalled products
                               │
  Silver/retail (ESCI) ────────┘  (product_id linkage for transaction-based recs)

  ---
  Gap summary — what needs building:

  ┌───────────────┬──────────────────────────────┬───────────────────────────────────────────────────────────────────────┐
  │     Layer     │           Current            │                                Missing                                │
  ├───────────────┼──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Gold pipeline │ nutrition-only dedup+enrich  │ cross-domain join with safety Silver                                  │
  ├───────────────┼──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ UC3 results   │ product_name, allergens, ... │ is_recalled, recall_class, recall_reason fields                       │
  ├───────────────┼──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ UC3 ranking   │ RRF score only               │ safety penalty: Class I → suppress, Class II → demote                 │
  ├───────────────┼──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ UC4 build()   │ nutrition enriched_df        │ recall filter before mining association rules                         │
  ├───────────────┼──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ ESCI linkage  │ None                         │ match retail.product_id → nutrition.product_name for transaction data │
  └───────────────┴──────────────────────────────┴───────────────────────────────────────────────────────────────────────┘

  OpenFDA in UC3/UC4 specifically:

  - UC3: post-filter on recall status before returning results. Already has allergens in result schema — recall_reason from OpenFDA IS allergen ground truth (more reliable
   than S1 extraction).
  - UC4: AssociationRuleMiner mines co-purchase patterns — recalled products in those patterns corrupt recommendations. Need a safety filter before mine_rules().
