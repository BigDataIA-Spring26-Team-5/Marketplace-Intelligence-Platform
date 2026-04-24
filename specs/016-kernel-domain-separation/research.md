# Research: Kernel / Domain Separation

**Date**: 2026-04-24
**Feature**: 016-kernel-domain-separation

---

## Decision 1: How does the registry load custom blocks from domain packs?

**Decision**: Use `importlib.util.spec_from_file_location` to load Python files from `domain_packs/<domain>/custom_blocks/*.py` at registry init time. Name blocks with a `<domain>__<block_name>` namespace key to prevent cross-domain collisions.

**Rationale**: The registry already uses class-based discovery for kernel blocks. `importlib` is stdlib — no new dependency. The namespace prefix (`nutrition__extract_allergens`) makes the block addressable in `block_sequence.yaml` without ambiguity.

**Alternatives considered**:
- `exec()` + `globals()` — rejected, too much implicit coupling and no isolation
- Require custom blocks to be installed as pip packages — rejected, too heavy for DE workflow

---

## Decision 2: What schema format does `_get_null_rate_columns()` read?

**Decision**: Use the existing `get_domain_schema(domain)` function from `src/schema/analyzer.py` which already loads `config/schemas/<domain>_schema.json` and returns a `UnifiedSchema` object. Required columns are those where `column.required == True` in the `UnifiedSchema.columns` dict.

**Rationale**: `get_domain_schema()` is already called in the graph and has its own LRU-style cache (`_schema_cache`). No new file-reading code needed in runner.py; just one import and one call.

**Alternatives considered**:
- Read `domain_packs/<domain>/schema.json` (the new domain pack artifact) — rejected for this phase. The domain pack schema.json is the wizard-generated config artifact; the authoritative runtime schema is `config/schemas/<domain>_schema.json`. Using two different schema files for the same domain in the same run would be inconsistent. Domain pack schema.json is an input to the setup wizard, not a runtime dependency of the kernel — that coupling comes in a later phase.
- Hardcode required columns in domain pack's `block_sequence.yaml` — rejected, violates DRY.

---

## Decision 3: How does prompt injection work without breaking the Redis yaml cache?

**Decision**: The prompt template is called at node entry (`analyze_schema_node`) rather than at module import time. `build_schema_analysis_prompt(domain)` reads `domain_packs/<domain>/prompt_examples.yaml`, formats it, and returns the complete prompt string. The cache is keyed on schema fingerprint — prompt content is not part of the cache key. Cache hits skip Agent 1 entirely; when a cache hit occurs, `build_schema_analysis_prompt` is never called.

**Rationale**: Prompt content changes are low-frequency (DE sets up a domain once). Cache key stability is preserved. Existing cache entries for nutrition remain valid.

**Alternatives considered**:
- Include domain pack hash in cache key — rejected, would invalidate all existing nutrition cache entries on deploy; unnecessary given how rarely prompt examples change.
- Lazy-load at module level with `@lru_cache(domain)` — rejected, module-level state is problematic across Streamlit hot-reloads (same class identity issue as `StreamlitLogHandler`).

---

## Decision 4: What happens to `src/enrichment/deterministic.py`?

**Decision**: Keep `deterministic.py` as a **generic rule executor** — the `deterministic_enrich()` function signature is unchanged. Remove the hardcoded `CATEGORY_RULES`, `DIETARY_RULES`, and `ORGANIC_PATTERN` constants from the file. Those constants move to `domain_packs/nutrition/enrichment_rules.yaml`. The enrichment rules loader (`src/enrichment/rules_loader.py` — new small file) reads the YAML and constructs `re.Pattern` objects at load time, then passes them to `deterministic_enrich()`.

**Rationale**: `LLMEnrichBlock` calls `deterministic_enrich()` by function reference. Keeping the function signature stable means `llm_enrich.py` requires no changes. The new rules_loader is the only new file added to `src/enrichment/`.

**Alternatives considered**:
- Delete `deterministic.py` entirely and inline rule execution in the domain pack loader — rejected, loses the S1/S2/S3 dispatch boundary that the constitution enforces.
- Keep `CATEGORY_RULES` in `deterministic.py` and add nutrition-specific conditional import — rejected, keeps the food tangle in the kernel.

---

## Decision 5: `_DQ_COLS` in runner.py — also replace or leave?

**Decision**: Replace `_DQ_COLS` (used by `_compute_block_dq()`) with the same `get_domain_schema(domain)` call as `NULL_RATE_COLUMNS`. `_compute_block_dq()` becomes `_compute_block_dq(df, domain)` and derives columns from the schema.

**Rationale**: `_DQ_COLS` is a separate hardcoded food list (`["product_name", "brand_name", "primary_category", "ingredients"]`). Fixing `NULL_RATE_COLUMNS` while leaving `_DQ_COLS` hardcoded would be an incomplete separation. Both constants serve the same purpose — identify quality-significant columns — and both should come from the same source of truth.

**Domain must flow to runner**: `PipelineRunner.run_chunked()` already receives `block_sequence` from caller. The caller (`run_pipeline_node`) has `state["domain"]` available. Pass `domain` as a constructor arg to `PipelineRunner` — it is already instantiated per run in `run_pipeline_node`.

**Alternatives considered**:
- Leave `_DQ_COLS` hardcoded and only fix `NULL_RATE_COLUMNS` — rejected, leaves a visible food-column tangle in the kernel.

---

## Decision 6: Nutrition food DAGs — migrate or leave?

**Decision**: Leave `usda_dag.py`, `openfda_incremental_dag.py`, and `off_incremental_dag.py` in `airflow/dags/` for this phase. They reference `config/schemas/nutrition_schema.json` which is not being removed. DAG factory (Phase 3 of revamp.md) is explicitly out of scope.

**Rationale**: Airflow DAGs are the last tangle layer. Migrating them requires the DAG factory to be built first. Touching them now would break the Airflow scheduler without a replacement. The spec explicitly scopes this out.

---

## Decision 7: How is `domain` passed to `PipelineRunner`?

**Decision**: Add `domain: str` as a constructor parameter to `PipelineRunner.__init__`. The `run_pipeline_node` in `graph.py` already constructs `PipelineRunner(block_registry)` — update that call to `PipelineRunner(block_registry, domain=state["domain"])`. The runner stores `self.domain` and uses it in `_get_null_rate_columns()` and `_compute_block_dq()`.

**Rationale**: `PipelineRunner` already has a clean constructor injection pattern. No global state. Tests can pass any domain string.

---

## Resolved Unknowns Summary

| Question | Answer |
|----------|--------|
| How load custom blocks? | `importlib.util.spec_from_file_location`, namespaced `domain__blockname` |
| Which schema file for null-rate cols? | `config/schemas/<domain>_schema.json` via existing `get_domain_schema()` |
| Does yaml cache key change? | No — schema fingerprint only, prompt content excluded |
| What happens to `deterministic.py`? | Becomes generic executor; rules extracted to `enrichment_rules.yaml` via new `rules_loader.py` |
| Fix `_DQ_COLS` too? | Yes — same fix as `NULL_RATE_COLUMNS` |
| Migrate food DAGs? | No — out of scope for this phase |
| How is `domain` passed to runner? | Constructor arg; updated in `run_pipeline_node` |
