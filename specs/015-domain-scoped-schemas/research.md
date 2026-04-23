# Research: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Feature**: 015-domain-scoped-schemas
**Date**: 2026-04-22

---

## Decision 1 — Schema Loader Strategy: Rename vs. Overload

**Decision**: Rename `get_unified_schema()` → `get_domain_schema(domain: str = "nutrition")` and update all 4 call sites. No compatibility alias.

**Rationale**: Constitution v2.0.0 Principle I explicitly retires `unified_schema.json` as a governance artifact. A compatibility alias would leave a code path that could accidentally load the wrong schema if domain is omitted. Renaming forces every call site to be explicitly domain-aware. The call sites are all local (no external consumers of this function) and the domain is available at every call site via `PipelineState["domain"]` or `config["domain"]`.

**Alternatives considered**:
- Keep `get_unified_schema()` as a thin wrapper calling `get_domain_schema("nutrition")` — rejected because it hides the domain requirement and makes future refactors harder.
- Add a `domain` param to the existing function — same as renaming with an alias; rejected for same reason.

---

## Decision 2 — Schema Cache: Singleton → Dict

**Decision**: Change `_schema_cache: UnifiedSchema | None` (analyzer.py line 18) to `_schema_cache: dict[str, UnifiedSchema] = {}`, keyed by domain string.

**Rationale**: Multiple domains may be loaded in the same process (e.g., tests run multiple domains). A singleton cache would return the first-loaded domain schema for all subsequent calls regardless of domain. The dict approach keeps the lazy-load performance benefit while being domain-correct.

**Alternatives considered**:
- Per-domain module-level variables — rejected as non-scalable (requires code change for each new domain).
- No cache (always load from disk) — rejected; disk reads add latency on every node invocation.

---

## Decision 3 — Domain Schema File Structure

**Decision**: All three domain schema files (`nutrition_schema.json`, `safety_schema.json`, `pricing_schema.json`) use the same JSON structure as the current `unified_schema.json`. `nutrition_schema.json` and `safety_schema.json` contain identical column sets to the current file. `pricing_schema.json` omits enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`).

**Rationale**: The current `unified_schema.json` was never domain-differentiated — it uses `"domain": "all"` for every column. The migration re-homes this flat schema into domain files without changing the column semantics. Domain-specific divergence (e.g., safety-specific columns like `recall_class`) is out of scope for this feature per the spec.

**Alternatives considered**:
- Derive pricing schema by dynamically excluding enrichment columns — rejected; the schema file is the explicit contract, not a derived view.
- Single shared base schema with per-domain override files — rejected; adds indirection and violates Principle IX's "one schema file per domain" rule.

---

## Decision 4 — Silver Normalization Location

**Decision**: `_silver_normalize()` is a private function added to `src/agents/graph.py`, called inside `run_pipeline_node` after `runner.run_chunked()` returns. It is NOT part of `PipelineRunner` or `BlockRegistry`.

**Rationale**: `run_pipeline_node` already has access to `domain` and `unified` (the loaded domain schema). Adding normalization here keeps it out of the block framework (satisfying FR-006/SC-006) while keeping it at the execution boundary where the schema object is readily available. Putting it in `runner.py` would require passing the full domain schema into the runner config, which already carries it — but the runner is block-execution machinery; post-execution schema enforcement is a pipeline-orchestration concern.

**Alternatives considered**:
- Add as last block in sequence (a `silver_normalize` block) — explicitly rejected by constitution Principle IX.
- Add in `save_output_node` — possible, but `run_pipeline_node` is the right seam: normalization produces the Silver DataFrame that the rest of the pipeline (including run log, quarantine logic, DQ scoring) should see in its final form.

---

## Decision 5 — dq_score_pre Recomputation After Normalization

**Decision**: After `_silver_normalize()` adds null-filled required columns, call `compute_dq_score(df, weights)` directly (imported from `src.blocks.dq_score`) and overwrite `df["dq_score_pre"]`. Do NOT re-invoke the `dq_score_pre` block through the registry.

**Rationale**: `compute_dq_score` is a pure function — it takes a DataFrame and weights dict and returns a Series. No block framework machinery needed. The `dq_score_pre` block itself just calls `compute_dq_score(df, weights)` at line 86 of `dq_score.py`. Direct invocation is equivalent and avoids running the block's `audit_entry()` bookkeeping a second time (which would double-count the block in the waterfall log).

**Condition**: Recomputation only needed when `_silver_normalize()` adds at least one null-filled column. If the DataFrame already had all schema columns, `dq_score_pre` is unchanged.

**Alternatives considered**:
- Adjust existing scores with a penalty factor — rejected; fragile, not idempotent, harder to validate.
- Skip recomputation and let downstream `dq_score_post` capture the impact — rejected; `dq_score_pre` is the pre-enrichment baseline; inflated pre-scores would make `dq_delta` appear larger than it is.

---

## Decision 6 — Silver Write and Gold Concat Location

**Decision**: Silver parquet write and Gold concatenation happen in `save_output_node` (graph.py), not in `run_pipeline_node`. A new conditional branch is added alongside the existing `pipeline_mode == "silver"` / `"full"` branches.

**Rationale**: `save_output_node` is already the write boundary — it handles GCS Silver write, local CSV write, quarantine write, and run log write. Adding Silver-local write and Gold concat here keeps all I/O in one place. `run_pipeline_node` should remain pure compute (no I/O side effects beyond the runner's chunk cache).

**Silver file naming**: `output/silver/<domain>/<source_name>.parquet` where `source_name = Path(source_path).stem` (consistent with existing `source_name` derivation at line 460 of graph.py).

**Alternatives considered**:
- Write Silver parquet inside `runner.run_chunked()` — rejected; runner is unaware of output paths.
- Separate `write_silver_node` — rejected; violates seven-node graph lock (Principle IX / FR-013).

---

## Decision 7 — `ColumnSpec` Default Value

**Decision**: Do NOT add a `default_value` field to `ColumnSpec`. Silver normalization fills missing columns with `pd.NA` unconditionally. Configured defaults are deferred to a future feature.

**Rationale**: No column in the current `unified_schema.json` has a configured default value. `ColumnSpec` already has `model_config = {"extra": "allow"}` so the JSON files can include `"default_value"` in the future without breaking parsing — the value will land in `model_extra`. Adding it to the model now would add untested code paths with no test coverage.

**Alternatives considered**:
- Add `default_value: Any = None` to `ColumnSpec` and read it in `_silver_normalize()` — rejected for this feature; adds surface area with no current use case.

---

## Call Sites to Update

| File | Line | Current call | Updated call |
|------|------|-------------|-------------|
| `src/agents/orchestrator.py` | 229 | `get_unified_schema()` | `get_domain_schema(domain)` |
| `src/agents/orchestrator.py` | 673 | `get_unified_schema()` | `get_domain_schema(domain)` |
| `src/agents/graph.py` | 86 | `get_unified_schema()` | `get_domain_schema(domain)` |
| `src/agents/graph.py` | 206 | `get_unified_schema()` | `get_domain_schema(domain)` |
| `src/schema/analyzer.py` | 355 | `derive_unified_schema_from_source(...)` | adds `domain` param to write path |

## Files Modified (complete list)

| File | Nature of change |
|------|-----------------|
| `config/schemas/nutrition_schema.json` | New — migrated from unified_schema.json |
| `config/schemas/safety_schema.json` | New — identical to nutrition for now |
| `config/schemas/pricing_schema.json` | New — nutrition minus enrichment columns |
| `src/schema/analyzer.py` | Rename function, update cache, update path |
| `src/agents/orchestrator.py` | 2 call sites updated; import updated |
| `src/agents/graph.py` | 2 call sites updated; add `_silver_normalize`; update `save_output_node` |
| `src/agents/prompts.py` | Text references "unified schema" → "domain schema" |
| `README.md` | Remove unified_schema.json references |
| `CLAUDE.md` | Remove unified_schema.json references |
