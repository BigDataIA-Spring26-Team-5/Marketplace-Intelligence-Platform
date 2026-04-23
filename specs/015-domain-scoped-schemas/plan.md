# Implementation Plan: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Branch**: `aqeel` | **Date**: 2026-04-22 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/015-domain-scoped-schemas/spec.md`

---

## Summary

Retire `config/unified_schema.json` as the singleton schema contract. Replace with per-domain files at `config/schemas/<domain>_schema.json`. Update `get_unified_schema()` → `get_domain_schema(domain)` across 4 call sites. Add Silver normalization (post-block DataFrame enforcement) and Gold concatenation (full domain rebuild from Silver Parquet Store) as internal steps in `run_pipeline_node` / `save_output_node` — no new graph nodes.

---

## Technical Context

**Language/Version**: Python 3.11
**Primary Dependencies**: pandas, Pydantic v2, LangGraph, LiteLLM, pyarrow (for parquet write)
**Storage**: Local filesystem (`output/silver/<domain>/`, `output/gold/`) for Silver and Gold output; GCS for the silver-mode pipeline branch (unchanged)
**Testing**: pytest
**Target Platform**: Linux (Fedora), local dev
**Project Type**: CLI + Streamlit pipeline
**Performance Goals**: Silver normalization adds <50ms per run (pure DataFrame column reorder); Gold concat scales with Silver store size (I/O bound)
**Constraints**: Seven-node graph order locked; no new BlockRegistry blocks; `unified_schema.json` must not be loaded after this feature ships
**Scale/Scope**: 3 domain schema files; 4 call sites updated; 2 new I/O steps in save_output_node

---

## Constitution Check

*Constitution v2.0.0 gates — verified pre-design.*

| Gate | Status | Notes |
|------|--------|-------|
| Domain-schema alignment documented | ✓ PASS | `config/schemas/<domain>_schema.json` is the explicit schema contract; `unified_schema.json` retired |
| Agent responsibilities within three-agent architecture | ✓ PASS | No agent role changes; Agent 1 prompt updated to reference domain schema, not its logic |
| Transformations use declarative YAML, not runtime Python | ✓ PASS | Silver normalization is not an Agent 2 output; it is fixed deterministic Python inside `run_pipeline_node` |
| HITL approval points unchanged | ✓ PASS | Gate 1 (schema mapping review) and Gate 2 (quarantine review) are unchanged |
| Enrichment safety fields deterministic-only | ✓ PASS | `_silver_normalize()` preserves enrichment columns from block output; does not infer them |
| DQ scoring intact | ✓ PASS | `dq_score_pre` recomputed post-normalization using same `compute_dq_score()` function |
| Generated mapping persistence unchanged | ✓ PASS | YAML write path (`write_mapping_yaml(domain, ...)`) is unchanged |
| Silver normalization NOT a registered block | ✓ PASS | `_silver_normalize()` is a private function in graph.py, not in BlockRegistry |
| Gold concatenation NOT a registered block | ✓ PASS | Gold concat is inline code in `save_output_node`, not in BlockRegistry |
| Seven-node graph order locked | ✓ PASS | No new graph nodes; Silver norm + Gold concat are internal to existing nodes |

---

## Project Structure

### Documentation (this feature)

```text
specs/015-domain-scoped-schemas/
├── plan.md              ← this file
├── research.md          ← decisions + alternatives
├── data-model.md        ← schema structure, Silver/Gold entity definitions
├── quickstart.md        ← verification commands
├── spec.md              ← feature specification
└── checklists/
    └── requirements.md
```

### Source Code Changes

```text
config/
├── schemas/                          ← NEW directory
│   ├── nutrition_schema.json         ← NEW (migrated from unified_schema.json)
│   ├── safety_schema.json            ← NEW (same as nutrition for now)
│   └── pricing_schema.json           ← NEW (nutrition minus enrichment columns)
└── unified_schema.json               ← retained on disk, no longer loaded

src/
├── schema/
│   └── analyzer.py                   ← MODIFIED: get_domain_schema(), SCHEMAS_DIR, cache refactor
├── agents/
│   ├── orchestrator.py               ← MODIFIED: 2 call sites + import
│   ├── graph.py                      ← MODIFIED: 2 call sites + _silver_normalize() + save_output_node
│   └── prompts.py                    ← MODIFIED: text references "unified schema" → "domain schema"

README.md                             ← MODIFIED: remove unified_schema.json references
CLAUDE.md                             ← MODIFIED: remove unified_schema.json references
```

---

## Implementation Phases

### Phase A — Domain Schema Files

**Files**: `config/schemas/nutrition_schema.json`, `config/schemas/safety_schema.json`, `config/schemas/pricing_schema.json`

**Steps**:
1. Create `config/schemas/` directory.
2. Write `nutrition_schema.json` — exact copy of `config/unified_schema.json` (all 16 columns + dq_weights).
3. Write `safety_schema.json` — identical to `nutrition_schema.json`.
4. Write `pricing_schema.json` — 12 columns: remove `allergens`, `primary_category`, `dietary_tags`, `is_organic` (the 4 enrichment columns).

**Validation**: `poetry run python -c "from src.schema.analyzer import get_domain_schema; [get_domain_schema(d) for d in ['nutrition','safety','pricing']]"` must not raise.

---

### Phase B — Schema Loader Refactor (`src/schema/analyzer.py`)

**Steps**:

1. Replace `UNIFIED_SCHEMA_PATH = CONFIG_DIR / "unified_schema.json"` with `SCHEMAS_DIR = CONFIG_DIR / "schemas"`.
2. Change `_schema_cache: UnifiedSchema | None = None` → `_schema_cache: dict[str, UnifiedSchema] = {}`.
3. Rename `get_unified_schema()` → `get_domain_schema(domain: str = "nutrition") -> UnifiedSchema`:
   - Path: `SCHEMAS_DIR / f"{domain}_schema.json"`
   - Cache key: `domain`
   - FileNotFoundError message: `f"config/schemas/{domain}_schema.json not found. Create it or pass a valid domain."`
4. Update `_reset_schema_cache()` to accept optional `domain: str | None = None` param (clear one or all).
5. Update `save_unified_schema(schema)` → `save_domain_schema(schema: UnifiedSchema, domain: str)` — writes to `SCHEMAS_DIR / f"{domain}_schema.json"`.
6. Update `derive_unified_schema_from_source(...)` — the `domain` param was already present (line 355) but unused; now use it to write to the correct domain path via `save_domain_schema(schema, domain)`.
7. Update module `__all__` / exports if present.

**Invariants**:
- Global `_schema_cache` dict — no race condition risk (single-threaded pipeline).
- `get_domain_schema("nutrition")` called with default domain must still work when only one schema file exists (e.g., during incremental migration).

---

### Phase C — Call Site Updates

**4 call sites**, all in `src/agents/`:

| File | Line | Change |
|------|------|--------|
| `src/agents/orchestrator.py` | ~229 | `get_unified_schema()` → `get_domain_schema(domain)` |
| `src/agents/orchestrator.py` | ~673 | `get_unified_schema()` inside `_deterministic_corrections(...)` call → `get_domain_schema(domain)` |
| `src/agents/graph.py` | ~86 | `get_unified_schema()` in `plan_sequence_node` → `get_domain_schema(domain)` |
| `src/agents/graph.py` | ~206 | `get_unified_schema()` in `run_pipeline_node` → `get_domain_schema(domain)` |

**Import update** in each file: `from src.schema.analyzer import get_domain_schema` (remove `get_unified_schema`).

**Note on `plan_sequence_node` (graph.py ~86)**: domain is already extracted at line 66 (`domain = state.get("domain", "nutrition")`). Pass it directly: `get_domain_schema(domain)`.

**Note on `run_pipeline_node` (graph.py ~205)**: domain extracted at line 205 (`domain = state.get("domain", "nutrition")`). Pass it directly.

---

### Phase D — `_silver_normalize()` and Silver Write

**File**: `src/agents/graph.py`

**Step 1**: Add private function `_silver_normalize(df, domain_schema, dq_weights)` — see data-model.md for exact algorithm.

**Step 2**: In `run_pipeline_node`, after `result_df, audit_log = runner.run_chunked(...)`, add:

```python
# Silver normalization: enforce domain schema column set post-block-sequence
result_df = _silver_normalize(result_df, unified, config["dq_weights"])
```

(At this point, `unified` is already the loaded domain schema from `get_domain_schema(domain)` at line ~206.)

**Step 3**: `result_df` is stored in state as `working_df` and flows to `save_output_node` — no state shape change needed.

---

### Phase E — Silver Parquet Write + Gold Concatenation

**File**: `src/agents/graph.py`, inside `save_output_node`

Add a new branch (alongside `pipeline_mode == "silver"` and `pipeline_mode == "full"`):

```python
# Always write Silver local parquet + rebuild Gold (domain-local mode)
# This runs regardless of pipeline_mode for the local ETL pipeline.
domain = state.get("domain", "nutrition")
silver_local_dir = OUTPUT_DIR / "silver" / domain
silver_local_dir.mkdir(parents=True, exist_ok=True)
silver_local_path = silver_local_dir / f"{source_name}.parquet"
df.to_parquet(silver_local_path, index=False)
logger.info("Silver: %d rows → %s", len(df), silver_local_path)

# Gold concatenation: scan all Silver parquets for domain, rebuild Gold
silver_files = sorted(silver_local_dir.glob("*.parquet"))
if silver_files:
    gold_df = pd.concat([pd.read_parquet(p) for p in silver_files], ignore_index=True)
    gold_dir = OUTPUT_DIR / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_path = gold_dir / f"{domain}.parquet"
    gold_df.to_parquet(gold_path, index=False)
    logger.info("Gold: %d rows → %s", len(gold_df), gold_path)
else:
    logger.warning("No Silver parquet files for domain '%s' — Gold write skipped", domain)
```

**Placement**: Add this block inside the `try:` block, after the existing `if pipeline_mode == "silver": ... else: ...` block. This means both the GCS Silver write (silver mode) AND the local Silver/Gold write happen — they are not mutually exclusive. If GCS write is needed (silver mode), it still happens; local Silver/Gold is additive.

**Return dict update**: Add `"silver_local_path"` and `"gold_path"` to the returned state dict for run log capture.

---

### Phase F — Prompt Text Update

**File**: `src/agents/prompts.py`

Minimal changes — only human-readable text strings, not format placeholders:

1. `SCHEMA_ANALYSIS_PROMPT`: Change section header `## Unified Output Schema` → `## Domain Output Schema`.
2. Any inline description of "the unified schema" in prompt body text → "the domain schema".
3. `FIRST_RUN_SCHEMA_PROMPT`: "There is no unified schema yet" → "There is no domain schema yet for this source".

The Python variable `{unified_schema}` (the format placeholder injected at call time) is NOT renamed — it would require updating the `.format(unified_schema=...)` call site and is purely internal machinery.

---

### Phase G — Documentation

**`README.md`**:
- Section "Unified schema is auto-generated once, then reused" → update title and body to describe domain schemas.
- Replace `config/unified_schema.json` with `config/schemas/<domain>_schema.json` in architecture description.

**`CLAUDE.md`**:
- "Unified schema is auto-generated once, then reused" section → rework to describe per-domain schema files.
- Remove `config/unified_schema.json` as the canonical target schema reference.
- Update "Development Workflow quality gates" if `unified-schema alignment` appears.

---

## Complexity Tracking

No constitution violations. No complexity table required.

---

## Risk Notes

| Risk | Mitigation |
|------|-----------|
| Tests import `get_unified_schema` directly | Grep tests for `get_unified_schema`; update imports |
| Redis cache fingerprint includes schema version — domain schema change could cause stale cache hits | `_compute_schema_fingerprint` hashes source column names + domain + schema_version. Schema version string is unchanged by this feature. No cache invalidation needed — fingerprint already includes domain. |
| `save_output_node` writes Silver local parquet in ALL pipeline modes including "silver" (GCS) runs | Acceptable — the local Silver/Gold artifacts are additive. GCS write is unaffected. If local disk is not desired for GCS runs, gate the Silver/Gold block on `pipeline_mode == "full"`. Decision deferred to implementation. |
| `output/silver/<domain>/` accumulates parquets indefinitely — no TTL | Out of scope per spec. Cleanup task deferred. |
